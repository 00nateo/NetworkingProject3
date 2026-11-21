#!/usr/bin/env python3
# CS 4254 Project 3: Fakebook crawler.

import calendar
import gzip
import re
import socket
import sys
import threading
import time
import zlib
from collections import deque
from html.parser import HTMLParser
from urllib.parse import (
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)


TARGET_HOST = "sprinter2.cs.vt.edu"
TARGET_PORT = 80
BASE_URL = "http://sprinter2.cs.vt.edu/fakebook/"
LOGIN_URL = "http://sprinter2.cs.vt.edu/accounts/login/?next=/fakebook/"

USER_AGENT = "cs4254-fakebook-crawler/1.0"
SOCKET_TIMEOUT = 10.0
MAX_REDIRECTS = 5
MAX_ATTEMPTS = 5
BACKOFF_BASE = 1.0
NUM_WORKERS = 6
REQUEST_DELAY = 0.0
FLAGS_WANTED = 5

FLAG_RE = re.compile(r"FLAG:\s*([A-Za-z0-9]{64})")

# Skip account links during the crawl.
SKIP_PATH_FRAGMENTS = (
    "/accounts/logout",
    "/accounts/login",
)


def eprint(*args, **kwargs):
    # Write to stderr so stdout stays available for flags.
    kwargs["file"] = sys.stderr
    print(*args, **kwargs)


def log(msg):
    eprint(f"[crawler] {msg}")


def canonicalize(url, base=None):
    # Resolve relative URLs and drop fragments while keeping query strings.
    if base is not None:
        url = urljoin(base, url)

    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        return None

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    query = parts.query
    return urlunsplit((scheme, netloc, path, query, ""))


def is_target_host(url):
    parts = urlsplit(url)
    host = parts.hostname
    return host == TARGET_HOST


def in_crawl_scope(url):
    # Keep the crawl on the target host under /fakebook/.
    if not is_target_host(url):
        return False
    parts = urlsplit(url)
    return parts.path.startswith("/fakebook/")


def should_skip(url):
    parts = urlsplit(url)
    return any(fragment in parts.path for fragment in SKIP_PATH_FRAGMENTS)


class HTTPResponse:
    # Parsed HTTP response with Set-Cookie values kept in arrival order.

    def __init__(self, status=None, reason="", version="HTTP/1.1",
                 headers=None, set_cookies=None, body=b""):
        self.status = status
        self.reason = reason
        self.version = version
        self.headers = headers if headers is not None else {}
        self.set_cookies = set_cookies if set_cookies is not None else []
        self.body = body

    def header(self, name, default=None):
        return self.headers.get(name.lower(), default)

    @property
    def text(self):
        # Decode the body using the declared charset, with a UTF-8 fallback.
        content_type = self.header("content-type", "")
        charset = "utf-8"
        for param in content_type.split(";")[1:]:
            key, _, value = param.partition("=")
            if key.strip().lower() == "charset":
                charset = value.strip().strip("\"'")
                break
        try:
            return self.body.decode(charset, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    @property
    def is_html(self):
        # Use Content-Type when present, otherwise look for HTML markup.
        content_type = self.header("content-type")
        if content_type is not None:
            return "html" in content_type.lower()
        return self.body.lstrip().startswith(b"<")


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_http_date(value):
    # Parse an RFC 1123 date into epoch seconds
    try:
        fields = value.replace(",", " ").split()
        day = int(fields[1])
        month = _MONTHS[fields[2][:3].lower()]
        year = int(fields[3])
        hour, minute, second = (int(x) for x in fields[4].split(":"))
    except (IndexError, KeyError, ValueError):
        return None
    return calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))


def domain_matches(host, domain):
    # exact host, or a suffix of it on a '.' boundary.
    host = (host or "").lower()
    domain = (domain or "").lower().lstrip(".")
    return host == domain or host.endswith("." + domain)


def path_matches(request_path, cookie_path):
    # cookie path is a prefix, on a '/' boundary or exactly
    if not request_path:
        request_path = "/"
    if not cookie_path:
        cookie_path = "/"
    if request_path == cookie_path:
        return True
    if not request_path.startswith(cookie_path):
        return False
    if cookie_path.endswith("/"):
        return True
    return request_path[len(cookie_path)] == "/"


class CookieJar:
    # Thread-safe cookie store keyed by name for the single-host crawl

    def __init__(self):
        self._cookies = {}
        self._lock = threading.Lock()

    def update_from_response(self, set_cookie_values, request_url):
        # Store or expire cookies from the response
        host = urlsplit(request_url).hostname
        with self._lock:
            for raw in set_cookie_values:
                parts = raw.split(";")
                name, _, value = parts[0].strip().partition("=")
                name = name.strip()
                value = value.strip()
                if not name:
                    continue
                domain = host
                path = "/"
                expires_attr = None
                max_age = None

                for attr in parts[1:]:
                    attr = attr.strip()
                    if not attr:
                        continue
                    attr_name, _, attr_value = attr.partition("=")
                    attr_name = attr_name.strip().lower()
                    attr_value = attr_value.strip()
                    if attr_name == "domain" and attr_value:
                        domain = attr_value.lstrip(".").lower()
                    elif attr_name == "path" and attr_value:
                        path = attr_value
                    elif attr_name == "max-age":
                        try:
                            max_age = int(attr_value)
                        except ValueError:
                            pass
                    elif attr_name == "expires":
                        expires_attr = parse_http_date(attr_value)

                #  Max-Age wins over Expires when both are sent
                if max_age is not None:
                    expires = None if max_age <= 0 else time.time() + max_age
                    delete = max_age <= 0
                elif expires_attr is not None:
                    expires = expires_attr
                    delete = expires_attr <= time.time()
                else:
                    expires = None      # session cookie lives until we exit
                    delete = False

                if delete:
                    self._cookies.pop(name, None)
                else:
                    self._cookies[name] = {
                        "value": value,
                        "domain": domain,
                        "path": path,
                        "expires": expires,
                    }

    def header_for(self, url):
        # Return matching, unexpired cookies in name order
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        path = parts.path or "/"
        now = time.time()

        pairs = []
        with self._lock:
            for name in sorted(self._cookies):
                cookie = self._cookies[name]
                if cookie["expires"] is not None and cookie["expires"] <= now:
                    continue
                if not domain_matches(host, cookie["domain"]):
                    continue
                if not path_matches(path, cookie["path"]):
                    continue
                pairs.append("%s=%s" % (name, cookie["value"]))

        return "; ".join(pairs) if pairs else None

    def clear(self):
        with self._lock:
            self._cookies.clear()

    def names(self):
        with self._lock:
            return sorted(self._cookies)


class SocketReader:
    # buffer bytes that a socket read returns past the current delimiter or length

    def __init__(self, sock):
        self.sock = sock
        # bytes received but not yet consumed
        self.buf = b""      

    def read_until(self, delim):
        # Read through the delimiter and return the bytes before it
        while True:
            i = self.buf.find(delim)
            if i != -1:
                result = self.buf[:i]
                self.buf = self.buf[i + len(delim):]
                return result
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed while waiting for %r" % (delim,))
            self.buf += chunk

    def read_exact(self, n):
        #Read n bytes or raise if the connection closes early
        while len(self.buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed after %d of %d bytes" % (len(self.buf), n))
            self.buf += chunk
        result = self.buf[:n]
        self.buf = self.buf[n:]
        return result

    def read_all(self):
        #Read until EOF 
        #used when neither chunked nor Content-Length is set
        out = [self.buf]
        self.buf = b""
        while True:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            out.append(chunk)
        return b"".join(out)

    def close(self):
        #close the socket
        try:
            self.sock.close()
        except OSError:
            pass


class HTTPParseError(Exception):
    pass


class HTTPClient:
    #build HTTP requests and parse response

    def __init__(self, cookie_jar=None):
        self.cookie_jar = cookie_jar

    def build_request(self, method, url, extra_headers=None, body=None):
        parts = urlsplit(url)

        target = parts.path or "/"
        if parts.query:
            target += "?" + parts.query

        lines = [
            "%s %s HTTP/1.1" % (method, target),
            "Host: %s" % parts.netloc,
            "User-Agent: %s" % USER_AGENT,
            "Accept: text/html,application/xhtml+xml,*/*;q=0.8",
            "Connection: close",
        ]

        if self.cookie_jar is not None:
            cookie = self.cookie_jar.header_for(url)
            if cookie:
                lines.append("Cookie: %s" % cookie)

        if body is not None:
            lines.append("Content-Length: %d" % len(body))
            lines.append("Content-Type: application/x-www-form-urlencoded")

        for name, value in (extra_headers or {}).items():
            lines.append("%s: %s" % (name, value))

        head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
        return head + (body or b"")

    def open_connection(self, host, port=TARGET_PORT):
        sock = socket.create_connection((host, port), timeout=SOCKET_TIMEOUT)
        sock.settimeout(SOCKET_TIMEOUT)
        return sock

    def read_response(self, reader):
        status_line = reader.read_until(b"\r\n")
        if not status_line:
            raise HTTPParseError("empty response (connection closed)")

        # "HTTP/1.1 200 OK" -> at most 3 fields
        fields = status_line.decode("latin-1").split(" ", 2)
        if len(fields) < 2 or not fields[0].startswith("HTTP/"):
            raise HTTPParseError("bad status line: %r" % (status_line,))
        version = fields[0]
        try:
            status = int(fields[1])
        except ValueError:
            raise HTTPParseError("bad status code: %r" % (status_line,))
        reason = fields[2] if len(fields) > 2 else ""

        headers = {}
        set_cookies = []            # list so repeated Set-Cookie is not lost
        while True:
            line = reader.read_until(b"\r\n")
            if not line:
                break
            name, _, value = line.decode("latin-1").partition(":")
            name = name.strip().lower()
            value = value.strip()
            if not name:
                continue
            if name == "set-cookie":
                set_cookies.append(value)
                continue
            if name in headers:
                headers[name] += ", " + value
            else:
                headers[name] = value

        # Body
        #chunked beats Content-Length when both are present
        transfer = headers.get("transfer-encoding", "").lower()
        if "chunked" in transfer:
            body = self.read_chunked_body(reader)
        elif "content-length" in headers:
            try:
                length = int(headers["content-length"])
            except ValueError:
                raise HTTPParseError("bad Content-Length: %r"
                                     % (headers["content-length"],))
            body = reader.read_exact(length)
        elif status in (204, 304) or 100 <= status < 200:
            body = b""              
            #these cannot carry a body
        else:
            body = reader.read_all()

        #Decode content after removing the transfer framing
        encoding = headers.get("content-encoding", "").lower()
        if encoding:
            body = self.decompress(body, encoding)

        return HTTPResponse(status, reason, version, headers, set_cookies, body)

    def read_chunked_body(self, reader):
        # Reassemble chunks and consume the trailers 
        chunks = []
        while True:
            size_line = reader.read_until(b"\r\n")
            if not size_line:
                raise HTTPParseError("truncated chunked body (no size line)")

            # "1a3;ext=value" -> 0x1a3
            size_field = size_line.split(b";", 1)[0].strip()
            try:
                size = int(size_field, 16)
            except ValueError:
                raise HTTPParseError("bad chunk size: %r" % (size_line,))

            if size == 0:           # last chunk then trailers until blank line
                while reader.read_until(b"\r\n"):
                    pass
                break

            chunks.append(reader.read_exact(size))
            if reader.read_exact(2) != b"\r\n":
                raise HTTPParseError("missing CRLF after chunk data")

        return b"".join(chunks)

    def decompress(self, body, encoding):
        #decode gzip and deflate bodies
        encoding = encoding.strip().lower()
        try:
            if encoding in ("gzip", "x-gzip"):
                return gzip.decompress(body)
            if encoding == "deflate":
                try:
                    return zlib.decompress(body)
                except zlib.error:
                    # Some servers send raw deflate with no zlib wrapper
                    return zlib.decompress(body, -zlib.MAX_WBITS)
        except (OSError, zlib.error) as exc:
            raise HTTPParseError("cannot decode %s body: %s" % (encoding, exc))
        return body                 


class PageParser(HTMLParser):
    #collect links and secret flags from one HTML page

    def __init__(self, base_url=None):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links = []       
        self.flags = []
        self._in_flag_tag = False
        self._flag_buffer = []
        self.inputs = {}

    def reset_state(self):
        #clear collected data so the instance can be reused
        self.links = []
        self.flags = []
        self.inputs = {}
        self._in_flag_tag = False
        self._flag_buffer = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)

        if tag == "a":
            href = attrs.get("href")
            if href:
                self.links.append(urljoin(self.base_url, href) if self.base_url else href)
        elif tag == "h2":
            classes = attrs.get("class", "").split()
            if "secret_flag" in classes:
                self._in_flag_tag = True
                self._flag_buffer = []
        elif tag == "input":
            name = attrs.get("name")
            if name:
                self.inputs[name] = attrs.get("value", "")

    def handle_endtag(self, tag):
        if tag == "h2" and self._in_flag_tag:
            text = "".join(self._flag_buffer)
            match = FLAG_RE.search(text)
            if match:
                self.flags.append(match.group(1))
            self._in_flag_tag = False
            self._flag_buffer = []

    def handle_data(self, data):
        if self._in_flag_tag:
            self._flag_buffer.append(data)


def extract_form_fields(html):
    parser = PageParser()
    parser.feed(html)
    parser.close()
    return parser.inputs


class Session:
    # Manage cookies, redirects, and retries for HTTP requests

    def __init__(self):
        self.jar = CookieJar()
        self.client = HTTPClient(self.jar)
        self.requests_made = 0

    def fetch(self, url, method="GET", form=None, redirects=0, extra_headers=None, should_stop=None):
        #Fetch a URL and return its 200 response or None.
        #
        #Update cookies after each response Resolve redirects against the current
        #URL then check the target host, and cap the redirect chain
        #302 or 303 after a POST should continue as a GET without the form body.
        #
        #Skip 403 and 404 responses
        #Retry 500 responses, back off on 503,
        #and retry transient connection and parse errors.
        #
        if not is_target_host(url):
            log(f"skipping URL outside Fakebook: {url}")
            return None

        attempt = 0
        server_errors = 0
        while attempt < MAX_ATTEMPTS:
            if should_stop is not None and should_stop():
                log(f"stopping retry of {url}, crawl already has enough flags")
                return None
            try:
                parts = urlsplit(url)
                request_bytes = self.client.build_request(
                    method, url, extra_headers=extra_headers, body=form
                )
                sock = self.client.open_connection(parts.hostname, parts.port or TARGET_PORT)
                reader = SocketReader(sock)
                try:
                    sock.sendall(request_bytes)
                    resp = self.client.read_response(reader)
                finally:
                    reader.close()
            except (OSError, HTTPParseError) as exc:
                log(f"transport error on {url}: {exc}")
                attempt += 1
                self.backoff(attempt)
                continue

            self.requests_made += 1
            self.jar.update_from_response(resp.set_cookies, url)

            if resp.status in (301, 302, 303, 307):
                if redirects >= MAX_REDIRECTS:
                    log(f"too many redirects for {url}")
                    return None
                location = resp.header("location")
                if not location:
                    log(f"redirect with no Location header: {url}")
                    return None
                target = canonicalize(location, base=url)
                if target is None:
                    return None
                if method == "POST":
                    #should_stop
                    return self.fetch(target, "GET", None, redirects + 1, should_stop=should_stop)
                return self.fetch(target, method, form, redirects + 1, should_stop=should_stop)

            if resp.status == 200:
                return resp

            if resp.status in (403, 404):
                log(f"{resp.status} on {url}, abandoning")
                return None

            if resp.status == 500:
                log(f"500 on {url}, retrying")
                server_errors = min(server_errors + 1, 5)
                self.backoff(server_errors)
                continue

            if resp.status == 503:
                log(f"503 on {url}, backing off harder")
                attempt += 1
                self.backoff(attempt + 2)
                continue

            log(f"unhandled status {resp.status} on {url}, abandoning")
            return None

        log(f"giving up on {url} after {MAX_ATTEMPTS} attempts")
        return None


    def post_form(self, url, fields):
        body = urlencode(fields).encode("ascii")
        headers = {
            "Referer": url,
            "Origin": "http://%s" % TARGET_HOST,
        }
        return self.fetch(url, method="POST", form=body, extra_headers=headers)

    def backoff(self, attempt):
        delay = min(BACKOFF_BASE * (2 ** attempt), 30.0)
        log(f"backing off {delay:.1f}s (attempt {attempt})")
        time.sleep(delay)


def login(session, username, password):
    login_page = session.fetch(LOGIN_URL)
    if login_page is None:
        eprint("could not load login page")
        return False

    fields = extract_form_fields(login_page.text)
    fields["username"] = username
    fields["password"] = password

    resp = session.post_form(LOGIN_URL, fields)
    if resp is None:
        eprint("login POST failed")
        return False

    if "sessionid" not in session.jar.names():
        eprint("login failed: no sessionid cookie, check username/password")
        return False

    if not is_logged_in(session):
        eprint("login failed: still on login page after posting credentials")
        return False

    return True


def is_logged_in(session):
    resp = session.fetch(BASE_URL)
    if resp is None:
        return False
    fields = extract_form_fields(resp.text)
    return "password" not in fields


class Crawler:
    # Use the condition to guard the frontier, seen URLs, and flags. 
    #Workers should wait if the queue is empty while a fetch is still in progress
    

    def __init__(self, session, workers=NUM_WORKERS):
        self.session = session
        self.workers = workers
        self.frontier = deque()
        self.seen = set()
        self.flags = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.active = 0

    def run(self, start_url=BASE_URL):
        with self.lock:
            self._enqueue(start_url)

        threads = []
        for worker_id in range(self.workers):
            t = threading.Thread(target=self._worker, args=(worker_id,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return self.flags

    def _worker(self, worker_id):
        while True:
            with self.lock:
                while not self.frontier and self.active > 0 and not self.done:
                    self.cond.wait()

                if self.done or not self.frontier:
                    self.cond.notify_all()
                    return

                url = self.frontier.popleft()
                self.active += 1

            resp = self.session.fetch(url, should_stop=lambda: self.done)

            links = []
            flags = []
            if resp is not None and resp.is_html:
                parser = PageParser(base_url=url)
                parser.feed(resp.text)
                parser.close()
                links = parser.links
                flags = parser.flags

            with self.lock:
                self._record_flags(flags)
                for link in links:
                    self._enqueue(link)
                self.active -= 1
                self.cond.notify_all()

    def _enqueue(self, url):
        target = canonicalize(url, base=BASE_URL)
        if target is None:
            return
        if not in_crawl_scope(target):
            return
        if should_skip(target):
            return
        if target in self.seen:
            return
        self.seen.add(target)
        self.frontier.append(target)

    def _record_flags(self, flags):
        for flag in flags:
            if flag not in self.flags:
                self.flags.append(flag)
                log(f"found {len(self.flags)}/{FLAGS_WANTED} flags")

    @property
    def done(self):
        return len(self.flags) >= FLAGS_WANTED

    def stats(self):
        return len(self.seen), len(self.frontier), len(self.flags)


def parse_args(argv):
    # Return the credentials or print usage and return None
    if len(argv) != 3:
        eprint(f"usage: {argv[0]} [username] [password]")
        return None
    return argv[1], argv[2]


def main(argv):
    info = parse_args(argv)
    if info is None:
        return 2
    username, password = info

    session = Session()
    if not login(session, username, password):
        eprint("login failed")
        return 1

    crawler = Crawler(session)
    flags = crawler.run()

    if len(flags) < FLAGS_WANTED:
        eprint(f"only found {len(flags)}/{FLAGS_WANTED} flags")
        return 1

    for flag in flags[:FLAGS_WANTED]:
        print(flag)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
