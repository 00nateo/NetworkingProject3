#!/usr/bin/env python3
"""CS 4254 Project 3: Fakebook crawler. See instructionsp3.txt for requirements."""


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
    """Write to stderr so stdout stays available for flags."""
    kwargs["file"] = sys.stderr
    print(*args, **kwargs)


def log(msg):
    eprint(f"[crawler] {msg}")


def canonicalize(url, base=None):
    """Resolve relative URLs and drop fragments while keeping query strings."""
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
    """Keep the crawl on the target host under /fakebook/."""
    if not is_target_host(url):
        return False
    parts = urlsplit(url)
    return parts.path.startswith("/fakebook/")


def should_skip(url):
    parts = urlsplit(url)
    return any(fragment in parts.path for fragment in SKIP_PATH_FRAGMENTS)


class HTTPResponse:
    """Parsed HTTP response with Set-Cookie values kept in arrival order."""

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
        """Decode the body using the declared charset, with a UTF-8 fallback."""
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
        """Use Content-Type when present, otherwise look for HTML markup."""
        content_type = self.header("content-type")
        if content_type is not None:
            return "html" in content_type.lower()
        return self.body.lstrip().startswith(b"<")


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_http_date(value):
    """Parse an RFC 1123 date into epoch seconds."""
    try:
        fields = value.replace(",", " ").split()
        # ['Wed', '17', 'Nov', '2027', '20:17:15', 'GMT']
        day = int(fields[1])
        month = _MONTHS[fields[2][:3].lower()]
        year = int(fields[3])
        hour, minute, second = (int(x) for x in fields[4].split(":"))
    except (IndexError, KeyError, ValueError):
        return None
    return calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))


def domain_matches(host, domain):
    """RFC 6265 5.1.3: exact host, or a suffix of it on a '.' boundary."""
    host = (host or "").lower()
    domain = (domain or "").lower().lstrip(".")
    return host == domain or host.endswith("." + domain)


def path_matches(request_path, cookie_path):
    """RFC 6265 5.1.4: cookie path is a prefix, on a '/' boundary or exactly."""
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
    """Thread-safe cookie store keyed by name for the single-host crawl."""

    def __init__(self):
        self._cookies = {}
        self._lock = threading.Lock()

    def update_from_response(self, set_cookie_values, request_url):
        """Store or expire cookies from the response."""
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

                # RFC 6265 5.3: Max-Age wins over Expires when both are sent.
                if max_age is not None:
                    expires = None if max_age <= 0 else time.time() + max_age
                    delete = max_age <= 0
                elif expires_attr is not None:
                    expires = expires_attr
                    delete = expires_attr <= time.time()
                else:
                    expires = None      # session cookie: lives until we exit
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
        """Return matching, unexpired cookies in name order."""
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
    """Buffer bytes that a socket read returns past the current delimiter or length."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""      # bytes received but not yet consumed

    def read_until(self, delim):
        """Read through the delimiter and return the bytes before it."""
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
        """Read n bytes or raise if the connection closes early."""
        while len(self.buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed after %d of %d bytes" % (len(self.buf), n))
            self.buf += chunk
        result = self.buf[:n]
        self.buf = self.buf[n:]
        return result

    def read_all(self):
        """Read until EOF (used when neither chunked nor Content-Length is set)."""
        out = [self.buf]
        self.buf = b""
        while True:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            out.append(chunk)
        return b"".join(out)

    def close(self):
        """Close the socket; ignore errors."""
        try:
            self.sock.close()
        except OSError:
            pass


class HTTPParseError(Exception):
    """Raised when the peer sends something that is not valid HTTP."""
    pass


class HTTPClient:
    """Build HTTP requests and parse responses. The session manages cookies."""

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

        # "HTTP/1.1 200 OK" -> at most 3 fields; the reason may contain spaces.
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
        set_cookies = []            # list, so repeated Set-Cookie is not lost
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

        # Body: chunked beats Content-Length when both are present.
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
            body = b""              # these cannot carry a body
        else:
            body = reader.read_all()

        # Decode content after removing the transfer framing.
        encoding = headers.get("content-encoding", "").lower()
        if encoding:
            body = self.decompress(body, encoding)

        return HTTPResponse(status, reason, version, headers, set_cookies, body)

    def read_chunked_body(self, reader):
        """Reassemble chunks and consume the trailers (RFC 9112, section 7.1)."""
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

            if size == 0:           # last chunk; then trailers until blank line
                while reader.read_until(b"\r\n"):
                    pass
                break

            chunks.append(reader.read_exact(size))
            if reader.read_exact(2) != b"\r\n":
                raise HTTPParseError("missing CRLF after chunk data")

        return b"".join(chunks)

    def decompress(self, body, encoding):
        """Decode gzip and deflate bodies."""
        encoding = encoding.strip().lower()
        try:
            if encoding in ("gzip", "x-gzip"):
                return gzip.decompress(body)
            if encoding == "deflate":
                try:
                    return zlib.decompress(body)
                except zlib.error:
                    # Some servers send raw deflate with no zlib wrapper.
                    return zlib.decompress(body, -zlib.MAX_WBITS)
        except (OSError, zlib.error) as exc:
            raise HTTPParseError("cannot decode %s body: %s" % (encoding, exc))
        return body                 # unknown/identity encoding: leave as-is


class PageParser(HTMLParser):
    """Collect links and secret flags from one HTML page."""

    def __init__(self, base_url=None):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links = []       # absolute-or-raw hrefs, deduped later
        self.flags = []
        self._in_flag_tag = False
        self._flag_buffer = []

    def reset_state(self):
        """Clear collected data so the instance can be reused."""
        pass

    def handle_starttag(self, tag, attrs):
        """TODO: Collect links and form fields, and start buffering text in flag headings."""
        pass

    def handle_endtag(self, tag):
        """TODO: Match FLAG_RE against the buffered text when a flag heading closes."""
        pass

    def handle_data(self, data):
        """TODO: Buffer flag text across parser callbacks."""
        pass


def extract_form_fields(html):
    """TODO: Collect input names and values, including hidden login fields."""
    pass


class Session:
    """Manage cookies, redirects, and retries for HTTP requests."""

    def __init__(self):
        self.jar = CookieJar()
        self.client = HTTPClient(self.jar)
        self.requests_made = 0

    def fetch(self, url, method="GET", form=None, redirects=0):
        """TODO: Fetch a URL and return its 200 response or None.

        Update cookies after each response. Resolve redirects against the current
        URL, check the target host, and cap the redirect chain. A 302 or 303
        after a POST should continue as a GET without the form body.

        Skip 403 and 404 responses. Retry 500 responses, back off on 503,
        and retry transient connection and parse errors.
        """
        pass

    def post_form(self, url, fields):
        """TODO: URL-encode the fields and send a POST with Referer and Origin."""
        pass

    def backoff(self, attempt):
        """TODO: Sleep for a capped exponential delay and log the retry."""
        pass


def login(session, username, password):
    """TODO: Fetch the login form and submit the credentials with its hidden fields.

    Preserve the CSRF cookie and form token, then check for a sessionid
    cookie and verify that the Fakebook home page is accessible.
    """
    pass


def is_logged_in(session):
    """TODO: Fetch the home page and check whether it leads back to login."""
    pass


class Crawler:
    """Breadth-first crawl over Fakebook.

    Use the condition to guard the frontier, seen URLs, and flags. Workers
    should wait if the queue is empty while a fetch is still in progress.
    """

    def __init__(self, session, workers=NUM_WORKERS):
        self.session = session
        self.workers = workers
        self.frontier = deque()
        self.seen = set()
        self.flags = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

    def run(self, start_url=BASE_URL):
        """TODO: Seed the frontier, start and join the workers, and return the flags."""
        pass

    def _worker(self, worker_id):
        """TODO: Fetch pages outside the lock and record their links and flags under it."""
        pass

    def _enqueue(self, url):
        """TODO: Queue unseen, in-scope URLs. The caller holds the lock."""
        pass

    def _record_flags(self, flags):
        """TODO: Record unique flags and log progress."""
        pass

    @property
    def done(self):
        """True when enough flags have been found."""
        pass

    def stats(self):
        """Return (visited, queued, flags) for progress logging."""
        pass


def parse_args(argv):
    """Return the credentials or print usage and return None."""
    if len(argv) != 3:
        eprint(f"usage: {argv[0]} [username] [password]")
        return None
    return argv[1], argv[2]


def main(argv):
    """TODO: Log in, crawl, and print exactly five flags on success."""
    info = parse_args(argv)
    if info is None:
        return 2
    username, password = info
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
