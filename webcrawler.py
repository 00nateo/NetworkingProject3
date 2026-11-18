#!/usr/bin/env python3
"""
CS 4254 Fall 2026 -- Project 3: Web Crawler (Fakebook)
USAGE (must be exactly this):
    ./webcrawler [username] [password]

OUTPUT CONTRACT (graded literally):
    - stdout: exactly five lines, the five secret flags, one per line, nothing else.
    - stderr: all logging, progress, errors, debugging.
    - exit 0 on success; non-zero on unrecoverable error (bad credentials, no flags).

LEGALITY (read instructionsp3.txt section 6 -- this is graded, zero credit if violated):
    ALLOWED: socket, ssl, urllib.parse, html.parser, xml, gzip, zlib, re, threading
    BANNED:  http.client, urllib.request, requests, httpx, aiohttp, pycurl, scrapy,
             http.cookiejar, http.cookies, urllib2/urllib3, shutil-out to curl/wget

    => Every byte on the wire is built by hand in build_request().
    => Every cookie is parsed by hand in CookieJar.
    => urljoin() from urllib.parse is fine and should be used for relative links.

FILE MAP:
    S1  imports + constants
    S2  logging helpers
    S3  URL utilities
    S4  Response container
    S5  CookieJar
    S6  SocketReader          <- buffered socket I/O (write first, test first)
    S7  HTTPClient            <- request bytes out, response parsed in
    S8  PageParser            <- links + flags out of HTML
    S9  Session               <- fetch() with retry/redirect policy  <-- ALL error handling lives here
    S10 login()               <- CSRF dance
    S11 Crawler               <- frontier, seen-set, workers
    S12 main()                <- argv, orchestration, exact stdout

BUILD ORDER :
    1. S1/S2/S12  -> `./webcrawler` prints usage to stderr, exits 2.
    2. S3         -> tiny asserts on canonicalize() / in_crawl_scope().
    3. S6         -> smoke-test read_until() against a raw socket.
    4. S7         -> fetch a page from a LOCAL `python3 -m http.server`; print status+len(body).
    5. S5+S7      -> two sequential requests, prove Set-Cookie is echoed back.
    6. S8         -> dump all <a href> and any flags from one saved page.
    7. S9         -> redirects, 403/404, 500/503, timeouts (see the matrix below).
    8. S10        -> log in against Fakebook; verify you land on /fakebook/.
    9. S11        -> single-threaded BFS, find 5 flags.
   10. S11        -> add workers (<= 8 total threads), then gzip, then keep-alive.

ERROR  (implement in Session.fetch, document in README.txt):
    200              parse body, extract links + flags
    301/302/303/307  follow Location (absolute OR path-only -> urljoin); POST becomes GET;
                     cap at MAX_REDIRECTS to avoid a redirect loop
    403 / 404        abandon this URL and continue crawling (do NOT abort the crawl)
    500              retry the SAME url with backoff (server returns it at random)
    503              sleep, back off harder, retry (means "you are too aggressive")
    timeout / reset  retry once, then abandon the URL
    malformed bytes  parse error -> retry once -> abandon
    bad credentials  message to stderr, exit non-zero
    non-text/html    skip parsing, do not enqueue links
"""

# =============================================================================
# S1. IMPORTS + CONSTANTS
# =============================================================================
# TODO: uncomment the imports you actually need as you implement each section.
# from __future__ import annotations

import gzip          # S7: decompress Content-Encoding: gzip
import re            # S8: flag pattern; S10: hidden input scraping
import socket        # S6: the only way bytes reach the network
import sys           # S2/S12: stderr, argv, exit codes
import threading     # S11: worker threads + locks
import time          # S9: backoff sleeps
import zlib          # S7: gzip via zlib if you prefer streaming decompression
from collections import deque                     # S11: frontier
from html.parser import HTMLParser                # S8: link + flag extraction
from urllib.parse import (                        # S3: URL handling ONLY (allowed)
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)

# NOTE: no `import http.*`, no `import urllib.request`. See the header block.

TARGET_HOST = "sprinter2.cs.vt.edu"   # ONLY this host may be crawled (section 7)
TARGET_PORT = 80
BASE_URL = "http://sprinter2.cs.vt.edu/fakebook/"          # crawl start point
LOGIN_URL = "http://sprinter2.cs.vt.edu/accounts/login/?next=/fakebook/"

USER_AGENT = "cs4254-fakebook-crawler/1.0"   # be honest/polite; do not impersonate a browser
SOCKET_TIMEOUT = 10.0        # seconds per socket op
MAX_REDIRECTS = 5            # per fetch chain
MAX_ATTEMPTS = 5             # per URL, for 500 / 503 / transient errors
BACKOFF_BASE = 1.0           # seconds; sleep = BACKOFF_BASE * 2**attempt (cap it)
NUM_WORKERS = 6              # keep TOTAL connections <= 8 (section 7)
REQUEST_DELAY = 0.0          # optional politeness sleep per request
FLAGS_WANTED = 5

# TODO: confirm this regex against the real markup in instructionsp3.txt section 5:
#   <h2 class="secret_flag" style="color:red">FLAG: <64 alphanumerics></h2>
# Prefer scraping the h2 text in PageParser over a raw regex; keep this as a fallback.
FLAG_RE = re.compile(r"FLAG:\s*([A-Za-z0-9]{64})")

# Paths that must never be crawled:
#   /accounts/logout/  -> destroys your session (and Django expires the cookie)
#   /accounts/login/   -> pointless after login, and can bounce you out
#   anything implying a mutation on a GET: del=, remove, unfriend, delete
# TODO: fill in once you have seen the real link shapes in DevTools.
SKIP_PATH_FRAGMENTS = (
    "/accounts/logout",
    "/accounts/login",
)


# =============================================================================
# S2. LOGGING HELPERS
# =============================================================================
def eprint(*args, **kwargs):
    """print() to stderr. ALL debug output goes through here.

    stdout is reserved for exactly five flag lines (see OUTPUT CONTRACT).
    GET THIS RIGHT EARLY: a stray print() in the crawl loop costs credit.
    """
    kwargs["file"] = sys.stderr
    print(*args, **kwargs)


def log(msg):
    """Timestamped progress line to stderr, e.g. '[crawl] visited=123 queue=456'."""
    eprint(f"[crawler] {msg}")


# =============================================================================
# S3. URL UTILITIES
# =============================================================================
def canonicalize(url, base=None):
    """Return a normalized absolute URL, or None if it is not usable.

    Rules (each one prevents a specific bug):
      - urljoin(base, url) resolves relative hrefs and path-only Locations.
      - lowercase scheme + host (http://Sprinter2... == http://sprinter2...)
      - default empty path to "/"
      - DROP the fragment ('#top') -> fragments are not sent to the server
      - KEEP the query string -> friend lists are paginated with ?page=2
      - reject non-http(s) schemes: mailto:, javascript:, tel:, data:
      - strip a trailing "#" / whitespace

    This is also the dedupe key for `seen`, so it MUST be deterministic.
    """
    if base is not None:
        url = urljoin(base, url)

    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        return None

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path if parts.path else "/"
    query = parts.query
    fragment = "" #never sent to server

    return urlunsplit((scheme, netloc, path, query, fragment))


def is_target_host(url):
    """True iff the URL's host is TARGET_HOST (section 7: domain check).

    TODO: decide how strict to be about ports and 'www.' prefixes.
    """
    parts = urlsplit(url)
    host = parts.hostname
    return host is not None and host == TARGET_HOST


def in_crawl_scope(url):
    """True iff this is a URL we are willing to enqueue.

    This is `is_target_host` AND a path allow-list. The host check satisfies the
    assignment; the path check protects your session and your 15-minute budget.
    """
    if not is_target_host(url):
        return False
    parts = urlsplit(url)
    return parts.path.startswith("/fakebook/")


def should_skip(url):
    """True for logout/login/mutation links that would break the crawl.

    TODO: extend SKIP_PATH_FRAGMENTS after inspecting real Fakebook links.
    """
    parts = urlsplit(url)
    return any(fragment in parts.path for fragment in SKIP_PATH_FRAGMENTS)
    


# =============================================================================
# S4. RESPONSE CONTAINER
# =============================================================================
class HTTPResponse:
    """One parsed HTTP response. Pure data -- no I/O happens here.

    Attributes to set in __init__:
        status      int      e.g. 200
        reason      str      e.g. "OK"
        version     str      e.g. "HTTP/1.1"
        headers     dict     lower-cased key -> value  (last one wins; fine)
        set_cookies list     raw Set-Cookie values, ORDER PRESERVED
        body        bytes    already de-chunked and decompressed
    """

    def __init__(self, status=None, reason="", version="HTTP/1.1",
                 headers=None, set_cookies=None, body=b""):
        self.status = status
        self.reason = reason
        self.version = version
        self.headers = headers if headers is not None else {}
        self.set_cookies = set_cookies if set_cookies is not None else []
        self.body = body


    def header(self, name, default=None):
        """Case-insensitive header lookup (headers dict is stored lower-cased)."""
        return self.headers.get(name.lower(), default)

    @property
    def text(self):
        """body decoded for HTML parsing.

        TODO: decode using the charset from Content-Type if present, else utf-8
              with errors="replace". Never crash on weird bytes.
        """
        content_type = self.header("content-type", "")
        charset = "utf-8"
        for param in content_type.split(";")[1:]:
            key, _,value = param.partition("=")
            if key.strip().lower() == "charset":
                charset = value.strip().strip("\"'")
                break
        try:
            return self.body.decode(charset, errors = "replace")
        except LookupError:
            return self.body.decode("utf-8", errors = "replace")


    @property
    def is_html(self):
        """True if Content-Type indicates HTML (or is absent but body looks like HTML).

        Used to skip images/PDFs/etc. before spending time on parsing.
        """
        content_type = self.header("content-type")
        if content_type is not None:
            return "html" in content_type.lower()
        return self.body.lstrip().startswith(b"<")


# =============================================================================
# S5. COOKIE JAR  (hand-written: http.cookiejar is BANNED)
# =============================================================================
class CookieJar:
    """Minimal RFC 6265-ish store.

    Fakebook/Django needs, in practice:
        - sessionid   (login session)      -> must survive the whole crawl
        - csrftoken   (needed by the login POST)
        - an expired cookie must be DELETED (that is how logout works)

    Suggested internal shape:
        self._cookies: dict[name] = {"value": str, "domain": str,
                                     "path": str, "expires": float|None}
        self._lock: threading.Lock (S11 reads this from many threads)
    """

    def __init__(self):
        self._cookies = {}
        self._lock = threading.Lock()

    def update_from_response(self, set_cookie_values, request_url):
        """Ingest every Set-Cookie header from one response.

        TODO: for each raw value:
            - split on ';' -> first part is name=value, rest are attributes
            - parse attributes case-insensitively: Domain, Path, Expires,
              Max-Age, Secure, HttpOnly, SameSite
            - default Domain = request host, default Path = "/" (or the
              directory of the request path -- pick one and be consistent)
            - Max-Age <= 0 or Expires in the past -> DELETE the stored cookie
            - otherwise store/replace
        Must be thread-safe if S11 is threaded.
        """
        host = urlsplit(request_url).hostname
        with self._lock:
            for raw in set_cookie_values:
                parts = raw.split(";")
                name, _, value = parts[0].strip().partition("=")
                name = name.strip()
                value = value.strip()
                domain = host
                path = "/"
                expires = None
                delete = False

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
                            if int(attr_value) <= 0:
                                delete = True
                        except ValueError:
                            pass
                    elif attr_name == "expires":
                        pass
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
        """Build the value for the request's `Cookie:` header, or None.

        TODO: join "name=value" for cookies whose domain/path match `url`
              and which are not expired, separated by "; ".
              Ordering does not matter to Django, but be deterministic.
        """
        pass

    def clear(self):
        """Forget everything (useful between tests)."""
        pass


# =============================================================================
# S6. SOCKET READER  -- buffered byte-level I/O
# =============================================================================
class SocketReader:
    """Wraps a connected socket and hands out bytes in HTTP-friendly pieces.

    THIS CLASS IS THE HARDEST 45 LINES IN THE PROJECT. Implement and smoke-test
    it before anything else, because a bug here looks like "server is broken".

    Why buffered: recv() returns arbitrary chunk boundaries. read_until(b"\\r\\n")
    may already have buffered bytes belonging to the body, so you cannot just
    recv() again -- you must keep leftover bytes in self.buf.
    """

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""      # bytes received but not yet consumed

    def read_until(self, delim):
        """Return bytes up to (and excluding) `delim`, consuming it too.

        TODO: loop recv(); search self.buf for delim; keep the remainder.
              Raise HTTPParseError/ConnectionError at EOF so S9 can retry.
        """
        while True: 
            i = self.buf.find(delim)
            if i != -1:
                result = self.buf[0:i]
                self.buf = self.buf[i + len(delim):]
                return result
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed while waiting for %r" % (delim,))
            self.buf += chunk

    def read_exact(self, n):
        """Return exactly n bytes (for Content-Length bodies).

        TODO: serve from self.buf first, then recv() until satisfied.
              EOF before n bytes -> raise (truncated response).
        """
        while len(self.buf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed after %d of %d bytes" % (len(self.buf),n))
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


# =============================================================================
# S7. HTTP CLIENT  -- build request bytes, parse response bytes
# =============================================================================
class HTTPParseError(Exception):
    """Raised when the peer sends something that is not valid HTTP."""
    pass


class HTTPClient:
    """Stateless-ish transport. Owns no cookies -- Session passes them in.

    One instance is fine; all state per request is local. If you add
    keep-alive later, this class becomes the place that owns open sockets.
    """

    def __init__(self, cookie_jar=None):
        self.cookie_jar = cookie_jar    # used only to build the Cookie header

    # ---- request side -------------------------------------------------------
    def build_request(self, method, url, extra_headers=None, body=None):
        """Return the complete request as bytes, ready for sendall().

        Required HTTP/1.1 shape:
            {METHOD} {path?query} HTTP/1.1\\r\\n
            Host: sprinter2.cs.vt.edu\\r\\n
            User-Agent: ...\\r\\n
            Accept: text/html,application/xhtml+xml,*/*;q=0.8\\r\\n
            Accept-Encoding: gzip        (only after gzip decoding works!)
            Connection: close            (start here; keep-alive is optional)
            Cookie: ...                  (if the jar has any for this URL)
            Content-Length: N            (POST only)
            Content-Type: application/x-www-form-urlencoded   (POST only)
            \\r\\n
            {body}

        TODO: the request-target is p.path + "?" + p.query -- NOT the full URL.
              Getting this wrong yields a 400 or an empty reply.
        TODO: Content-Length must be the BYTE length of the encoded body.
        """
        parts = urlsplit(url)

        # The request-target is the *path*, never the full URL.
        target = parts.path or "/"
        if parts.query:
            target += "?" + parts.query

        lines = [
            "%s %s HTTP/1.1" % (method, target),
            "Host: %s" % parts.netloc,
            "User-Agent: %s" % USER_AGENT,
            "Accept: text/html,application/xhtml+xml,*/*;q=0.8",
            # Accept-Encoding is deliberately off until decompress() is proven;
            # re-enable it only after a gzip response round-trips correctly.
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

        # Headers are latin-1 per the RFC; the body may be arbitrary bytes.
        head = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")
        return head + (body or b"")

    def open_connection(self, host, port=TARGET_PORT):
        """socket.create_connection + settimeout. Returns the raw socket.

        TODO: no TLS needed (http://), so skip ssl entirely. If you ever need
              https, wrap with ssl.create_default_context().wrap_socket().
        """
        sock = socket.create_connection((host, port), timeout=SOCKET_TIMEOUT)
        sock.settimeout(SOCKET_TIMEOUT)
        return sock

    # ---- response side ------------------------------------------------------
    def read_response(self, reader):
        """Parse one complete response from a SocketReader -> HTTPResponse.

        Steps, in order:
            1. status line:  "HTTP/1.1 200 OK"  -> version, status, reason
            2. headers until a blank line; collect Set-Cookie into a LIST
               (a dict would silently drop all but the last cookie)
            3. body:
                 - Transfer-Encoding: chunked  -> read_chunked_body()
                 - else Content-Length: N      -> read_exact(N)
                 - else                        -> read_all()
            4. Content-Encoding: gzip  -> decompress AFTER de-chunking
            5. return HTTPResponse(...)

        TODO: tolerate "HTTP/1.0" servers, missing reason phrases, and folded
              headers; raise HTTPParseError on genuine garbage.
        """
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
            if not line:            # blank line = end of headers
                break
            name, _, value = line.decode("latin-1").partition(":")
            name = name.strip().lower()
            value = value.strip()
            if not name:            # malformed line: skip rather than die
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

        # Decompress AFTER de-chunking: chunking is a transfer encoding,
        # gzip is a content encoding, and they stack in that order.
        encoding = headers.get("content-encoding", "").lower()
        if encoding:
            body = self.decompress(body, encoding)

        return HTTPResponse(status, reason, version, headers, set_cookies, body)

    def read_chunked_body(self, reader):
        """Reassemble a chunked body (RFC 9112 section 7.1).

        Format:
            <hex-size>\\r\\n
            <that many bytes>\\r\\n
            ... repeated ...
            0\\r\\n
            \\r\\n              (trailer section; may contain headers -- discard)

        TODO: hex size may have extensions after ';' -- ignore them.
              Do not forget to consume the CRLF after each chunk's data.
        """
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
        """Undo Content-Encoding. Handle 'gzip' (and 'deflate' if seen).

        TODO: gzip.decompress() is fine for a whole-body decode; use zlib with
              wbits=47 if you want to auto-detect gzip/zlib from one call.
        """
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


# =============================================================================
# S8. HTML PARSING  -- links and flags only (html.parser is ALLOWED)
# =============================================================================
class PageParser(HTMLParser):
    """Collect <a href> links and any secret flags from one HTML page.

    Use as: p = PageParser(); p.feed(html); p.links -> [...]; p.flags -> [...]

    NOTE: html.parser is tolerant but does not resolve relative URLs and does
    not unescape entities in attribute values for you in every Python version;
    resolve hrefs against the page URL in S9/S11, not here, OR pass base_url in.
    """

    def __init__(self, base_url=None):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links = []       # absolute-or-raw hrefs, deduped later
        self.flags = []       # 64-char flag strings found on this page
        self._in_flag_tag = False
        self._flag_buffer = []

    def reset_state(self):
        """Clear collected data so the instance can be reused."""
        pass

    def handle_starttag(self, tag, attrs):
        """TODO:
            - tag == 'a'  -> read the href attribute, append (resolve if base_url)
            - tag == 'h2' and class contains 'secret_flag' -> set self._in_flag_tag
            - tag == 'input' -> optionally record form fields (reused by login())
        """
        pass

    def handle_endtag(self, tag):
        """TODO: close the flag buffer on </h2> and run FLAG_RE over it."""
        pass

    def handle_data(self, data):
        """TODO: if self._in_flag_tag, accumulate text (flag text may be split
        across several handle_data() calls -- do NOT regex each fragment alone).
        """
        pass


def extract_form_fields(html):
    """Return {input_name: input_value} for every <input> on a page.

    Used by login(): you need csrfmiddlewaretoken AND any other hidden fields
    the form carries (section 8 warns the form "may not be as simple as it
    initially appears").

    TODO: reuse PageParser (record inputs there) instead of a second parser.
    """
    pass


# =============================================================================
# S9. SESSION  -- fetch() with the full retry / redirect / error policy
# =============================================================================
class Session:
    """Ties HTTPClient + CookieJar together and enforces the crawl policy.

    EVERY row of the ERROR/BEHAVIOR MATRIX in the module docstring is
    implemented here and nowhere else. Keep it that way: one place to debug.
    """

    def __init__(self):
        self.jar = CookieJar()
        self.client = HTTPClient(self.jar)
        self.requests_made = 0        # useful for stderr progress + README stats

    def fetch(self, url, method="GET", form=None, redirects=0):
        """Fetch one URL and return an HTTPResponse, or None to mean "give up".

        Contract:
            - returns HTTPResponse only for a genuine 200 with a body
            - returns None for 403, 404, unparseable, or attempts exhausted
            - follows redirects internally (does not return the 3xx)

        Pseudocode to implement:
            attempt = 0
            while attempt < MAX_ATTEMPTS:
                try:  send request bytes; read response
                except (timeout, ConnectionError, HTTPParseError):
                      attempt += 1; sleep(backoff); continue
                jar.update_from_response(resp.set_cookies, url)
                if resp.status in (301,302,303,307,308) and redirects < MAX_REDIRECTS:
                      target = canonicalize(resp.header('location'), base=url)
                      method becomes GET; return self.fetch(target, 'GET', redirects+1)
                if resp.status == 200:  return resp
                if resp.status in (403, 404): return None
                if resp.status == 500:  attempt += 1; continue        # retry same URL
                if resp.status == 503:  sleep(long backoff); continue # retry same URL
                return None                                           # 4xx/other: abandon
            return None

        TODO: 303/302 after a POST must become a GET with no body.
        TODO: cap the backoff so a bad URL cannot eat the 15-minute budget.
        """
        pass

    def post_form(self, url, fields):
        """urlencode(fields) -> bytes -> fetch(url, method='POST', form=body).

        TODO: add Referer and Origin headers here (Django CSRF checks them).
        """
        pass

    def backoff(self, attempt):
        """Sleep for min(BACKOFF_BASE * 2**attempt, cap). Log it."""
        pass


# =============================================================================
# S10. LOGIN  -- reverse-engineer the Fakebook form (section 8)
# =============================================================================
def login(session, username, password):
    """Log in and return True on success.

    THE CSRF DANCE (most common way to lose a day):
        1. GET LOGIN_URL with an empty jar -> server sets a csrftoken cookie
        2. parse the <form> -> collect ALL hidden inputs, especially
           csrfmiddlewaretoken (and any others; do not hardcode four fields)
        3. POST the same URL with:
             username, password, csrfmiddlewaretoken=<value from step 2>,
             next=/fakebook/  (only if the form actually has it),
           plus Referer: LOGIN_URL and Origin: http://sprinter2.cs.vt.edu
        4. a success is a 302 to /fakebook/ AND a sessionid cookie in the jar
        5. verify by fetching BASE_URL and checking you are NOT redirected
           back to the login page

    TODO: open DevTools -> Network -> Preserve log, log in manually, and copy
          the exact request body + headers. Diff that against yours.
    TODO: bad credentials -> log to stderr and return False (main() exits non-zero).
    """
    pass


def is_logged_in(session):
    """Sanity check: fetch BASE_URL and confirm we did not land on the login form.

    TODO: compare the final URL and/or look for a logout link / profile link.
    """
    pass


# =============================================================================
# S11. CRAWLER  -- frontier, dedupe, workers, flags
# =============================================================================
class Crawler:
    """Breadth-first crawler over Fakebook.

    Shared state (all guarded by self.lock):
        self.frontier  deque of URLs to visit
        self.seen      set of canonicalized URLs already queued/visited
        self.flags     list of flags found, insertion order, deduped
        self.active    int count of workers currently inside fetch()
                       (needed for correct termination: the frontier is
                        momentarily empty while requests are in flight)

    Termination rule: a worker exits when the frontier is empty AND
    self.active == 0. With a plain `while frontier:` loop plus threads you will
    either exit early or spin forever. Use a threading.Condition.
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
        """Seed the frontier, start workers, join them, return self.flags.

        TODO: single-threaded first (workers=1). Add threads only after a full
              crawl produces 5 flags. Keep workers <= 8 in total.
        TODO: stop early once FLAGS_WANTED flags are collected (saves minutes).
        """
        pass

    def _worker(self, worker_id):
        """One crawl loop. Runs until the termination rule fires.

        Per iteration:
            1. pop a URL under the lock (wait on cond if empty but active > 0)
            2. mark active += 1, release lock
            3. resp = session.fetch(url)
            4. if resp and resp.is_html: parse links + flags
            5. record new flags (under lock, dedupe, log count to stderr)
            6. enqueue unseen in-scope links (under lock)
            7. active -= 1, notify cond
        """
        pass

    def _enqueue(self, url):
        """canonicalize, filter (host, scope, skip-list), dedupe, append.

        Caller must hold the lock (or document otherwise).
        """
        pass

    def _record_flags(self, flags):
        """Merge newly found flags under the lock; log 'found 3/5' to stderr."""
        pass

    @property
    def done(self):
        """True when enough flags have been found."""
        pass

    def stats(self):
        """Return (visited, queued, flags) for progress logging."""
        pass


# =============================================================================
# S12. MAIN
# =============================================================================
def parse_args(argv):
    """Return (username, password) or None after printing usage to stderr.

    TODO: exactly two args after the program name; do not accept flags.
    """
    if len(argv) != 3:
        eprint(f"usage: {argv[0]} [username] [password]")
        return None
    return argv[1], argv[2]



def main(argv):
    """Orchestrate: parse args -> login -> crawl -> print exactly five lines.

    TODO:
        - usage error        -> stderr, return 2
        - bad credentials    -> stderr, return 1
        - crawl error        -> one line to stderr (assignment allows this), return 1
        - success            -> for flag in flags[:FLAGS_WANTED]: print(flag)
        - missing flags      -> log how many were found to stderr, return 1
        - NOTHING else may be written to stdout, ever.
    """
    info = parse_args(argv)
    if info is None:
        return 2
    username, password = info
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
