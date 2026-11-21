CS4254 Project 3 - Fakebook Web Crawler README
11/20/2026

Nate Obcemea (nateo) Denis Melara (denismel)

High-level approach:
The crawler opens a plain TCP socket to sprinter2.cs.vt.edu and writes every
HTTP/1.1 request as a request line, Host, User-Agent, Accept, Connection: close
and an optional Cookie header and Content-Length/Content-Type when the request
carries a POST body. The request target is the path and query string from the
URL, and the whole header block is joined with \r\n and terminated with a blank
line before the body is added. A SocketReader wraps the raw socket and buffers
whatever recv() responds with. The read_until() scans that buffer for \r\n and
keeps any leftover bytes for the next call instead of getting rid of them and
read_exact() does the same thing for a known Content-Length.

Responses are parsed in the same where the status line is split on the first
two spaces so a phrase containing a space does not break it and headers are
read line by line until a blank line, and every Set-Cookie header is appended
to a list rather than a dict so a second Set-Cookie does not overwrite the
first. For the Transfer-Encoding the chunked is checked before Content-Length,
since most Fakebook pages are chunked. read_chunked_body() reads each
hex size, skips any chunk extensions, and reads the data and trailing
\r\n. A zero-length chunk marks the end, after which the reader consumes
the trailers. The body is then decoded with gzip or zlib if needed.

CookieJar parses each Set-Cookie header and stores the value, domain, path, and
expiry. The domain defaults to the request host and the path defaults to /.
Max-Age takes priority over Expires when building the Cookie header, so the jar
skips expired cookies and checks the domain and path against the request URL.

Session.fetch() follows 301, 302, 303, and 307 redirects up to
MAX_REDIRECTS, checks the destination host, and follows a redirected
POST with a GET. It skips 403 and 404 responses, retries 500 responses
with exponential backoff maxxed at 30 seconds, and backs off longer
for 503. Socket and parse errors also go through the retry loop.
login() reads the form fields from the login page, fills in the username
and password, and sends them with the Referer and Origin headers. It
checks for a sessionid cookie and makes sure the home page loads without
showing the login form.

The crawler uses a deque for URLs to visit and a set for URLs it has
already seen. Workers take a URL under the lock, fetch and parse it
outside the lock, then lock again to record flags and queue new links.
A threading Condition lets workers wait when the queue is empty and
another fetch might still turn up more links. PageParser extracts anchor
links and buffers text inside secret_flag headings so it can match the
flag when the heading closes.

Challenges:
Two typos in CookieJar raised AttributeError. The lock constructor
needed a capital L in threading.Lock(), and the cookie attribute parser
needed to call strip() before lower().

The h2 and input branches in PageParser were indented under the anchor
tag branch, so the parser never reached them. The input condition also
used = where it needed ==. Fixing both let the parser pick up flags and
login fields.

We fixed a few typos in Session.fetch(), then traced a NameError to the
redirect branch. The line that set target was inside the missing-Location
check, after its return. Moving it out of that block let the crawler
follow the 302 after a successful login.

Workers could still be retrying a 500 or 503 after the crawler found
all five flags so we added a should_stop callback to Session.fetch() so
workers check the flag count before sending another request.

Testing:
We ran python3 -m py_compile as we made changes. We started by fetching
one page and checking the status code and body length. Then we sent two
requests to confirm that the cookie set by the first came back in the
second request. To test login, we checked for a sessionid cookie and
confirmed that the Fakebook home page loaded without a password field.

We ran the full crawler against sprinter2.cs.vt.edu a bunch of times to
check that it recovered from random 500 responses and the logs showed
retries with increasing delays, and each run found five flags in about
a minute.
