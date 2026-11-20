CS4254 Project 3 - Fakebook Web Crawler README
11/20/2026

Nate Obcemea (nateo) Denis Melara (denismel)

High-level approach:
The crawler opens a plain TCP socket to sprinter2.cs.vt.edu and writes every
HTTP/1.1 request as a hand-built byte string: request line, Host, User-Agent,
Accept, Connection: close, an optional Cookie header built from our own
cookie store, and Content-Length/Content-Type when the request carries a
POST body. The request-target is the path plus query string from the URL,
never the full URL, and the whole header block is joined with \r\n and
terminated with a blank line before the body is appended. A SocketReader
wraps the raw socket and buffers whatever recv() hands back, since recv()
can return anywhere from one byte to a full OS buffer at a time and does
not respect our delimiters; read_until() scans that buffer for \r\n and
keeps any leftover bytes for the next call instead of discarding them,
and read_exact() does the same thing for a known Content-Length.

Responses are parsed in the same byte-level way: the status line is split
on the first two spaces so a reason phrase containing a space does not
break the parse, headers are read line by line until a blank line, and
every Set-Cookie header is appended to a list rather than a dict so a
second Set-Cookie does not overwrite the first. Transfer-Encoding: chunked
is checked before Content-Length, because the server sends most Fakebook
pages chunked rather than with a fixed length; read_chunked_body() reads
a hex size line, ignores anything after a ';' in that line (chunk
extensions), reads exactly that many bytes plus the trailing \r\n, and
repeats until it reads a zero-length chunk, after which it consumes the
trailer lines up to the final blank line. Content-Encoding is decoded
after de-chunking, not before, using gzip.decompress for gzip and zlib
for deflate.

Cookies are tracked by a CookieJar we wrote ourselves, since
http.cookiejar and http.cookies are both disallowed. update_from_response
splits each raw Set-Cookie value on ';', treats the first piece as
name=value and every piece after it as an attribute, and stores Domain
(defaulting to the request host), Path (defaulting to /), and an expiry
computed from Max-Age or Expires, with Max-Age taking priority over
Expires if both are present, matching RFC 6265. header_for() rebuilds the
Cookie header for a given URL by checking domain_matches() (exact host or
a suffix on a '.' boundary) and path_matches() (prefix on a '/' boundary)
against every stored cookie, skipping anything already expired.

Session.fetch() is the one place that decides what to do with a status
code. 301/302/303/307 are followed by recursively calling fetch() on the
canonicalized Location target, converting POST to GET if the original
request was a POST, and capping the recursion at MAX_REDIRECTS so a
redirect loop cannot run forever. 403 and 404 give up on that URL and
return None. 500 retries the same URL with exponential backoff
(BACKOFF_BASE * 2**attempt, capped at 30 seconds); 503 does the same but
with extra padding added to the backoff, since a 503 means the server is
telling us to slow down. Socket and parse errors are caught in the same
retry loop. login() GETs the login page, scrapes every hidden <input> on
the form with a reused HTMLParser subclass instead of hardcoding
csrfmiddlewaretoken, overlays the username and password, and POSTs it
with Referer and Origin headers set, since Django's CSRF check looks at
those. Success is checked two ways: a sessionid cookie must be present
in the jar, and fetching the Fakebook homepage afterward must not show a
login form.

The Crawler keeps a deque frontier of URLs to visit and a set of URLs
already seen so friend lists that link back and forth to each other do
not cause an infinite loop. Worker threads pop a URL under a lock, fetch
and parse it outside the lock (so the lock is not held during network
I/O), then record any new flags and enqueue any new in-scope links back
under the lock. A threading.Condition handles the stopping case where the
frontier is briefly empty but another worker is still in flight and may
add more URLs; workers wait on it and are woken whenever another worker
finishes a fetch. PageParser extracts both the <a href> links and the
flag text out of <h2 class="secret_flag"> by buffering text across
handle_data() calls and only matching the flag regex once the closing
</h2> is seen, since the flag text is not guaranteed to arrive as a
single callback.

Challenges:
The CookieJar's lock was first written as threading.lock() with a
lowercase l, which raised AttributeError the instant a Session was
constructed, since the class is threading.Lock with a capital L. In the
same method, an attribute name was normalized with
attr_name.strip.lower() with the call on strip missing, so .lower() was
being called on a bound method object instead of a string, which also
raised AttributeError rather than doing anything useful.

In PageParser.handle_starttag, the branches for <h2> and <input> were
first written as elif clauses hanging off of "if href:" instead of off
of the outer "if tag == 'a':". Since that inner if only runs at all when
tag is already "a", the h2 and input branches could never execute, so no
flags and no login form fields were ever found, with no error raised to
point at why. The input branch also had "elif tag = 'input':" with a
single equals sign, which is an outright SyntaxError, but even once that
was fixed the indentation bug above still made it silently useless until
the whole if/elif chain was lifted out to the correct level.

Session.fetch had a mix of simple typos (open_connections instead of
open_connection, build_request called with a variable named header that
was never defined instead of url, and body=from, where from is a
reserved word in Python and cannot be used as a variable name) and one
more serious bug: in the redirect branch, target = canonicalize(location,
base=url) was written one line too low, nested inside "if not location:"
and after its return statement. That put it on a dead code path and
meant target was never assigned on the normal case where a Location
header is actually present, so a NameError fired on every redirect,
which includes the 302 Fakebook sends after a successful login. This one
was easy to miss by inspection because the surrounding code looked
reasonable; it only showed up by tracing which branch target was
actually assigned on versus which branch referenced it.

A later issue was not a crash but a performance problem: once the
crawler had already found all 5 flags, worker threads that were already
in the middle of retrying a 500 or 503 on some other URL had no way of
knowing the crawl was finished, so they kept retrying with growing
backoff before the program could exit. We fixed this by adding an
optional should_stop callback to Session.fetch that is checked before
every retry attempt, and the Crawler passes it a check of its own done
flag, so a worker now exits a stuck retry loop as soon as enough flags
exist instead of waiting out the full MAX_ATTEMPTS.

Testing:
Every change was run through python3 -m py_compile before anything else,
since an indentation or syntax mistake is far easier to catch immediately
than after a multi-minute crawl fails partway through. The HTTP layer was
brought up in stages before the full crawler was ever run: SocketReader
and HTTPClient were first used to fetch a single page and print its
status code and body length, then two sequential requests were used to
confirm a Set-Cookie from the first response was correctly sent back as a
Cookie header on the second, and only after that did login() get tested
on its own, checking for a sessionid cookie in the jar and confirming
that fetching the Fakebook homepage no longer contained a password field.

The full crawler was then run against the live sprinter2.cs.vt.edu server
multiple times rather than just once, specifically because the server
intentionally returns random 500s (confirmed in stderr logs showing
retries with doubling backoff, 2.0s then 4.0s, matching
BACKOFF_BASE * 2**attempt) and we wanted to see it recover from that
reliably rather than by luck. Every run found all 5 flags in roughly a
minute. stdout cleanliness was checked directly with
"./webcrawler user pass 2>/dev/null | wc -l" to confirm exactly 5 lines
reached stdout and nothing else, since stray debug output on stdout would
break the graded output even if the crawl itself worked correctly.
