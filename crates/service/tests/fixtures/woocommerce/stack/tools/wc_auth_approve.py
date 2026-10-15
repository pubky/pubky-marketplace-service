#!/usr/bin/env python3
"""Plays the merchant: logs in to WP admin and approves a wc-auth request.

  ADMIN_PASSWORD=... python3 wc_auth_approve.py 'https://localhost:8443/wc-auth/v1/authorize?...' CA_PEM

Prints the final redirect location (the return_url with success=1) and exits 0
when WooCommerce reported success.
"""
import html, http.cookiejar, os, re, ssl, sys, urllib.parse, urllib.request

authorize_url, ca = sys.argv[1], sys.argv[2]
context = ssl.create_default_context(cafile=ca)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=context),
    urllib.request.HTTPCookieProcessor(jar),
    NoRedirect(),
)


def fetch(url, data=None):
    request = urllib.request.Request(url, data=data)
    try:
        response = opener.open(request)
    except urllib.error.HTTPError as error:
        response = error
    return response.status, response.headers, response.read().decode("utf-8", "replace")


origin = "{0.scheme}://{0.netloc}".format(urllib.parse.urlparse(authorize_url))
fetch(origin + "/wp-login.php")
status, headers, _ = fetch(
    origin + "/wp-login.php",
    urllib.parse.urlencode(
        {"log": "admin", "pwd": os.environ["ADMIN_PASSWORD"], "wp-submit": "Log In", "testcookie": "1"}
    ).encode(),
)
assert status == 302, f"login failed: {status}"
jar.set_cookie(http.cookiejar.Cookie(0, "wordpress_test_cookie", "WP Cookie check", None, False, "localhost", False, False, "/", True, False, None, True, None, None, {}))
status, headers, body = fetch(authorize_url)
location = headers.get("Location")
while status in (301, 302) and location:
    status, headers, body = fetch(urllib.parse.urljoin(origin, location))
    location = headers.get("Location")
match = re.search(r'href="([^"]*access_granted[^"]*)"', body)
assert match, f"no grant link on the authorize page (status {status}): {body[:300]}"
status, headers, body = fetch(html.unescape(match.group(1)))
print(headers.get("Location") or f"status {status}: " + re.sub(r"<[^>]+>|\s+", " ", body[-600:]))
sys.exit(0 if status == 302 and "success=1" in (headers.get("Location") or "") else 1)
