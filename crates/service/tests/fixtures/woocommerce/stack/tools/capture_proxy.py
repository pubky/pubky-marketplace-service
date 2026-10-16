#!/usr/bin/env python3
"""Records every request the connector sends to WooCommerce and every answer.

A TLS-terminating reverse proxy in front of the stack's own proxy:

  client --https--> capture_proxy (:8443) --https--> nginx (:8444) --> WordPress

It writes one JSON file per label to OUT_DIR. The label is set by the test
with a control request the proxy answers itself (never forwarded):

  POST https://localhost:8443/__fixture/label?name=catalog

Recorded per exchange: method, path, query without the credential, how the
request authenticated (basic / query / none), the JSON body with the webhook
secret blanked, the status, a few response headers and the response body.
Credentials never reach the files.

  python3 capture_proxy.py OUT_DIR [LISTEN_PORT] [UPSTREAM_PORT]
"""
import http.client, json, os, ssl, sys, threading, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

out = sys.argv[1]
listen = int(sys.argv[2]) if len(sys.argv) > 2 else 8443
upstream_port = int(sys.argv[3]) if len(sys.argv) > 3 else 8444
here = os.path.dirname(os.path.abspath(__file__))
certs = os.path.join(here, "..", "certs")
os.makedirs(out, exist_ok=True)

upstream_context = ssl.create_default_context(cafile=os.path.join(certs, "ca.pem"))
lock = threading.Lock()
state = {"label": "unlabelled", "exchanges": {}}
KEEP_HEADERS = ("content-type", "x-wp-total", "x-wp-totalpages", "location", "retry-after")
SECRET_FIELDS = ("secret",)


def save():
    for label, exchanges in state["exchanges"].items():
        with open(os.path.join(out, f"{label}.json"), "w") as handle:
            json.dump({"label": label, "exchanges": exchanges}, handle, indent=1, ensure_ascii=False)
            handle.write("\n")


def parse_body(raw, content_type):
    if not raw:
        return None, None
    text = raw.decode("utf-8", "replace")
    try:
        return json.loads(text), "json"
    except ValueError:
        return text, "text"


def blank_secrets(value):
    if isinstance(value, dict):
        return {k: ("<secret>" if k in SECRET_FIELDS and isinstance(v, str) else blank_secrets(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [blank_secrets(v) for v in value]
    return value


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def handle_any(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/__fixture/label":
            name = urllib.parse.parse_qs(parsed.query).get("name", ["unlabelled"])[0]
            with lock:
                state["label"] = name
                state["exchanges"].setdefault(name, [])
            return self.reply(204, b"")
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        auth = "none"
        if "consumer_key" in dict(query):
            auth = "query"
        elif self.headers.get("Authorization", "").lower().startswith("basic "):
            auth = "basic"
        clean_query = [[k, v] for k, v in query if k not in ("consumer_key", "consumer_secret")]
        forward_headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection", "content-length")}
        connection = http.client.HTTPSConnection("localhost", upstream_port, context=upstream_context, timeout=120)
        connection.request(self.command, self.path, body=body or None, headers=forward_headers)
        response = connection.getresponse()
        payload = response.read()
        request_body, request_kind = parse_body(body, self.headers.get("Content-Type"))
        response_body, response_kind = parse_body(payload, response.getheader("Content-Type"))
        if request_kind == "json":
            request_body = blank_secrets(request_body)
        record = {
            "method": self.command,
            "path": parsed.path,
            "query": clean_query,
            "auth": auth,
            "request_body": request_body,
            "status": response.status,
            "response_headers": {h: response.getheader(h) for h in KEEP_HEADERS if response.getheader(h) is not None},
            "response_kind": response_kind,
            "response_body": response_body,
        }
        with lock:
            state["exchanges"].setdefault(state["label"], []).append(record)
            save()
        self.send_response(response.status)
        for header, value in response.getheaders():
            if header.lower() in ("transfer-encoding", "connection", "content-length"):
                continue
            self.send_header(header, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def reply(self, status, payload):
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = handle_any

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("0.0.0.0", listen), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(os.path.join(certs, "server.pem"), os.path.join(certs, "server.key"))
server.socket = context.wrap_socket(server.socket, server_side=True)
print(f"capture proxy on {listen} -> localhost:{upstream_port}, writing to {out}", flush=True)
server.serve_forever()
