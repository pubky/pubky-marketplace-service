#!/usr/bin/env python3
"""HTTPS sink for what WooCommerce sends outward: the wc-auth key callback and
webhook deliveries. Each request is written, unmodified, to OUT_DIR as one JSON
file (method, path, headers, raw body base64) and answered 200.

  python3 receiver.py OUT_DIR [PORT]
"""
import base64, json, os, ssl, sys, time, itertools
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

out = sys.argv[1]
port = int(sys.argv[2]) if len(sys.argv) > 2 else 18443
os.makedirs(out, exist_ok=True)
counter = itertools.count(1)
here = os.path.dirname(os.path.abspath(__file__))
certs = os.path.join(here, "..", "certs")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        record = {
            "method": "POST",
            "path": self.path,
            "headers": {k.lower(): v for k, v in self.headers.items()},
            "body_base64": base64.b64encode(body).decode(),
            "received_at": time.time(),
        }
        name = f"{next(counter):04d}-{int(time.time() * 1000)}.json"
        with open(os.path.join(out, name), "w") as handle:
            json.dump(record, handle, indent=1)
        reply = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    do_GET = do_POST

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(os.path.join(certs, "server.pem"), os.path.join(certs, "server.key"))
server.socket = context.wrap_socket(server.socket, server_side=True)
print(f"receiver on {port}, writing to {out}", flush=True)
server.serve_forever()
