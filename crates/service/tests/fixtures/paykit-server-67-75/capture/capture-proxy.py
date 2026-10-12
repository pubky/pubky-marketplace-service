#!/usr/bin/env python3
import argparse
import http.client
import json
import pathlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--handoff", required=True)
    parser.add_argument("--consumer-handoff", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    handoff = json.loads(pathlib.Path(args.handoff).read_text())
    upstream = urlsplit(handoff["base_url"])
    handoff["base_url"] = f"http://127.0.0.1:{args.port}"
    consumer_handoff = pathlib.Path(args.consumer_handoff)
    consumer_handoff.write_text(json.dumps(handoff, indent=2) + "\n")
    consumer_handoff.chmod(0o600)
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    counts = {}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.forward()

        def do_POST(self):
            self.forward()

        def forward(self):
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length)
            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower() not in {"host", "content-length", "connection"}
            }
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=30)
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = response.getheaders()

            self.send_response(response.status)
            for name, value in response_headers:
                if name.lower() not in {"connection", "content-length", "transfer-encoding"}:
                    self.send_header(name, value)
            self.send_header("content-length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

            prefix = "/marketplace/payment-requests/"
            if self.path.startswith(prefix):
                route = self.path[len(prefix):].replace("/", "_")
                with lock:
                    counts[route] = counts.get(route, 0) + 1
                    ordinal = counts[route]
                    name = route if ordinal == 1 else f"{route}_{ordinal}"
                    document = {
                        "name": name,
                        "server_revision": args.revision,
                        "request": {
                            "method": self.command,
                            "path": self.path,
                            "content_type": self.headers.get("content-type"),
                            "signature": self.headers.get("x-paykit-signature"),
                            "body": body.decode("utf-8"),
                        },
                        "response": {
                            "status": response.status,
                            "content_type": dict((k.lower(), v) for k, v in response_headers).get("content-type"),
                            "body": response_body.decode("utf-8"),
                        },
                    }
                    (out / f"{name}.json").write_text(json.dumps(document, indent=2) + "\n")
            connection.close()

        def log_message(self, format, *values):
            print(format % values, flush=True)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"proxy ready http://127.0.0.1:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
