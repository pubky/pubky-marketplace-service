#!/usr/bin/env python3
"""Moves large response bodies out of the recorded exchange files.

WordPress's REST index (`GET /wp-json/`) is over a megabyte and is part of
every scenario. Each body above the limit is written once, minified, to
exchanges/_blobs/<id>.json and the exchange keeps {"$blob": "<id>"}. The
replay resolves the reference. Idempotent; blobs no exchange references are
removed.

  python3 externalize_blobs.py EXCHANGES_DIR
"""
import hashlib, json, os, sys

LIMIT = 64 * 1024
directory = sys.argv[1]
blobs = os.path.join(directory, "_blobs")
os.makedirs(blobs, exist_ok=True)

for name in sorted(os.listdir(directory)):
    if not name.endswith(".json"):
        continue
    path = os.path.join(directory, name)
    data = json.load(open(path))
    changed = False
    for exchange in data["exchanges"]:
        body = exchange.get("response_body")
        if body is None or (isinstance(body, dict) and "$blob" in body):
            continue
        minified = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        if len(minified) < LIMIT:
            continue
        blob_id = hashlib.sha256(minified.encode()).hexdigest()[:16]
        with open(os.path.join(blobs, f"{blob_id}.json"), "w") as handle:
            handle.write(minified + "\n")
        exchange["response_body"] = {"$blob": blob_id}
        changed = True
    if changed:
        with open(path, "w") as handle:
            json.dump(data, handle, indent=1, ensure_ascii=False)
            handle.write("\n")

referenced = set()
for name in os.listdir(directory):
    if name.endswith(".json"):
        for exchange in json.load(open(os.path.join(directory, name)))["exchanges"]:
            body = exchange.get("response_body")
            if isinstance(body, dict) and "$blob" in body:
                referenced.add(body["$blob"])
for name in os.listdir(blobs):
    if name.endswith(".json") and name[:-5] not in referenced:
        os.remove(os.path.join(blobs, name))
