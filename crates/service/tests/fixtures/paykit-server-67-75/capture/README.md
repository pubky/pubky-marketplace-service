# paykit-server #67/#75 Marketplace lifecycle fixtures

The seven JSON files one directory up are HTTP exchanges captured from a
running `pubky/paykit-server` at exact #75 head
`80352cebb011d2f57318170068ae4ec24af6a417` (tree
`20e584cb520acea32280cc71f9b49d661bbd1519`). That commit contains #67's
reviewed lifecycle head `ca0a88e8f3f34874ec0ee0b41dc672b238043397`.

#67 was squash-merged to `master` as `c7fe8efd`. A later provenance check
repeated this capture procedure at that merged producer commit: method, path,
status, content type, field order, payment reference, and operation id matched.
Only ephemeral keys, invoice ids, timestamps, and signatures differed. These
files remain the original byte-preserving captures; they were not rewritten
to look like a capture from the squash commit.

Nothing in the request or response envelopes was written by hand. Each file
stores the method, path, absent/present content type, exact canonical request
body, `x-paykit-signature`, response status, response content type, and exact
response body. The captures cover a new prepare and exact replay, activate,
signed active status, resolve, a second prepare, and void.

## What was real

The producer used its production `Server`, signed-service authentication,
Marketplace lifecycle/status services, PostgreSQL 18, migrations, workers,
an ephemeral Pubky testnet, real Paykit SDK sessions, and a real creator
Bitcoin account. Requests crossed TCP through `capture-proxy.py`; the proxy
forwarded bytes to the producer and recorded both sides. The producer's
`EmptyChain` test transport returned no Bitcoin observations, so the signed
status is an active, undetected invoice while proposal delivery is pending.

Marketplace used the production `PaykitClient` from PR #95 intake head
`122add84c2daeb7649679a1bfdea4e7d9b200256`. Fixed order UUIDs make its
references, operation ids, request bodies, and signatures deterministic.
Producer-issued invoice UUIDs and database timestamps are volatile at
capture time but frozen verbatim in the fixtures; replay tests compare and
parse them rather than regenerating them.

No secret is stored here. Creator/reader identifiers and request signatures
are public protocol values. The disposable creator/reader secret keys and
PostgreSQL URL existed only in mode-0600 handoff files and were deleted after
capture. The signing seed is the repository's documented test seed (`66`
repeated 32 times), never a deployment key.

## Capture again

Use two clean clones plus PostgreSQL. In the producer clone:

```sh
git checkout 80352cebb011d2f57318170068ae4ec24af6a417
export TEST_DATABASE_URL='postgres://postgres:<password>@127.0.0.1:5432/postgres'
export PAYKIT_HARNESS_TRUSTED_SEED='6666666666666666666666666666666666666666666666666666666666666666'
export PAYKIT_HARNESS_HANDOFF=/tmp/paykit-producer.json
cargo test --locked -p paykit-server-e2e --test marketplace_live_harness \
  serve_for_marketplace -- --ignored --exact --nocapture
```

After the producer writes its handoff, in the Marketplace clone:

```sh
git checkout 122add84c2daeb7649679a1bfdea4e7d9b200256
git apply crates/service/tests/fixtures/paykit-server-67-75/capture/marketplace-capture-test.patch
python3 crates/service/tests/fixtures/paykit-server-67-75/capture/capture-proxy.py \
  --handoff /tmp/paykit-producer.json \
  --consumer-handoff /tmp/paykit-consumer.json \
  --out /tmp/paykit-capture \
  --revision 80352cebb011d2f57318170068ae4ec24af6a417
```

In another shell in the Marketplace clone:

```sh
export DATABASE_URL='postgres://postgres:<password>@127.0.0.1:5432/postgres'
export PAYKIT_LIVE_HANDOFF=/tmp/paykit-consumer.json
cargo test -p marketplace-service --test paykit_live_test \
  capture_upstream_marketplace_exchanges -- \
  --ignored --exact --nocapture --test-threads=1
```

Signal the producer by creating `/tmp/paykit-producer.json.stop`, then delete
both handoff files. Rename the successful proxy outputs only:

- `prepare.json` -> `prepare_new.json`
- `prepare_2.json` -> `prepare_replay.json`
- `activate.json` -> `activate.json`
- `status.json` -> `status_active.json`
- `resolve.json` -> `resolve_paid_manually.json`
- `prepare_3.json` -> `prepare_for_void.json`
- `void.json` -> `void.json`

Change only each fixture's top-level `name` to match its filename. Preserve
all nested request/response strings byte for byte. Update revision/tree pins
in `tests/common/paykit_server_67_75.rs` only after verifying ancestry and
review provenance.
