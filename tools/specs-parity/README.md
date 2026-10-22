# specs-parity

The service publishes the attestor's profile and annotations. Their shape,
validation and proof are those of `pubky-app-specs` (`marketplace_attestor.rs`),
which the index verifies them with. The service cannot depend on that crate:
it needs `pubky` 0.9, whose `pkarr` pins a pre-release `ed25519` that cannot
share a dependency graph with the `pubky` 0.16 the service uses. The service
therefore holds a port (`crates/service/src/attestor_records.rs`), and this
tool holds the port to the crate.

It is its own workspace with its own lockfile, pinned to the specs tag
`v0.6.2-marketplace.16` by commit. It reads
`crates/service/tests/fixtures/attestor_records/cases.json` and, for every case,
signs the record with the specs crate:

- a case marked `ok` must sign, produce the `expected` canonical record and
  verify through `from_verified_json` (the index's loader) under its owner's URI
  and not under another owner's;
- a case marked `reject` must be refused;
- the specs test vector (`specs-vector-v1.json`) must verify.

The service's `attestor_records_test` runs the same file against the port, so
the two are equal byte for byte on every case.

```sh
cd tools/specs-parity
cargo run --release
```

After moving the pinned specs revision on purpose, regenerate the expected
records from the crate and review the diff:

```sh
BLESS=1 cargo run --release
```

CI runs it as the `specs-parity` job. The test key in the fixtures is the
specs vector's (32 bytes of `0x07`); it signs nothing real.
