Copied verbatim from `pubky-app-specs` commit
[`695c0f7`](https://github.com/BitcoinErrorLog/pubky-app-specs/blob/695c0f7af3ac02e1b726d8a82eee17f114d2da59/src/test_fixtures/price-weight-vectors.json)
(`0.6.2-marketplace.15`), file `src/test_fixtures/price-weight-vectors.json`
(SHA-256 `50a803c19a0d1c199b805267fb8c1ea9bfd8f65418add97a440945a4cfcf27c7`).
These are the shared vectors every issuer and verifier of the purchase
attestation's `price_decade` claim asserts against. The service checks
`priceDecade` against `attestor::price_decade`; `weight` is the verifiers'
side and is not computed here. Replace the file, never edit it, when the
specs vectors change.
