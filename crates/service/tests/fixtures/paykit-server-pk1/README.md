# paykit-server multi-asset prepare and activate (PK1)

Response bodies emitted by the signed `POST /marketplace/payment-requests/prepare`
and `/activate` routes of paykit-server PR-A
([pubky/paykit-server](https://github.com/pubky/paykit-server) branch carrying
`f3697b7abe44fb42a7eb9addd8d4f1bacdd02bce`, base `f9c24db` = v0.1.0-rc16),
captured by its `paykit-server-e2e/tests/marketplace_multi_asset.rs` over
PostgreSQL with only the BTC/USD feed doubled at 81,000 USD per BTC. The
capture replaces only the values the database or RNG chooses (`invoice_id`,
`prepare_expires_at`, `activated_at`, `payment_deadline`) with fixed ones;
every other byte is as emitted. Nothing in these files is written by hand.

The files carry bodies only. The HTTP status of each error is the one the
capture README states:

| File | Status |
| --- | --- |
| `seller-setup-pending-{bitcoin,usdt,both}.json` | 503 |
| `asset-unsupported.json` | 409 |
| `payment-window-exceeds-cap.json` | 400 |
| `operation-conflict.json` | 409 |
| `total-mismatch.json` (activate) | 409 |

`prepare-usd-derived-*` are requests without the optional `accepted_assets`
filter; `prepare-<denomination>-<filter>` are the nine explicit combinations
(`usd` is 500 cents, `btc` 50,000 sats, `usdt` 5,000,000 millionths). The
status route's USDT bodies (paykit-server PR-B) are not captured yet.
`tests/single_paykit_captured_test.rs` replays these bodies through the real
client.
