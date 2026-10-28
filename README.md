# Pubky Marketplace Transaction Service

Server-authoritative Rust service for marketplace inventory, reservations,
checkout/orders, and payments, per
[ADR-0019](https://github.com/pubky/pubky-marketplace/blob/master/docs/adr/0019-marketplace-transaction-authority.md)
(Marketplace Transaction Authority) and
[ADR-0022](https://github.com/pubky/pubky-marketplace/blob/master/docs/adr/0022-marketplace-transaction-service-rust.md)
(Rust implementation). The TypeScript prototype engine
([`services/marketplace/src/transaction-service.ts`](https://github.com/pubky/pubky-marketplace/blob/master/services/marketplace/src/transaction-service.ts)
in `pubky/pubky-marketplace`)
is the executable specification; its test cases are ported command by command
before each command is enabled here.

The HTTP API for seller automation (sessions, inventory, exports, the event
feed, webhooks) is documented in [`docs/api.md`](docs/api.md).

## Workspace layout

```
crates/domain      Canonical contracts: command envelope + payload validation,
                   money, aggregate ids, error codes, state machines, and the
                   emit-contracts binary.
crates/service     axum HTTP service: Postgres schema (sqlx migrations),
                   Pubky AuthToken auth, command executor with
                   idempotency, command handlers, Locks lifecycle
                   verification (encrypted correlations, HMAC lookup
                   tokens, Lock Server client), background worker runtime
                   (see "Background workers": 24 lease-guarded tasks plus
                   the webhook delivery worker). Besides the server binary
                   (`marketplace-service`) it builds the operator binaries
                   `refusal-audit-admin` and `paykit-attempts-admin`.
contracts/         state-machines.json — the machine-readable state machine
                   contract emitted from crates/domain.
                   postal-region-rules.json — country table for
                   `delivery_address.region` (required vs optional; US/CA/AU
                   ISO 3166-2 suffixes). Shop should consume this file.
                   endpoints.json — generated request schemas and reason
                   catalog of the two shared-manual seller review endpoints
                   (confirm-bitcoin-payment, bitcoin/resolve); samples/ —
                   checked response samples (inventory, projections,
                   attestations). Both are guarded by
                   crates/service/tests/contracts_test.rs.
docs/              api.md (seller automation API), migrations.md and the
                   operations notes (lock order, listing deletion follower,
                   PayPal refund IPN, released Paykit attempts, refusal-audit
                   operations).
scripts/           ci-test.sh (the CI test split), the pre-push gate
                   (prepush*.sh), and release-proof helpers (`scripts/release/`).
docker-compose.yml Dev/test PostgreSQL 17 on port 55432.
```

## Setup

Requirements: Rust (1.89+), Docker.

```sh
docker compose up -d --wait          # PostgreSQL 17 on localhost:55432
export DATABASE_URL='postgres://marketplace:marketplace@localhost:55432/marketplace'
```

## Run

```sh
cargo run -p marketplace-service
```

`DATABASE_URL` alone is not enough: the service refuses to start unless it
also has

- `HOMESERVER_URL` — the homeserver the `listing.sync` command reads
  seller-signed listing records from (an http(s) URL);
- `NEXUS_URL` — the marketplace index the moderation gate reads (an http(s)
  URL; see [Listing moderation](#listing-moderation));
- `REFUSAL_AUDIT_DATABASE_URL` and `REFUSAL_AUDIT_RETENTION_DATABASE_URL` —
  two separate Postgres URLs that log in as
  `marketplace_refusal_audit_writer_login` and
  `marketplace_refusal_audit_retention` respectively (each with a password,
  and neither equal to `DATABASE_URL` or to the other). Migrations 0032 and
  0037 create those roles; the tests set their passwords with
  `ALTER ROLE ... PASSWORD` and `GRANT CONNECT ON DATABASE` (see
  `claim_named_login` in `crates/service/tests/common/mod.rs`);
- `REFUSAL_AUDIT_HMAC_ROOT_B64` (32 bytes, canonical base64) and
  `REFUSAL_AUDIT_HMAC_KEY_EPOCH` (positive integer), plus the optional pair
  `REFUSAL_AUDIT_HMAC_PREVIOUS_ROOT_B64` /
  `REFUSAL_AUDIT_HMAC_PREVIOUS_KEY_EPOCH` during a rotation (both or neither);
- `REFUSAL_AUDIT_BACKUP_EXPIRY_ATTESTED`,
  `REFUSAL_AUDIT_REPLICA_EXPIRY_ATTESTED` and
  `REFUSAL_AUDIT_RESIDUAL_RISK_ACCEPTED`, each `true`.

The refusal-audit operator binary (`refusal-audit-admin`) additionally uses
`REFUSAL_AUDIT_ADMIN_DATABASE_URL`; see
[`docs/refusal-audit-operations.md`](docs/refusal-audit-operations.md).
Everything else in the table below is optional.

Migrations in `crates/service/migrations/` are applied automatically at boot
(they can also be applied manually with `sqlx migrate run` from
`crates/service/`). Rules for writing a new migration, and the numbers
reserved by open pull requests, are in [`docs/migrations.md`](docs/migrations.md).

**Migration 0018 (remove operator authority) remediates rows.** Before it
narrows the order-state and annotation-outcome check constraints, it
completes any `disputed` order (no arbiter exists any more, so an order
awaiting adjudication completes as if undisputed — a seller-favour default,
owner-approved for the staging test data; production had no rows when it
was applied) and
deletes the operator annotation outcomes
(`dispute_resolved_for_buyer`/`dispute_resolved_for_seller`/
`attestation_disavowed`, artefacts with no meaning in a peer-to-peer
marketplace), briefly lifting and re-creating the annotations append-only
trigger to do so. It also re-adds `orders_total_balance` as **NOT VALID**
(never validated): staging's historical paid orders were charged a real
tax, so their stored `total_minor` includes the removed `tax_minor` — those
totals are what buyers actually paid and the receipts attest them, so they
are kept as-is, while the constraint still enforces
`total = subtotal + shipping` for every new insert/update. There is
**no DOWN migration** — dropped tables and
columns cannot be restored — so production had to be empty or backed up
before applying (it was empty as of 2026-09-05). The migration is applied
everywhere this service runs; this paragraph is history, not a pending
step.

## Deployment

The service ships as the container image built from the [`Dockerfile`](Dockerfile)
(published by `.github/workflows/production-image.yml` as
`ghcr.io/pubky/pubky-marketplace-service`). Team deployments (staging, then
production) run on Synonym's GCP and are deployed through
[pubky/shop-migration](https://github.com/pubky/shop-migration); infrastructure,
pins and environment values live there. This repository carries no
host-specific infrastructure configuration, and personal development hosting is
out of scope for it.

The release-proof helpers in [`scripts/release/`](scripts/release/) take every
target from environment variables and carry no default host; see the header of
each script.

## Configuration

Configuration is environment-based. Window keys and their defaults also live in
[`.env.example`](.env.example) (config-parity with `Config::from_env`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres connection string |
| `HOMESERVER_URL` | required | homeserver base URL (http(s)) the `listing.sync` command fetches seller-signed records from; the service refuses to boot without it |
| `NEXUS_URL` | required | base URL (http(s)) of the marketplace index (Nexus) the service asks whether a listing is moderated before it accepts a checkout, a bid or an offer on it; the service refuses to boot without it. See [Listing moderation](#listing-moderation) |
| `REFUSAL_AUDIT_*` | required | refusal-audit logins, HMAC key pair and attestations; see [Run](#run) |
| `BIND_ADDR` | `127.0.0.1:8080` | HTTP listen address |
| `ALLOWED_ORIGINS` | empty | comma-separated exact CORS origins |
| `AUTH_TOKEN_WINDOW_SECONDS` | `120` | acceptance window around server time for an AuthToken's signing timestamp |
| `AUTH_SESSION_TTL_SECONDS` | `86400` | session token lifetime |
| `WORKER_INTERVAL_SECONDS` | `10` | background worker pass interval |
| `WORKER_LEASE_SECONDS` | `30` | worker task/outbox lease duration |
| `LISTING_DELETION_PASS_BUDGET_MS` | `10000` | time budget of one listing-deletion follower pass; must be at most half of `WORKER_LEASE_SECONDS`. See [`docs/listing-deletion-follower.md`](docs/listing-deletion-follower.md) |
| `LOCKS_SNAPSHOT_RETENTION_DAYS` | `90` | days Locks lifecycle snapshots are kept before the purge (≥ 1) |
| `FX_FEED_URL` | fixed production URL | FX rate feed for the sampler. A release binary ignores this variable and always uses the fixed URL; only in-process test configuration repoints it |
| `AUTOMATION_RATE_LIMIT_PER_MINUTE` | `120` | seller automation rate limit per `(actor, endpoint class)`; see [`docs/api.md`](docs/api.md#rate-limits) |
| `AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER` | `2` | burst capacity multiplier (max 10); rate × multiplier must be at most 20000 |
| `INVENTORY_AUTHENTICATED_RATE_LIMIT_PER_MINUTE` | `120` | inventory API rate per session (1–10000) |
| `INVENTORY_ANONYMOUS_RATE_LIMIT_PER_MINUTE` | `20` | inventory API rate for requests without a valid session |
| `INVENTORY_RATE_LIMIT_BURST_MULTIPLIER` | `2` | inventory burst multiplier (max 10) |
| `EVENT_RETENTION_DAYS` | `30` | seller event feed retention; older cursors answer `410 cursor_expired` |
| `WEBHOOK_*` | see docs | webhook delivery tuning (`WEBHOOK_MAX_ATTEMPTS` 12, `WEBHOOK_MAX_AGE_HOURS` 24, `WEBHOOK_RETRY_BASE_SECONDS` 5, `WEBHOOK_RETRY_MAX_SECONDS` 3600, `WEBHOOK_MAX_ENDPOINTS_PER_SELLER` 100, and the worker/purge knobs); full list in [`docs/api.md`](docs/api.md#delivery) |
| `MARKETPLACE_GRANT_*`, `GRANT_FLOW_*`, `GRANT_RESULT_*`, `SHOP_*` | unset | Shop BFF grant-flow configuration (flow disabled unless `MARKETPLACE_GRANT_FLOW_ENABLED`); see [`docs/api.md`](docs/api.md#grant-flow-routes-shop-bff) |
| `MARKETPLACE_GRANT_SESSION_ENABLED`, `MARKETPLACE_GRANT_SESSION_CLIENT_IDS`, `MARKETPLACE_GRANT_SESSION_MAX_PROOF_AGE_SECONDS` | `false`, unset, `120` | Marketplace sessions minted from the Shop's sign-in grant (needs `PUBLIC_SERVICE_ORIGIN` when enabled); see [`docs/api.md`](docs/api.md#grant-sessions) |
| `LOCKS_SERVER_URL` | unset | Lock Server base URL; setting it enables Locks verification |
| `LOCKS_BUNDLE_ENCRYPTION_KEY` | unset | 32-byte hex key encrypting bundle ids at rest (XChaCha20-Poly1305) |
| `LOCKS_LOOKUP_HMAC_KEY` | unset | 32-byte hex key for the HMAC-SHA256 correlation lookup token; must differ from the encryption key |
| `LOCKS_PAYMENT_WINDOW_SECONDS` | `3600` | hold window armed by the `payment.register_locks` lock point (≥ 60); the correlation window IS the hold window |
| `CHECKOUT_HOLD_WINDOW_SECONDS` | `900` | unbound leftover cancel for ordinary `pending_payment` rows with `stock_held=false` and no payment method (≥ 60). Ordinary `checkout.create` does not hold stock |
| `FIAT_PAYMENT_WINDOW_SECONDS` | `600` | hold window armed by a PayPal payment-method bind (`POST /v0/orders/{id}/payment-method`) (≥ 60). Default 10 minutes; a deployment may set a longer override |
| `BITCOIN_PAYMENT_WINDOW_SECONDS` | `1800` | hold window armed by a bitcoin payment-method bind; the same instant is Paykit `expires_at` (fork), or Paykit's activation-time deadline plus 60 s once an upstream request is active — never longer than the invoice the buyer is shown (≥ 60). Honor a deployment value ≥ 60; do not cap below a deployment override |
| `SANDBOX_PAYMENT_WINDOW_SECONDS` | `900` | hold window armed by the sandbox lock point (`payment.sandbox_advance`'s first transition out of `awaiting_entitlement`) (≥ 60) |
| `DROP_CLAIM_WINDOW_SECONDS` | `600` | hold window armed at checkout for drop-bound orders (lock-at-claim) (≥ 60) |
| `LOCKS_POLL_SECONDS` | `30` | minimum interval between lifecycle lookups per pending correlation |
| `ATTESTOR_SECRET_KEY` | unset | 32-byte hex Ed25519 secret of the attestor identity (ADR 0024); its z-base-32 public key is the attestor pubky |
| `ATTESTOR_ORDER_SALT` | unset | 32-byte hex salt for `order_ref` hashing; must stay stable for the attestor identity's lifetime |
| `ATTESTOR_PUBLISH_ENABLED` | `false` | `true` publishes the attestor's profile and its refund and return annotations to its own homeserver (see [Attestor records](#attestor-records)); needs the two `ATTESTOR_*` secrets above and the five settings below. Off, the service starts, behaves and calls out exactly as before |
| `ATTESTOR_HOMESERVER` | unset | pubky (52-character z-base-32) of the homeserver that holds the attestor's account; the attestor's PKARR record must point at it. Required when publishing is enabled |
| `ATTESTOR_OPERATOR_URL` | unset | https URL of the operator's website, written into the profile. Required when publishing is enabled |
| `ATTESTOR_POLICY_URL` | unset | https URL of the attestor's issuance policy page, written into the profile. Required when publishing is enabled |
| `ATTESTOR_KEYS` | unset | JSON array of the keys the attestor has issued with, each `{"pubky", "validFrom", "validUntil"?, "compromisedFrom"?}` (RFC 3339 datetimes with an offset, at most 16 keys). Must list the current attestor key. Add a rotated key with its `validUntil`, or `compromisedFrom` for a leaked one. Required when publishing is enabled |
| `ATTESTOR_OPERATOR_NAME` | `Pubky Market` | operator display name in the profile (1 to 100 characters) |
| `ATTESTOR_SUCCESSOR` | unset | pubky of the attestor that takes over from this one, to announce a rotation in the profile |
| `STRIPE_KEY_ENCRYPTION_KEY` | unset | 32-byte hex key sealing seller secrets (Shippo API tokens) at rest; setting it enables the `/v0` payment-methods surface. The name is a legacy of the removed Stripe rail and is kept so deployments need no change |
| `ARBITRUM_RPC_URL` | unset | Arbitrum One JSON-RPC endpoint (http(s)); setting it checks a recorded USDT refund's transaction on Arbitrum. See [USDT refund destination](#usdt-refund-destination) |
| `PAYKIT_SERVER_URL` | unset | paykit-server base URL; setting it enables the bitcoin method |
| `PAYKIT_REQUEST_SIGNING_KEY` | unset | 32-byte hex ed25519 seed signing paykit-server requests. Its pubky-formatted public key goes into paykit-server's `[signed_services] trusted_public_keys`, beside the Locks key, when `PAYKIT_SERVER_API=upstream`; on the fork (`fork`, the default) it is `marketplace.trusted_public_key`. See [Paykit server API](#paykit-server-api) |
| `PAYKIT_SERVER_API` | `fork` | which paykit-server the service speaks to: `fork` (today's production server) or `upstream` (`pubky/paykit-server` with the signed-services allowlist, first shipped in v0.1.0-rc9, #55; the latest tagged release is v0.1.0-rc11). Set only together with `PAYKIT_SERVER_URL`. See [Paykit server API](#paykit-server-api) |
| `PAYKIT_SETUP_STATUS_TIMEOUT_SECONDS` | `8` | how long the signed upstream `POST /setup/status` may take, in whole seconds (1–30). paykit-server answers `ready` for a set-up seller in 2 to 3.5 s, so it does not share the 3 s timeout of the other availability reads. Set only together with `PAYKIT_SERVER_URL` |
| `PAYKIT_MAX_PREPARE_TTL_SECONDS` | `900` | largest remaining upstream Paykit preparation TTL Marketplace accepts (60–86400). Align with producer `prepare_ttl_seconds`; a longer response is voided and its bind/inventory transaction rolls back. Up to 60 additional seconds are accepted only for bounded cross-service clock skew |
| `PAYMENT_CONFIRMATION` | `seller` | who marks an order paid: `seller` (the seller's attestation is the only way Bitcoin, USDT and PayPal money pays an order; Paykit's report and PayPal's notification only inform the seller) or `automatic` (the earlier behaviour, the seller's own Bitcoin option included). Frozen on each order at its payment start. See [Seller-confirmed payment](#seller-confirmed-payment) |
| `USDT_PAYMENTS_ENABLED` | `false` | the USDT feature flag (`true`/`1` or `false`/`0`). On, `/health` reports `usdt_payments.available`, the seller consent and readiness keys exist, and a `paykit` order may be paid in USDT; off, none of that exists and every Bitcoin and PayPal response is unchanged. See [USDT payments](#usdt-payments-flagged-default-off) |
| `PAYKIT_MARKETPLACE_ASSETS` | `BTC` | the assets the deployed paykit-server's Marketplace prepare can carry: comma-separated `BTC` (required) and `USDT`. `USDT` is accepted only with `PAYKIT_SERVER_API=upstream`, and USDT is offered only when `PAYKIT_MARKETPLACE_CONTRACT` is `multi_asset` too. See [The single Paykit bind](#the-single-paykit-bind) |
| `PAYKIT_MARKETPLACE_CONTRACT` | `legacy` | which request body the service sends to paykit-server's Marketplace prepare and activate: `legacy` (rc16 `amount_sats`, Bitcoin only) or `multi_asset` (a denomination and amount, optionally narrowed). `multi_asset` needs `PAYKIT_SERVER_API=upstream` and is independent of USDT: set it as soon as the deployed paykit-server speaks the multi-asset contract (which refuses the legacy body), with USDT still off |
| `PAYKIT_POLL_SECONDS` | `15` | minimum interval between paykit status polls per pending bitcoin order |
| `PAYKIT_RAIL_STALE_SECONDS` | `60` | stale-out window for cached Paykit rail and seller claim availability; must be at least `PAYKIT_POLL_SECONDS` |
| `DELIVERY_ASSUME_DAYS` | `14` | default for the days after shipment when the worker marks a `shipped` order `delivered` on server time (no carrier tracking feed), flagging the projection `delivery_assumed` (≥ 1). The same period counts from payment for a paid manual digital order the seller never marks delivered. Applies only to an order whose seller left `deliveryAssumeDays` unset on their shop record; see [Seller-set periods](#seller-set-periods) |
| `AUTO_COMPLETE_DAYS` | `14` | default for the days after delivery when the worker completes a `delivered` order on server time, unless a return/cancel request is open (≥ 1). Applies only to an order whose seller left `autoCompleteDays` unset on their shop record; see [Seller-set periods](#seller-set-periods) |
| `REVIEW_BLIND_WINDOW_DAYS` | `0` | days after an order becomes reviewable during which its reviews are double-blind (0 to 90; `0` is off and changes nothing). Inside the window each side's review and the reviewer's attestations are withheld from the other until both have reviewed or the window ends, and a seller's review after it is refused. Keep it at `0` until the Shop handles pending reviews; see [Reviews](docs/api.md#double-blind-reviews) |
| `DELIVERY_SWEEP_BATCH_SIZE` | `100` | rows claimed per inner delivery/auto-complete sweep pass (≥ 1); the worker loops until a short batch or the per-lease cap |
| `PUBLIC_APP_ORIGIN` | unset | the web app's public URL: an origin, plus the mount path when the Shop is served under one (e.g. `https://shop.pubky.app`, or `https://pubky.app/shop`); no query, fragment, credentials or `.`/`..` path segments. When set, hosted checkouts that support a return destination (PayPal `_xclick`) send the buyer back to `{value}/marketplace/orders` after commit/cancel |
| `PUBLIC_SERVICE_ORIGIN` | unset | this service's own public origin; when set, PayPal checkout links carry `notify_url={origin}/v0/paypal/ipn` so PayPal's IPN (postback-verified, matched against seller email + exact order total) pays the order under `PAYMENT_CONFIRMATION=automatic` or tells the seller under `seller`, and records the seller's PayPal refunds and the buyer's reversals on it. Unset, PayPal stays participant-attested |
| `PAYPAL_IPN_VERIFY_URL` | `https://ipnpb.paypal.com/cgi-bin/webscr` | PayPal's IPN validation endpoint; tests point it at a local double |
| `PAYPAL_CHECKOUT_URL` | `https://www.paypal.com/cgi-bin/webscr` | PayPal `_xclick` checkout base. HTTPS only; host must be `www.paypal.com` (live) or `www.sandbox.paypal.com`. Default stays live. |
| `REQUIRE_ENCRYPTED_DELIVERY_ADDRESS` | `false` | `true`/`1` refuses a checkout or award checkout that carries a plaintext `delivery_address` (422 `INVALID_COMMAND`, reason `encrypted_delivery_required`); only sealed envelopes are accepted. Leave unset until the Shop version that seals is the deployed one (see [Sealed delivery address](#sealed-delivery-address)) |
| `EU_FENCE_ENABLED` | `false` | `true`/`1` turns on the EU scope fence (legal launch plan C2-C4): `buyer_country` becomes required on the selling commands, a buyer in the EU-27 or GB buys only from sellers whose shop record says `sellerStatus: "private"`, and sellers whose shop country is in the EU-27 stop at 29 paid sales or EUR 2,000 of paid orders a calendar year (checked at checkout and again at payment). Off, `buyer_country` is accepted and stored and nothing is refused on these grounds. See [EU scope fence](#eu-scope-fence) |
| `DIGITAL_REGION_BLOCK_ENABLED` | `false` | `true`/`1` refuses `checkout.create` (which carries drop claims) and `offer.checkout` that contain a digital line (any digital delivery kind, or a Locks listing) when `payload.buyer_country` is in the EU-27 or is `GB`: `DIGITAL_UNAVAILABLE_IN_REGION` (409), reason `eu_buyer` or `gb_buyer` for the declared country, `eu_ip` or `gb_ip` for the request's own country (read from `CLIENT_COUNTRY_HEADER`), `buyer_region_unknown` when the edge sent none; such a command must carry `buyer_country`. Independent of `EU_FENCE_ENABLED`. Off, nothing is refused. See [Digital sales to EU and UK buyers](#digital-sales-to-eu-and-uk-buyers) |
| `REGION_MODES` | unset | per-region commerce modes, `SELECTOR=MODE,...`: `SELECTOR` is an ISO 3166-1 alpha-2 code, `EU` (the 27 member states; later entries override earlier ones) or `UNKNOWN`; `MODE` is `open`, `nuanced` or `blocked`; `none` opens every region. Unset keeps `EU_FENCE_ENABLED` and `DIGITAL_REGION_BLOCK_ENABLED` exactly as they were. Set, it wins and those two are ignored (a startup warning names each one that is also set). A malformed value stops the service at start. Launch value: `EU=blocked,GB=blocked,US=blocked`. See [Region modes](#region-modes) |
| `CLIENT_COUNTRY_HEADER` | `x-client-region` | Name of the request header the trusted edge sets to the client's country (ISO alpha-2) from its IP. Read by the digital hold (`DIGITAL_REGION_BLOCK_ENABLED`) and by every commerce check of `REGION_MODES`. GCP: a load-balancer custom request header `X-Client-Region:{client_region}`. The edge must set it itself and overwrite any value the client sent. Missing or unusable with the digital hold on: digital lines are refused (`buyer_region_unknown`) and an `ALERT digital_region_client_country_missing` is logged |
| `ADDRESS_SEARCH_UPSTREAM_URL` | `https://photon.komoot.io` | Photon geocoder behind `POST /v0/address/suggest`. HTTPS only (http for loopback in tests). Point it at a self-hosted Photon to stop using the public instance |
| `ADDRESS_SEARCH_DISABLED` | unset | `1`/`true` turns the address proxy off; the route answers `address_search_unavailable` |
| `ADDRESS_SEARCH_CLIENT_IP_HEADER` | unset | name of a header set by a trusted edge that carries the client address (last value is used); unset keys the per-client limit on the socket peer |
| `ADDRESS_SEARCH_CLIENT_PER_MINUTE` | `30` | sustained address searches per client per minute (burst 8), 1–10000 |
| `ADDRESS_SEARCH_UPSTREAM_PER_SECOND` | `2` | address searches sent upstream per second across all clients (burst 3 seconds' worth), 1–10000 |
| `LISTING_AVAILABILITY_CLIENT_IP_HEADER` | falls back to `ADDRESS_SEARCH_CLIENT_IP_HEADER` | name of a header set by a trusted edge that carries the client address (last value is used) for the per-client limit on `GET /v0/listings/{seller}/{listing_id}/availability`; with neither set the limit keys on the socket peer |
| `LISTING_AVAILABILITY_CLIENT_PER_MINUTE` | `120` | sustained public availability reads per client per minute (burst 30), 1–10000 |
| `SANDBOX_PAYMENTS_ENABLED` | `false` | accept `payment.sandbox_advance` at all; must stay `false` on any deployment handling real orders |
| `PICKUP_DETAILS_ENCRYPTION_KEY` | unset | 32-byte hex key sealing local-pickup details and pinned payment snapshots at rest (XChaCha20-Poly1305); must differ from the Locks key material; pickup is OFF without it |
| `PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS` | unset | optional previous pickup key for the dual-key read window during rotation; the re-seal worker migrates both sealed families to the current key |
| `DIGITAL_DELIVERY_ENCRYPTION_KEY` | unset | 32-byte hex key sealing digital delivery at rest (XChaCha20-Poly1305): the seller's deliverable versions (file key, link or text), each order's pinned copy and the buyer's delivery email. It is the only switch: unset, digital delivery is OFF, `/health` reports `digital_delivery_available: false` and checkout refuses digital lines (`digital_delivery_unavailable`). Must differ from `LOCKS_BUNDLE_ENCRYPTION_KEY`, `LOCKS_LOOKUP_HMAC_KEY`, the pickup keys and `PRIV_DATA_KEY_ENCRYPTION_KEY` (checked at boot). The service refuses to boot while sealed digital rows exist and no configured key opens them, so keep a backup outside the host |
| `DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS` | unset | optional previous digital delivery key for the dual-key read window during rotation; the re-seal worker moves all three sealed families to the current key. Keep it set until a sweep finds zero rows under it |
| `DIGITAL_DELIVERY_MAX_BYTES` | `52428800` | largest digital file, in plaintext bytes (≥ 1); exposed as `/health.digital_delivery_max_bytes` so the Shop's picker enforces the same cap |
| `BUYER_EMAIL_RETENTION_DAYS` | `30` | days the sealed delivery email of a paid "I'll email it" order is kept after the order ends (completed, cancelled, refunded or closed), before the purge worker deletes it (≥ 1) |
| `BUYER_EMAIL_UNPAID_RETENTION_DAYS` | `7` | days the sealed delivery email of a cancelled or expired unpaid checkout is kept before the purge deletes it (≥ 1) |
| `PRIV_DATA_KEY_ENCRYPTION_KEY` | unset | 32-byte hex key sealing per-user `/priv` data keys at rest (XChaCha20-Poly1305); must differ from the Locks, pickup and digital delivery keys; `GET /v1/me/priv-keys` answers `priv_keys_unavailable` without it, and the service refuses to boot while sealed rows exist without it |
| `PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS` | unset | optional previous priv data key sealing key for the dual-key read window during rotation; the re-seal worker moves every row to the current key. Keep it set until the pass reports zero rows under the previous key |
| `CONNECTORS_ENABLED` | `false` | Turns on the store connectors (Shopify, WooCommerce): the `/v1/connectors` routes, the connector status and the `connector-worker` binary (same image, its own command). Off, every connector route answers `connectors_disabled` and nothing else changes. Needs `CONNECTOR_PUBLIC_BASE_URL` and `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY`. Optional `CONNECTOR_RETURN_URL` (the Shop's connection page; WooCommerce sends the merchant back to it after approving, and links to its origin from the order note). WooCommerce needs no further names, but `CONNECTOR_PUBLIC_BASE_URL` must be an HTTPS origin the store can reach, because WooCommerce posts its keys and webhooks there. Connection URLs handed to merchants are built from `CONNECTOR_PUBLIC_BASE_URL`. The Shopify provider is built in (GraphQL Admin API 2026-10) and needs no Shopify environment values; the merchant sets the three privacy webhook topics in their app's own configuration (see "Shopify" in `docs/api.md`). Worker tuning: `CONNECTOR_PUSH_LEASE_SECONDS`, `CONNECTOR_PROVIDER_TIMEOUT_SECONDS`, `CONNECTOR_MAX_ATTEMPTS`, `CONNECTOR_BACKOFF_BASE_SECONDS`, `CONNECTOR_BACKOFF_MAX_SECONDS`, `CONNECTOR_STOCK_POLL_SECONDS`, `CONNECTOR_WORKER_TICK_MILLIS` |
| `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY` | unset | 32-byte hex key sealing store credentials, a merchant's own app credentials and connector recipient keys at rest (XChaCha20-Poly1305); must differ from every other sealing key (checked at boot); the service and the worker refuse to boot while sealed connector values exist and no configured key opens them |
| `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS` | unset | optional previous connector key for the dual-key read window during rotation; the worker re-seals every value to the current key |
| `SHOPIFY_PUBLIC_APP_CLIENT_ID`, `SHOPIFY_PUBLIC_APP_CLIENT_SECRET` | unset | reserved for Pubky's own public Shopify app, the only Shopify values that will ever live in the environment; not read yet. A merchant's own custom app brings its client id and secret through `PUT /v1/connectors/{id}/credentials`, sealed per connection, never in the environment |
| `PICKUP_DISPUTE_RETENTION_DAYS` | `30` | days a cancelled-after-payment order's pinned pickup snapshot is retained as the dispute exhibit when no refund evidence ever lands, before the ordinary terminal-order purge takes it (≥ 1) |

### Listing moderation

pubky.app's moderation (a moderator account's tags on content and users)
removes content from the marketplace index. The index reports a listing's
state at `GET {NEXUS_URL}/v0/listing/{seller}/{listing_id}/moderation`
(`moderated` is true when the listing itself, or its seller, is moderated).
Before the service accepts `checkout.create`, `offer.checkout`,
`offer.create`, `offer.counter`, `offer.accept` or `auction.place_bid` it
asks, and refuses a moderated listing with `LISTING_MODERATED` (HTTP 409, audited as
`invalid_state`) without writing anything. An order created before its listing
was moderated is gated again where its payment starts: the buyer's
`POST /v0/orders/{id}/payment-method` (any method) and the first
`payment.prepare_locks` on its payment answer `LISTING_MODERATED` (the bind
with `reason: "listing_moderated"`) before an attempt number is spent, a hold
is armed or Paykit is called, and the order stays `pending_payment` until its
hold lapses. A re-bind of the method already bound, a Locks payment already
pinned to Locks, `payment.register_locks`, and any request from someone other
than the order's buyer never ask the index, so money already in flight is not
stranded and the moderation state is not disclosed to a stranger. Withdrawing or rejecting an offer,
and closing an auction, sell nothing and stay open. A command id that already
has a stored result is answered from it without asking the index. The check
runs before the command's transaction, so a slow index never holds a domain
connection.

When the index cannot answer (transport failure, timeout, a non-success
status such as the 404 of an index that predates the route, or a body without
`moderated`), the service **fails open** and logs at ERROR
`ALERT the index moderation lookup is unavailable; allowing the command
without a moderation check`. The index is not the transaction authority
(ADR 0019), so an outage there does not stop sales; the exception is a
listing the gate has seen moderated: it stays refused until the index answers
that it is clear (`ALERT the index could not confirm a moderated listing is
still moderated; refusing it on the last moderated answer`). Only "moderated"
answers are cached (5 minutes, per listing); a "clear" answer is never cached,
so a moderation placed meanwhile is seen on the next command and lifting one
takes effect once the cached answer expires. Orders created before a listing
was moderated are not cancelled by this check.

### Region modes

`REGION_MODES` says what each region may do on Pubky Market, so each legal
outcome is a configuration change and not a rebuild. It is read once at start.

```
REGION_MODES=EU=blocked,GB=blocked,US=blocked   # launch
REGION_MODES=EU=nuanced,GB=nuanced,US=blocked   # counsel accepts the nuanced design for the EU and UK
REGION_MODES=EU=blocked,GB=blocked              # the US is unlocked
REGION_MODES=EU=nuanced,GB=nuanced              # both: the behaviour before this setting
REGION_MODES=none                               # emergency open: every region open
```

- **Grammar.** Comma-separated `SELECTOR=MODE`, case-insensitive, spaces around
  entries ignored. `SELECTOR` is an ISO 3166-1 alpha-2 code (stored uppercase),
  `EU` (AT BE BG CY CZ DE DK EE ES FI FR GR HR HU IE IT LT LU LV MT NL PL PT
  RO SE SI SK; the United Kingdom is `GB`, `UK` is refused) or `UNKNOWN`. Later
  entries override earlier ones, so `EU=nuanced,DE=blocked` blocks only
  Germany. A country no entry names is `open`. `none` must be the whole value.
  An empty value, an entry that is not `SELECTOR=MODE`, an unknown mode or an
  unknown selector stops the service at start with an error naming the entry.
- **Modes.** `open`: no commerce restriction. `nuanced`: today's behaviour, the
  [EU scope fence](#eu-scope-fence) (C3: a buyer there buys only from a seller
  who declared private; C4: the annual limit for sellers resident in the EU-27,
  applied when that seller's country is nuanced, and the re-confirmation point
  for buyers in a nuanced EU country) and the [digital hold](#digital-sales-to-eu-and-uk-buyers)
  (C3b), each for the countries marked `nuanced`, whatever the two legacy flags
  say. `blocked`: no buying, no starting a payment, no selling and no paid
  unlocks for people in the region.
- **`UNKNOWN`** is the mode of a request whose IP country cannot be determined
  (header absent, malformed, `XX`, `T1`). Omitted, it is the strictest mode any
  country has: with one region blocked an unknown origin is blocked for
  commerce, and with only nuanced regions it is held for digital items only, as
  before. `UNKNOWN=open` lets an unlocated buyer through.
- **Back-compat.** With `REGION_MODES` unset, `EU_FENCE_ENABLED` and
  `DIGITAL_REGION_BLOCK_ENABLED` behave exactly as before: each turns on its
  own gate for the EU-27 and GB as nuanced regions, and nothing is ever
  blocked. Setting `REGION_MODES` replaces both: nuanced regions run C3, C4 and
  C3b together, and the service logs a warning for each legacy flag still set.
- **Flip order.** The Shop reads the same grammar (`PUBKY_RUNTIME_REGION_MODES`).
  To tighten, change the Shop first and then this service; to loosen, change
  this service first and then the Shop, so the Shop never offers what the
  service still refuses and never hides what the service accepts. The Lock
  Server's `POST /proof-bundles` list (451 for `blocked` and `nuanced`
  regions) is edge configuration and moves with them.

What a blocked region cannot do, decided from three facts: the **declared
country** (the buyer's `buyer_country`, or the seller's shop country from their
own shop record), the **IP country** (the header named by
`CLIENT_COUNTRY_HEADER`, set and overwritten by the trusted edge) and the
**unknown origin**. A buyer command is refused when the declared country OR the
IP country is blocked, or when the IP country is unknown and `UNKNOWN` resolves
to `blocked`; declaring Switzerland from a US IP, or sending no header, does
not get around it. Only commands that start new commerce are checked, before
the command's transaction and after a stored replay is answered:

| Entry point | Refused with | Reasons |
|---|---|---|
| `checkout.create`, `offer.create`, `offer.checkout`, `auction.place_bid` | `COMMERCE_UNAVAILABLE_IN_REGION` (409) | `buyer_region_blocked` (declared country), `buyer_ip_blocked`, `buyer_region_unknown`; `buyer_country` is optional (the IP country decides), and required only while a region is `nuanced` |
| the same four, on a listing whose seller's shop country is blocked | `SELLER_UNAVAILABLE_IN_REGION` (409) | `seller_region_blocked` (the shop record is read, fail closed) |
| `payment.prepare_locks`, `POST /v0/orders/{id}/payment-method` (the first bind) | as above | the order's declared country, the request's IP and the seller's shop country |
| `listing.register`, `listing.sync`, `drop.sync`, `PUT /v0/sellers/me/payment-config` | `SELLER_UNAVAILABLE_IN_REGION` (409) | `seller_region_blocked` (shop country), `seller_ip_blocked` (the seller's own request); an unknown origin is `COMMERCE_UNAVAILABLE_IN_REGION` `buyer_region_unknown` |

`listing.sync` is refused only when it would publish or update a live listing;
a sync of a record that is gone, paused, ended or removed is never refused, so a
takedown always goes through. `POST /v1/listings/sync-many` has no browser IP,
so only the seller's shop country applies to it.

Never checked, so that nobody is trapped with money in flight: PayPal IPN and
other webhooks, Paykit callbacks, payment confirmation (`payment.sandbox_advance`,
`payment.register_locks`, the Bitcoin confirmation and resolve routes, fiat
mark-paid and confirm-received), refunds, cancellation, returns, fulfillment,
shipping and delivery confirmation, reviews, reading orders, receipts and
queries, listing takedowns, the connectors' inbound webhooks and health. A
re-bind of the payment method an order already carries is a read, not a start.

The refusal messages are the Shop's copy: "Buying and selling aren't available
in your region yet." (`buyer_region_blocked`, `buyer_ip_blocked`), "We can't
tell where you are, so buying and selling are paused." (`buyer_region_unknown`),
"Selling isn't available in your region yet." (the seller's own commands) and
"This seller can't sell on Pubky Market right now." (a buyer command on a
blocked seller). With no blocked region in force nothing here reads the
database, a shop record or the header.

### EU scope fence

Legal launch plan C1-C4, behind `EU_FENCE_ENABLED` (default off), or for the
nuanced regions of [`REGION_MODES`](#region-modes). The seller
declares who they are in their own shop record (`sellerStatus`: `private` or
`trader`; absent or anything else is undeclared and counts as a business) and
their shop country (`location.countryCode`); the buyer's client sends the
buyer's country in clear text as `payload.buyer_country` on `checkout.create`,
`offer.create`, `offer.checkout` and `auction.place_bid`. The service stores it
(`orders`, `offers` and `bids` have `buyer_country`; an auction's order takes
the winning bidder's latest country) and freezes the seller's declared status
and country on the order with the delivery periods (`orders.seller_status`,
`orders.seller_country`). Those two things happen with the flag off too.

With the flag on:

- **Regional gate.** A buyer in the EU-27 or GB buys only from a seller who
  declared `private`: `SELLER_UNAVAILABLE_IN_REGION` (409) with `reason`
  `business_seller` or `undeclared_seller`. Every other buyer country is
  unaffected. GB is in the gate because UK consumers buying from a business
  have a 14-day withdrawal right under the Consumer Contracts Regulations
  2013. The annual limit and the re-confirmation point below stay EU-only
  (DAC7), so a GB buyer never counts toward them.
- **Annual limit.** A seller whose shop country is in the EU-27 stops at 29
  sales or EUR 2,000 in total per calendar year (UTC), whichever comes first
  (29 sales of any size, or the euro total, whichever is reached first).
  **Only paid orders count**: a confirmed payment, even if the order was later
  cancelled or refunded. An open order (`pending_payment`) never uses a slot,
  so unpaid checkouts cannot block a seller. Past the limit, a new order is
  refused with `SELLER_ANNUAL_LIMIT_REACHED` (409). Order totals convert to
  euro with the persisted reference rates (`fx_rate_samples` medians), per
  currency, rounding up; with no reference the order is refused as
  `UPSTREAM_UNAVAILABLE` rather than waved through. Counting happens under a
  per-seller advisory lock taken before any listing lock
  ([lock order](docs/lock-order.md)). An offer or a bid on a seller already at
  the limit is refused up front; an auction's winning order is not refused when
  it is created, since the bidder already won.
- **The limit is checked again where money is taken.** Because open orders
  reserve nothing, many can be open when the last slot goes. Payment start
  (the Bitcoin or fiat bind, Locks preparation, the sandbox) refuses with
  `SELLER_ANNUAL_LIMIT_REACHED` when the seller is at the limit or the payment
  would cross it, so the buyer is not asked to pay. A payment already in flight
  when the last slot goes cannot be refused, because the marketplace never
  holds funds: at confirmation the payment goes to `manual_review`, the order
  is not marked paid and no receipt is issued, and the seller can only record
  it as `refunded` (resolving it as `paid` answers `refund_required`). The check counts the seller's other paid orders
  under the same per-seller lock, so two payments confirming at once cannot
  both take the last slot, and uses the seller's country frozen on the order at
  payment start, so a payment never waits on the homeserver. An `ALERT
  eu_seller_limit_at_payment` is logged each time. The same check covers a
  late settlement (refund required) and the seller's own Bitcoin confirmation.
- **No country, no cap.** A seller whose shop record has no usable
  `location.countryCode` (missing, empty, or not two letters) has no residence
  and no annual limit: the fence never reads a missing or unknown country as
  EU. The regional gate and the re-confirmation point still apply to them.
- **Re-confirmation.** A private seller of any country who reaches the same
  numbers in sales to EU buyers is refused further EU sales
  (`SELLER_UNAVAILABLE_IN_REGION`, `reason`
  `private_seller_reconfirmation_required`) until their shop record carries a
  `sellerStatusConfirmedAt` (RFC 3339, not in the future) later than those
  sales; the count restarts at the confirmation. Paid sales count here too.
  The point is checked when an order is created and again where money is
  taken, like the annual limit: the order freezes the seller's
  `sellerStatusConfirmedAt` at its payment start
  (`orders.seller_status_confirmed_at`, migration 0080), and
  `region_fence::recheck_at_payment` counts the private seller's paid sales to
  EU buyers from that date under the same per-seller lock. Past the point,
  payment start answers `SELLER_UNAVAILABLE_IN_REGION` with `reason`
  `private_seller_reconfirmation_required` and the buyer never pays; money
  already on chain goes to `manual_review` exactly as for the annual limit
  (not marked paid, no receipt, refund only). A seller who re-confirms after
  an order's payment started does not release that payment; the next order's
  payment starts from the new date.
- A seller's shop record is read before the command's transaction, like the
  lifecycle gate, and fails closed: an unreadable record is
  `UPSTREAM_UNAVAILABLE` (`HOMESERVER_URL` is already required to boot).

The Shop reads `GET /v1/sellers/{seller_pubky}/annual-limit` (see
`docs/api.md`) for the seller's counters.

### Digital sales to EU and UK buyers

Legal launch plan C3b, behind `DIGITAL_REGION_BLOCK_ENABLED` (default off,
independent of `EU_FENCE_ENABLED`), or for the nuanced regions of
[`REGION_MODES`](#region-modes). With `REGION_MODES` a nuanced country outside
the EU-27 and GB is held too, with `reason` `region_buyer` (declared) or
`region_ip` and the message "Digital items aren't available to buyers in your
region yet."; an unknown IP country is held unless `UNKNOWN` is `open`. EU VAT presumes a platform that authorises
the charge or the delivery of an electronic service to be its supplier
(IR 282/2011 Art. 9a), and the UK has the same platform rule. The service
releases file, link and text items and Locks items itself, so with the flag on
a `checkout.create` (a drop claim is one) or an `offer.checkout` that contains a
digital line is refused with `DIGITAL_UNAVAILABLE_IN_REGION` (409, audited as
`invalid_state`) when `payload.buyer_country` is in the EU-27 or is `GB`.
`reason` is `eu_buyer` or `gb_buyer`.

The declared country is not trusted on its own, since a buyer in the EU can
declare Switzerland. The request's own country is read too, from the header named
by `CLIENT_COUNTRY_HEADER` (default `x-client-region`) that the trusted edge sets
from the client's IP, and a digital line is refused when EITHER country is in the
EU-27 or is GB: `reason` is `eu_buyer`/`gb_buyer` for the declared country (checked
first) and `eu_ip`/`gb_ip` for the IP country. With the flag on and no usable IP
country (header absent, malformed, or the unknown markers `XX` and `T1`), digital
lines fail closed with `reason: buyer_region_unknown` and an
`ALERT digital_region_client_country_missing` is logged; physical lines never
need the header. Commands that do not arrive over HTTP from a buyer (automation,
connectors) carry no edge country and are refused for digital lines the same way.
Deploy note: the load balancer must set the header on every request
and overwrite any client-sent value.

- A line is digital when its listing is a Locks listing, when the buyer chose
  the `digital` fulfillment for it, or when `digital` is the only method the
  listing publishes. A listing that publishes digital next to shipping or
  pickup is physical when the buyer chose one of those. Email and message
  items count: Synonym's automatic payment confirmation may authorise the
  charge even when the seller delivers by hand.
- A command with a digital line must carry `buyer_country`; one without does
  not need it on this flag's account.
- Offers on digital items are already refused
  (`offers_unavailable_for_digital`), and auctions ship only.
- Buyers from every other country, and physical lines, are unaffected.

### Seller-confirmed payment

`PAYMENT_CONFIRMATION` decides who turns observed money into a paid order. It
is frozen on the order when its payment starts (`orders.payment_confirmation`,
migration 0091), so changing it applies to new orders only, and `/health`
reports it as `payment_confirmation`. An order whose payment started before
0091 reads as `automatic`.

**`seller` (the default).** The service still presents the payment request
and watches for the money, but what it sees is a hint for the seller. Only the
seller's attestation pays the order:

- **Bitcoin (Paykit, upstream):** every order takes the seller-confirmation
  flow below, whatever the shop record says.
- **USDT (Paykit, upstream):** an included, on-time transfer of the offered
  amount enters the same `awaiting_seller_confirmation` window instead of
  paying. The paid rail and the transfer's finality are recorded at entry and
  finality keeps being polled while the seller decides; a transfer that is no
  longer reported is marked `disappeared` and its finality `reverted`. The
  seller confirms with the same `POST /v0/orders/{id}/confirm-bitcoin-payment`
  (`usdt_payment_seen` tells them it arrived). Physical fulfilment still waits
  for a final transfer after the order is paid.
- **PayPal:** a postback-verified `Completed` notification that matches the
  seller's email, the currency and the amount still stores the PayPal payment
  id and receiver snapshot, so refund and reversal notifications keep
  matching. It does not pay: it stamps `gateway_payment_seen_at`, records the
  PayPal transaction id as the order's reference, extends the hold to the
  24-hour seller window, and notifies the seller (`paypal_payment_seen`). The
  seller pays the order with `POST /v0/orders/{id}/fiat/confirm-received`
  (`fiat_verification: "seller-attested"`). After the window the ordinary
  expiry cancels the order; a later seller confirmation takes the late-money
  path. A confirmation is refused with `payment_taken_back` while PayPal
  reports the money refunded or reversed.
- The buyer cannot cancel once money is seen (a Paykit sighting or a PayPal
  notification), and `next_actor` is the seller.
- Late money, short money and money on both rails keep their review routes. An
  order on the retired fork API keeps its Paykit creator's own mode. Locks
  orders are paid by the Lock Server's lifecycle as before.

**`automatic`.** The earlier behaviour: a matching Paykit report or a
verified PayPal notification pays the order. A seller whose shop record says
`bitcoinConfirmation: "seller"` still confirms their own Bitcoin payments
(legal launch plan B4, `orders.seller_confirms_bitcoin`, frozen at the payment
start; NULL when the record was unreadable, read as not opted in).

**The seller-confirmation window.** Money that matches the order's amount
enters `awaiting_seller_confirmation`, the hold (and an auction winner's
reservation) is extended 24 hours, and Paykit's report only informs the seller
(`bitcoin_payment_seen`). The seller's confirmation is the only way the order
is paid. After 24 hours without it the payment goes to manual review.

- **Underpaid money is not shown to the seller as matching.** A `confirmed`
  report whose amount does not match goes to manual review with the
  amount-mismatch reason, exactly as for any other seller, and an unmatched
  `detected` sighting stays a display-only detection. The amount flag a report
  carries is stored with the observation, and a confirmation of an observation
  that says the amount did not match is refused
  (`confirmation_observation_mismatch`).
- **Locks orders are not held.** A Locks order is paid when the Lock Server
  reports the lifecycle completed; the Lock Server issues the unlock itself, so
  the service cannot hold something it does not release
  (`a_sellers_bitcoin_confirmation_option_does_not_hold_a_locks_order`).
- A late settlement still takes the late path whatever the mode.

### Listing lifecycle

A seller pauses, ends or removes a listing by rewriting `state` in its record
at `/pub/pubky.app/marketplace/v1/listings/{listing_id}` on their homeserver;
the listing aggregate tracks stock only. Before the service accepts a command
that sells a listing it reads the record of every listing involved, from
`HOMESERVER_URL`, and refuses with `LISTING_NOT_ACTIVE` (HTTP 409, audited as
`invalid_state`) and `reason` `listing_paused`, `listing_ended`,
`listing_removed` or `listing_not_active`. A record the homeserver answers
404 for is `listing_removed`; a record with no `state` field predates the
field and counts as `active`. A listing the service has already tombstoned is
not read and keeps its `NOT_FOUND` refusal.

- A new commitment (`checkout.create`, `offer.create`, `offer.counter`,
  `offer.accept`, `auction.place_bid`) is refused unless every record is
  `active`.
- A continuation of a commitment made while the listing was on sale
  (`offer.checkout`, the first `payment.prepare_locks` of a payment,
  `auction.close`, and `POST /v0/orders/{id}/payment-method`, which answers
  with the same code and `reason`) is refused only when a record is removed:
  the seller cannot fulfil it, so no payment may start. A pause or an end
  passes, so a seller who pauses to stop new buyers does not strand an
  accepted offer or an order awaiting payment.
- The auction close worker closes a removed listing's due auction unsold, as
  it does a deleted one. When the homeserver cannot answer, it closes on
  stored state: closing takes no payment, and the winner's payment start
  refuses a removal on its own.

Withdrawing or rejecting an offer and the rest of the payment flow are not
checked. Each `(seller, listing)` is read once per command, a checkout's
records concurrently (at most 8 at a time).

The record is the seller's authority over whether the listing is for sale, so
the check **fails closed**: when the homeserver cannot answer (transport
failure, a non-success status other than 404, a non-JSON body, or no answer
to all of the command's reads within 12 s) the command is refused with the
retriable `UPSTREAM_UNAVAILABLE` (HTTP 503) and nothing is written. Like the
moderation check it runs before the command's transaction, so a slow
homeserver never holds a domain connection, and a command id with a stored
result is answered from it without a read.

Before deploying this check, sweep for sellable listings whose record is
missing (they would start refusing as `listing_removed`) with
[`scripts/listing-record-sweep.sh`](scripts/listing-record-sweep.sh).

### Public payment configuration availability

`GET /v0/sellers/{pubky}/payment-config` always returns HTTP 200. Its
`bitcoin_available` field means the seller enabled Bitcoin and has a claimed
Paykit account; `bitcoin_offer_available` is the cached rail-wide Paykit
creation gate. Both must be true before a client offers Bitcoin. The service
refreshes both values every `PAYKIT_POLL_SECONDS` and serves the last known
value during an upstream failure until `PAYKIT_RAIL_STALE_SECONDS`; after that
it serves `false` rather than returning 503.
A seller's new Paykit claim becomes visible to buyers within
`PAYKIT_POLL_SECONDS`.

The endpoint is unauthenticated, so it returns booleans only:
`paypal_available` (a merchant email is stored). It never returns the seller's
PayPal email or any other payout or contact identifier.
The PayPal email reaches a buyer only inside the `fiat_checkout_url` of an
order they bound to PayPal (`POST /v0/orders/{id}/payment-method`), which only
that order's buyer and seller can read. The seller reads their own stored
values from the authenticated `GET /v0/sellers/me/payment-config`.

### Payment-method seller set

`GET /v0/payment-methods/{method}/sellers` is public and feeds the
marketplace's payment-method filter. It answers with seller keys only:
`{ "method": "paypal", "sellers": ["<pubky>", …], "next_after": "<pubky>" | null }`.

- **Methods:** `bitcoin` and `paypal` always. `usdt` only while
  `USDT_PAYMENTS_ENABLED` is on. Anything else answers
  `invalid_payment_method`.
- **Source:** the stored configuration only, never Paykit:
  - Bitcoin is `bitcoin_enabled` on a deployment with a Paykit client.
  - PayPal is a stored merchant email.
  - USDT is the seller's opt-in on a deployment whose prepare can offer USDT.
  - A degraded Paykit therefore drops no seller. Checkout still verifies the
    rail live.
- **Scope:** only sellers with a live registered listing (not deleted, not
  sold) are listed. Drop editions are registered listings too.
- **Paging:** ordered by key, with `after` (the previous page's last key) and
  `limit` (1 to 500, default 500).

The seller's own view (`GET` and `PUT /v0/sellers/me/payment-config`) also
carries `bitcoin_setup`: `ready`, `setup_required` or `unavailable`, asked of
paykit-server on every call (the same question as `bitcoin_available`, not
read from the availability cache, and answered whether or not Bitcoin is
switched on). The Shop uses it to confirm a Bitkit setup without a Lock Server
session. `unavailable` covers a Paykit outage, an answer outside the contract,
and a deployment without Paykit; it is never a reason to start an
authorization flow. When the ask fails but paykit-server answered `ready` for
this seller within the last 60 seconds, the answer is `ready`, so one slow or
failed call does not tell a ready seller to set up Bitkit. Only `ready` is
remembered: `setup_required` is always passed through and forgets it, and
`unavailable` is never remembered. Every failed readiness ask is logged at
warn as `paykit setup status unavailable` (upstream) or
`paykit account lookup unavailable` (fork), with a `reason` (`timeout`,
`transport`, `signing`, `non_2xx`, `body_not_json`, `out_of_contract`,
`paykit_unavailable`; the fork's `bad_body`), the seller's 8-character prefix
and the elapsed time. A seller who never saved anything reads the empty
configuration (epoch `updated_at`) instead of `null`.

On the upstream Paykit API (`PAYKIT_SERVER_API=upstream`) the same view also
carries `bitcoin_setup_action`: `setup`, `reconnect` or `null`, the Bitkit flow
for a seller whose `bitcoin_setup` is `setup_required` (`null` otherwise). The
key is absent on the fork, which has no reconnect flow. paykit-server's
`setup_required` means the authority is absent or invalid, so it cannot tell a
seller it has never seen from one it holds whose reconnect stopped after the
credentials were stored, and its initial `/setup` refuses a held seller (it
never deletes a Creator). The service therefore records the first time it read
a seller as `ready` (`paykit_known_sellers`, migration 0078): a recorded seller
is told `reconnect`, any other `setup`. `usdt_setup_action` follows the same
rule when both USDT answers are `setup_required`.

Stripe is not a payment method: the service has no Stripe client, binding
`stripe` answers `invalid_method`, and `PUT /v0/sellers/me/payment-config`
answers `stripe_removed` to a non-empty `stripe_payment_link` or
`stripe_restricted_key`. The seller's own view no longer carries
`stripe_payment_link` or `stripe_restricted_key_set`. The columns
`seller_payment_configs.stripe_payment_link` and
`stripe_restricted_key_ciphertext` are unused and always `NULL`: migration
0058 clears every stored value, and nothing writes them after that. Orders
bound to Stripe before the removal keep `payment_method = 'stripe'` and read
`fiat_verification: 'processor'`, and the `payments.adapter` and
`orders.payment_method` check constraints still admit `stripe` for them.

During deployment ordering, an older paykit-server response without
`bitcoin_offer_available` is compatible: `electrum: "ready"` or
`electrum: { "state": "ready" }` maps to `true`, and any other shape maps to
`false`. When present, the field is authoritative.

### Paykit server API

`PAYKIT_SERVER_API` is the one switch between the fork and upstream
paykit-server. It decides what the `x-paykit-signature` header signs and how
seller readiness is read; everything else about the client is unchanged.

| | `fork` (default) | `upstream` |
| --- | --- | --- |
| Signature covers | the canonical JSON body | `"paykit-http-signature-v1\0" + METHOD + "\0" + path + "\0" + raw body` (the path is the query-free request path, base-URL prefix included) |
| Trusted key | `marketplace.trusted_public_key` | an entry of `[signed_services] trusted_public_keys` (non-empty allowlist, beside the Locks key) |
| Seller readiness | public `GET /v0/accounts/{creator}` (`claimed`) | signed `POST /setup/status` with `{"asset":"BTC","creator":"pubky..."}` |

Upstream readiness answers one of three states. Only exactly `ready` makes
`bitcoin_available` true. `setup_required` means the seller has to set up
Paykit: the public projection reports `bitcoin_available: false` for it, and
the service never starts an authorization flow itself.
`unavailable`, any non-2xx answer, a transport failure and any body outside
the closed `{"status": ...}` contract are outages: the availability cache
retries every `PAYKIT_POLL_SECONDS` and serves the last known value until
`PAYKIT_RAIL_STALE_SECONDS`, then `false`. The public `bitcoin_available` and
`bitcoin_offer_available` fields keep their meaning. The rail-wide
`bitcoin_offer_available` gate is the public, unsigned `GET /health/ready` in
both modes (upstream answers `status`, `postgres`, `electrum`,
`paykit_delivery`, `outbox`; only `status: ready` with `electrum: ready` is
true, and a 503 is an outage).

#### Cutover runbook

This build supports both payment-request lifecycle contracts. `fork` uses the
original prepare, activate, void, resolve, and status routes; `upstream` uses
the signed Marketplace lifecycle routes with producer-issued invoice identity
and deadlines. PayPal is not affected by the setting.

The flip is safe only when no fork request still needs this service to
observe it. A request is pinned to the stack that issued it
(`paykit_stack_id`, `paykit_stack_endpoint`); activation, voiding and the
seller's resolution dial that pinned endpoint whatever the configuration, but
status polling asks the configured server only, so a fork request polled
under `upstream` is not observed and its payment is not seen.

1. **Register the key.** Add the service's public key (the public half of
   `PAYKIT_REQUEST_SIGNING_KEY`) to upstream's
   `[signed_services] trusted_public_keys`.
2. **Stop new fork requests.** Bitcoin is offered only while
   `bitcoin_available` and `bitcoin_offer_available` are both true, and
   `bitcoin_offer_available` follows the fork's `GET /health/ready`. Taking
   that gate down on the fork withdraws Bitcoin from the Shop within
   `PAYKIT_POLL_SECONDS`. It does not stop a client calling
   `POST /v0/orders/{id}/payment-method` directly.
3. **Drain.** Run the gate query below on the production database until it
   returns `0 | 0`. It counts the fork requests that are preparing, active
   (`pending`), `detected` or `awaiting_seller_confirmation`, those whose
   payment expired but are still inside the 24 h late window or have a
   detection, and the released attempts still `watching`. Each clears by
   itself: a payment confirms, or its window and the 24 h late window pass
   (a released attempt closes at the first check 24 h after its expiry; a
   detection keeps it watched for up to 7 days, then it moves to
   `needs_review` with an `ALERT` and no longer blocks the gate).

   <!-- paykit-cutover-gate-query -->
   ```sql
   SELECT
     (SELECT count(*) FROM orders o JOIN payments p ON p.order_id = o.id
       WHERE p.adapter = 'paykit'
         AND o.paykit_request_state IN
             ('preparing', 'pending', 'detected', 'awaiting_seller_confirmation')
         AND (p.state = 'awaiting_entitlement'
              OR (p.state = 'expired'
                  AND (o.paykit_request_state = 'detected'
                       OR o.paykit_expires_at + interval '24 hours' > now())))
     ) AS orders_in_flight,
     (SELECT count(*) FROM paykit_superseded_attempts
       WHERE state = 'watching') AS released_attempts_watching;
   ```

4. **Flip.** In one deployment set `PAYKIT_SERVER_API=upstream` and repoint
   `PAYKIT_SERVER_URL`, on staging first.
5. **Check.** The log carries no line with code
   `paykit_fork_row_polled_under_upstream`. The service writes one
   `ALERT fork-issued paykit request polled under PAYKIT_SERVER_API=upstream`
   line per request per poll (every `PAYKIT_POLL_SECONDS`), naming the
   request's reference and seller. Any such line means a fork request was
   still in flight: roll back, then drain again.

**Rollback.** Set `PAYKIT_SERVER_API=fork` (or unset it) and restore
`PAYKIT_SERVER_URL`. A status poll that cannot be read fails closed and
changes nothing, so fork requests polled under `upstream` are untouched and
resume on the next poll. The same rule holds in reverse: a request issued by
one server is observed only while the service is configured for that server,
so roll back before requests issued by the upstream server exist, or drain
them first.

**Retiring the fork server** is a separate step from the flip. The seller's
resolution of a payment in manual review is delivered to the pinned fork
endpoint, so keep the fork server running until this query returns `0 | 0`
(the §C.16 conditions 7 and 6: no order awaiting seller confirmation or in
manual review pinned to a stack, and every `paykit.resolve` row delivered or
acknowledged):

<!-- paykit-cutover-decommission-query -->
```sql
SELECT
  (SELECT count(*) FROM orders o JOIN payments p ON p.order_id = o.id
    WHERE o.paykit_stack_id IS NOT NULL
      AND (o.paykit_request_state = 'awaiting_seller_confirmation'
           OR p.state = 'manual_review')) AS orders_pinned_awaiting_resolution,
  (SELECT count(*) FROM paykit_resolve_outbox
    WHERE delivery_state = 'queued'
       OR (delivery_state = 'terminal_unresolved'
           AND acknowledged_at IS NULL)) AS resolutions_unfinished;
```

### USDT payments (flagged, default off)

The service can carry USDT (USDT0 on Arbitrum One) as a payment method beside
Bitcoin and PayPal. It is built behind `USDT_PAYMENTS_ENABLED`, default off.
USDT is paid through the one `paykit` method (see
[The single Paykit bind](#the-single-paykit-bind)); the separate `usdt` method
stays refused.

- **Model.** `orders.payment_method` stays the buyer-facing label and gains
  `usdt`. What the buyer sends and where it settles live in
  `orders.payment_asset`, `payment_network`, `payment_amount_minor`,
  `payment_exponent` and `payment_quote_basis` (migration 0053), all NULL
  unless the order is paid in USDT. They are distinct from
  `orders.paykit_asset` (migration 0052; `BTC` on an upstream bind via
  `PaymentAsset`, NULL on the fork), which is what a Paykit request is
  *denominated* in.
  The order's `currency` and `total_minor` stay the price of record.
  Canonical option ids are `{method}.{asset}.{network}`:
  `paykit.btc.bitcoin`, `paykit.usdt.arbitrum-one` and `paypal.fiat`
  (`payment_attempt::PaymentOption`). Stripe has no option: the rail is
  retired, and a historical `stripe` order keeps its label.
- **Quote.** USDT is offered only on `USD/2` orders, at exact parity:
  `usdt_millionths = total_cents x 10_000`, no rate source and no rounding
  (`PaymentTerms::usdt_at_parity`). Listings are never priced in USDT.
- **Projection.** An order that carries terms adds `payment_asset`,
  `payment_network`, `payment_amount_minor`, `payment_exponent` and
  `payment_quote_basis` to its order projection (participants, list and
  single). Every other order's projection is byte-identical to a build
  without them. The terms are shown whatever the flag says: it gates new
  offers, never an order that already exists.
- **Flag.** On, `GET /health` gains `"usdt_payments": {"available": true}`
  and `POST /v0/orders/{id}/payment-method` accepts `{"method":"usdt"}`.
  Off, the key is absent and `usdt` is an `invalid_method` with today's
  message. The Shop shows USDT only when its own flag is on and `/health`
  reports it.
- **Deploy unit.** The flag-on payment-config keys (`usdt_enabled`,
  `usdt_setup`, `usdt_setup_action`, `usdt_available`) exist only from the
  seller readiness slice below, so the slice that adds the flag and the one
  that adds those keys deploy together: with the flag on and only the first
  deployed, a `PUT` carrying `usdt_enabled` is still refused as an unknown
  field. Keep `USDT_PAYMENTS_ENABLED` off until both are deployed.
- **Seller readiness.** While the flag is on, a seller's own payment config
  (`GET`/`PUT /v0/sellers/me/payment-config`) also carries
  `usdt_enabled` (the seller's Shop-level consent, stored in
  `seller_accepted_payment_options`), `usdt_setup` (`ready`, `setup_required`
  or `unavailable`) and `usdt_setup_action` (`setup`, `reconnect` or `null`),
  and the public config (`GET /v0/sellers/{pubky}/payment-config`) carries
  `usdt_available`. With the flag off none of these keys exists, a `PUT`
  carrying `usdt_enabled` is refused as the unknown field it is, and
  paykit-server is only ever asked about Bitcoin (`bitcoin_setup`).
  - Readiness comes only from paykit-server's signed `POST /setup/status`
    (`PAYKIT_SERVER_API=upstream`): `{accepted_asset: "USDT"}` for the seller, and the
    authority-only `{}` body to tell a seller that only lacks USDT
    (`reconnect`) from one whose authority is missing or invalid. That one is
    `setup` unless the service has read the seller as `ready` before, which
    means paykit-server holds them, so `reconnect`
    (`paykit_known_sellers`). The authority is looked up only when USDT is not
    ready. `unavailable`, a non-2xx answer
    or an answer outside the closed `{"status": ...}` contract is
    `usdt_setup: "unavailable"` with no action: the service never turns it
    into an authorization flow. On the fork, or without Paykit, `usdt_setup`
    is `unavailable` and paykit-server is not asked.
  - `usdt_available` is true only when the deployment speaks to upstream
    paykit-server, `PAYKIT_MARKETPLACE_ASSETS` lists `USDT` and
    `PAYKIT_MARKETPLACE_CONTRACT` is `multi_asset` (the bind's own
    preconditions, so a buyer is never offered an option every bind refuses),
    the seller consented, and the USDT status is `ready`. It
    is cached per (seller, asset) like Bitcoin's. The seller's own `GET` and
    `PUT` read paykit-server uncached, so a Bitkit setup or reconnect shows
    at once.
  - A seller who never saved anything reads the empty configuration (epoch
    `updated_at`) instead of `null`, flag on or off.
  - `PUT` replaces the rails, but an absent or `null` `usdt_enabled` leaves
    the stored consent unchanged; only an explicit boolean changes it. The
    consent is not gated on readiness, and a seller who is not ready is never
    offered USDT.
  - **USDT readiness needs a paykit-server that has `accepted_asset` on
    `/setup/status`** ([pubky/paykit-server#70](https://github.com/pubky/paykit-server/pull/70), a draft until it is merged and released). USDT
    is asked as `{"accepted_asset":"USDT","creator":...}`, the payment
    asset, never as the `asset` denomination; Bitcoin keeps
    `{"asset":"BTC",...}` and the authority lookup keeps `{"creator":...}`,
    so flag-off bytes are unchanged. On such a server a Bitcoin-only seller
    who declined the USDT address is `setup_required` with action `reconnect`,
    and `ready` once a reconnect adds the address. paykit-server rc10 and
    rc11 answered `ready` to `asset: "USDT"` for any ready seller on a
    `[usdt]` deployment, so they cannot tell these sellers apart.
  - **Deploy order.** A paykit-server without the field refuses the USDT body
    with `400 invalid_request`. That is a non-2xx answer, so USDT fails closed
    as `usdt_setup: "unavailable"` with no action, no authority lookup, and
    `usdt_available: false`. Upgrade paykit-server to a build with
    `accepted_asset` first (a paykit-server release containing #70), then switch `USDT_PAYMENTS_ENABLED` on. The
    captured exchanges, including that refusal, are in
    `crates/service/tests/fixtures/paykit-server-u4/`.
- **Bind.** A legacy `{"method":"usdt"}` bind is always refused with
  `usdt_unavailable` and leaves the order untouched (unbound, unheld, no
  attempt spent): USDT is paid through `paykit`, whose request carries every
  asset the seller accepts. The log says which prerequisite is missing: not
  the upstream API, USDT absent from `PAYKIT_MARKETPLACE_ASSETS`, or that the
  method to bind is `paykit`.

### The single Paykit bind

`POST /v0/orders/{id}/payment-method` with `{"method":"paykit"}` creates one
Paykit payment request that carries every asset the seller accepts (Bitcoin
and USDT). The buyer picks the rail in Bitkit and the order settles on the
rail that was actually paid. The older `{"method":"bitcoin"}` bind keeps
working unchanged (Bitcoin only, its projection unchanged).

- **Which request.** With `PAYKIT_MARKETPLACE_CONTRACT=multi_asset` (upstream
  only) the prepare body is `{amount, asset, creator, description,
  operation_id, payment_window_seconds, reader, reference}` plus an optional
  `accepted_assets`: the denomination (`asset` is `USD` for a `USD/2` order,
  `BTC` for a `SAT/0` or `BTC/8` order) and its amount in base units as a
  digit string (cents, satoshis). paykit-server derives every asset the
  seller can receive and fixes the Bitcoin rate once, at prepare. The service
  sends `accepted_assets` only to narrow: `["BTC"]` when USDT is not wanted
  (the seller's consent or `USDT_PAYMENTS_ENABLED` is off, or
  `PAYKIT_MARKETPLACE_ASSETS` does not list `USDT`: the new body with USDT
  off); `["USDT"]` when the seller's Bitcoin switch is off (a seller who takes
  only USDT) or the FX quote or rate cannot be read but USDT is available (a
  `["BTC"]` request is the fallback when only the USDT amount cannot be
  checked, see below). Activation names the same denomination and amount
  (`{amount, asset, creator, invoice_id}`), and the response must echo the
  persisted terms. Against the fork, or with `legacy`, `paykit` is the legacy
  Bitcoin request exactly as the `bitcoin` bind sends it (`amount_sats`, the
  service FX quote for USD orders).
- **Guards.** Whatever paykit-server offers is checked before the bind
  records anything: a USD order's USDT amount is exactly the order total at
  parity (`cents x 10_000` millionths) and its Bitcoin amount must sit within
  the FX deviation bound (`MAX_DEVIATION_BPS`) of the service's own quote and
  inside the 1,000-sat minimum and the cap; a satoshi-priced order's Bitcoin
  amount is exactly the price and its USDT amount must sit within the same
  bound of what the price is worth at the service's own rate. A mismatch
  refuses the bind and voids the prepared invoice. Every order currency the
  service converts (`USD/2`, `SAT/0`, `BTC/8`) offers every rail the seller
  enabled. A `paykit` bind needs the seller's Bitcoin switch or the USDT
  consent; neither is `method_unavailable`.
- **Order projection.** A `paykit` order carries `payment_method: "paykit"`,
  `accepted_assets` (what the request offers: `["BTC"]`, `["USDT"]` or
  `["BTC","USDT"]`) and `payment_amounts` (what each asset costs in its own
  base units: satoshis, USDT0 millionths). Once paid, `payment_asset`,
  `payment_network`, `payment_amount_minor`, `payment_exponent` and
  `payment_quote_basis` name the observed rail (`BTC` / `bitcoin`, exponent 8,
  basis `native` or `fixed_rate`; `USDT` / `arbitrum-one`, exponent 6, basis
  `parity` or `fixed_rate`); they are absent until then. A USDT payment also
  carries `payment_finality`. Legacy `bitcoin` and `usdt` orders keep their
  projection.
- **Public seller config** (flag on only). `paykit_available` (the seller can
  receive Bitcoin or USDT through Paykit) and
  `accepted_assets` beside the existing `bitcoin_available` and
  `usdt_available`. With the flag off the keys are absent.
- **Settlement.** Bitcoin settles exactly as before. USDT is paid at verified
  inclusion (at least one confirmation, the amount matched, on time); a
  transfer for the wrong amount or after the window goes to manual review and
  never pays the order. The paid rail's asset and amount are recorded when
  the payment settles (or enters review) and the payment row is retyped to it;
  the receipt of a USDT order records the USDT0 amount. A matched USDT
  transfer whose reported amount is not the one offered for the request goes
  to review as well.
- **Finality (USDT).** `payment_finality` is `pending` from inclusion,
  `final` once paykit-server reports the transfer finalized, `reverted` when
  an included transfer disappears before it is final (it returns to `pending`
  or `final` if it reappears; `final` is terminal). It never changes whether
  the order is paid. While it is `pending` or `reverted`, `fulfillment.ship`,
  `fulfillment.mark_ready` and `fulfillment.confirm_pickup` are refused with
  `INVALID_STATE` and `error.reason: "payment_not_final"`. The poller keeps
  polling a paid `paykit` order for finality, and for money on the rail that
  did not pay it until the request's observation tail ends.
- **Both rails paid.** Money confirmed on both rails before the order settled
  routes the payment to the existing manual-review path (`amount_mismatch`,
  the seller notified, the hold kept): the rail paykit-server names as paid
  first is the order's paid rail, the other rail is named beside it in the
  seller's `paykit_observation` (`other_rail_asset`, `other_rail_amount`,
  `other_rail_reference`), and nothing is refunded or settled automatically.
  Money on the other rail after the order settled is held for an operator as
  `needs_review` (`payment_settled`) like money on a released attempt, once
  per order. USDT on a released attempt is held for an operator
  (`other_rail`) and never restored automatically.
- **Refund refusals** (`bitcoin/resolve` with `refunded`, 409, closed
  `error.reason`): `refund_rail_unknown` (both rails were paid and
  paykit-server named no first one, so no rail was recorded and a refund has
  none to follow: resolve as `paid` or `abandoned` and return each payment
  from the seller's wallet) and `refund_amount_unrecordable` (a USDT amount on
  a USD order that is not a whole number of cents, so it cannot be recorded as
  a refund: return it from the wallet and resolve as `abandoned`).
- **Finality bound.** A USDT payment is polled for finality for 7 days from
  `payment_finality_at` while `pending` or `reverted`. A payment still in either state past
  that bound is held for an operator (`needs_review`, `detected_unconfirmed`,
  `observation.status` `usdt_finality_pending` or `usdt_finality_reverted`,
  `ALERT usdt_finality_pending_too_long`, listed by `paykit-attempts-admin
  list`; the operator checks the transfer on Arbitrum and dismisses or
  refunds, see `docs/paykit-released-attempts.md`); the order stays paid and
  fulfilment stays blocked. A reappearing transfer restarts the clock.
- **Refunds follow the paid rail.** A USDT-paid order (`payment_asset` `USDT`,
  or the legacy `usdt` method) takes the buyer-confirmed Arbitrum address and
  an Arbitrum transaction hash; a Bitcoin-paid order refunds as before.
- **Deploy order.** Deploy paykit-server with the multi-asset Marketplace
  contract first (prepare and activate, and USDT on the status route), then
  this service. Until `PAYKIT_MARKETPLACE_CONTRACT` is set the service speaks
  the legacy request and behaves as before, so it can be deployed ahead of
  paykit-server. Once paykit-server is live set
  `PAYKIT_MARKETPLACE_CONTRACT=multi_asset` (the legacy body is refused from
  then on); USDT stays off until `PAYKIT_MARKETPLACE_ASSETS=BTC,USDT` and
  `USDT_PAYMENTS_ENABLED=true` are set too, so the release and the USDT
  switch roll out separately. Migration 0060 is additive.

### USDT refund destination

A USDT payment is refunded by the seller from their own wallet (a normal USDT
send in Bitkit): the service never holds or moves funds, and Paykit has no
refunds. The seller needs an address to send to, and the address a payment came
from is not assumed to take a refund (the buyer may have paid from an
exchange). So the buyer confirms one on the order. This applies to orders paid
in USDT only (the legacy `usdt` method, or a `paykit` order that settled on the
USDT rail); every other order is unchanged. It is not gated by
`USDT_PAYMENTS_ENABLED`: an order that already exists can always be refunded.

- **`refund.confirm_destination`** (buyer of the order). Payload
  `{"order_id": "<uuid>", "address": "0x<40 hex>"}`, closed (no other field).
  The address is `0x` and 40 hex digits; a mixed-case address must match its
  EIP-55 checksum, while an all-lower or all-upper one carries none and is
  accepted. Surrounding whitespace is trimmed and the address is stored as
  entered. The order needs to be paid in USDT and the payment received
  (`confirmed`) or in manual review. The buyer may replace the address
  until a refund is recorded; every confirmation advances the order revision,
  appends the event `refund.destination_confirmed` and sends the seller the
  `refund_destination_confirmed` notification. There is no way to ask for "the
  address I paid from": paykit-server does not expose the payer address, so the
  only source is `buyer_entered`.
- **Projection.** A USDT order projects `refund_destination` to its
  participants on `GET /v1/orders` and `GET /v1/orders/{id}`, and on the
  `refund.confirm_destination` result: `null` until the buyer confirms, then
  `{"address", "network": "arbitrum-one", "asset": "USDT", "source":
  "buyer_entered", "confirmed_at"}`. Orders of any other method have no
  `refund_destination` key. It is never in a public projection or a log. The
  USDT amount is the order's `payment_amount_minor` (millionths).
- **Recording the refund.** On a USDT order `refund.record_external` and the
  manual-review `refunded` resolution (`POST /v0/orders/{id}/bitcoin/resolve`)
  need the buyer's confirmed address (`refund_destination_required`, 409) and a
  reference that is the Arbitrum transaction hash, `0x` and 64 lower-case hex
  digits (`invalid_refund_reference`, 422). The resolution reference cap is 64
  printable characters for a Bitcoin order and unchanged; a USDT order takes the
  66-character hash instead. The address is copied into `external_refund` as
  `destination_address`, so a later change cannot rewrite the record. A
  manual-review USDT refund records the quoted USDT amount in order units
  (cents) for a USD order, and in the paid asset's own units (USDT0
  millionths) for an order priced in anything else.
- **Checking it on Arbitrum.** With `ARBITRUM_RPC_URL` set,
  `refund.record_external` on a USDT order reads the transaction's receipt and
  sums the USDT0 it sent to the confirmed address. A hash Arbitrum doesn't show
  (`refund_transaction_not_found`), a reverted transaction
  (`refund_transaction_reverted`) or one that sent the address no USDT0
  (`refund_transaction_wrong_recipient`) is refused with 422, and so, with or
  without the RPC, is a hash already recorded on another order
  (`refund_transaction_already_recorded`). Otherwise the
  record keeps `received_units` (USDT0 millionths) and `checked_at`, so a short
  refund reads as short. An unreachable RPC records the refund unchecked.
- **What it does not do.** Without `ARBITRUM_RPC_URL`, and on the manual-review
  `refunded` resolution, the record is what the seller said they did. A
  `refunded` resolution toward Paykit is an annotation only (it does not verify
  or execute a refund).
- **Refusals** (closed `error.reason` tokens): `invalid_refund_destination`
  (422), `refund_destination_required` (409) and `invalid_refund_reference`
  (422). A wrong sender, order method or payment state is a plain
  `UNAUTHORIZED` or `INVALID_STATE`.
- **Storage.** `order_refund_destinations` (migration 0054), one row per order.

### Address autocomplete proxy

`POST /v0/address/suggest` with `{"q": "42 Union Street New Bedford", "country": "US"}`
(`q` 4–120 characters, `country` optional) returns up to five address
suggestions built from OpenStreetMap data through Photon, plus the required
`attribution` (`© OpenStreetMap contributors`, link to the ODbL notice). The
public Nominatim instance forbids autocomplete; Photon is built for it.

- Public and unauthenticated: a query is never tied to a session. The query is
  a POST body, never logged, and never stored; cache keys are salted hashes in
  memory only, with a 30-minute TTL and 4096 entries.
- The upstream sees this service's address, a fixed User-Agent and the query:
  no cookies, Referer, forwarded address or Accept-Language.
- Limits: per client (30 per minute, burst 8) and across all clients toward
  the upstream (2 per second by default). Identical concurrent queries share
  one upstream call. The upstream gets 6 seconds per call (2 to connect).
- Failures: a timeout, transport failure, 5xx or unreadable answer fails that
  lookup with 503 `address_search_unavailable` and `Retry-After: 2`. Three in
  a row, or one upstream 429, open a breaker: 10 seconds first, doubling on
  each reopening up to 60, and a 429's own `Retry-After` is honoured up to 10
  minutes. When the cooldown ends, one lookup probes the upstream (others get
  `Retry-After: 1`); success closes the breaker and resets the cooldown.
  Cached answers still serve while it is open. Logs record the failure cause
  (`timeout`, `connect`, `transport`, or the upstream status) and its latency,
  never the query.
- A street-only match keeps the buyer's typed leading house number on line 1
  (`precision: "street"`); an OpenStreetMap house match is `precision: "house"`.
  `region` is Photon's state name; clients map it to their own codes.

### Two-phase Paykit payment requests

This section describes the original **fork** mode (`PAYKIT_SERVER_API=fork`,
the default). Upstream mode uses signed prepare, activate, void, resolve, and
status routes with producer-issued invoice identity and deadlines; its fixed
inventory-hold boundary is documented in
[`docs/paykit-upstream-inventory-hold.md`](docs/paykit-upstream-inventory-hold.md),
and what the upstream prepare request carries (an exact-price amount, no
description) in
[`docs/paykit-upstream-request-fields.md`](docs/paykit-upstream-request-fields.md).

Bitcoin binding is a two-phase protocol with paykit-server (the message
contract is design §B.11.3, verbatim; this section describes the
marketplace side only). Phase 1 (`POST /v0/payment-requests`) runs inside
the bind transaction and returns the prepared invoice — invoice id, the
issuing stack's `stack_id`, the minted nonce and `total_sats`, the expiry
pair, the allocation mode, and a fingerprint of the derived address (never
the address; §B.0). The bind persists that pin in the same transaction —
including `paykit_stack_endpoint`, the base URL the phase-1 call used, so
later messages route back to the issuing stack even after
`PAYKIT_SERVER_URL` is repointed — together with a durable
`paykit.activate` outbox row. Bind and activation intent commit atomically
or not at all; a phase-1 refusal rolls everything back (fail-closed). A
commit call that reports an error after a phase-1 success has an ambiguous
outcome — the write may be durable — so the bind is re-read before anything
is voided: only a confirmed-absent bind triggers one best-effort courtesy
void (paykit's 15-minute prepare reaper is the guarantee), a durable bind
is carried forward by its activation row, and an unreadable state alerts
and is left to the hold-expiry reconciliation below.
`total_sats` (`price + nonce`) is the figure the marketplace charges,
displays and records: the order projection, the payment step and the
receipt all carry it. The phase-1 response is checked against the request
before anything is persisted: `total_sats` must equal `price + nonce`, and
the echoed `expires_at` must equal the hold deadline exactly (the wire
format truncates to UTC seconds, so the check is exact at second
precision). Either mismatch is a terminal refusal — no bind, one courtesy
void, alerted — and the persisted `paykit_expires_at` is the local hold
deadline, never the echoed value.

Activation state on the order:

| State | Meaning | Buyer sees |
| --- | --- | --- |
| `preparing` | phase 1 returned, bind committed, activation not yet confirmed | "Preparing your Bitcoin payment…" |
| `active` | `activate` returned 2xx | the Payment Request in their wallet |
| `voided` | prepare voided or reaped; the bind is released | "Bitcoin payment could not be prepared — choose a payment method again" |

While `preparing`, `paykit_request_state` is also `preparing` and the
verification poll does not claim the order; the activation delivery flips
it to `active`/`pending` in one transaction with the outbox mark, so a
redelivered row cannot apply twice. Terminal activation errors
(`prepare_expired`, `invoice_finalized`, `unknown_invoice`, and the
mismatch 409s) void the bind atomically — hold released, payment row
restored to its pre-bind state, `payment.bitcoin_prepare_voided` emitted —
and the mismatch and unknown-invoice cases alert; transient failures — and
a well-formed 200 whose `state` is not `observing`, which is a malformed
success — retry under the ordinary outbox lease.

**Hold expiry of a `preparing` order** (coordinator decision; design
§B.11.2 leaves this cell undefined — W9.3 reconcile): when the hold
deadline lapses while activation is still undelivered, the worker first
takes the activate row's lease (a delivery in flight skips the order that
tick), then voids the invoice at its persisted endpoint with reason
`hold_expired`. A successful void (or an already-final prepare) runs the
ordinary expiry effects in one transaction; `invoice_finalized` means
paykit already activated (a lost 2xx), so the order goes `active`/`pending`
and the ordinary pending-bitcoin expiry takes it next tick; an unreachable
paykit is retried each tick until 30 minutes past the deadline, after which
the marketplace voids locally, releases the hold, and defers the remote
void to a `paykit.void` outbox row that retries for up to 24 hours (alert
`paykit_unreachable_at_void`). Residual: a lost-response activation
immediately followed by a paykit outage can leave paykit `observing` an
invoice for an order the marketplace voided — bounded because paykit's
`expires_at` equals the hold deadline, so the invoice expires there too.

**Buyer cancel of a Bitcoin order.** `order.cancel_request` on an unpaid
Paykit-rail order expires its payment (`awaiting_entitlement → expired`)
in the cancel transaction, so money that still reaches the request takes
the late-money fork. A `preparing` request is voided in the same
transaction: activation `voided`, the undelivered `paykit.activate` row
stamped, and a `paykit.void` row (`reason=order_cancelled`) enqueued. The
activation arm never activates an order that left `pending_payment`; it
voids instead (`reason=order_not_payable`). An activate that committed at
paykit before the void landed (activate `200 observing` after the local
void, or void `409 invoice_finalized`) re-tracks the order as
`active`/`pending` with alert `paykit_activated_after_cancel`, and the
observation-tail poll keeps watching it. A request that was already
published stays `pending` after the cancel for the same reason: paykit
cannot retract a published request, and `resolve(abandoned)` would stop
paykit observing it. Once money is observed (`detected`,
`awaiting_seller_confirmation`, `confirmed`) the buyer's cancel is refused.

### `shared_manual` checkout and manual-review resolution (W1.15)

A seller whose Paykit creator is `shared_manual` confirms Bitcoin payments
manually: no chain observation moves the order to `paid`. The strict
`paykit.bitcoin_status/v2` contract gates every poll — a missing/wrong
`contract_version` or a missing/unknown `allocation_mode` fails closed
(never an automatic transition input) — and the creator's CURRENT mode
decides: `exclusive` confirms automatically as before, while a matching,
non-late `shared_manual` observation enters
`orders.paykit_request_state = 'awaiting_seller_confirmation'`, extends
the inventory hold to a bounded 24-hour seller-confirmation window, and
keeps polling on a status-only basis (facts refresh; nothing advances).
A late observation (payment already expired) never enters the state — it
takes the existing late path straight to `manual_review`. The stored
observation is audit-only; there is no seller-facing Marketplace read path
for it. Sellers confirm from their Paykit stack view, while the service uses
the stored facts to preserve the audit trail and validate the resolution.

Three exits, each one CAS with every effect in the same transaction:

- **Seller confirm** (`POST /v0/orders/{id}/confirm-bitcoin-payment`,
  body `{reason}`): the session pubky is resolved order → listing → seller
  (403 `not_order_seller` for anyone else) BEFORE the idempotency lookup;
  the audit row derives `confirmed_txid`/`confirmed_amount_sats` and the
  frozen observation from the stored Paykit observation, never the body
  (a disagreeing body-supplied value is rejected with
  `confirmation_observation_mismatch`), with
  `confirmation_basis = 'seller_attestation'`. Idempotent on the order id.
- **The 24-hour seller-window reaper**: the order's request state becomes
  `confirmed` and the payment `manual_review` with
  `manual_review_entered_at` stamped (the hold is PRESERVED), starting the
  two-business-day seller-response SLA (alert only) and the seven-day
  inactivity clock.
- **Seller resolve / the seven-day reaper**
  (`POST /v0/orders/{id}/bitcoin/resolve`, Paykit-Bitcoin scope only):
  one shared CAS (`state='manual_review' AND resolution_outcome IS NULL`)
  applies `paid` (existing confirmation effects; late entries reacquire
  inventory in lock order or fail `409 stock_unavailable`), `refunded`
  (payment stays `confirmed` with `resolution_outcome='refunded'`,
  validated external refund reference, order `refunded_external`), or
  `abandoned` (payment `expired`, order cancelled, hold released). The
  reaper is the same abandoned branch with
  `resolution_basis='seller_unresponsive'`; a later seller resolve is 409.
  `Idempotency-Key` (a UUID) replays same-key/same-body, conflicts
  same-key/different-body, and refuses any different key with
  `409 already_resolved`.

Every terminal outcome writes exactly one audit row, one event, and one
`paykit.resolve` outbox row pinned by stack identity AND bind-time
endpoint. The delivery arm dials the ROW'S endpoint (never the current
default), compares the readiness `stack_id` before sending (a mismatch
sends nothing and terminates `stack_pin_mismatch` with an alert), and maps
every response class to exactly one outcome: permanent refusals terminate
visibly; 401/403 retry with an immediate first alert; 408/425/429, 5xx
and transport failures retry under the hard one-hour
`delivery_deadline`; `Retry-After` on 429/503 is honoured as
`min(deadline, max(backoff_due_at, retry_after_due_at))` — a floor, never
an undercut. Terminal rows block the §C.16 condition-6 drain until
acknowledged; the infrastructure acknowledgement terminates DELIVERY only
and never touches the order outcome. Condition 7 keeps the old stack
retained until
`NOT EXISTS (orders pinned to it awaiting seller confirmation or payments
in manual_review)` — only seller resolution or the seven-day reaper
clears it. No path in this flow can un-pay an order.

The drain orchestration primitives are deliberately service/ops-only:
`condition_seven_clear` checks for live order/payment pins,
`condition_six_blocking_rows` counts undelivered or unacknowledged resolve
rows, `terminate_resolve_delivery` terminates delivery for one outbox row,
and `acknowledge_resolve_row` records an operator acknowledgement. They do
not expose HTTP authority or change an order outcome; future cross-repo drain
orchestration consumes them.

The three `LOCKS_*` secrets/URL are all-or-nothing: the service **fails
closed at startup** on a partial configuration (a URL without keys, or keys
without a URL), rather than running with verification silently disabled or
bearer material unprotected. With none of them set the deployment is
sandbox-only: `payment.register_locks` is refused and the lifecycle poller
is not scheduled.

`PICKUP_DETAILS_ENCRYPTION_KEY` is all-or-none: when it is set the boot
probe (`assert_pickup_sealing_coherent`, run after migrations) attempts one
real open — current key, then previous — across BOTH sealed pickup families
(details versions and pinned snapshots) and refuses to start when sealed
rows exist that no configured key opens. When it is unset, pickup is OFF:
`pickup_details.set` is refused and no details are ever stored. Independently,
when `SANDBOX_PAYMENTS_ENABLED` is on, `pickup_details.set` and the buyer
pickup reveal are refused outright, and `/health` reports
`pickup_available: false`. Key rotation: deploy with both keys, run the
re-seal worker to completion (zero rows in either family remain under the
previous key), then remove the previous key.

`SANDBOX_PAYMENTS_ENABLED` is the server-side boundary for the sandbox
payment adapter. Every checkout starts on the `sandbox` adapter until a real
method is bound, and `payment.sandbox_advance` lets the buyer drive that
payment to `paid` by command — which on a durable deployment would be a
self-serve paid state with no money moving. The client's transport
allowlist refuses to send the command to the durable service, but that is a
UX courtesy, not a boundary; with the flag at its default the service
rejects the command outright (`INVALID_COMMAND`, 422) regardless of what
any client sends.

The two `ATTESTOR_*` secrets are likewise all-or-nothing (fail closed at
startup on a partial pair). With neither set, reviews still work but no
purchase attestations are issued (`review.create` results carry no
`attestation`), no attestor annotations are recorded, and the weekly seller
stat-attestation job does not run.
With both set, `review.create` issues a durable compact-JWS purchase
attestation per reviewed listing inside the review transaction, all sharing
the order's `order_ref` (re-fetchable via
`GET /v1/orders/{id}/review-attestations`, and the first line's via
`GET /v1/orders/{id}/review-attestation`, participant-scoped, idempotent),
the D2 amount-band consent gate applies (`attestation.set_band_consent`
command + `GET /v1/sellers/{pubky}/band-consent` read), external refunds
append `attestation_annotations` rows keyed by the
salted `order_ref`, and the worker signs weekly per-seller stat attestations
into `seller_stat_attestations`. The annotations are published to
the attestor's homeserver when `ATTESTOR_PUBLISH_ENABLED` is set (see
[Attestor records](#attestor-records)); stat attestations are Phase 3 of the
trust & reputation plan (`pubky/pubky-marketplace`
[`docs/ecommerce/trust-reputation-plan.md`](https://github.com/pubky/pubky-marketplace/blob/master/docs/ecommerce/trust-reputation-plan.md)) and
are not published by this service yet — those rows accumulate here as the
publisher's ground truth.

#### Attestor records

With `ATTESTOR_PUBLISH_ENABLED=true` the worker signs in to the attestor's
homeserver as the attestor key and writes two kinds of record, so that a refund
or a return shows next to the review it concerns and a key rotation can be
announced. The index serves them
(`GET /v0/marketplace/attestor/{id}/profile` and `/annotations?order_refs=`).

- **The profile**, `/pub/pubky.app/marketplace/v1/attestor/profile`: the
  operator (`ATTESTOR_OPERATOR_NAME`, `_URL`, `ATTESTOR_POLICY_URL`), the key
  list and successor (`ATTESTOR_KEYS`, `ATTESTOR_SUCCESSOR`) and the issuance
  policy, which the service states from what it does: the edit window, the
  blind window (`REVIEW_BLIND_WINDOW_DAYS`), `amountBand: "both-sides
  consent"`, `minOrderValue: null` and `priceDecade`. `priceDecade` is `never`
  while the purchase attestations this service issues carry no price decade
  (`ISSUES_PRICE_DECADE` in `attestor.rs`, which the change that signs the
  claim sets to `true`, turning the profile to `always`).
- **One annotation per attested order**,
  `.../attestor/annotations/{order_ref}`: the order's `returned` outcome
  (written when the seller records `return.receive`) and `refunded` outcome
  (written when a refund is recorded), each with its day, in the order they
  happened. Only an `order_ref` this attestor issued a purchase attestation
  for is ever written, and no annotation is written before it has an outcome.
  `refund_reversal_cancelled` is not an outcome: it records that PayPal
  cancelled a refund reversal and the order went back to the state the refund
  had replaced, so it takes back the latest `refunded`; an annotation left
  with no outcome is written once more, empty, so the public record stops
  claiming the refund.

Every record is signed (a compact JWS with a detached payload over the
RFC 8785 canonical record) with the attestor key, so a verifier needs no trust
in the homeserver. A record is written when its content differs from what
`attestor_publications` says was written; an annotation's `revision` is raised
by one on every change, and the index replaces an annotation by a higher
revision. One publisher runs at a time (the `attestor_publish` worker lease,
renewed before every write). A failed write is retried with a backoff that
doubles from 15 seconds to an hour, kept in `attestor_publish_attempts`, so it
holds across restarts and instances; a pass writes at most
`DELIVERY_SWEEP_BATCH_SIZE` records. The record types are those of
`pubky-app-specs` (tag `v0.6.2-marketplace.16`); the service holds a port of
them, kept equal to the crate by `tools/specs-parity` (see
[`tools/specs-parity/README.md`](tools/specs-parity/README.md)).

**Before enabling it.** The index (`marketplace-nexus` with the attestor
records, IX5) must be live first: it has no backfill, so a record written before
it is indexed only when it is written again. The attestor's account must exist
on the homeserver the index polls (it indexes only that one), and the attestor's
PKARR record must point at `ATTESTOR_HOMESERVER`; the publisher refuses to write
to any other homeserver. Startup refuses to run with publishing enabled and a
setting missing, invalid, or a key list that omits the attestor's own key.

With an attestor configured, participants can also fetch a **receipt
attestation** (`GET /v1/receipts/{receipt_id}/attestation`,
participant-scoped exactly like `GET /v1/receipts/{id}`; without an
attestor the fetch is 404, like the review-attestation re-fetch): a compact
JWS (`alg: EdDSA`, `typ: pubky-order-receipt+v1`) signed by the attestor,
attesting the paid order's facts so the portable receipt document a buyer
or seller publishes on their own homeserver stays verifiable after this
operator disappears ("credible exit for orders"). The response is
`{ "receipt_attestation": { "jws", "claims" } }`. Claims, in normative
order:

| Claim | Value |
| --- | --- |
| `v` | `1` |
| `iss` | the attestor pubky (z-base-32; decodes to the Ed25519 verification key) |
| `buyer` | the order's buyer pubky |
| `seller` | the order's seller pubky |
| `order` | the order UUID, lowercase hyphenated |
| `receipt` | the receipt UUID, lowercase hyphenated |
| `total_minor` | the order's total in minor units |
| `currency` | the order's currency |
| `exponent` | the order's currency exponent |
| `paid_at` | the receipt's stored creation instant as the canonical wire timestamp (RFC 3339, milliseconds, `Z`) — string-equal to `issued_at` on `GET /v1/receipts/{id}` |
| `iat` | the receipt's stored creation instant as epoch seconds |

Every claim derives from stored rows (the receipt's creation instant, the
order's totals) — never from the current time — so the JWS is
deterministic: repeated fetches by either participant return the
byte-identical token, and nothing is stored; the endpoint re-signs on
demand.

Participants in a paid DROP order (see Drops below) can additionally fetch
an **edition attestation**
(`GET /v1/receipts/{receipt_id}/edition-attestation`): a compact JWS
(`alg: EdDSA`, `typ: pubky-drop-edition+v1`) attesting which numbered
edition of the drop the order received. The response is
`{ "edition_attestation": { "jws", "claims" } }`. Absent receipts, foreign
receipts, non-drop orders (no `drop_aggregate_id`/`edition` on the order),
and attestor-less deployments are all 404 with the same body as
`GET /v1/receipts/{id}`, indistinguishably. Claims, in normative order (the
specs-fork verifier for `pubky-drop-edition+v1` checks the serialized
payload byte-for-byte, so the order is the wire contract):

| Claim | Value |
| --- | --- |
| `v` | `1` |
| `iss` | the attestor pubky (z-base-32; decodes to the Ed25519 verification key) |
| `buyer` | the order's buyer pubky |
| `seller` | the drop owner's pubky (the order's seller) |
| `drop` | the DROP ID (the seller's record identifier, parsed from the aggregate id's `drop:{seller}_{drop_id}` shape) — never the aggregate id |
| `edition` | the order's edition, 1-based |
| `of` | the drop's `total_quantity` |
| `receipt` | the receipt UUID, lowercase hyphenated |
| `iat` | the receipt's stored creation instant as epoch seconds — deterministic, never wall clock |

Like the receipt attestation, every claim derives from stored rows, so
repeated fetches by either participant return the byte-identical token and
nothing is stored.

Endpoints: every route, with its auth requirement, is tabulated in
[`docs/api.md`](docs/api.md#route-table); the seller automation routes
(sessions, inventory, exports, event feed, webhooks) are specified there in
full. The buyer and seller application routes are described in the feature
sections below (see "Read projections" for the role-scoped reads).

## Test

Integration tests run against real Postgres; `#[sqlx::test]` provisions an
isolated database per test and applies the migrations.

```sh
docker compose up -d --wait
export DATABASE_URL='postgres://marketplace:marketplace@localhost:55432/marketplace'
cargo test --workspace
```

Gates (the same as CI): `cargo fmt --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`bash scripts/ci-test.sh`. The script exists because several integration
binaries mutate cluster-global Postgres roles and must run one test at a time
after the parallel ones, so a bare `cargo test --workspace` can fail on role
contention. CI runs the toolchain 1.89.0 against PostgreSQL 16; the
`docker-compose.yml` database is PostgreSQL 17. The pre-push hook
(`scripts/prepush.sh`) runs the fast gate for the packages a change touches;
`PREPUSH_FULL=1` runs everything.

## Command envelope (ADR-0019 §3)

All mutations go through `POST /v1/commands` with a closed, versioned JSON
envelope; unknown fields and unsupported versions are rejected
(`deny_unknown_fields`):

```json
{
  "version": 1,
  "command_id": "uuid",
  "aggregate_id": "listing:<seller-pubky>_<listing-id>",
  "expected_revision": 7,
  "issued_at": "2026-08-19T22:00:00.000Z",
  "kind": "listing.register",
  "payload": {}
}
```

- `command_id` is the idempotency key, unique per authenticated actor. An
  exact replay returns the stored prior result without re-executing; the same
  `command_id` with a different canonical payload returns HTTP 409
  `IDEMPOTENCY_CONFLICT`. Canonicalization hashes the parsed, normalized
  command (key order, timestamp formatting, and schema defaults do not affect
  the hash).
- `expected_revision` is a required envelope integer (`0..=2^53-1`). It is
  **not** compare-and-swapped on every command. CAS commands refuse a stale
  value with 409 `REVISION_CONFLICT` and `current_revision`. ADR-0019 §3
  scopes that check to stale accepted-offer, stock, auction, and order
  writes. `listing.sync` and `drop.sync` are **convergent**: any
  authenticated actor may sync (provenance is the homeserver fetch), and a
  stale or mismatched `expected_revision` is accepted — send `0`.
- Server time is authoritative; `issued_at` is diagnostic only.
- Failures are not stored: retrying a rejected command re-executes it
  (matching the prototype engine).

### Per-command `expected_revision`

Derived from the command handlers. Order-action kinds share
`guard_order_action` (`crates/service/src/handlers/mod.rs:146`). The
envelope field is always parsed; "ignored" means the handler does not
compare it.

| Command | Envelope `expected_revision` | Source |
| --- | --- | --- |
| `listing.register` | CAS 409 vs listing `server_revision` | `register_listing.rs:115` |
| `listing.sync` | Convergent — ignored; any actor; stale accepted | `sync_listing.rs:12` (never read) |
| `drop.sync` | Convergent — ignored; any actor; stale accepted | `drops.rs:98` (never read) |
| `drop.cancel` | CAS 409 vs drop `revision` | `drops.rs:388` |
| `drop.release_listings` | CAS 409 vs drop `revision` | `drops.rs:485` |
| `inventory.reserve` | CAS 409 vs listing `server_revision` | `reserve_inventory.rs:36` |
| `checkout.create` | Envelope must be `0` (`INVALID_COMMAND`); each `payload.lines[].expected_revision` is CAS 409 vs that listing | `checkout.rs:27`, `checkout.rs:86` |
| `offer.create` | CAS 409 vs listing `server_revision` | `offers.rs:84` |
| `offer.counter` | CAS 409 vs offer `revision` | `offers.rs:306` |
| `offer.accept` | CAS 409 vs offer `revision` | `offers.rs:587` |
| `offer.checkout` | CAS 409 vs offer `revision` | `offer_checkout.rs:67` |
| `offer.reject` | CAS 409 vs offer `revision` | `offers.rs:825` |
| `offer.withdraw` | CAS 409 vs offer `revision` | `offers.rs:891` |
| `auction.place_bid` | CAS 409 vs listing `server_revision` | `auction.rs:98` |
| `auction.close` | CAS 409 vs listing `server_revision` | `auction.rs:292` |
| `payment.sandbox_advance` | CAS 409 vs payment `revision` | `payment.rs:76` |
| `payment.prepare_locks` | CAS 409 vs payment `revision` | `locks.rs:145` |
| `payment.register_locks` | CAS 409 vs payment `revision` | `locks.rs:393` |
| `order.cancel_request` | CAS 409 vs order `revision` | `cancellation.rs:51` |
| `order.cancel_approve` | CAS 409 vs order `revision` | `cancellation.rs:185` |
| `fulfillment.ship` | CAS 409 vs order `revision` | `fulfillment.rs:34` |
| `fulfillment.update_tracking` | CAS 409 vs order `revision` | `fulfillment.rs` (`update_tracking`) |
| `fulfillment.confirm_delivery` | CAS 409 vs order `revision` | `fulfillment.rs:107` |
| `pickup_details.set` | Envelope ignored; `payload.expected_version` is CAS 409 vs the pickup version counter | `pickup.rs:270` |
| `pickup_details.clear` | Envelope ignored; `payload.expected_version` is CAS 409 vs the pickup version counter | `pickup.rs:346` |
| `fulfillment.mark_ready` | CAS 409 vs order `revision` | `fulfillment.rs:193` |
| `fulfillment.confirm_pickup` | CAS 409 vs order `revision` | `fulfillment.rs:272` |
| `return.request` | CAS 409 vs order `revision` | `returns.rs:40` |
| `return.cancel` | CAS 409 vs order `revision` | `returns.rs` |
| `return.approve` | CAS 409 vs order `revision` | `returns.rs:171` |
| `return.receive` | CAS 409 vs order `revision` | `returns.rs:171` |
| `refund.record_external` | CAS 409 vs order `revision` | `returns.rs:251` |
| `refund.confirm_destination` | CAS 409 vs order `revision` | `refund_destination.rs:27` |
| `review.create` | CAS 409 vs order `revision` | `reviews.rs:223` |
| `review.update` | CAS 409 vs order `revision` | `reviews.rs:778` |
| `review.withdraw` | CAS 409 vs order `revision` | `reviews.rs:896` |
| `attestation.set_band_consent` | CAS 409 vs stored consent revision (`0` on first write) | `attestation.rs:47` |

Handler paths are under `crates/service/src/handlers/`.
`POST /v1/inventory/adjust` is a separate HTTP route (not this envelope)
and does enforce wire `expected_revision` with HTTP 409 `revision_conflict`
(`crates/service/src/inventory.rs:730`).

Commands implemented: `listing.register`, `listing.sync` (convergent; any
actor may sync), `drop.sync` (convergent; any actor may sync),
`drop.cancel`, `drop.release_listings` (see Drops), `inventory.reserve`,
`checkout.create`, `offer.create`, `offer.counter`, `offer.accept`,
`offer.checkout`, `offer.reject`, `offer.withdraw`, `auction.place_bid`,
`auction.close`, `payment.sandbox_advance`, `payment.prepare_locks`,
`payment.register_locks`, `order.cancel_request`, `order.cancel_approve`,
`fulfillment.ship`, `fulfillment.update_tracking`,
`fulfillment.confirm_delivery`, `pickup_details.set`,
`pickup_details.clear`, `fulfillment.mark_ready`,
`fulfillment.confirm_pickup` (see Local pickup), `return.request`,
`return.approve`, `return.cancel`, `return.receive`, `refund.record_external`,
`refund.confirm_destination` (see USDT refund destination),
`review.create`, `review.update` and `review.withdraw` (these two this
service only), and `attestation.set_band_consent`.
Server-driven transitions (reservation expiry, offer expiry, auction close
on server time, outbox delivery) run in the background worker runtime, not
as client commands. All other command kinds are rejected by the envelope
contract until they are ported with their tests.

## Inventory semantics: only a payment locks an item

Checkout does not hold stock. `checkout.create` validates the cart against
the pinned listing snapshot and **refuses when the requested quantity is
not available at that instant** (`INSUFFICIENT_INVENTORY`, the pinned copy
`Checkout quantity is unavailable.`) — but that check is advisory: the
command creates the immutable order snapshot and its sandbox-adapter
payment and moves **no inventory**. The order starts with `stock_held =
false` and no hold window; the listing's `server_revision` does not bump.
Abandoned carts therefore block nobody, and racing checkouts never contend:
any number of buyers can hold orders against the same stock — only payments
compete for it.

**Payment lock points.** The inventory hold is acquired at the moment a
payment starts. Each lock point atomically moves `available → reserved` for
the order's quantities under the listing row lock — failing with
`INSUFFICIENT_INVENTORY` and the pinned copy
`The listing sold out before this payment started.` when the stock is gone —
and arms a bounded server-time hold window on the order
(`orders.hold_expires_at`):

| Lock point | Window |
| --- | --- |
| `payment.register_locks` | `LOCKS_PAYMENT_WINDOW_SECONDS` (default 3600) — the existing correlation window IS the hold window; one window concept, not two |
| `POST /v0/orders/{id}/payment-method` (PayPal) | `FIAT_PAYMENT_WINDOW_SECONDS` (default 600, min 60) |
| `POST /v0/orders/{id}/payment-method` (bitcoin) | `BITCOIN_PAYMENT_WINDOW_SECONDS` (default 1800, min 60) — equals Paykit `expires_at` |
| An auction winner's order (`auction.close`, worker close) | The winning reservation's 30 minutes from the close. It is the order's `hold_expires_at` from creation, and a bind never moves it. A bind after the reservation ended is refused (409 `INVALID_STATE`, reason `auction_window_ended`) |
| `payment.sandbox_advance` transitioning OUT of `awaiting_entitlement` (the first transition; gated by `SANDBOX_PAYMENTS_ENABLED` as always) | `SANDBOX_PAYMENT_WINDOW_SECONDS` (default 900, min 60) |

A lock point on an order that ALREADY holds stock never double-decrements
(idempotent by order): a drop-bound order re-arms its window to the lock
point's own span (see Drops). An ordinary order acquires at the first
lock point (bind / Locks / sandbox) and re-arms nothing on a later one. A lock
point on an order that is no longer `pending_payment` is refused — a
cancelled order can never grab stock. Auction orders are outside this
mechanism entirely: the winner's hold is the winning `reservations` row,
taken at close and swept by reservation expiry exactly as before.

**Confirmation.** `confirm_order` (sandbox command, Locks worker, paykit
worker, and both fiat verification legs all converge there) requires the
order to hold stock — every confirming path passes a lock point first by
construction, so a missing hold is an `INVARIANT_VIOLATION` — and converts
`reserved → sold` exactly as before, clearing the hold flags in the same
update.

**Window expiry.** The worker's `payment_window` task sweeps pending,
unconfirmed orders whose hold window elapsed on server time: the hold
releases (`reserved → available`, crediting a stamped drop first under the
shared lock order), the payment moves to `expired`
(`awaiting_entitlement → expired`, the machine's `payment_window` edge),
and the ORDER is cancelled (`pending_payment → cancelled`, the contract's
`server`-triggered `payment_window` variant) with the stored
`cancellation_reason` `payment window elapsed`. Post-expiry the buyer
simply checks out again. Payments under `manual_review` are deliberately
not swept (a human decides those — their money may be real); a sandbox
payment the buyer drove to `detected` has no `detected → expired` edge, so
its lapsed order cancels and restocks with the payment record left
untouched, exactly as buyer cancellation leaves it. Confirmed orders are
`paid` and are never touched by the sweep. Money observed AFTER expiry is
never dropped: a late Locks completion, a late paykit settlement (when the
payment was already detected on-chain), and a late fiat verification all
route the expired payment to `manual_review`.

**Cancellation.** Buyer/seller cancellation of a pending order releases the
hold ONLY when one exists — an unheld pending order releases nothing, and
the quantity-balance `CHECK` constraints prove no negative movement can
happen.

**Drops keep lock-at-claim.** See Drops below: the FCFS race is the
product, so drop gating still debits the drop AND moves the listing unit at
checkout — but the claim is bounded by `DROP_CLAIM_WINDOW_SECONDS` (default
600) from checkout, and a payment lock point re-arms the window to its own
(longer) span. A claimed-but-never-paid drop order expires through the same
sweep, restocking the drop, freeing the buyer's per-drop cap, and releasing
the listing unit.

**Migration 0012 backfill.** Under the old semantics every
`pending_payment` checkout order had decremented its listings at checkout,
so migration 0012 marks them all `stock_held = true` (auction orders
excluded — their hold is the reservation row): orders with a Locks
correlation keep the correlation's window as their hold window; orders with
a bound payment method get `now() + interval '1 hour'` (the fiat window
default); every other pending order — legacy abandoned carts included —
gets `now() + interval '1 hour'`, so listings bricked by abandoned carts
self-clean through the new worker within the hour.

## Post-purchase lifecycle

The post-purchase commands drive the canonical order machine
(`processing → shipped → delivered → completed` plus the return
and refund branches):

- **Payments and receipts.** `payment.sandbox_advance` (buyer only) drives
  the sandbox payment adapter; the service records these transitions and
  **never observes, holds, or moves funds**. Its first transition out of
  `awaiting_entitlement` is a payment lock point (see Inventory semantics
  above): it acquires the order's inventory hold and arms the sandbox hold
  window. Confirmation is the transition
  that issues the durable receipt (`receipts` table, one per order/payment,
  append-only) and moves the order to `paid`. The receipt `content_hash` is
  the BLAKE3 hex digest of the canonical snake_case receipt payload
  (`order_id`, `payment_id`, `total`, `issued_at`, in that field order).
  Confirmation also converts the held inventory from reserved to sold and
  marks the winning auction reservation converted — the
  `payment_confirmation` triggers declared in the contract. If a winning
  auction hold has already lapsed on server time, confirmation is refused
  (`INVALID_STATE`) rather than overselling. (The `pubky/pubky-marketplace` client
  currently refuses to send `payment.sandbox_advance` to the durable
  service as a matter of client policy; the command exists here so the
  post-purchase lifecycle is real and testable end to end.) Real payments
  are advanced only by server-side verification of the Locks lifecycle —
  see "Locks verification" below; the same confirmation transition (receipt,
  inventory conversion, notification) is shared by both paths.
- **Cancellation.** `order.cancel_request` (buyer only, from
  `pending_payment`/`paid`/`processing`): an unpaid order cancels
  immediately, and its held stock — ONLY when a payment lock point (or a
  drop claim) actually acquired one; an unheld pending order releases
  nothing — returns to the listing; a paid order moves
  to `cancel_requested` awaiting the seller. `order.cancel_approve` (seller
  only, from `cancel_requested`) moves the order to `cancelled` and returns
  the sold quantities to available under the same quantity-balance
  constraint (see the divergence table for the resulting listing
  `sold → available` transition). Cancellation never touches a confirmed
  payment: it stays confirmed with its receipt intact (an unpaid Paykit
  payment expires; see "Buyer cancel of a Bitcoin order"), and
  the only money path out of a cancelled order is the externally evidenced
  `refund.record_external` — the service never claims to move funds. An
  auction winner's cancel releases the winning hold through the reservation
  compare-and-swap (`active → released`); if the hold already lapsed on
  server time, the expiry sweep has returned the unit and the cancel
  succeeds without double-releasing. A cancelled order refuses every
  subsequent payment, fulfillment, return, and cancellation
  command.
- **Fulfillment.** `fulfillment.ship` (seller, with a carrier and an optional
  tracking number — both trimmed printable strings, deliberately not enums;
  the soft carrier vocabulary the reference client writes is documented on
  `ShipOrderPayload.carrier`; an absent, null or empty number ships the order
  without tracking), `fulfillment.update_tracking` (seller; adds or edits the
  carrier and number while the order is `shipped`, records each change in
  `shipment.tracking_history` and notifies the buyer; see
  [docs/api.md](docs/api.md#shipping-and-tracking-commands)) and
  `fulfillment.confirm_delivery` (buyer) drive
  `paid/processing → shipped → delivered`. There is no separate
  seller-marks-delivered command: the buyer's confirmation is the delivery
  transition, as in the prototype. `delivered → completed` is reached
  through `review.create`.
- **Seller-set periods.** <a id="seller-set-periods"></a>The seller, not the
  platform, sets how long after shipment delivery is assumed and how long
  after delivery the order completes: two optional integer-day fields on the
  seller's shop record on their homeserver
  (`/pub/pubky.app/marketplace/v1/shop.json`), `deliveryAssumeDays` and
  `autoCompleteDays`, each 1 to 60. At an order's first payment lock point
  (the payment-method bind, `payment.prepare_locks`, and the sandbox
  advance) the service reads the seller's shop record and freezes the two
  periods on the order (`orders.delivery_assume_days`,
  `orders.auto_complete_days`, `orders.delivery_terms_captured_at`; both
  periods are projected on the order as `delivery_assume_days` and
  `auto_complete_days`). Capture happens once: a later change to the shop
  record never moves an open order's clock, and a second lock point on the
  same order does not read the record again. The sweeps (`delivery_assume`,
  `order_auto_complete`) then use each order's own period, and the same two
  periods govern a paid manual digital order ("I'll email it" / "I'll send
  it in messages") the seller never marks delivered: the delivered
  assumption counts from payment rather than shipment (see
  [Digital delivery](#digital-delivery)). A period the
  seller left unset (absent, not an integer, or outside 1 to 60) stays NULL on
  the order and the sweeps apply `DELIVERY_ASSUME_DAYS` /
  `AUTO_COMPLETE_DAYS`; a seller with no shop record, a deployment without a
  homeserver, and every order that predates migration 0056 read the same
  way. An unreadable shop record (transport failure or a non-404 error)
  never blocks a payment: the order freezes the configured defaults (both
  periods NULL, capture stamped) and the service logs
  `ALERT seller_terms_fallback_unreadable_shop_record` with the seller pubky
  and order id, so payments are not coupled to homeserver availability. The
  seller's periods are not applied to that order later, even once the
  record is readable again.
- **`guarantee_policy_version`.** Retired. `checkout.create` and
  `offer.checkout` do not require it, and no order response, export or
  stored row carries it any more (`orders.guarantee_policy_version` is no
  longer written; the column stays, nullable, because migrations are
  additive-only). A command that still carries it (an older Shop) is
  accepted when it is `1`, ignored, and any other value is `INVALID_COMMAND`.
- **Returns.** `return.request` (buyer, from `delivered`/`completed`, capped
  at the order total), `return.cancel` (buyer, while `return_requested`: the
  order goes back to `delivered` or `completed`, the return is cleared and the
  seller is notified; once the seller acted it is `return_not_cancellable`),
  `return.approve` and `return.receive` (seller). The
  return sub-state (`requested → approved → received → refunded`) is a
  canonical machine in the contract artifact.
- **External refunds.** `refund.record_external` (seller, from
  `return_received`/`cancelled`) records **independently supplied
  seller evidence** — an external transaction id of at least 8 characters —
  and advances the order to `refunded_external`. The service never claims
  to have moved funds (ADR-0019 §7); a CHECK constraint keeps the recorded
  amount within the order total.
- **PayPal refunds and reversals.** A postback-verified `Refunded`,
  `Reversed`, or `Canceled_Reversal` IPN is never dropped: it is recorded
  once per PayPal `txn_id` on the order whose gateway-verified payment is its
  `parent_txn_id`, in every order state, after checking the receiver against
  the snapshot taken when that payment was verified; otherwise it is held in
  `gateway_refund_inbox` (applied later when the parent payment is recorded).
  `external_refund.amount_minor` is the running refund. A partial refund
  keeps the order's state; the refund that reaches the total moves the order
  to `refunded_external` (server trigger `paypal_refund`), resolving an open
  cancel or return request. A reversal sets `payment_reversed_at`; a canceled
  reversal clears it and returns the order to the state the reversal replaced
  (`paypal_reversal_cancelled`), and the reputation worker stops counting it.
  A refund never restocks. Contract table:
  [`docs/paypal-refund-ipn.md`](docs/paypal-refund-ipn.md).
- **Reviews.** `review.create` (participants, from
  `delivered`/`completed`/`closed`): a buyer reviews each listing of the
  order (v2 `items`, with optional per-order `seller` ratings written on
  every item review; v1 `rating`/`text` rates the first line), a seller
  reviews the buyer once per order, keyed to the first line. One review per
  order, role and listing is enforced by the
  `reviews_one_per_order_role_listing` database constraint rather than
  application logic — the inserts happen before the order's revision
  compare-and-swap, so a same-item race is decided by the constraint and a
  race on different items by the compare-and-swap. Within 24 hours of their
  first review of the order a buyer may add items, and `review.update` (this
  service only) lets either reviewer revise ratings and text, and
  `review.withdraw` (this service only) records that the author removed their
  public review, so another device does not republish it
  (`reviews.public_removed_at`, shown to the author only). See
  [Reviews](docs/api.md#reviews).

Every post-purchase command is participant-scoped, enforces
`expected_revision` compare-and-swap, replays
idempotently by `command_id`, appends one immutable event per accepted
command, and emits outbox notifications exactly where the prototype did
(`payment_confirmed`, `order_cancelled` — to the seller on a cancellation
request, to the buyer on approval — `order_shipped`, `tracking_updated`,
`order_delivered`,
`return_updated`,
`refund_recorded`,
`review_received`). `review.update` and `review.withdraw` emit none — the
prototype had no counterpart to copy. With `REVIEW_BLIND_WINDOW_DAYS` above
0 the review notifications are `review_waiting`, `review_released` and
`review_received` at release; see [Reviews](docs/api.md#double-blind-reviews).

## Local pickup (Wave 7 safe subset)

Listings publish `fulfillmentMethods` (`shipping` | `pickup` | both) on the
owner-signed record, echoed at `listing.register`/`listing.sync`. Checkout
splits one order per (seller, fulfillment) — several pickup lines from one
seller share one pickup order; pickup orders charge no shipping and store no
buyer address (a pickup-only checkout presenting one is rejected
`INVALID_COMMAND`). Every order carries a required `fulfillment` column.
Digital lines (`fulfillment = 'digital'`) are a third split key: a cart with
shipped, pickup and digital lines becomes one order per (seller, method), and
a digital-only checkout takes no address and charges no shipping (see
[Digital delivery](#digital-delivery)).

### Sealed delivery address

The buyer's client seals the delivery address to the seller's published
delivery key (RFC 9180 HPKE, base mode; DHKEM(X25519, HKDF-SHA256),
HKDF-SHA256, ChaCha20-Poly1305 — the suite pubky-core uses for grant
approval) and sends the envelope instead of `delivery_address`:

- `checkout.create` takes `delivery_address_envelopes`: one
  `{seller_pubky, key_id, envelope}` per shipped seller and none for any
  other seller. `offer.checkout` takes one
  `delivery_address_envelope: {key_id, envelope}`. Sending both the
  plaintext address and an envelope, or an envelope for an order that ships
  nothing, is refused `INVALID_COMMAND`.
- `envelope` is 2,096 bytes (`enc`, 32, then the padded ciphertext, 2,064) as
  unpadded base64url (2,795 characters); `key_id` is 32 lowercase hex
  characters. The service checks only the shape, stores the bytes in
  `orders.delivery_address_envelope` and `orders.delivery_key_id`, and has no
  key and no code path that opens one. Refusals never echo an envelope.
- An auction winner's order is created when the auction closes, before the
  winner has said where to ship, so it carries no address. The winner sends
  it with the payment method: `POST /v0/orders/{id}/payment-method` takes
  `delivery_address_envelope: {key_id, envelope}` (or, where the deployment
  still allows plaintext, `delivery_address`). The bind refuses an auction
  order that has none (409 `INVALID_STATE`, reason `delivery_address_required`),
  refuses a plaintext address when `REQUIRE_ENCRYPTED_DELIVERY_ADDRESS` is on
  (422 `INVALID_COMMAND`, reason `encrypted_delivery_required`), and refuses an
  address on any other order (reason `delivery_address_not_accepted`). The
  order projection carries `auction_aggregate_id` and
  `shipping_address_required`, true while the winner still owes the address.
  The envelope's authenticated data names the order id, which the seller's
  projection returns as `command_id`.
- The seller's single-order projection returns
  `delivery_address: {format: "hpke_v1", key_id, command_id, envelope}`.
  `command_id` is the checkout command id the buyer's client bound into the
  envelope's authenticated data. Orders that predate the change project
  `{format: "plaintext_v1", address}` until they reach a final state.
- The `delivery_address_purge` worker task clears `delivery_address`,
  `delivery_address_envelope` and `delivery_key_id` on every order in a final
  state (`completed`, `cancelled`, `refunded_external`, `closed`), 500 rows
  per tick, with no retention window. It needs no key. The seller's client
  decrypts and uses the address before the order ends; after that the
  service holds nothing.
- Migration 0057 adds the two columns, forbids a plaintext address and an
  envelope on one row, and clears the plaintext address from every order
  already in a final state.
- The service no longer calls Shippo. The seller's client buys labels
  directly with a device-local token, and records the tracking number
  through the existing ship command. The `/v0/sellers/me/shipping-config`
  and `/v0/orders/{id}/shipping/*` routes are gone. Rows already in
  `shipping_configs` are left in place and nothing reads them; the
  `orders.shipping_label` column stays as a read-only legacy projection.
  Deploy this release after the Shop version that buys labels itself.

The seller's pickup details (spot or address, instructions, availability
windows with their IANA zone) live ONLY in the service, sealed
(XChaCha20-Poly1305, AAD-bound, fresh nonce per seal) under
`PICKUP_DETAILS_ENCRYPTION_KEY`, written exclusively by
`pickup_details.set` (whole-payload upsert, monotonic per-listing version
through the surviving `listing_pickup_version_counters` row, CAS on
`expected_version`) and `pickup_details.clear` (retains only versions pinned
by paid, non-terminal orders; the counter survives so versions never
restart). No sync path can null them. Payment confirmation pins, per pickup
line and inside the receipt transaction, `version_at_payment` plus a sealed
snapshot (AAD = order id ‖ line index ‖ version) and the confirming adapter;
the buyer-only reveal (`GET /v1/orders/{id}/pickup-details`,
`Cache-Control: no-store`) serves that pinned snapshot — never current
details — gated on the durable payment fact (`receipt_id IS NOT NULL`),
refused on terminal orders, on sandbox deployments, and whenever the pinned
adapter was `payment.sandbox_advance` regardless of the current flag. The
seller's owner read is `GET /v1/listings/{aggregate_id}/pickup-details`.
The first successful reveal stamps `first_revealed_at`.

The order machine gains `ready_for_pickup`: `fulfillment.mark_ready`
(seller) arms it; `fulfillment.confirm_pickup` (buyer OR seller, from
`paid` or `ready_for_pickup`) writes the one-row `pickup_handovers` record
(server instant, confirming role) and emits `fulfillment.delivered` like a
shipped delivery; a seller-actor confirm is refused while a post-payment
terms change is unresolved. The buyer holds two unilateral exits via
`order.cancel_request` — a post-payment terms change (version bump or
clear) and the bounded withdrawal window (first reveal until handover
confirm; `mark_ready` does not close it) — which cancel immediately with
the distinct `order.cancelled_terms_change` event, release inventory
through approve's path, and are excluded whole (including any
`refund.recorded_external` leg) from the reputation worker's
`terminated_badly`. Pickup completions count toward reputation only on a
buyer-confirmed handover or a dispute-free auto-complete; the auto-complete
sweep coalesces the handover instant for pickup orders, locking
`FOR UPDATE OF orders`.

## Digital delivery

A listing offers digital delivery by listing `digital` among its
`fulfillmentMethods`, alone or beside `shipping` and `pickup`; an auction is
refused. After registering, the seller sets what buyers receive with
`digital_delivery.set` (and removes it with `digital_delivery.clear`). Five
kinds exist:

| Kind | What the buyer gets | Where it lives |
| --- | --- | --- |
| `file` | A download, up to `DIGITAL_DELIVERY_MAX_BYTES` | AES-256-GCM ciphertext made in the seller's browser and stored on the seller's homeserver; the service reads it once at `set` to check its length and BLAKE3, then keeps only the sealed file key. It never decrypts a file |
| `link` | A URL to open | Sealed in the service |
| `text` | A text or licence key, the same for every buyer | Sealed in the service. A per-buyer key list is not supported and is refused |
| `email` | The seller emails it after payment | The buyer's address, sealed, shown to the seller once paid |
| `message` | The seller sends it in messages | Nothing is stored; the seller marks it delivered |

Each `set` is a new version from a per-listing counter that survives `clear`,
so a version never repeats. Checkout refuses a listing with no current
deliverable (`digital_delivery_not_ready`) and asks for a delivery email only
when the order has an `email` line.

Payment confirmation is the one release step, so Bitcoin (Paykit) and PayPal
behave alike: nothing is released or shown to the seller while a payment is
pending. When the payment confirms, the receipt transaction pins each instant
line (`file`, `link`, `text`): it re-seals the current version's payload under
the order, so a later replacement or clear never changes what that buyer paid
for. If the deliverable is gone by then, the order is not paid and is marked
`refund_required`. A `sandbox_advance` confirmation never delivers.

The buyer opens one line at a time with
`GET /v1/orders/{id}/digital-delivery/{line_index}` (`Cache-Control:
no-store`, buyer only, 30 reads per minute per buyer and 10 per order). The
read re-checks entitlement and logs the access in the same transaction, and
fails closed if the log write fails; the log is the delivery evidence the
seller reads at `GET /v1/orders/{id}/digital-evidence`. The purchase keeps
downloading while the order stands, including `completed`. For `email` and
`message` lines the seller reads the address at
`GET /v1/orders/{id}/delivery-email` and records the delivery with
`fulfillment.deliver_digital` (`channel`: `email` or `message`); when every
manual line is marked the order moves `paid` to `delivered`. A seller who
never marks it does not strand the order: the `delivery_assume` sweep moves a
paid manual digital order `paid` to `delivered` once the order's frozen
delivered-assumption period (the seller's `deliveryAssumeDays`, else
`DELIVERY_ASSUME_DAYS`) has elapsed since the receipt was issued, flagging it
`delivery_assumed` and notifying both parties exactly as for a shipped order.
The `order_auto_complete` sweep then completes it after the frozen
auto-complete period (`autoCompleteDays`, else `AUTO_COMPLETE_DAYS`), which
starts the sealed address's retention clock. An open cancellation request or
a PayPal reversal outstanding on the order holds the assumption; instant-only
orders are delivered at payment and never wait for it. A background
worker deletes the sealed address after its retention (see
`BUYER_EMAIL_RETENTION_DAYS`), and `/ready` reports `unavailable` if the purge
falls behind.

The entitlement ends, and reads are refused, when the order is `cancelled`,
`refunded_external` or `closed` (`delivery_ended`). PayPal can also take the
money back after delivery, so a verified PayPal notification withholds the
purchase without waiting for a state change (the design's DD3):

- A `Reversed` notification, full or partial, sets `payment_reversed_at` on
  the order. While it is set, the buyer's download is refused
  (`payment_reversed`) and the seller can no longer read the buyer's email.
  A full reversal also moves the order to `refunded_external`. A
  `Canceled_Reversal` that restores everything clears the flag and the order
  and its downloads come back.
- A partial `Refunded` notification keeps the order's state, records the
  amount in `external_refund`, and withholds the purchase for good
  (`payment_refunded`): a refund is final. A full refund ends the order.
- Both participants are notified of each notification, and the order's
  `payment_reversed_at` and `external_refund` fields are how the Shop shows it.
  Details: [`docs/paypal-refund-ipn.md`](docs/paypal-refund-ipn.md).

A digital order never enters the physical lifecycle: ship, delivery
confirmation, return and offer commands are refused on it, and a seller's
refund after delivery is recorded straight from `delivered` or `completed`
(`refund.record_external`). A cancellation after the buyer opened an instant
line keeps that line sold. `/health` reports `digital_delivery_available` and
`digital_delivery_max_bytes`.

## Personal data held for delivery and payment

What the service keeps about buyers beyond the order itself, verified against
the code and pinned by tests.

**Buyer delivery email** (digital `email` lines only; `order_delivery_emails`).

- Encrypted at rest. The address is sealed with XChaCha20-Poly1305 under
  `DIGITAL_DELIVERY_ENCRYPTION_KEY`, bound to the order and buyer
  (`buyer-email/v1|{order_id}|{buyer_pubky}`), and exists nowhere else:
  command results, events, notifications, projections and logs never carry it
  (`delivery_email_sentinel_absent_everywhere` scans every text, JSON and
  bytea column and the captured logs). Without the key, checkout refuses
  digital lines.
- Purged on a schedule. The `delivery_email_purge` worker task sets
  `email_ciphertext` to `NULL` and stamps `purged_at`, keeping only `emailed_at`:
  30 days after `orders.ended_at` for an order that was paid (a receipt
  exists) and has reached `completed`, `cancelled`, `refunded_external` or
  `closed` (`BUYER_EMAIL_RETENTION_DAYS`), and 7 days after `ended_at` for an
  unpaid checkout that was cancelled or expired
  (`BUYER_EMAIL_UNPAID_RETENTION_DAYS`). `ended_at` is stamped when the order
  enters one of those states and later writes do not move it.
  `/ready` answers `delivery_email_purge_overdue` while an address is more
  than a day past its retention.
- The clock starts when the order ends. A paid order whose seller never marks
  the email delivered and never ends is a live order and keeps its sealed
  address until it does.

**PayPal records.** Only a postback-verified IPN is read, and only these
values are stored:

- the payment's `txn_id` (`orders.paypal_txn_id`, `fiat_transaction_ref`) and
  the seller's PayPal account it was verified against (`receiver_email` and
  `receiver_id`, `orders.paypal_receiver_email` and `paypal_receiver_id`);
- for a refund, reversal or canceled reversal: its `txn_id`, `parent_txn_id`,
  status and amount (`order_gateway_refunds`); one that cannot be applied yet
  waits in `gateway_refund_inbox` with ten allow-listed fields (`payment_status`,
  `txn_id`, `parent_txn_id`, `custom`, `mc_gross`, `mc_currency`,
  `receiver_email`, `business`, `receiver_id`, `reason_code`).

No payer detail (`payer_email`, `payer_id`, `first_name`, `last_name`,
`address_*`, `payer_status`, `residence_country` and the rest) is stored or
logged. `paypal_ipn_stores_nothing_about_the_payer` posts the full fixture
notifications, with all those fields, and scans every column and the logs for
them. The buyer-typed transaction reference of `POST
/v0/orders/{id}/fiat/mark-paid` is the buyer's own input, not IPN data.

## Drops (ADR-0026)

Timed, limited releases. A drop is registered by the convergent `drop.sync`
command from the seller-signed homeserver record, gates `inventory.reserve`
and `checkout.create` on its bound listings (schedule, remaining stock, and
the per-buyer cap, all inside the hold's transaction), and always derives
its state from server time plus the paid count: `announced` → `live` →
`ended_closed` / `ended_sold_out` / `ended_cancelled` (all ends terminal).

**Lock-at-claim, bounded.** Unlike ordinary listings ("only a payment locks
an item" — see Inventory semantics above), drop-bound checkout still holds
stock AND debits the drop's remaining/per-buyer counters AT CHECKOUT: the
FCFS race is the product. The claim is no longer forever, though — the
order's hold window arms immediately with `DROP_CLAIM_WINDOW_SECONDS`
(default 600), and a payment lock point re-arms it to its own (longer)
span. A claimed-but-never-paid order expires through the payment-window
sweep, which credits the drop (restocking a live drop and freeing the
buyer's per-drop cap), releases the listing unit, and cancels the order.
Editions and the sell-out transition on confirmation are unchanged.

**Single-line drop checkout.** Editions map one order to one unit, so a
checkout containing a drop-bound line must contain EXACTLY that one line
with quantity 1. Anything else is refused with 422 `INVALID_COMMAND` and
the pinned copy `A drop order is one unit of one listing per checkout.`

**Editions.** The exactly-once payment confirmation path (sandbox command,
Locks worker, fiat verify all converge there) increments the drop's
`paid_quantity` under the drop row lock; the new value IS the order's
edition — 1-based and gapless over paid orders, stored on
`orders.edition` and serialized on order projections alongside
`drop_aggregate_id`. When `paid_quantity` reaches `total_quantity` the drop
transitions to the terminal `ended_sold_out` and the seller receives a
`drop_sold_out` notification. Participants can fetch the signed edition
attestation for the order's receipt (see the claim table above).

**`drop.release_listings`** (seller only, `expected_revision`
compare-and-swap): allowed only from the three ended states; from
`announced`/`live` it is refused with 409 `INVALID_STATE` and the pinned
copy `Listings release only after the drop ends.` Releasing removes the
drop's listing bindings from gating consideration entirely
(`drop_listings.released`), so the listings sell again as ordinary open
inventory — until then an ended binding keeps refusing checkout so a
concluded drop's listing never quietly falls back to open sale.

**Public projection** — `GET /v0/drops/{seller_pubky}/{drop_id}`, no
session: `{ "drop": { seller_pubky, drop_id, aggregate_id, state, format,
starts_at, ends_at, stock_display, total_quantity, per_buyer_limit,
remaining, remaining_band, revision, server_time } }`. `server_time` is the
service clock now, in the canonical wire timestamp format — clients correct
countdowns from it. The read takes no row lock unless a scheduled
transition is due (announced past `startsAt`, or live past `endsAt`); then
it locks the drop row and applies the same lazy server-time transitions
gating uses, so a public read never shows `announced` after `startsAt`. A
`200` carries `Cache-Control: public, max-age=1`, kept below the Shop's 2 s
poll around `startsAt` because a cached copy skews the `server_time`
countdown correction by up to its age; a `404` or `500` carries
`Cache-Control: no-store`, so a drop that is synced right after a miss is
seen on the next read. Stock-display redaction is
SERVER-side; an exact count never leaves the service under bands/hidden:

| `stock_display` | `remaining` | `remaining_band` |
| --- | --- | --- |
| `exact` | the exact remaining count | `null` |
| `bands` | `null` | `plenty` (> 25% of total), `low` (≤ 25%), `last_few` (≤ 5%, minimum threshold one unit) |
| `hidden` | `null` | `null` |

**Seller projection** — `GET /v1/drops/{aggregate_id}` (Bearer session):
the seller only; absent and foreign drops are both 404. Full facts: exact
remaining, `paid_quantity`, distinct buyer count, state, schedule, caps,
`stock_display`, listing ids, revision, `server_time`.

**Buyer ready-check** — `GET /v1/drops/{aggregate_id}/me` (Bearer session):
any authenticated user reads their own per-drop counters:
`{ "purchased", "per_buyer_limit", "remaining_allowance" }` from
`drop_purchases` (purchased is zero without a counter row).

## Locks verification (task 4.5)

Real (non-sandbox) payments are settled outside this service: the buyer
submits a proof bundle to a Lock Server, Locks obtains an invoice from
Paykit Server, Bitcoin is observed, and the Locks verification lifecycle
completes. This service **independently verifies that completion** against
the Lock Server before advancing the order — a client claim can never
advance a payment (ADR-0019 §7).

- **Registration.** `payment.register_locks` (buyer only, payment in
  `awaiting_entitlement`) stores the encrypted correlation between the
  payment/order and the Locks lifecycle identity `{creator, bundle_id}`.
  The bundle id is the buyer's cryptographically random lifecycle handle —
  a bearer secret — and the lock resource's creator must equal the order's
  seller.   The correlation binds order id, buyer, creator, BLAKE3 lock
  resource hash, amount, asset, and payment policy version (`1`)
  (upstream-integration "Transaction-service correlation"). Registration
  is a payment lock point (see Inventory semantics above): it acquires the
  order's inventory hold and arms the payment window — the correlation
  window IS the hold window. It flips the payment to the `locks` adapter,
  which permanently refuses `payment.sandbox_advance`, and does **not**
  advance the payment state.
- **Digital delivery, no delivery address.** A Locks listing is a digital
  listing: a record that publishes `digital` with a `digitalLock` registers
  (`listing.register`, `listing.sync`) with the `digital` method, like any
  other digital listing. A listing with a lock publishes **exactly**
  `[digital]`: `listing.register` refuses a lock beside `shipping` or
  `pickup` with an `INVALID_COMMAND` issue on `payload.fulfillment_methods`,
  and `listing.sync` refuses such a record (or one that omits its methods,
  which default to shipping) as `INVALID_STATE` ("does not satisfy
  registration invariants") with the same issue; nothing is registered or
  healed. What it sells is delivered by the Lock Server, so
  its `checkout.create` line is `fulfillment: "digital"`: a digital order
  with no shipping charge and no delivery address (a checkout that presents
  one is refused `INVALID_COMMAND`). Unlike other digital listings it needs
  neither `DIGITAL_DELIVERY_ENCRYPTION_KEY` nor a seller deliverable, and
  its lock prices the item alone. A listing without a lock refuses
  `digital` unless it publishes it. Auctions stay shipping-only.
- **Lock resources.** A listing's `digitalLock.policyUri` is
  `[pubky://]<creator><prefix><LOCK_ID>.json`, where `<prefix>` is the
  fork's `/pub/locks.app/` or upstream Locks v0.1.0-rc10's
  `/pub/app.locks/` (pubky/locks#50). The service stores the resource
  under the prefix it was given and fetches the lock document from the
  seller's homeserver at that path; it never mints a path of its own, so
  the prefix needs no setting. The lock id is the content address and the
  lookup HMAC covers `{creator, bundle_id}` only, so neither depends on
  the prefix.
- **Correlation secrecy.** The bundle id is stored only as
  XChaCha20-Poly1305 ciphertext (random 24-byte nonce, payment id as
  associated data, so ciphertexts cannot be transplanted between rows);
  queries and uniqueness use an HMAC-SHA256 lookup token, never the raw
  value. The unique token also means one lifecycle identity can never
  correlate two orders. Nothing serializes the correlation row: no read
  projection, command result, event, outbox intent, log, or error carries
  the bundle id or lock resource
  (`bundle_and_lock_resource_never_leave_the_correlation_store` asserts
  this across every surface).
- **Verification.** The worker's `locks_verification` task claims due
  pending correlations (bounded batches, one lookup per correlation per
  `LOCKS_POLL_SECONDS`), decrypts the bundle id, and performs a real
  `POST /verification-task-lookups` against the configured Lock Server
  (pinned contract commit `ba49a777`; tests drive the same code path
  through a fake lifecycle client — production only ever constructs the
  HTTP client). Verification is a pure function of what Locks reports:
  - `completed` → the payment advances `awaiting_entitlement → confirmed`
    **exactly once**, enforced by the payment-state compare-and-swap plus
    the `events_one_payment_confirmed` unique index rather than
    application logic; the confirmation applies the same effects as the
    sandbox path (order `paid`, receipt issued, inventory converted,
    `payment_confirmed` outbox intent). Duplicate or reordered completions
    are harmless no-ops.
  - `pending` / `in_progress` / not-found / transport or status failure →
    the payment stays untouched and the correlation stays pending (Locks
    v1 leaves transport/status failures pending and exposes no terminal
    payment failure to the viewer).
  - `failed` / `expired` (the Locks task aged out) → recorded as an
    upstream fact and polling stops; the payment is **not** expired by it.
- **Expiry vs late completion.** The `payment_window` task sweeps the
  ORDER's hold window (`LOCKS_PAYMENT_WINDOW_SECONDS`, armed at
  registration — the correlation window IS the hold window): once it
  elapses on server time with the payment still `awaiting_entitlement`,
  the hold restocks, the payment moves to `expired`, and the order is
  cancelled with the stored reason `payment window elapsed` — deliberately
  separate from upstream failure. The correlation keeps polling after the
  window (bounded by the Lock Server's own task ageing), so a completion
  verified **after** marketplace expiry moves the payment
  `expired → manual_review` and is retained — never a confirmation of a
  dead order, never silently discarded. A verified completion whose order
  can no longer be confirmed (e.g. cancelled while pending, or a lapsed
  auction hold) also goes to `manual_review`.
- **History.** Every observed upstream status change and every marketplace
  action on it is appended to `payment_locks_observations` (append-only by
  trigger, statuses only — no bearer material), so late-completion and
  reconciliation history is durable.
- **Crash safety.** A verification claim only stamps `last_checked_at`;
  every effect commits in its own guarded transaction. A holder that dies
  between lookup and effect leaves the correlation pending, and any
  instance resumes it after the poll interval — bounded, abortable,
  resumable, with the same lease discipline as every other worker task.

## Authentication (task 3.2)

Session establishment verifies a **Pubky AuthToken** — a signed, time-bound
proof of key ownership produced by the Pubky auth flow. The original
challenge–response handshake required the client to sign a nonce, which no
real browser client can do: in the normal Pubky App flow the user signs in
through Pubky Ring (an external signer device) and the app never holds the
secret key. The challenge endpoint has been removed.

1. The app runs the Pubky auth flow; `awaitToken()` resolves after the user
   approves on their signer device and yields an `AuthToken`. The app sends
   `token.toBytes()` (canonical postcard binary) as the raw request body of
   `POST /v1/auth/sessions`.
2. The service verifies the bytes with the
   [`pubky-common`](https://crates.io/crates/pubky-common) crate (pinned at
   0.16.1), built from the same `pubky/pubky-homeserver` repository as the
   `@synonymdev/pubky` SDK, so client and server share one implementation of
   the token format. The token's public key becomes the authenticated actor.
   The token's verified capabilities are returned in the mint response and
   persisted, normalized, in `auth_sessions.capabilities`. They gate the
   inventory and seller automation routes (a session needs read and write on
   `/pub/pubky.app/marketplace-service/v1/`; otherwise `403
   capability_required`), and `/priv` access for `GET /v1/me/priv-keys`.
   Commands and the buyer/seller application routes are not capability gated:
   their authorization is per command and per resource, keyed on the actor
   pubky. See [`docs/api.md`](docs/api.md#authentication-and-sessions).

   Client and server do **not** have to be on the same version. The signature
   encoding was refactored between minor versions, so this was measured rather
   than assumed: a token signed with 0.8.0 verifies under 0.11.0, and a token
   signed with 0.11.0 verifies under the 0.8.0 SDK. Both directions round-trip.
   `AuthToken` (`pubky-common/src/auth/auth_token.rs`) is byte-identical
   between 0.11.0 and 0.16.1, so tokens from either SDK verify here.
   Reproduce by signing with `AuthToken::sign` under each version and
   cross-verifying with `AuthToken::verify` and the SDK's `AuthToken.verify`.
   This was measured when the client shipped SDK 0.8.0; the client
   (`pubky/pubky-marketplace`) now ships `@synonymdev/pubky` 0.16.0 (the live Shop
   still runs 0.11 JS until the next release), and the service pins 0.16.1.
3. Replay protection is enforced by the service, not assumed from the
   token: the token's `(public key, timestamp)` identity is recorded in
   `auth_token_uses`, so each token is single-use; its signing timestamp
   must fall within `AUTH_TOKEN_WINDOW_SECONDS` of the authoritative server
   clock (the library independently rejects tokens more than 3 minutes from
   system time). Server time is authoritative throughout.
4. On success the service returns its own opaque 32-byte session token
   (base64url) with a TTL; only the SHA-256 of the token is stored.
5. Every route except the public ones requires `Authorization: Bearer <token>`
   (`POST /v1/commands` is the main one); middleware
   resolves the actor pubky from the stored hash. There are no trust-me
   actor headers, and body fields can never select a different actor.

Establishing a marketplace session therefore requires a signer approval
(`awaitToken()` blocks on it). Whether that approval is folded into the
app's existing sign-in (one prompt granting marketplace capability to every
user) or kept as a separate first-transaction approval (scoped authority,
one extra prompt) is an open product decision, not a technical one.

CORS is restricted to the exact origins in `ALLOWED_ORIGINS`.

## Read projections

Role-scoped snake_case read models so a client can render state and supply
`expected_revision` on its next command. Every endpoint requires the same
Bearer session as `/v1/commands`; every projection carries the aggregate's
current revision (`server_revision` on listings, `revision` on offers,
orders, and payments).

| Endpoint | Scope | Body |
| --- | --- | --- |
| `GET /v1/listings/{aggregate_id}` | any authenticated user (public catalog data) | the listing/inventory projection: quantities, state, `server_revision`, unit price, sale format, and the auction state when present |
| `GET /v1/drops/{aggregate_id}` | the drop's seller only; absent and foreign drops are both 404 | the seller's full drop facts: exact remaining, `paid_quantity`, distinct buyer count, state, schedule, caps, `stock_display`, listing ids, revision, `server_time` |
| `GET /v1/drops/{aggregate_id}/me` | any authenticated user (their own counters only) | `{ "purchased", "per_buyer_limit", "remaining_allowance" }` from the caller's per-drop purchase row (zeros without one) |
| `GET /v1/offers` | offers where the caller is buyer or seller | `{ "offers": [...] }` |
| `GET /v1/orders` | orders where the caller is buyer or seller | `{ "orders": [...] }`, each order with its embedded `payment` projection plus the `shipment`, `return_request`, `external_refund`, and `reviews` sub-objects (client `orderSchema` field names) |
| `GET /v1/orders/{id}` | participants only; absent and foreign orders are both 404 | one order with the same embedded projections |
| `GET /v1/payments/{id}` | participants only | one payment projection |
| `GET /v1/receipts/{id}` | issuer (seller) and recipient (buyer) only | one receipt: ids, participants, `total`, `content_hash`, `issued_at` (client `receiptSchema` shape) |
| `GET /v1/notifications` | the recipient only | `{ "notifications": [...] }` — each with an optional `amount` (money JSON, null on rows delivered before amounts existed): present only where the recipient already sees the figure in a role-scoped projection — the offer amount on offer notifications, the auction's visible price on `outbid`/`auction_won`/`auction_ended` |

Object-level participation is enforced in the SQL `WHERE` clause: the
authenticated actor is bound as a query
parameter, so a non-participant's query cannot match another user's rows.
Single-object endpoints return 404 for absent **and** foreign aggregates,
so they do not reveal whether an aggregate exists.

**Pagination.** List endpoints accept `?limit=` between 1 and 200
(default 50); out-of-range values are rejected with 422
`INVALID_COMMAND`. Ordering is newest-first and stable:
`created_at DESC, id DESC`. There is no cursor yet; the bounded limit is
the whole convention.

**Redaction (ADR-0019 §8).** Projections never expose:

- `orders.delivery_address` — private delivery detail (legacy plaintext
  orders). The buyer receives it back once, in the checkout command result
  they authored; read projections and every post-purchase command result
  omit it for both participants. The seller of a paid shipped order reads it
  only in the single-order projection, as `delivery_address`.
- `orders.delivery_address_envelope` — the sealed address. Only the seller
  of a paid shipped order receives it, in the single-order projection; the
  buyer, any list and every command result omit it (see
  [Sealed delivery address](#sealed-delivery-address)).
- **The Locks bundle id and lock resource** (`access credentials or
  bundle_id` in ADR-0019 §8). The former plaintext
  `payments.locks_bundle_id` column has been **removed entirely**: the
  bundle id now exists only as XChaCha20-Poly1305 ciphertext in
  `payment_locks_correlations`, is queried only through an HMAC lookup
  token, and has no serialization path — no command result, read
  projection, event, outbox intent, notification, log, or error carries it
  (asserted by the redaction test in `tests/locks_test.rs`). The lock
  resource is stored only as a BLAKE3 hash.
- Reservation buyer identities and auction proxy-bid maximums; the
  auction's current `leader_pubky` stays visible because the auction state
  machine already exposes the leader to every bidder.

Participant-visible by decision (ambiguities resolved and documented):

- **Shipment carrier and tracking numbers** — participants need them to
  follow the shipment; they identify a parcel, not a person or address.
- **Notification amounts** — an optional money JSON on a delivered
  notification, carried only where the recipient already reads that exact
  figure in a role-scoped projection: the offer amount (both offer
  participants fetch the offer projection) and the auction's visible price
  (every bidder fetches the listing projection). Nothing address- or
  payment-bearing ever rides a notification payload.
- **Return `reason` and cancellation `reason`** — like offer messages,
  they are content exchanged between
  exactly the participants, served only to that same audience.
- **Review text** — reviews are authored for publication to the counter
  party; the projection audience is the two participants.

Offer `message`/`history` are returned: they are negotiation content
between exactly the two offer participants, the projection is readable by
exactly those two participants, and the offer command results already
return the same view to the same audience.

Not served, deliberately (no fabricated or empty-by-default reads):

- **Conversations/messages** — no durable tables exist; the `message.*`
  commands have not been ported.
- **Notification preferences** — the `notification.*` commands have not
  been ported.

## Background workers (tasks 3.4, 4.5)

One worker runtime (`crates/service/src/workers.rs`) drains 24 lease-guarded
server-time tasks: `reservation_expiry`, `drop_transitions`, `offer_expiry`,
`auction_close`, `outbox`, `locks_verification`, `paykit_verification`,
`payment_window`, `stat_attestations`, `delivery_autocomplete`,
`pickup_reseal`, `digital_reseal`, `priv_key_reseal`, `delivery_email_purge`,
`delivery_address_purge`, `read_rate_purge`, `pickup_retention`, `seller_confirmation_window`,
`manual_review_watch`, `paykit_resolve_delivery`, `fx_sampler`,
`refusal_audit_retention`, `listing_deletions` and `review_release`. Webhook delivery runs in a
separate worker (`spawn_webhook_worker` in `crates/service/src/automation.rs`;
see [`docs/api.md`](docs/api.md#webhooks)).

- **Leases.** Each task has a row in `worker_leases`; an instance takes (or
  renews) it with a conditional upsert and holds it for
  `WORKER_LEASE_SECONDS`. Two instances never drain the same task
  concurrently, and a holder that dies mid-lease is recovered by any
  instance once the lease lapses. Inside a task, due rows are additionally
  locked with `FOR UPDATE SKIP LOCKED`, so even a lease violation cannot
  double-process a row.
- **Outbox.** Intents are written in the command transaction (ADR-0019 §4)
  and delivered at least once: a claim stamps `lease_until` on the row, then
  each row's notification insert and `delivered_at` mark commit in one
  transaction. A crash between claim and delivery leaves the row leased but
  undelivered; it is redelivered after the lease lapses. The consumer side
  (the `notifications` table) dedups by `(event_id, recipient_pubky)`, so
  redelivery never duplicates the effect.
- **Review release.** The `review_release` task releases the double-blind
  reviews of every order whose window has ended (see
  [Reviews](docs/api.md#double-blind-reviews)): one conditional update per
  order, `WHERE reviews_released_at IS NULL`, so a replayed tick, a second
  instance or a late `review.create` racing it releases an order at most
  once. It runs whether or not `REVIEW_BLIND_WINDOW_DAYS` is still set.
- **Auction close on server time.** The worker closes active auctions whose
  `ends_at` has passed through the same code path as the seller's
  `auction.close` command; the `active` status guard plus the partial unique
  index `orders_one_winner_per_auction` make the close exactly-once even
  when the command and the worker race. Before closing, the same task starts
  `scheduled` auctions whose `starts_at` has passed (`auction.started`).
  Registration stores `scheduled` when the seller's start time is still
  ahead of the service clock; `auction.place_bid` and `auction.close` apply
  the same start first, keeping the revision the caller read.
- **Locks verification and the payment window.** The verification task
  (see "Locks verification" above) runs only when the deployment has Locks
  configured. The `payment_window` sweep always runs and covers EVERY armed
  hold window (Locks, fiat/bitcoin bind, sandbox, drop claims — see
  Inventory semantics above): a lapsed hold restocks, the payment expires,
  and the order cancels with the stored reason `payment window elapsed`.
  Both hold the same leases and carry the same crash-safety and
  at-most-once effect properties as the other tasks (payment CAS + unique
  event index).

## PostgreSQL invariants (ADR-0019 §4)

Each accepted command atomically persists the aggregate state/revision, one
immutable domain event, the idempotency result, and complete outbox intents,
in one transaction. Constraints enforce:

- one accepted result per actor + command id (`command_results` primary key);
- one winning order per auction (partial unique index on
  `orders.auction_aggregate_id`);
- at most one order per checkout command + seller group (partial unique index);
- non-negative inventory (`CHECK` on every quantity column) and quantity
  balance (`available + reserved + sold = total`);
- one payment-confirmed event per payment (partial unique index on `events`);
- one current revision per aggregate (compare-and-swap `UPDATE` plus
  `UNIQUE (aggregate_id, revision)` on `events`);
- the event log is append-only (`UPDATE`/`DELETE` rejected by trigger).

## State machine contract (task 3.3)

The canonical state/transition tables for listing, reservation, offer,
auction, order, payment, return, and drop live in one module:
`crates/domain/src/state_machines.rs`. The return machine is
the order's sub-state vocabulary the client's `orderSchema` already
declares (`returnRequest.state`). The machine-readable
artifact is emitted to `contracts/state-machines.json`:

```sh
cargo run -p marketplace-domain --bin emit-contracts
```

The domain test `contract_artifact_is_in_sync` fails whenever the JSON on
disk is stale, so the artifact cannot drift from the Rust tables. The
`pubky/pubky-marketplace` client validates its TypeScript contracts in
`src/libs/commerce` against this artifact in CI (cross-language contract
tests, per ADR-0022). Any artifact change here fails the client's
`state-machines.contract.test.ts` until the JSON is re-vendored — and the
addition of the `return` machine additionally requires the
client to register it in `commerceAggregateMachines` with matching state
enums, since that test asserts the aggregate sets match exactly.

## Postal region rules

`delivery_address.region` is required only for countries whose postal
system uses a subdivision. The table lives in
[`contracts/postal-region-rules.json`](contracts/postal-region-rules.json)
and is loaded by `crates/domain/src/postal.rs`.

| Kind | Countries | Stored `region` |
| --- | --- | --- |
| Closed ISO list | US, CA, AU | ISO 3166-2 suffix (`NY`, `ON`, `NSW`) |
| Required free text | AE, AR, BD, BR, CL, CN, CO, EG, ID, IN, KR, MX, MY, NG, PE, PH, PK, SA, TH, TR, VN, ZA | 1–100 characters |
| Optional | every other ISO 3166-1 alpha-2 code, including GB, DE, NL, FR, JP | empty string allowed; still serialized so clients never see `undefined` |

Shop currently duplicates this table in
`src/libs/commerce/postal-address.ts` (PR #77). **Shop follow-up:** vendor
or fetch `contracts/postal-region-rules.json` instead of maintaining a
second copy. US/CA/AU closed lists accept the suffix from Shop; full names
are accepted for one release, normalized to the suffix, and logged as
deprecated (country code only — no address values). `country_code` stays
required. Ship-from already allowed an empty region; label payloads omit
an empty `state` so packing slips and Shippo labels do not print a blank
or `"undefined"` subdivision line.

The cancellation port added one listing edge (`sold → available` via
`order.cancel_approve`), so re-vendoring for it also requires the client's
listing transition table to accept that edge.

The Locks verification port (task 4.5) changes the artifact again — the
client must re-vendor and its transition tables change (no state enums or
aggregate sets change):

- payment `awaiting_entitlement → confirmed` and
  `awaiting_entitlement → manual_review` gain the server trigger
  `locks_verification`;
- payment gains a new edge `expired → manual_review` via the server trigger
  `locks_late_completion`;
- order `pending_payment → paid` gains the server trigger
  `payment_confirmation` (the trigger the listing and reservation machines
  already declared);
- the payment aggregate's command list gains `payment.register_locks`.

## Resolved contract divergences

The prototype service enums and the client-side TypeScript contracts
(`src/libs/commerce/transaction-contracts.ts`, `state-machines.ts`) diverge.
This service is canonical and resolves them as follows:

| Area | Client contracts said | Canonical (this service) | Rationale |
| --- | --- | --- | --- |
| Wire casing | camelCase (prototype engine) | snake_case | ADR-0019 §3 shows the envelope in snake_case; the ADR is the authority. |
| Listing states | `draft/active/paused/reserved/sold/expired/removed` | `available/reserved/sold` | Catalog lifecycle belongs to seller-signed homeserver records; the transaction authority tracks inventory availability only (prototype engine semantics) and reads the record's state before it sells (see [Listing lifecycle](#listing-lifecycle)). |
| Reservation states | none (engine only had `active`) | `active/converted/released/expired` | The engine stored `expiresAt` but never swept it; this service implements server-time expiry, so the terminal states are modeled. |
| Payment states | missing `detected`, extra `created/window_elapsed/external_refund_required/refunded_external` | `awaiting_entitlement/detected/confirmed/expired/manual_review` | Prototype engine enums are the executable spec (ADR-0022). |
| Order states | extra `ready_for_pickup/return_in_transit/return_inspection`, missing `return_approved/return_received` | the prototype engine states minus `disputed` (operator adjudication removed) | Same; transitions are derived from the engine's command handlers. |
| Offer expiry | engine stored `expiresAt` and rejected late actions, but never swept | server-time sweep moves due offers to `expired` and emits `offer.expired` | Offer expiry is a real server-time transition here (offer machine already declared `offer_expiry`); the event kind is new because the engine had no sweep. |
| Auction close on server time | seller-only sandbox command | seller command **and** a worker close on `ends_at` | ADR-0019 makes server time authoritative; an auction must close without seller cooperation. Both paths share one implementation. |
| Auction close result | `auction_result` carried `listing` + `reservation` only | also creates and returns exactly one winning `order` + sandbox `payment` (delivery address null until provided) | The schema's `orders_one_winner_per_auction` invariant (ADR-0019 §4) is enforced at close; the order uses the final visible price and the checkout shipping policy so payment flows are uniform. |
| Notifications | synchronous in-memory append inside the command | outbox intents delivered at least once by the worker, deduped by `(event_id, recipient)` | ADR-0019 §4: side effects leave the command transaction only through the outbox. Notification preferences belong to the not-yet-ported `notification.*` commands. |
| Auth handshake | ed25519 challenge–response: the service issued a nonce for the client to sign | Pubky AuthToken verified with `pubky-common`; challenge endpoint removed | A browser client cannot sign — the secret key lives in the user's signer (Pubky Ring), and the SDK `Keypair` exposes no signing method. The AuthToken is the mechanism Pubky provides for exactly this proof; the service adds single-use and acceptance-window enforcement. |
| Payment projection | client `paymentSchema` requires `locks_bundle_id` | the field no longer exists: the bundle id lives only encrypted in `payment_locks_correlations` and is never serialized | ADR-0019 §8 forbids exposing `access credentials or bundle_id`. The client has made the field optional; it will never be sent. |
| Payment adapter | sandbox only (`adapter = 'sandbox'`) | `sandbox` or `locks`; `payment.register_locks` flips a payment to `locks`, which permanently refuses `payment.sandbox_advance` | ADR-0019 §7: a Locks-correlated payment advances only by independent server-side verification of a completed Locks result — never by a client claim. |
| Real payment advancement | none (the prototype had only the buyer-driven sandbox command) | server-driven: the worker verifies the Locks lifecycle (`POST /verification-task-lookups`) and confirms exactly once; the marketplace payment window expires on server time; a late completion goes to `manual_review` | ADR-0019 §7 and the upstream integration contract; Locks v1 leaves transport/status failures pending, so upstream trouble never expires a payment by itself. |
| Order delivery address | prototype order views carried the address | omitted from read projections (the client `orderSchema` never wanted it) | ADR-0019 §8: no private delivery details in exposed records. |
| Receipts | client `receiptSchema` + sandbox `GET /v1/receipts/{id}` | durable `receipts` table (append-only), issued exactly once on payment confirmation; `GET /v1/receipts/{id}` for issuer and recipient | The table and issuing transition now exist, so the endpoint serves real rows; `orders.receipt_id` is populated on confirmation. |
| Inventory after payment confirmation | prototype left the sold quantity in `reservedQuantity` forever | confirmation moves the order's line quantities reserved → sold and marks the winning auction reservation `converted` | The contract already declared listing `reserved → sold` and reservation `active → converted` under `payment_confirmation`; a durable ledger cannot hold quantities in a hold state forever. |
| Confirming a lapsed auction hold | impossible in the prototype (no reservation expiry) | refused with `INVALID_STATE` once the winner's 30-minute hold has expired on server time | This service sweeps expired holds back to `available`; confirming afterwards would sell inventory the order no longer holds. |
| Inventory release on cancelling a paid order | the prototype's `releaseOrderInventory` moved the quantity reserved → available (it never moved paid quantities out of `reservedQuantity`) | `order.cancel_approve` returns the cancelled order's sold quantities to available; the listing machine declares `sold → available` via `order.cancel_approve` | Payment confirmation here moves quantities reserved → sold (see above), so reversing a cancelled paid sale on the durable ledger necessarily reverses the sold quantities — otherwise every cancelled paid order would understock the listing forever. Same prototype semantics (held stock returns to available), expressed against the durable columns. |
| Cancelling a lapsed auction hold | impossible in the prototype (no reservation expiry) | the cancel succeeds; the reservation compare-and-swap (`active → released`) finds the hold already `expired`, so no quantities move | The sweep already returned the unit; releasing again would double-count it, and refusing would strand an order that can be neither paid (confirmation is refused, see above) nor cancelled. |
| Review uniqueness | application check on the in-memory order's `reviews` array | `reviews_one_per_order_role_listing` UNIQUE constraint on (order, role, listing); the review inserts precede the order revision CAS so a same-item race is decided by the constraint (`INVARIANT_VIOLATION`), a race on different items by the CAS (`REVISION_CONFLICT`), sequential duplicates by the ported check (`INVALID_STATE`) | ADR-0019 §4: one participant review per order/role/listing is a database invariant, not code. |
| Review editing | none (reviews were immutable) | `review.update` (this service only): the reviewer may revise ratings/text within 24 hours of their first review of the order, under the order's revision CAS with a `review.updated` event | The task's bounded edit window; the window is a documented policy constant (`REVIEW_EDIT_WINDOW_SECONDS`). No notification is emitted — the prototype had none to copy. |
| Review withdrawal | none (reviews were immutable) | `review.withdraw` (this service only): the author's own reviews of an order are marked `public_removed_at`, shown as `public_removed: true` to the author only and cleared by `review.update`, under the order's revision CAS with a `review.withdrawn` event | A removal must outlive the device that made it: the homeserver cannot tell a removed review from one overwritten by a later order, and the service already holds every review. No notification is emitted. |
| Conversations | client `conversationSchema` + sandbox `GET /v1/conversations` | no endpoint | No durable conversation/message tables exist; `message.*` commands are not ported. |
| Notification projection | client `notificationSchema` requires a positive `revision` | no `revision` field | Delivered notifications are immutable outbox-consumer rows, not revisioned aggregates; no `notification.*` command exists that would need an `expected_revision`. |

`processing` and `closed` (order) and `cancelled` (auction) exist in the
canonical enums but no current transition produces them; they are declared in
`unreachable_states` in the contract artifact and are reserved for future
commands, exactly as in the engine.

## Operations

- Structured logs via `tracing`; log fields are command kind, command id,
  aggregate id, revision, and outcome code — no payload contents, no
  addresses, no message bodies.
- `/health` is liveness plus a capability summary; `/ready` answers 503
  when the refusal-audit pipeline is not ready, the database probe fails, or
  buyer delivery emails are past their retention (the purge worker is not
  running). The service publishes no version endpoint. `/health` carries
  `commit`, the 12-character source SHA from the `GIT_COMMIT` image build arg
  (`unknown` when unset); the image digest and its
  `org.opencontainers.image.revision` label remain the authoritative record.
- The worker runtime runs in-process; every drain is guarded by leases plus
  `FOR UPDATE SKIP LOCKED`, so multiple service instances are safe.
