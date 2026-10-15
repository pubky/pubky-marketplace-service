# Seller automation API reference

This is the reference for the HTTP surface of the marketplace service that a
seller's own tooling (inventory sync, order export, webhook consumers) uses.
It is written from the code in `crates/service/src/` (`http.rs`,
`automation.rs`, `inventory.rs`, `auth.rs`, `config.rs`, `grant.rs`) and the
Rust tests next to it. Where the code and this page disagree, the code wins;
please fix the page.

The TypeScript client is the Shop SDK
[`@bitcoinerrorlog/pubky-shop`](https://www.npmjs.com/package/@bitcoinerrorlog/pubky-shop)
(v0.1.6 at the time of writing; source at
[`BitcoinErrorLog/pubky-shop`](https://github.com/BitcoinErrorLog/pubky-shop)).
Its README is the guide for using these endpoints from Node or a browser; this
page is the wire-level contract it is built against. For the command envelope
used by `POST /v1/commands` (buyer and seller state changes) see
[Command envelope](../README.md#command-envelope-adr-0019-3) in the README; it
is not repeated here.

Contents:

- [Conventions](#conventions)
- [Stability](#stability)
- [Route table](#route-table)
- [Authentication and sessions](#authentication-and-sessions)
- [Error envelopes](#error-envelopes)
- [Rate limits](#rate-limits)
- [Seller exports](#seller-exports)
- [Event feed](#event-feed)
- [Batch sync (`sync-many`)](#batch-sync-sync-many)
- [Inventory](#inventory)
- [Webhooks](#webhooks)
- [Store connectors](#store-connectors)
- [Operational endpoints](#operational-endpoints)
- [Grant-flow routes (Shop BFF)](#grant-flow-routes-shop-bff)
- [OpenAPI](#openapi)

## Conventions

- All request and response bodies are JSON (`content-type: application/json`)
  unless stated otherwise. `POST /v1/auth/sessions` is the one exception: its
  request body is raw bytes.
- Timestamps in responses are RFC 3339 UTC with millisecond precision
  (`2023-11-14T22:13:20.000Z`).
- Identifiers: a pubky is the z-base-32 public key string; a listing
  aggregate id is `listing:{seller_pubky}_{listing_id}`.
- Authenticated requests send `Authorization: Bearer <token>` where the token
  came from `POST /v1/auth/sessions`.
- CORS: only the exact origins in `ALLOWED_ORIGINS` are allowed. Allowed
  request headers are `Content-Type`, `Authorization` and `Idempotency-Key`;
  allowed methods are `GET`, `POST`, `PUT`, `PATCH`, `DELETE`. Server-to-server
  clients are not subject to CORS.
- Axum extractor failures (a body that is not valid JSON, a query parameter of
  the wrong type) are answered by the framework with a plain-text body and no
  envelope. Treat any non-JSON error body as a malformed request.

## Stability

This is a judgement from the code and its tests, not a published SemVer
promise. No consumer other than the Shop SDK is known.

| Surface | Status | Evidence |
| --- | --- | --- |
| `POST /v1/auth/sessions` token exchange and bearer format | Stable | Used by every authenticated surface; covered by `auth` tests. |
| Capability gate (inventory scope `/pub/pubky.app/marketplace-service/v1/` read+write) | Stable | Enforced by middleware and handlers. |
| Inventory adjust / projection (`schema_version: 1`) | Stable | Generated JSON Schemas (`inventory_adjust_schema`, `inventory_projection_schema`) and a checked contract sample at `contracts/samples/inventory.json`. |
| Webhook signing: header names, `v1=` HMAC framing, `Pubky-Webhook-Version: 1` | Stable | Pinned by `signature_framing_matches_the_cross_language_vector` and `outbound_headers_and_raw_body_are_exact_and_stable`. |
| Rate-limit behaviour and `429` envelope | Stable | Covered by automation tests. |
| Export and event envelopes (`seller_listing_export`, `seller_order_export`, `seller_event_feed`) | Experimental | They carry `schema_version: 1` but are built from `serde_json::Value`, with no generated schema or checked sample. Fields may be added. Record shapes inside `listings[]` follow the projection tables. |
| Event `data` object | Experimental | Always `{}` today; it is reserved for future per-event payloads. |
| Event `type` values | Experimental | The set is open and grows with the domain (see [Event feed](#event-feed)). Consumers must ignore unknown types. |
| Session `label` / `metadata` | Experimental | Informational only. |
| Grant-flow routes (`/v1/auth/grant-flows/*`) | Experimental, Shop-BFF internal | Not a public contract; see [below](#grant-flow-routes-shop-bff). |
| `/v0/*` routes | Experimental | The `v0` prefix is deliberate: payment, shipping and address routes can change. |

## Route table

"Session" means a valid bearer token. "Inventory capability" means a session
whose verified grant covers `/pub/pubky.app/marketplace-service/v1/` with read
and write. "Automation" means the inventory capability plus the
[automation rate limit](#rate-limits).

### Public (no bearer)

| Method | Path | Notes |
| --- | --- | --- |
| GET | `/health` | [Operational endpoints](#operational-endpoints) |
| GET | `/ready` | [Operational endpoints](#operational-endpoints) |
| POST | `/v1/auth/sessions` | Exchange an AuthToken for a bearer. |
| GET | `/v1/auth/sessions` | Needs a bearer with the inventory capability (checked in the handler); consumes the `session.admin` rate limit. |
| POST | `/v1/auth/grant-flows` | Grant-flow (Shop BFF). Only mounted when `MARKETPLACE_GRANT_FLOW_ENABLED` is set. |
| GET | `/v1/auth/grant-flows/{flow_id}` | Grant-flow (Shop BFF). |
| POST | `/v1/auth/grant-flows/{flow_id}/cancel` | Grant-flow (Shop BFF). |
| POST | `/v1/auth/grant-flows/{flow_id}/result-nonces` | Grant-flow (Shop BFF). |
| POST | `/v1/auth/grant-flows/{flow_id}/result-ticket` | Grant-flow (Shop BFF). |
| POST | `/v1/auth/grant-flows/{flow_id}/claim` | Grant-flow (Shop BFF). |
| GET | `/v0/sellers/{pubky}/payment-config` | Public payment configuration of a seller. |
| GET | `/v0/sellers/{pubky}/connector-status` | The stores a buyer may share an address with and each store's recipient public key. Public; answers an empty list while connectors are off. See [Store connectors](#store-connectors). |
| POST | `/v1/connectors/webhooks/woocommerce` | WooCommerce store webhook. Authenticity is the store's own signature. See [Store connectors](#store-connectors). |
| POST | `/v1/connectors/shopify/c/{connection_id}/webhooks` | Webhook from a merchant's own Shopify app, including the privacy topics; verified with that connection's app secret. |
| GET | `/v1/connectors/shopify/c/{connection_id}/launch` | The connection's App URL: verified with its app secret, then redirects to authorize. |
| GET | `/v1/connectors/shopify/c/{connection_id}/oauth/callback` | The connection's OAuth callback. |
| POST | `/v1/connectors/woocommerce/oauth/callback` | WooCommerce's key callback, `user_id` is the draft's nonce. |
| GET | `/v0/drops/{seller_pubky}/{drop_id}` | Public drop read. |
| GET | `/v0/listings/{seller_pubky}/{listing_id}/availability` | Stock state of one listing, state only. See [Public listing availability](#public-listing-availability). |
| POST | `/v0/address/suggest` | Address autocomplete. |
| POST | `/v0/paypal/ipn` | PayPal IPN callback. Unauthenticated by nature; authenticity is the PayPal postback. See [paypal-refund-ipn.md](paypal-refund-ipn.md). |

### Session

| Method | Path |
| --- | --- |
| POST | `/v1/commands` |
| GET | `/v1/listings/{aggregate_id}` |
| GET | `/v1/listings/{aggregate_id}/bids` |
| GET | `/v1/listings/{aggregate_id}/pickup-details` |
| GET | `/v1/listings/{aggregate_id}/digital-delivery` |
| GET | `/v1/drops/{aggregate_id}` |
| GET | `/v1/drops/{aggregate_id}/me` |
| GET | `/v1/offers` |
| GET | `/v1/orders` |
| GET | `/v1/orders/{id}` |
| GET | `/v1/orders/{id}/pickup-details` |
| GET | `/v1/orders/{id}/delivery-email` |
| GET | `/v1/orders/{id}/digital-evidence` |
| GET | `/v1/orders/{id}/digital-delivery/{line_index}` |
| GET | `/v1/orders/{id}/review-attestation` |
| GET | `/v1/sellers/{pubky}/band-consent` |
| GET | `/v1/payments/{id}` |
| GET | `/v1/receipts/{id}` |
| GET | `/v1/receipts/{id}/attestation` |
| GET | `/v1/receipts/{id}/edition-attestation` |
| GET | `/v1/notifications` |
| GET | `/v1/me/priv-keys` |
| POST | `/v1/me/priv-keys/release` |
| PUT, GET | `/v0/sellers/me/payment-config` |
| POST | `/v0/orders/{id}/payment-method` |
| POST | `/v0/orders/{id}/fiat/mark-paid` |
| POST | `/v0/orders/{id}/fiat/confirm-received` |
| POST | `/v0/orders/{id}/confirm-bitcoin-payment` |
| POST | `/v0/orders/{id}/bitcoin/resolve` |
| GET, POST | `/v1/connectors` |
| GET | `/v1/connectors/alerts` |
| POST | `/v1/connectors/alerts/{id}/resolve` |
| DELETE | `/v1/connectors/{id}` |
| GET | `/v1/connectors/{id}/catalog` |
| PUT | `/v1/connectors/{id}/credentials` |
| GET, PUT | `/v1/connectors/{id}/mappings` |
| GET, PUT | `/v1/connectors/{id}/selection` |
| PUT | `/v1/connectors/{id}/woocommerce/keys` |
| GET | `/v1/connectors/{id}/pushes` |
| POST | `/v1/connectors/{id}/pushes/{order}/retry` |

These are the buyer and seller application routes. Authorization on them is
per resource (participants only) and per command (keyed on the session's
actor pubky); they are described in the README sections for the feature each
belongs to. Reads of sealed material (`pickup-details`, `delivery-email`,
`digital-*`) answer `Cache-Control: no-store`.

### Inventory capability

| Method | Path | Notes |
| --- | --- | --- |
| POST | `/v1/inventory/adjust` | [Inventory](#inventory) |
| GET | `/v1/inventory/listings/{aggregate_id}` | [Inventory](#inventory) |

### Automation (inventory capability and automation rate limit)

| Method | Path | Rate class | Notes |
| --- | --- | --- | --- |
| PATCH | `/v1/auth/sessions/{id}` | `session.admin` | [Session management](#session-management) |
| DELETE | `/v1/auth/sessions/{id}` | `session.admin` | [Session management](#session-management) |
| GET | `/v1/sellers/{pubky}/listings` | `export.listings` | [Seller exports](#seller-exports) |
| GET | `/v1/listings/{seller}/{listing_id}` | `export.listings` | [Seller exports](#seller-exports) |
| GET | `/v1/sellers/{pubky}/orders` | `export.orders` | [Seller exports](#seller-exports) |
| GET | `/v1/sellers/{pubky}/events` | `events.read` | [Event feed](#event-feed) |
| POST | `/v1/listings/sync-many` | `listing.sync_many` | [Batch sync](#batch-sync-sync-many) |
| POST | `/v1/webhooks` | `webhook.admin` | [Webhooks](#webhooks) |
| POST | `/v1/webhooks/{id}/rotate` | `webhook.admin` | [Webhooks](#webhooks) |
| DELETE | `/v1/webhooks/{id}` | `webhook.admin` | [Webhooks](#webhooks) |

Middleware order on the automation routes is: bearer check (`401`), inventory
capability check (`403 capability_required`), automation rate limit (`429`),
then the handler. Request bodies are limited to 64 KiB (inventory: 4096 bytes).

## Authentication and sessions

### Create a session: `POST /v1/auth/sessions`

The request body is the raw postcard-encoded Pubky `AuthToken` bytes (not
JSON, not base64). The service verifies the token with `pubky-common` 0.16.1.
A token is accepted when its timestamp is within `AUTH_TOKEN_WINDOW_SECONDS`
(120) of server time (the library's own window is 180 s) and is single use:
the `(public key, timestamp)` identity is recorded in `auth_token_uses`, so a
replay is rejected.

`201 Created`:

```json
{
  "token": "<32 random bytes, base64url without padding>",
  "session_id": "<uuid>",
  "pubky": "<z-base-32 public key>",
  "capabilities": ["/pub/pubky.app/marketplace-service/v1/:rw"],
  "expires_at": "2026-10-13T08:00:00.000Z"
}
```

- The session lives `AUTH_SESSION_TTL_SECONDS` (86400) seconds. Only the
  SHA-256 of the token is stored; the token cannot be recovered later.
- `capabilities` is the normalized verified grant from the AuthToken. It is
  persisted in `auth_sessions.capabilities` and enforced by the inventory
  capability middleware and handlers. A token whose grant lacks the inventory
  scope still gets a session, but cannot use the inventory or automation
  routes (`403 capability_required`).
- The grant a seller tool should request for automation is
  `/pub/pubky.app/marketplace-service/v1/:rw`. The Shop sign-in flow requests
  `/pub/pubky.app/marketplace-service/v1/:rw,/priv/pubky.app/:rw` (the second
  scope is only needed for `GET /v1/me/priv-keys` and
  `POST /v1/me/priv-keys/release`, see [private data key
  custody](priv-key-custody.md)).
- Failures use the [auth envelope](#error-envelopes): `401` for an invalid,
  stale or replayed token.

### Using the token

Send `Authorization: Bearer <token>`. A missing, malformed, unknown, revoked
or expired token is `401` with `{"error":{"message":"..."}}`.

### Session management

`GET /v1/auth/sessions` (inventory capability; `session.admin` rate limit)
lists the caller's own sessions, newest first, at most 200, including revoked
ones:

```json
{
  "schema_version": 1,
  "sessions": [
    {
      "id": "<uuid>",
      "label": "warehouse-sync",
      "metadata": {},
      "capabilities": ["/pub/pubky.app/marketplace-service/v1/:rw"],
      "created_at": "2026-10-12T08:00:00.000Z",
      "expires_at": "2026-10-13T08:00:00.000Z",
      "last_used_at": "2026-10-12T09:30:00.000Z",
      "revoked_at": null
    }
  ]
}
```

`PATCH /v1/auth/sessions/{id}` sets a label and metadata on one of the
caller's sessions. Body (unknown fields are rejected):

```json
{ "label": "warehouse-sync", "metadata": { "host": "wh-01" } }
```

- `label` is optional; when present it must be non-empty, at most 80 bytes and
  contain no control characters.
- `metadata` defaults to `{}`; it must be a JSON object of at most 2048 bytes.
- Invalid input is `422`. A revoked session or a session owned by someone else
  is `404`.
- Success: `{"schema_version":1,"id":"<uuid>","label":"...","metadata":{...}}`.

`DELETE /v1/auth/sessions/{id}` revokes a session: `204`. It is idempotent for
the caller's own session. A foreign or unknown id is `404`. Revoking takes
effect immediately for every route.

Errors on these three routes use the minimal auth envelope
`{"error":{"message":"..."}}` (no `code`); only a missing capability returns
the coded `capability_required` envelope.

## Error envelopes

The service has four envelope shapes. Which one you get depends on where the
request failed, so clients should branch on HTTP status first, and parse
`error.code` when it is present.

1. Auth and session routes (bearer check, `create_session`, session
   `PATCH`/`DELETE`):

   ```json
   { "error": { "message": "invalid marketplace bearer" } }
   ```

2. Automation routes (exports, event feed, webhooks, sync-many):

   ```json
   { "ok": false, "error": { "code": "invalid_cursor", "message": "The cursor is invalid." } }
   ```

3. Inventory routes, and `capability_required` from the capability gate:

   ```json
   {
     "schema_version": 1,
     "ok": false,
     "error": {
       "code": "revision_conflict",
       "message": "...",
       "current_revision": 7
     }
   }
   ```

   `current_revision` appears only on revision conflicts. `rate_limited` adds
   `limit_class`.

4. Commands (`POST /v1/commands`, and each item inside `sync-many`):

   ```json
   {
     "ok": false,
     "error": { "code": "REVISION_CONFLICT", "message": "...", "current_revision": 3 }
   }
   ```

   Codes here are SCREAMING_SNAKE_CASE and map to HTTP status as follows.

   | Code | HTTP |
   | --- | --- |
   | `INVALID_COMMAND` | 422 |
   | `UNAUTHORIZED` | 403 |
   | `NOT_FOUND` | 404 |
   | `REVISION_CONFLICT`, `IDEMPOTENCY_CONFLICT`, `INSUFFICIENT_INVENTORY`, `INVARIANT_VIOLATION`, `OFFER_EXPIRED`, `INVALID_STATE`, `AUCTION_CLOSED`, `BID_TOO_LOW`, `SELLER_REGISTRATION_REQUIRED` | 409 |
   | `AWARD_EXPIRED`, `AWARD_ALREADY_CONVERTED`, `AWARD_QUANTITY_MISMATCH`, `AWARD_VARIANT_MISMATCH`, `AWARD_LISTING_CHANGED`, `AWARD_HOLD_MISSING` | 409 |
   | `LISTING_MODERATED`, `LISTING_NOT_ACTIVE` | 409 |
   | `UPSTREAM_UNAVAILABLE` | 503 (retriable) |

   A command error may also carry `issues` and `reason` fields.

   `LISTING_MODERATED` refuses `checkout.create`, `offer.checkout`,
   `offer.create`, `offer.counter`, `offer.accept`, `auction.place_bid` and
   the first `payment.prepare_locks` of a payment, and it refuses
   `POST /v0/orders/{id}/payment-method` (with `reason: "listing_moderated"`)
   on an order whose listing was moderated after the order was created,
   when the marketplace index reports the listing, or its seller, as
   moderated (see [Listing moderation](../README.md#listing-moderation)). It is a
   409, not a 403, because clients read a 403 on a command as a session problem.

   `LISTING_NOT_ACTIVE` refuses `checkout.create`, `offer.create`,
   `offer.counter`, `offer.accept` and `auction.place_bid` when the seller's
   current homeserver record for a listing is not `active`. `reason` is
   `listing_paused`, `listing_ended` or `listing_removed` (also used when the
   homeserver no longer has the record), or `listing_not_active` for any other
   state. `offer.checkout`, the first `payment.prepare_locks`, `auction.close`
   and `POST /v0/orders/{id}/payment-method` are refused only for
   `listing_removed`. When the homeserver cannot answer, those commands are
   refused with `UPSTREAM_UNAVAILABLE` instead (see
   [Listing lifecycle](../README.md#listing-lifecycle)).

### Automation error codes

| HTTP | `error.code` | Where |
| --- | --- | --- |
| 403 | `capability_required` | Any inventory or automation route without the inventory scope. |
| 403 | `seller_required` | Export and event routes when the `{pubky}` / `{seller}` path segment is not the session's pubky. |
| 404 | `not_found` | Unknown or deleted listing; unknown webhook. |
| 409 | `webhook_exists` | Webhook URL already registered for this seller. |
| 410 | `cursor_expired` | Event cursor older than the retention window. |
| 422 | `invalid_limit` | `limit` outside 1..200. |
| 422 | `invalid_cursor` | Cursor not decodable, or not issued for this seller / this feed. |
| 422 | `invalid_batch` | `sync-many` body invalid or outside 1..100 items. |
| 422 | `invalid_webhook_url` | Webhook URL fails the URL rules. |
| 429 | `rate_limited` | See [Rate limits](#rate-limits). |
| 429 | `webhook_endpoint_limit` | Seller already has `WEBHOOK_MAX_ENDPOINTS_PER_SELLER` (100) endpoints. |
| 502 | `listing_record_unavailable` | Single-listing export when the homeserver record cannot be read. |
| 500 | `internal` | Unexpected failure; safe to retry with backoff. |

The inventory error codes are listed with the [inventory endpoints](#inventory).

## Rate limits

Two independent limiters guard the authenticated surfaces, and the public
[listing availability](#public-listing-availability) read has its own
per-client limiter. None emits `X-RateLimit-*` headers; the only signal is the
`429` response and its `Retry-After` header.

### Automation limiter

A token bucket per `(actor pubky, endpoint class)`, stored in
`automation_rate_limits`.

- Refill rate: `AUTOMATION_RATE_LIMIT_PER_MINUTE` (default 120) per minute,
  continuously (`rate / 60` tokens per second).
- Burst capacity: rate times `AUTOMATION_RATE_LIMIT_BURST_MULTIPLIER` (default
  2, maximum 10), capped at 20000. A bucket starts full.
- Each request costs one token. A whole `sync-many` batch costs one token.
- Classes are derived from the request path:

  | Class | Matches |
  | --- | --- |
  | `session.admin` | Paths starting with `/v1/auth/sessions` (including `GET /v1/auth/sessions`, which spends the token in its handler) |
  | `listing.sync_many` | `/v1/listings/sync-many` |
  | `webhook.admin` | `/v1/webhooks...` |
  | `export.orders` | Paths ending in `/orders` |
  | `events.read` | Paths ending in `/events` |
  | `export.listings` | Everything else on the automation router |

  Classification is by path suffix, so a listing whose id literally ends in
  `orders` or `events` is accounted in that other class. This only affects
  which bucket is charged.
- When the bucket is empty: `429` with `Retry-After: <integer seconds>`
  (rounded up, minimum 1) and
  `{"ok":false,"error":{"code":"rate_limited","message":"..."}}`.

### Inventory limiter

Separate buckets in `inventory_rate_limits`, keyed by the session token hash,
with classes `inventory.adjust` and `inventory.read`.

| Variable | Default | Range |
| --- | --- | --- |
| `INVENTORY_AUTHENTICATED_RATE_LIMIT_PER_MINUTE` | 120 | 1..10000 |
| `INVENTORY_ANONYMOUS_RATE_LIMIT_PER_MINUTE` | 20 | |
| `INVENTORY_RATE_LIMIT_BURST_MULTIPLIER` | 2 | max 10 |

Capacity must be at most 20000. A `429` always carries `Retry-After: 1` and an
inventory-shaped envelope with `limit_class`. An exact idempotent replay of an
earlier adjustment returns its stored `200` before the limiter is consulted.

### Public availability limiter

`GET /v0/listings/{seller_pubky}/{listing_id}/availability` takes no bearer, so
it is limited per client address instead. The limiter is an in-memory token
bucket keyed by a salted hash of the address; nothing is persisted. Each
request costs one token and a bucket holds 30. The client address is the last
value of the trusted header named by `LISTING_AVAILABILITY_CLIENT_IP_HEADER`
(falling back to `ADDRESS_SEARCH_CLIENT_IP_HEADER`) when it is set and valid,
otherwise the socket peer. Set the header when the service sits behind a proxy,
or every client shares the proxy's bucket.

| Variable | Default | Range |
| --- | --- | --- |
| `LISTING_AVAILABILITY_CLIENT_PER_MINUTE` | 120 | 1..10000 |
| `LISTING_AVAILABILITY_CLIENT_IP_HEADER` | unset | a header name |

When the bucket is empty: `429` with `Retry-After: <integer seconds>`,
`Cache-Control: no-store` and
`{"ok":false,"error":{"code":"rate_limited","message":"..."}}`.

## Public listing availability

### `GET /v0/listings/{seller_pubky}/{listing_id}/availability`

No bearer. It answers one question for a viewer whose session has lapsed: is
another buyer holding this item. It exists because the full listing projection
(`GET /v1/listings/{aggregate_id}`) is a session read.

```json
{
  "aggregate_id": "listing:<pubky>_poster-01",
  "state": "reserved",
  "server_time": "2026-10-13T07:45:00.000Z"
}
```

- `state` is the listing's stock state, `available`, `reserved` or `sold`. It
  is the same `state` the projection carries. `reserved` means a buyer's
  in-flight payment holds the last units; it returns to `available` when the
  hold lapses or is cancelled.
- Nothing else is exposed: no quantity, buyer, order, hold deadline or price.
  An auction listing returns its stock state like any other.
- `404 NOT_FOUND` for an unknown or tombstoned listing (a listing whose seller
  deleted its record).
- A `200` carries `Cache-Control: public, max-age=5`. A `404` and a `429` carry
  `no-store`.
- Rate limited per client; see
  [Public availability limiter](#public-availability-limiter).

## Seller exports

All three read endpoints require that the path pubky equals the session's
pubky; otherwise `403 seller_required`. They are read-only and paginated.

Query parameters:

- `limit` — default 50, range 1..200, otherwise `422 invalid_limit`.
- `cursor` — opaque base64url value from a previous `next_cursor`; otherwise
  `422 invalid_cursor`.

Responses carry `ETag: "<sha256 hex of the JSON body>"` and honour
`If-None-Match`, answering `304` when the body is unchanged.

### `GET /v1/sellers/{pubky}/listings`

```json
{
  "schema_version": 1,
  "kind": "seller_listing_export",
  "seller_pubky": "<pubky>",
  "listings": [
    {
      "record_uri": "pubky://<pubky>/pub/pubky.app/marketplace/v1/listings/<listing_id>",
      "record": { "...": "homeserver listing record" },
      "record_bytes_base64": "<exact record bytes>",
      "record_sha256": "<hex>",
      "projection": { "...": "server-side projection: revision, stock, state" }
    }
  ],
  "next_cursor": null
}
```

- Ordered by `(revision, id)`; the cursor encodes `[revision, identity]`.
- Deleted listings are not exported.
- If the homeserver record cannot be read for an entry, that entry is
  `{"record_status":"unavailable","record_uri":"...","projection":{...}}` (or
  `"listing_id"` in place of the projection) instead of failing the page.
- `record_bytes_base64` and `record_sha256` let a consumer detect any change
  to the stored record without re-serializing it.
- `next_cursor` is `null` on the last page.

### `GET /v1/listings/{seller}/{listing_id}`

Same entry shape for one listing, wrapped as
`{"schema_version":1,"kind":"seller_listing","listing":{...}}`. `404 not_found`
if unknown or deleted; `502 listing_record_unavailable` if the homeserver
record cannot be read.

### `GET /v1/sellers/{pubky}/orders`

```json
{
  "schema_version": 1,
  "kind": "seller_order_export",
  "seller_pubky": "<pubky>",
  "orders": [ { "id": "...", "buyer_pubky": "...", "state": "paid" } ],
  "next_cursor": null
}
```

Ordered by `(revision, id)`. Each order has a fixed set of columns and
deliberately excludes payment details and delivery addresses:

`id`, `buyer_pubky`, `seller_pubky`, `revision`, `state`, `lines`,
`subtotal_minor`, `shipping_minor`, `total_minor`, `currency`, `exponent`,
`receipt_id`, `edition`, `drop_aggregate_id`,
`cancellation_reason`, `stock_held`, `hold_expires_at`, `shipment`,
`delivery_assumed`, `delivery_assume_days`, `auto_complete_days` (the seller's
periods frozen on the order, `null` when the seller left one unset),
`return_request`, `external_refund`, `fulfillment`,
`payment_method`, `created_at`, `updated_at`.

Money is in minor units with `currency` and `exponent`. Anything a seller
needs to ship an order (the delivery address) is read through the
participant-scoped application routes, not through this export.

## Event feed

`GET /v1/sellers/{pubky}/events?limit=&cursor=`

```json
{
  "schema_version": 1,
  "kind": "seller_event_feed",
  "seller_pubky": "<pubky>",
  "events": [
    {
      "schema_version": 1,
      "id": "018f47d2-6a27-7c23-a49d-6b21bb770120",
      "cursor": "AAAAAAAAAAc",
      "sequence": 7,
      "seller_pubky": "<pubky>",
      "aggregate_id": "listing:<pubky>_poster-01",
      "revision": 3,
      "type": "listing.synced",
      "occurred_at": "2023-11-14T22:13:19.000Z",
      "data": {}
    }
  ],
  "next_cursor": null
}
```

- The feed contains events on the seller's own listings, orders, offers and
  drops, ordered by `sequence` (global, strictly increasing).
- `cursor` is the sequence as 8 bytes big-endian, base64url without padding.
  Pass the `cursor` of the last event you processed as `?cursor=` to continue.
- `next_cursor` is only set when more events are immediately available (the
  page was full). On the last page it is `null`, so a poller must resume from
  the last event's own `cursor` (not from `next_cursor`) and keep the previous
  one when a page is empty.
- Retention is `EVENT_RETENTION_DAYS` (default 30). A cursor older than the
  window returns `410 cursor_expired`: perform a full export
  (`/listings` and `/orders`) and restart from the newest event. A cursor that
  was not issued for this seller is `422 invalid_cursor`.
- `type` is the domain event kind. Observed kinds include `listing.registered`,
  `listing.synced`, `inventory.adjusted`, `inventory.reserved`,
  `order.created`, `order.cancel_requested`, `order.cancelled`,
  `order.completed`, `payment.confirmed`, `payment.expired`,
  `fulfillment.shipped`, `fulfillment.delivered`, `offer.created`,
  `offer.accepted`, `auction.bid_placed`, `drop.started`, `return.requested`,
  `refund.recorded_external` and `review.created`. The list is not exhaustive
  and not guaranteed to be closed; ignore unknown types.
- `data` is always `{}` today. Treat the event as a change notification: fetch
  the aggregate (listing export, order export) for the new state, using
  `revision` to discard stale reads.

## Batch sync (`sync-many`)

`POST /v1/listings/sync-many` asks the service to re-read the homeserver
record for up to 100 listings and converge its projection. It is useful after
bulk edits to listing records.

```json
{
  "listings": [
    { "seller_pubky": "<pubky>", "listing_id": "poster-01" },
    { "seller_pubky": "<pubky>", "listing_id": "poster-02" }
  ]
}
```

Unknown fields are rejected. An empty list or more than 100 items is
`422 invalid_batch`.

The response is always `207 Multi-Status` when the request itself is valid:

```json
{
  "schema_version": 1,
  "kind": "listing.sync_many",
  "results": [
    {
      "seller_pubky": "<pubky>",
      "listing_id": "poster-01",
      "status": 200,
      "result": { "ok": true, "version": 1, "revision": 4, "event_ids": ["..."], "...": "..." }
    },
    {
      "seller_pubky": "<pubky>",
      "listing_id": "poster-02",
      "status": 404,
      "result": { "ok": false, "error": { "code": "NOT_FOUND", "message": "..." } }
    }
  ]
}
```

Semantics:

- Each item is an independent `listing.sync` command executed sequentially
  under the caller's actor with a fresh random `command_id` and
  `expected_revision: 0`. The batch is not atomic; partial success is normal.
  Inspect every `results[].status`.
- `status` is the HTTP status the same command would have returned on
  `POST /v1/commands`; `result` is the [command envelope](#error-envelopes)
  (shape 4) for failures.
- `listing.sync` is convergent. Any authenticated actor may sync any listing,
  and an existing aggregate is not a conflict. A record that is unchanged or
  older than the projection is a no-op that returns the current revision. A
  re-sync at the same record revision refreshes derived fields (shipping,
  fulfillment methods, digital lock) and bumps the aggregate revision with a
  `listing.synced` event.
- Failure modes per item: `NOT_FOUND` (record missing, or listing tombstoned
  once the seller's event stream confirms deletion); `UPSTREAM_UNAVAILABLE`
  (503; homeserver unreachable, retry later); `INVALID_STATE` (record cannot be
  interpreted, has an invalid digital lock, or fails registration invariants,
  including a digital lock whose methods are not exactly `[digital]`);
  `SELLER_REGISTRATION_REQUIRED` (auction listings need registration);
  `INVALID_COMMAND` (homeserver sync is disabled on this deployment).
- The whole batch costs one token of the `listing.sync_many` bucket, not one
  per item.

## Inventory

### `POST /v1/inventory/adjust`

Applies a signed stock delta to a listing, with optimistic concurrency and
idempotency. The session must hold the inventory capability and must own the
listing (`403 seller_ownership_required` otherwise).

```json
{
  "schema_version": 1,
  "kind": "inventory.adjust",
  "aggregate_id": "listing:<pubky>_poster-01",
  "listing_id": "poster-01",
  "expected_revision": 4,
  "delta": -2,
  "idempotency_key": "3f0c2f6e-8d6b-4f55-9a1c-2f1d8f0e7b11",
  "variant": { "sku": "POSTER-A2" },
  "external_ref": { "channel": "warehouse", "external_id": "adj-10492" }
}
```

Unknown fields are rejected (`422 invalid_request`).

| Field | Rule |
| --- | --- |
| `schema_version` | Must be `1`. |
| `kind` | Must be `inventory.adjust`. |
| `aggregate_id` | 1..256 chars; must equal `listing:{seller_pubky}_{listing_id}` for the session's pubky. |
| `listing_id` | 1..128 chars from `[A-Za-z0-9_.-]`. |
| `expected_revision` | Integer at least 1: the listing revision you last read. |
| `delta` | Non-zero integer within +/-1,000,000. |
| `idempotency_key` | UUID, version 1 through 8, RFC 4122 variant. |
| `variant` | Optional `{id?, sku?}`. It is an assertion against the listing's sole enabled variant, not a selector. |
| `external_ref` | Optional `{channel, external_id}`; `channel` matches `^[a-z0-9][a-z0-9._-]{0,31}$`, `external_id` at most 128 chars. |

Success, `200`:

```json
{
  "schema_version": 1,
  "ok": true,
  "result": {
    "aggregate_id": "listing:<pubky>_poster-01",
    "listing_id": "poster-01",
    "server_revision": 5,
    "event_id": "<uuid>",
    "stock": {
      "authority": "listing_total",
      "total": 18,
      "available": 15,
      "reserved": 3,
      "sold": 0
    }
  }
}
```

The emitted event kind is `inventory.adjusted`. Replaying the identical body
with the same `idempotency_key` returns the stored `200` unchanged. The same
key with a different body is `409 idempotency_conflict`.

Errors (inventory envelope):

| HTTP | `code` | Meaning |
| --- | --- | --- |
| 422 | `invalid_request` | Schema or field validation failed. |
| 403 | `capability_required` | Session lacks the inventory scope. |
| 403 | `seller_ownership_required` | Listing belongs to another seller. |
| 404 | `listing_not_found` | No such listing. |
| 409 | `revision_conflict` | `expected_revision` is stale; `current_revision` is returned. Re-read and retry. |
| 409 | `idempotency_conflict` | Key reused with a different body. |
| 409 | `external_reference_conflict` | The `external_ref` was already used (reported when it was already recorded and `expected_revision` is stale). |
| 409 | `variant_record_conflict` | The homeserver record's owner, listing id or revision does not match the registered listing. |
| 409 | `variant_authority_unsupported` | The homeserver record is unreadable as JSON or does not have exactly one valid enabled variant. |
| 409 | `variant_assertion_mismatch` | `variant.id` / `variant.sku` does not match the enabled variant. |
| 503 | `variant_lookup_unavailable` | Homeserver lookup failed; retry. |
| 409 | `inventory_invariant_violation` | The adjustment would break a stock invariant. |
| 409 | `stock_overflow` | Total would exceed the maximum. |
| 409 | `negative_stock` | Available stock would go below zero. |
| 429 | `rate_limited` | Inventory limiter; `Retry-After: 1`. |
| 500 | `internal` | Unexpected failure. |

### `GET /v1/inventory/listings/{aggregate_id}`

```json
{
  "schema_version": 1,
  "kind": "inventory_projection",
  "aggregate_id": "listing:<pubky>_poster-01",
  "seller_pubky": "<pubky>",
  "listing_id": "poster-01",
  "server_revision": 5,
  "stock": { "authority": "listing_total", "total": 18, "available": 15, "reserved": 3, "sold": 0 }
}
```

Any session holding the inventory capability may read any listing's
projection; this read is not restricted to the listing's seller. `404
listing_not_found` for an unknown listing. A sample of both documents is kept
in `contracts/samples/inventory.json`, and the JSON Schemas are generated from
the Rust types (`inventory_adjust_schema()` and
`inventory_projection_schema()` in `crates/service/src/inventory.rs`).

## Webhooks

Webhooks push the [event feed](#event-feed) entries to an HTTPS endpoint of
yours. Delivery is at least once; deduplicate on the event id.

### Manage endpoints

`POST /v1/webhooks` with `{"url":"https://hooks.example.com/pubky"}`:

`201 Created`

```json
{
  "schema_version": 1,
  "webhook": {
    "id": "<uuid>",
    "url": "https://hooks.example.com/pubky",
    "key_id": "<uuid>",
    "created_at": "2026-10-12T08:00:00.000Z"
  },
  "secret": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
}
```

- The `secret` is returned once. It is 32 random bytes, base64url without
  padding. Store it; it cannot be read again.
- URL rules: `https` only, port 443 only, no credentials, no fragment, and no
  private, loopback, link-local or other special-purpose literal IP. Hostnames
  are re-resolved at delivery time and every resolved address must be public
  (the connection is pinned to the checked address). Failures are
  `422 invalid_webhook_url`.
- `409 webhook_exists` if the URL is already registered for this seller;
  `429 webhook_endpoint_limit` at the per-seller cap (default 100).
- Delivery starts from the current newest event sequence; there is no
  backfill. Use the [event feed](#event-feed) to catch up on history.
- There is no endpoint to list webhooks. Record the `id` when you create one.

`POST /v1/webhooks/{id}/rotate` with body `{}` issues a new secret and key id:
`{"schema_version":1,"id":"<uuid>","key_id":"<uuid>","secret":"..."}`. Events
queued after the rotation are signed with the new key; there is no overlap
window for new events. Deliveries already queued keep the key snapshot they
were queued with (including their retries), so a receiver should keep
accepting the previous key until it stops seeing retries (up to
`WEBHOOK_MAX_AGE_HOURS`, default 24).

`DELETE /v1/webhooks/{id}` returns `204`, stops delivery and dead-letters any
pending deliveries. `404 not_found` for an unknown id.

### Delivery

For each event the service sends a `POST` to the endpoint:

- `content-type: application/json`
- `Pubky-Webhook-Version: 1`
- `Pubky-Webhook-Id: <event id (uuid)>`
- `Pubky-Webhook-Timestamp: <unix seconds at the time of this attempt>`
- `Pubky-Webhook-Key-Id: <key id (uuid) from create or rotate>`
- `Pubky-Webhook-Signature: v1=<hex hmac-sha256>`

The body is the event JSON exactly as returned in the event feed
(`schema_version`, `id`, `cursor`, `sequence`, `seller_pubky`, `aggregate_id`,
`revision`, `type`, `occurred_at`, `data`). The body is serialized from stored
JSON, so key order and spacing are not something to rely on: always verify the
signature over the raw request bytes before parsing.

Delivery behaviour:

- Timeout 5 s, no redirects, response body read up to 64 KiB. Any `2xx` is
  success; anything else (including timeouts and network errors) is retried.
- Retries use exponential backoff from `WEBHOOK_RETRY_BASE_SECONDS` (5),
  doubling, capped at `WEBHOOK_RETRY_MAX_SECONDS` (3600).
- After `WEBHOOK_MAX_ATTEMPTS` (12) attempts or `WEBHOOK_MAX_AGE_HOURS` (24)
  hours, the delivery is dead-lettered. Reconcile through the event feed.
- A seller may have at most `WEBHOOK_MAX_PENDING_PER_SELLER` (10000) pending
  deliveries; terminal rows are purged after `WEBHOOK_TERMINAL_RETENTION_DAYS`
  (30).
- Worker tuning: `WEBHOOK_WORKER_INTERVAL_SECONDS` (10),
  `WEBHOOK_LEASE_SECONDS` (30), `WEBHOOK_ENQUEUE_BATCH_SIZE` (100),
  `WEBHOOK_ENQUEUE_ENDPOINTS_PER_PASS` (100), `WEBHOOK_PURGE_BATCH_SIZE`
  (500). Deliveries are sent in batches of 50.
- Ordering across events is not guaranteed once retries are involved. Use
  `sequence`/`revision` to order and `id` to deduplicate.

### Verifying a signature

Let `secret` be the base64url-decoded value returned by create or rotate.

1. Derive the signing key: `key = SHA-256(secret_bytes)`.
2. Build the signing input as the concatenation of the ASCII string `v1.`, the
   `Pubky-Webhook-Timestamp` value, `.`, the `Pubky-Webhook-Id` value, `.`, and
   the raw body bytes:
   `"v1." + timestamp + "." + event_id + "." + raw_body`.
3. Compute `HMAC-SHA256(key, signing_input)` and hex-encode it in lowercase.
4. Compare with the header value after the `v1=` prefix, in constant time.
5. Reject the delivery when the timestamp differs from your clock by more than
   your tolerance (the reference verifier takes the skew as a parameter; 300 s
   is a reasonable default), when `Pubky-Webhook-Key-Id` is not the key id you
   expect, or when the signature header is not `v1=` plus 64 hex characters.
6. Deduplicate on the event id. If the same id arrives again with the same
   payload SHA-256, acknowledge it without re-applying. If the payload hash
   differs, quarantine it rather than overwriting (this is
   `classify_webhook_replay`'s `Apply` / `AlreadyApplied` /
   `QuarantineChangedPayload` decision).

Worked example. Values:

```text
secret (base64url)  AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8   (bytes 0x00..0x1f)
key = SHA-256(secret)  630dcd2966c4336691125448bbb25b4ff412a49c732db2c8abc1b8581bd710dd
Pubky-Webhook-Id         018f47d2-6a27-7c23-a49d-6b21bb770120
Pubky-Webhook-Timestamp  1700000000
raw body (one line, 346 bytes, no trailing newline):
{"schema_version":1,"id":"018f47d2-6a27-7c23-a49d-6b21bb770120","cursor":"AAAAAAAAAAc","sequence":7,"seller_pubky":"8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo","aggregate_id":"listing:8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo_poster-01","revision":3,"type":"listing.synced","occurred_at":"2023-11-14T22:13:19.000Z","data":{}}
```

Expected header:

```text
Pubky-Webhook-Signature: v1=b570385c60d1b1a63078e17f4e420d94d74ef09bf7f05c2b246c252eab8b1d5a
```

Python receiver:

```python
import base64, hashlib, hmac, time

def verify(secret_b64url: str, expected_key_id: str, headers: dict, raw: bytes,
           max_skew: int = 300, now: float | None = None) -> bool:
    secret = base64.urlsafe_b64decode(secret_b64url + "=" * (-len(secret_b64url) % 4))
    key = hashlib.sha256(secret).digest()
    ts = headers["Pubky-Webhook-Timestamp"]
    event_id = headers["Pubky-Webhook-Id"]
    if headers["Pubky-Webhook-Key-Id"] != expected_key_id:
        return False
    if abs(int(ts) - (now if now is not None else time.time())) > max_skew:
        return False
    sig = headers["Pubky-Webhook-Signature"]
    if not sig.startswith("v1=") or len(sig) != 3 + 64:
        return False
    signing_input = b"v1." + ts.encode() + b"." + event_id.encode() + b"." + raw
    expected = hmac.new(key, signing_input, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig[3:])
```

Node receiver:

```js
import { createHash, createHmac, timingSafeEqual } from "node:crypto";

export function verify(secretB64url, expectedKeyId, headers, raw, maxSkew = 300) {
  const key = createHash("sha256").update(Buffer.from(secretB64url, "base64url")).digest();
  const ts = headers["pubky-webhook-timestamp"];
  const eventId = headers["pubky-webhook-id"];
  if (headers["pubky-webhook-key-id"] !== expectedKeyId) return false;
  if (Math.abs(Number(ts) - Date.now() / 1000) > maxSkew) return false;
  const sig = headers["pubky-webhook-signature"] ?? "";
  if (!/^v1=[0-9a-f]{64}$/i.test(sig)) return false;
  const expected = createHmac("sha256", key)
    .update(Buffer.concat([Buffer.from(`v1.${ts}.${eventId}.`), raw]))
    .digest("hex");
  return timingSafeEqual(Buffer.from(expected), Buffer.from(sig.slice(3).toLowerCase()));
}
```

`raw` must be the unparsed request body bytes (for example `express.raw()`),
not a re-serialization of parsed JSON.

The framing is pinned by a cross-language vector in the Rust tests: signing key
bytes `0x00..0x1f` (used directly as the HMAC key), timestamp `1700000000`,
event id `018f47d2-6a27-7c23-a49d-6b21bb770120` and body `{"ok":true}` produce
`340177cd5af321108cc3b5322ae9abf0c24c093df2a60172c413a73ca9d14931`. The
reference verifier, `verify_webhook` in `crates/service/src/automation.rs`,
returns `Malformed`, `Stale`, `WrongKey` or `InvalidSignature` for the failure
cases above.

## Store connectors

Off unless `CONNECTORS_ENABLED=true`: every `/v1/connectors` route then
answers `404 connectors_disabled` and `/health` carries no `connectors` key.
The seller routes take the same session bearer as the other seller routes and
act only on the session's own connections (another seller's id answers like an
unknown one, `404 connection_not_found`). The provider routes are public and
authenticated by the provider's signature or the single-use connect state.
Credentials and app secrets are never returned and never logged. The full contract, with JSON
shapes and error codes, is `connector-contract.md` in the commerce
integrations workstream; the behaviour is described in
`crates/service/src/connectors/`.

A Shopify store is connected through an app the merchant creates in their own
Shopify account. `POST /v1/connectors` creates a **draft connection** (random
id, 24 hours to live) and returns the connection's own URLs (launch, OAuth
callback, webhooks) under `/v1/connectors/shopify/c/{connection_id}/`, built
from `CONNECTOR_PUBLIC_BASE_URL`. The merchant pastes the app's client id and
secret, which `PUT /v1/connectors/{id}/credentials` seals per connection
(associated data `connector-cred|v1|{seller}|shopify|{shop}|{connection_id}`);
that connection's launch, OAuth and webhook signatures are verified with that
secret only. After a disconnect or an uninstall the secret is kept for up to
30 days (or until `shop/redact` is handled) so Shopify's privacy webhooks can
still be verified, and deleted at once for a draft that never installed. No
Shopify values live in the environment.

Environment names: `CONNECTORS_ENABLED`, `CONNECTOR_PUBLIC_BASE_URL`,
`CONNECTOR_RETURN_URL`, `CONNECTOR_CREDENTIAL_ENCRYPTION_KEY`,
`CONNECTOR_CREDENTIAL_ENCRYPTION_KEY_PREVIOUS`, and the tuning values
`CONNECTOR_PUSH_LEASE_SECONDS`, `CONNECTOR_PROVIDER_TIMEOUT_SECONDS`,
`CONNECTOR_MAX_ATTEMPTS`, `CONNECTOR_BACKOFF_BASE_SECONDS`,
`CONNECTOR_BACKOFF_MAX_SECONDS`, `CONNECTOR_STOCK_POLL_SECONDS`,
`CONNECTOR_WORKER_TICK_MILLIS`. `SHOPIFY_PUBLIC_APP_CLIENT_ID` and
`SHOPIFY_PUBLIC_APP_CLIENT_SECRET` are reserved for Pubky's own public app and
are not read yet. The worker is the `connector-worker` binary of the same
image.

### WooCommerce

A WooCommerce store is connected with its REST v3 API over HTTPS. The store
must be on HTTPS with a valid certificate, WordPress must use "Post name" (or
any other pretty) permalinks, WooCommerce must be active (this was captured
against WooCommerce 11.2.1 on WordPress 7.1.3; that WooCommerce release needs
WordPress 7.0 or newer), and the store's own address (`home`) must be the one
the seller enters. `POST /v1/connectors` with `{"provider":"woocommerce",
"domain":"shop.example"}` probes the site and refuses, with the reason in
`error.message` (`422 invalid_request`, or `invalid_domain` for the address
itself), plain HTTP, an address that is not a public domain name, a site
without pretty permalinks, a site without WooCommerce, a blocked REST API and
a site whose WordPress address differs from the one entered. A store that
cannot be reached answers `502 provider_unavailable`.

Two ways to finish the connection:

* **One click.** The `authorize_url` in the `201` answer is
  `{site}/wc-auth/v1/authorize` with `app_name=Pubky Shop`,
  `scope=read_write`, `user_id` (the draft's single-use nonce), `return_url`
  (`CONNECTOR_RETURN_URL`, or `params.return_url` when that is unset) and
  `callback_url` (`POST /v1/connectors/woocommerce/oauth/callback`, which must
  be HTTPS). The merchant approves in WordPress; WooCommerce posts
  `{key_id, user_id, consumer_key, consumer_secret, key_permissions}` to the
  callback. A key without `read_write` is refused.
* **Pasted keys**, for a site that blocks the callback:
  `PUT /v1/connectors/{id}/woocommerce/keys` with
  `{"consumer_key":"ck_...","consumer_secret":"cs_..."}` on the seller's own
  draft. `200` is the same `connector_connected` answer as the callback.
  Errors: `422 invalid_keys` (wrong shape), `422 credentials_rejected` (the
  store refuses the pair), `422 keys_read_only`, `409 not_pending` (no draft
  waiting, or it expired), `409 shop_in_use`, `404 connection_not_found`,
  `502 provider_unavailable`.

Either way the pair is checked against the store before anything is stored.
A harmless authenticated read decides how it is sent: HTTP Basic (the normal
way) or, when the host drops the `Authorization` header and Basic is refused
while the same pair works in the query string, `consumer_key` and
`consumer_secret` query parameters (WooCommerce accepts them over HTTPS). The
choice is recorded with the credential. The webhook secret, generated per
connection, is sealed with the key pair, and `product.updated`,
`order.updated` and `order.deleted` webhooks named `Pubky Shop ...` are
registered at `/v1/connectors/webhooks/woocommerce`, replacing ones left by an
earlier connection. A connection that cannot register them is not connected.
Disconnecting deletes them from the store.

Catalog: products are read in pages of 50 with their variations; simple and
variable products only, published or private. A simple product is one variant
whose id is the product id. Prices are the store's current price in minor
units of `woocommerce_currency`; stock is the exact count where WooCommerce
manages it, `0` for an unmanaged product marked out of stock, and `null`
otherwise. Descriptions are plain text, images are the product's own.

Orders: a paid Pubky order becomes `POST /orders` with `set_paid: true`,
status `processing` (WooCommerce takes the stock), line items by product and
variation id with the Pubky prices, the Pubky order id as order meta
`pubky_order_id` and in a zero-value fee line (WooCommerce's order search
matches item names but not custom meta), a customer note with the order id and
a link to the Shop orders page, and no billing details. Lines use the
`zero-rate` tax class: WooCommerce adds tax on top of API totals by the store's
rates, whatever "prices include tax" says, so without this the order would
total more than the buyer paid. The merchant books VAT on Pubky sales from the
Pubky order. The shipping address is
set only when the buyer shared it. A cancel after the push sets the order to
`cancelled` (WooCommerce restocks); a Pubky refund adds a refund to the order
with `api_refund: false`, so the store's books match and no money moves.

Webhooks are verified with the base64 HMAC-SHA256 of the raw body in
`X-WC-Webhook-Signature`; WooCommerce's unsigned ping (`webhook_id=N`) is
acknowledged. A completed order with a tracking number in the common
shipment-tracking meta (`_wc_shipment_tracking_items`, the AfterShip and
`_tracking_number` keys) ships the Pubky order; a store-side cancel, trash or
refund raises a seller alert and never refunds. The 15-minute poll
(`CONNECTOR_STOCK_POLL_SECONDS`) reads the catalog for mapped products because
WordPress cron is unreliable.

## Operational endpoints

All three are unauthenticated, uncached and safe to poll.

### `GET /health`

Liveness plus a capability summary. Always `200` while the process is up:

```json
{
  "status": "ok",
  "offer_checkout": true,
  "pickup_available": true,
  "digital_delivery_available": true,
  "digital_delivery_max_bytes": 1048576,
  "priv_keys_available": false,
  "paykit_rail": { "bitcoin_offer_available": true, "age_seconds": 12 }
}
```

The capability fields reflect how this deployment is configured (for example
`priv_keys_available` is `true` only when the `/priv` key-encryption key is
set). `paykit_rail.bitcoin_offer_available` and `paykit_rail.age_seconds` describe
the last successful Paykit rail probe and are both `null` until one has
succeeded. A health check must not treat these flags as failures.

### `GET /ready`

Readiness for traffic. `200 {"status":"ready"}` when the service can serve;
`503 {"status":"unavailable"}` when it cannot. A `503` is returned when the
refusal-audit pipeline is not ready, the database probe fails, or buyer
delivery emails are held beyond their retention window (in that case the body
also carries `"reason":"delivery_email_purge_overdue"`).

The service publishes no version endpoint. Identify the running build from the
deploy record: the digest of the image that is deployed (Railway, or GCP once
the service runs there) and its `org.opencontainers.image.revision` label.

## Grant-flow routes (Shop BFF)

The `/v1/auth/grant-flows/*` routes exist for the Shop's backend-for-frontend
to run the Pubky grant (sign-in with approval on another device) exchange on
behalf of a browser, and to hand the resulting session to the browser without
exposing it to the relay. They are mounted only when
`MARKETPLACE_GRANT_FLOW_ENABLED` is set, are authenticated by signed Shop BFF
requests rather than a bearer token, and are not a public contract. Do not
build third-party clients on them; use `POST /v1/auth/sessions` instead.

Configuration (defaults in parentheses): `MARKETPLACE_GRANT_CLIENT_ID` and
`MARKETPLACE_GRANT_RELAY_URL` (https) are required when enabled;
`MARKETPLACE_GRANT_FLOW_TTL_SECONDS` (300, 60..600),
`MARKETPLACE_GRANT_VERIFY_LEASE_SECONDS` (30, 10..60),
`MARKETPLACE_GRANT_EXCHANGE_TIMEOUT_SECONDS` (30, 1..120),
`MARKETPLACE_GRANT_RELAY_POLL_MILLISECONDS` (1000, 250..5000),
`MARKETPLACE_GRANT_MAX_LIVE_FLOWS` (1000),
`MARKETPLACE_GRANT_WORKER_BATCH_SIZE` (25),
`MARKETPLACE_GRANT_REAPER_BATCH_SIZE` (100),
`MARKETPLACE_GRANT_TERMINAL_RETENTION_SECONDS` (86400),
`MARKETPLACE_GRANT_CREATE_PER_IP_PER_MINUTE` (10),
`MARKETPLACE_GRANT_CREATE_PER_PUBKY_PER_MINUTE` (5),
`MARKETPLACE_GRANT_STATUS_PER_FLOW_PER_MINUTE` (60),
`MARKETPLACE_GRANT_RESULT_PER_PRINCIPAL_PER_MINUTE` (30) and
`SHOP_GRANT_ASSERTION_ISSUER` (`https://shop.pubky.app`).

Key material (all required when enabled; the previous-key pairs are optional
and only used during rotation): `GRANT_FLOW_ENCRYPTION_KEY_B64` with
`GRANT_FLOW_KEY_EPOCH`, `GRANT_FLOW_PREVIOUS_ENCRYPTION_KEY_B64` with
`GRANT_FLOW_PREVIOUS_KEY_EPOCH`, `GRANT_RESULT_HMAC_ROOT_B64` with
`GRANT_RESULT_HMAC_KEY_EPOCH`, `GRANT_RESULT_HMAC_PREVIOUS_ROOT_B64` with
`GRANT_RESULT_HMAC_PREVIOUS_KEY_EPOCH`, `SHOP_GRANT_ASSERTION_VERIFYING_KEYS_JSON`
and `SHOP_BFF_REQUEST_VERIFYING_KEYS_JSON`.

## OpenAPI

There is no OpenAPI document. Generating one mechanically is not possible
today: only some endpoints have typed schemas, and they are partial.

- Inventory adjust and projection have schemas generated with `schemars`
  (`inventory_adjust_schema()` / `inventory_projection_schema()`) and a checked
  sample at `contracts/samples/inventory.json`.
- The two shared-manual seller review endpoints
  (`POST /v0/orders/{id}/confirm-bitcoin-payment` and
  `POST /v0/orders/{id}/bitcoin/resolve`) have generated request schemas and a
  reason catalog in `contracts/endpoints.json`, checked by
  `crates/service/tests/contracts_test.rs`.
- Every other route, including all of the automation routes in this document,
  builds its responses with `serde_json::Value`, so a generated file would
  either be hand-written (and drift) or empty of detail.

Follow-up if an OpenAPI file is wanted: introduce typed request and response
structs for the export, event, webhook and session routes, derive their
schemas the same way, emit a document alongside `contracts/endpoints.json`, and
fail a test when the checked-in file differs from the generated one (the
existing contract snapshot tests are the pattern). In the meantime, the route
table above is checked against `crates/service/src/http.rs` by
`crates/service/tests/api_docs_test.rs`, so the set of paths cannot silently
drift; the request and response shapes are not checked.
