# Changelog

The service serves no version endpoint and is not tagged. A release is identified by its commit and by the digest of its image (`ghcr.io/<owner>/pubky-marketplace-service`, tag `sha-<commit>`), and each entry below names the Shop release it ships with. Image pins move in a closeout pull request, see [`.railway/README.md`](.railway/README.md#image-pins).

## For Shop v0.6.54 (draft)

> **Draft. The cut waits for [Shop #198](https://github.com/pubky/pubky-marketplace/pull/198) and [pubky-app #2821](https://github.com/pubky/pubky-app/pull/2821) to merge.** They are the embedded mount at `pubky.app/shop` and ship together with this service build; both are still open. Remove this notice and record the release commit and image digest at the cut.

Range: `109195a` (the service of Shop v0.6.53) to `main` at `d8c47a8`. Pull requests: #92, #109, #117, #118, #119, #120, #121, #122, #123, #124, #125.

### Added

- **Auction winner delivery address.** An auction order is created at close with no address, so `POST /v0/orders/{id}/payment-method` now takes `delivery_address_envelope` (or a plaintext `delivery_address` where the deployment still allows it) for an auction order. The bind refuses an auction order without one (409 `delivery_address_required`), refuses a plaintext address when `REQUIRE_ENCRYPTED_DELIVERY_ADDRESS` is on (422 `encrypted_delivery_required`), and refuses an address on any other order (`delivery_address_not_accepted`). The order projection gains `auction_aggregate_id` and `shipping_address_required`. (#122)
- **`paykit_expires_at` on order reads.** Once an upstream Paykit request is activated, reads expose its own deadline (null before activation), so the Shop shows the deadline the wallet shows. (#122)
- **`/priv` Phase 4, service half.** `POST /v1/me/priv-keys/release` lets an owner take custody of their data keys: it deletes the owner's sealed keys in one transaction, but only when the request names exactly the key ids the service holds (otherwise 409 `key_set_changed`). `GET /v1/me/priv-keys` then answers 409 `custody_released` for that owner and never mints a replacement. Migration `0055` adds the `user_priv_key_custody_releases` tombstone. No Shop build calls the release endpoint until the Shop's scoped-keys switch is on (gate closed at launch, decision EK1), so deploying this changes nothing for users. See [`docs/priv-key-custody.md`](docs/priv-key-custody.md). (#92)

### Changed

- **Auction winner deadline.** The winner order's `hold_expires_at` is the winning reservation's end (30 minutes from the close) from creation, and a bind no longer replaces it. A bind after the reservation ended is refused (409 `auction_window_ended`). (#122)
- **Auction winners pay with Bitcoin on upstream Paykit.** The activation guard read the bind stock hold, which an auction order never takes, so the request was voided before it reached the wallet. It now reads the winner's active reservation. The upstream payment window is capped at the reservation (reservation end minus 60 seconds), and a winner with under 5 minutes left is refused the Bitcoin bind (`auction_payment_window_too_short`). (#121)
- **Activation retry room.** The cap leaves room for one activation retry inside the winner's window. (#123, tests in #124)
- **Locks listings are digital end to end.** A listing with a Locks lock must publish exactly `[digital]`: `listing.register` refuses anything else with an `INVALID_COMMAND` issue on `payload.fulfillment_methods`, and `listing.sync` refuses such a record as `INVALID_STATE`. `checkout.create` accepts `fulfillment: "digital"` on a Locks line: a digital order with no shipping, no address, and no need for `DIGITAL_DELIVERY_ENCRYPTION_KEY` or a seller deliverable. Locks listings registered earlier with shipping are not rewritten and must be recreated. (#122)
- **`PUBLIC_APP_ORIGIN` accepts the Shop's mount path** (`https://pubky.app/shop`): an http(s) origin plus an optional path, no query, fragment, credentials or dot segments. PayPal `return` and `cancel_return` then land on `{value}/marketplace/orders`. Unchanged for a bare origin. (#117)
- **Pubky crates 0.16.1.** `pubky` and `pubky-common` move from `=0.11.0` to `=0.16.1`. `AuthToken` is byte-identical between the two, so tokens from either SDK verify. The service's own grant flow stays on legacy shared-secret bare grants. (#120)

### Fixed

- **Unpaid auction orders expire.** An auction order in `pending_payment` whose winning reservation lapsed is cancelled ("payment window elapsed") and its payment moved to `expired`; before, only a buyer cancel closed it. Money that arrives after the cancel re-takes the unit if it is free and completes the order once, otherwise the payment goes to `manual_review` with `refund_required`. An on-time payment first seen between the reservation sweep and the cancel takes the same path instead of parking in `manual_review` with no reason, and a failed re-acquire can no longer leave the winning reservation active (which sold the item twice). (#125)

### Removed

- **Server-side Shippo.** The `/v0/sellers/me/shipping-config` and `/v0/orders/{id}/shipping/*` routes and `SHIPPO_API_BASE` are gone: sellers buy labels from their own device (Shop v0.6.53 and later). Rows in `shipping_configs` stay and nothing reads them. Deploy only after a Shop that buys labels itself. (#109)

### Documentation and tests

- `docs/paykit-upstream-request-fields.md`: what the upstream prepare request carries (an exact-price amount, no description). (#119)
- Restores `main`'s test build and closes the Kimi P2/P3 notes on #115 and #117. (#118)
- An auction activation retry inside the slack is proven to succeed. (#124)

### Operator notes

- **Migration:** `0055` is the only new migration. `main` already carries `0056` to `0059`, so it applies after them; sqlx applies an unrecorded migration whatever its number, and it ran on the staging database that already held `0059`. With it the catalog is gapless, `0001` to `0059`.
- **Roll forward, never back, once any owner has released** custody of their `/priv` keys: a binary that predates `0055` does not know the tombstone and would mint a new key for a released owner ([`docs/priv-key-custody.md`](docs/priv-key-custody.md), [`docs/migrations.md`](docs/migrations.md)).
- **Configuration:** no new required variable. `SHIPPO_API_BASE` is no longer read. `PUBLIC_APP_ORIGIN` for the embedded mount is `<app origin>/shop`; for the other origin settings see the Shop's `docs/launch/shop-embedding.md`. If the grant flow is enabled, `SHOP_GRANT_ASSERTION_ISSUER` (default `https://shop.pubky.app`) must name the origin the Shop is served from.
- **Deploy order:** service, then Shop.
- **Behind a flag, off:** `REQUIRE_ENCRYPTED_DELIVERY_ADDRESS` and `USDT_PAYMENTS_ENABLED` are unchanged; `/priv` custody release is gated by the Shop.
