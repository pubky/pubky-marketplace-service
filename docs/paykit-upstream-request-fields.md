# Upstream Paykit prepare request: amount and description

Status: consumer notes for `PAYKIT_SERVER_API=upstream` against paykit-server
`v0.1.0-rc13` (Bitkit 2.6 RC run, `pubky/pubky-marketplace#123`).

## What the service sends

`POST {PAYKIT_SERVER_URL}/marketplace/payment-requests/prepare`, signed
(`x-paykit-signature`), body with exactly these members:

| Member                   | Value                                                            |
| ------------------------ | ---------------------------------------------------------------- |
| `amount_sats`            | The order's bitcoin price in satoshis.                           |
| `creator`                | The seller's key (`pubky<key>`).                                 |
| `reader`                 | The buyer's key (`pubky<key>`).                                  |
| `reference`              | `upstream_attempt_reference(order_id, attempt)`, a UUID v4.      |
| `operation_id`           | `marketplace-payment:{reference}:{attempt}`.                     |
| `payment_window_seconds` | The payment window this attempt arms.                            |

paykit-server rc13 reads the same six members
(`paykit-server/src/http/marketplace_preparation.rs`, `PrepareBody`) and binds
exactly those six into the idempotency fingerprint (`request_binding` in
`application/marketplace_preparation.rs`). It rejects a `reference` that is not
a canonical lowercase UUID v4, so a human-readable order reference cannot go in
that member.

## The amount is the exact price

Upstream does not add a unique amount. The prepare response carries
`{ invoice_id, state, total_sats, prepare_expires_at }` and no `nonce_sats`;
upstream gives each request a fresh derived address, so there is nothing to
disambiguate by amount. `PaykitClient::create_payment_request` accepts the
response only when `state == "prepared"` and `total_sats == amount_sats`, so an
upstream order's `paykit_total_sats` is always the price and the payment code
the fork mode draws (1–999 sats) is always zero.

Only fork mode (`PAYKIT_SERVER_API=fork`) mints `nonce_sats` and returns
`total_sats = amount_sats + nonce_sats`. Shop clients must not describe a
1–999 sat addition unless the deployment runs the fork API; the Shop's checkout
note is shown only for `paykitServerApi=fork`.

## The Bitkit request has no description

In the Bitkit 2.6 RC run the request shows `For: Not specified`. The prepare
route has no member that becomes the request's description: the payment request
terms paykit-server builds from a prepare (`PaymentRequestTerms` in
`paykit-lib`, rev `03d4f05`) are set from the amount, the payee reference, the
endpoint, the required app id and that endpoint's address, and no `metadata`
entry. `PaymentRequestTerms` has an application `metadata` map, but the prepare
route never fills it. An extra member in the body would not reach the terms and
is not part of the signed binding, so the service cannot supply a description
today.

## Proposal for paykit-server

Add an optional, plain-text `description` to the prepare body:

- Closed, short (for example up to 140 characters, no control characters), so
  it stays inside the signed canonical binding and the idempotency fingerprint.
- Carried into the payment request terms' application metadata, where Bitkit
  can render it as the request's purpose.
- Absent means the request is built exactly as today, so existing callers keep
  working.

Marketplace would send a generic label with the order's short reference, such
as `Pubky Shop order 8F3A91`, and never the listing title, so the Paykit server
and the buyer's wallet learn no more about the purchase than the buyer already
knows. The wire change is one optional member; the service change is one line
in `create_payment_request`, gated on the field being accepted by the
configured server.

Open question for Bitkit: which application metadata key renders as `For:`.
