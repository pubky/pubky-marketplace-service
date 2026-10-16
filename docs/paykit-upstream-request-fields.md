# Upstream Paykit prepare request: amount and description

Status: consumer contract for `PAYKIT_SERVER_API=upstream`. Enable only after
the target paykit-server supports the signed optional `description` field.

## What the service sends

`POST {PAYKIT_SERVER_URL}/marketplace/payment-requests/prepare`, signed with
`x-paykit-signature`, has these seven members for new Marketplace attempts:

| Member                   | Value                                                               |
| ------------------------ | ------------------------------------------------------------------- |
| `amount_sats`            | The order's bitcoin price in satoshis.                              |
| `creator`                | The seller's key (`pubky<key>`).                                    |
| `description`            | `Pubky Shop order ` plus the first six uppercase hex order-ID chars.|
| `operation_id`           | `marketplace-payment:{reference}:{attempt}`.                        |
| `payment_window_seconds` | The payment window this attempt arms.                               |
| `reader`                 | The buyer's key (`pubky<key>`).                                     |
| `reference`              | `upstream_attempt_reference(order_id, attempt)`, a UUID v4.         |

For order `8f3a91b2-1111-4111-8111-111111111111`, every attempt uses
`Pubky Shop order 8F3A91`. The label comes from the stable order UUID, not the
attempt reference. It contains no listing title or seller name. Its six-character
suffix is a display hint, not a unique ID or lookup key.

The complete canonical JSON body, including `description`, is covered by the
upstream HTTP signature. paykit-server includes a present description in its
immutable request binding and writes it unchanged to
`PaymentRequestTerms.metadata["note"]`. Bitkit uses that metadata key for its
`For:` field. A changed description for an existing `(creator, operation_id)` is
a conflict; an exact retry replays the original preparation.

The Fork request remains unchanged. It does not receive `description`.

## The amount is the exact price

Upstream does not add a unique amount. The prepare response carries
`{ invoice_id, state, total_sats, prepare_expires_at }` and no `nonce_sats`.
Each request has a fresh derived address, so amount does not disambiguate it.
`PaykitClient::create_payment_request` accepts the response only when
`state == "prepared"` and `total_sats == amount_sats`; therefore an upstream
order's `paykit_total_sats` remains its exact price.

Only Fork mode (`PAYKIT_SERVER_API=fork`) mints `nonce_sats` and returns
`total_sats = amount_sats + nonce_sats`. Shop clients must not describe a
1–999 sat addition unless the deployment runs the Fork API; the Shop checkout
note is shown only for `paykitServerApi=fork`.

## Rollout and compatibility

Deploy and verify paykit-server description support before enabling this
Marketplace build on a target stack. Unsupported servers reject the new closed
request shape. Marketplace must surface that refusal; it must not retry without
`description`.

No migration or backfill is required. Existing prepared attempts keep their
original terms. New attempts use the label. The attempt reference, operation ID,
response shape, amount handling, and lifecycle commands are otherwise unchanged.

Historical captures under `tests/fixtures/paykit-server-67-75` predate provider
description support and intentionally retain six-field prepare bodies. Current
consumer tests use a signature-verifying producer double until new provider
captures are available at a fixed reviewed provider revision.

## The multi-asset prepare (`PAYKIT_MARKETPLACE_CONTRACT=multi_asset`)

Once the deployment says the target paykit-server speaks the multi-asset
Marketplace contract, new attempts of the `paykit` and `bitcoin` binds send
this body instead (the legacy body above is unchanged and is still what the
fork, an rc16 upstream and any deployment that has not set the contract send):

| Member                   | Value                                                                   |
| ------------------------ | ----------------------------------------------------------------------- |
| `amount`                 | The request amount in base units as a digit string (cents or satoshis). |
| `asset`                  | The denomination: `USD` (any fiat price: a non-USD price is converted to USD cents at bind, `docs/multi-currency.md`), or `BTC` for a satoshi price. |
| `accepted_assets`        | Optional narrowing, a subset of `BTC`, `USDT`; never an extension.      |
| `creator`, `description`, `operation_id`, `payment_window_seconds`, `reader`, `reference` | As above. |

The response must repeat `amount` and `asset` and carry `accepted_assets` and
`payment_amounts` (one whole positive base-unit amount per offered asset);
anything else is refused. Activation sends `{amount, asset, creator,
invoice_id}` and the response must echo the persisted terms. The service
records `paykit_amount_minor`, `paykit_accepted_assets` and
`paykit_payment_amounts` on the order and checks the offer against the order
price before it records anything (README, "The single Paykit bind").
