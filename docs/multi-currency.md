# Multi-currency prices and fiat-at-bind quotes

Listings, offers, bids and orders are priced in USD, EUR, CHF, GBP, JPY, CAD,
AUD, or in Bitcoin (`BTC` at exponent 8, `SAT` at exponent 0). Every payment
amount is computed at checkout (bind) from a fresh rate from the configured FX
provider. Nothing is converted ahead of time, and the price of record
(`total_minor`, `currency`, `exponent` on the order) never changes.

## Currency registry

`marketplace_domain::currency::Currency` is the one registry:

| Code | Exponent | Feed ticker |
| --- | --- | --- |
| `USD` | 2 | `BTCUSD` |
| `EUR` | 2 | `BTCEUR` |
| `CHF` | 2 | `BTCCHF` |
| `GBP` | 2 | `BTCGBP` |
| `JPY` | 0 | `BTCJPY` |
| `CAD` | 2 | `BTCCAD` |
| `AUD` | 2 | `BTCAUD` |
| `BTC` | 8 | none (already sats) |
| `SAT` | 0 | none (already sats) |

Command validation (`listing.register`, `listing.sync` records, offers, bids)
accepts exactly these `(currency, exponent)` pairs. Anything else is refused
with `422 INVALID_COMMAND` and a stable token in `error.reason` (also on the
offending entry of `error.issues[]`):

| `error.reason` | Meaning | `issues[].path` |
| --- | --- | --- |
| `currency_unsupported` | the code is not in the registry (`SEK`, `USDT`, lower case, ...) | `payload.<money>.currency` |
| `currency_exponent_mismatch` | a registry code with the wrong exponent (`JPY/2`, `USD/3`, `SAT/8`) | `payload.<money>.exponent` |
| `locks_currency_unsupported` | a Locks-gated listing priced outside USD, BTC and SAT | `payload.unit_price.currency` |

The Lock Server counts only USD (cents) and BTC (sats), and the lock criterion
is fixed in the seller's own document, so a Locks listing in any other
currency could never be paid or verified; converting at lock creation is not
possible because the service does not create the lock. Auctions are
currency-generic: the winner's order carries the auction currency and binds
like any other order.

A bind of an order whose currency is outside the registry (an order created
before this registry) answers `409` with `reason: currency_unsupported` before
any feed or Paykit work. PayPal keeps charging such an order in its own
currency.

## Rates

In marketplace mode the service reads the pinned Blocktank response
(`https://api1.blocktank.to/api/fx/rates/btc`), carrying the seven `BTC<CCY>`
tickers. In single-seller mode (`STORE_MODE=single_seller`, including claim
mode, or `STORE_SELLER_PUBKY` set), the operator may configure `FX_FEED_URL` (a
custom Blocktank-format URL or exchange endpoint) and `FX_FEED_PROVIDER`
(`blocktank`, `kraken`, `coinbase`, `mempool`, or `bitstamp`). With the default
provider URLs, `blocktank`, `kraken` (`/0/public/Trades` without a `pair`
filter), `coinbase` (`/v2/exchange-rates?currency=BTC`; `api.coinbase.com`
refuses an empty query at boot because Coinbase defaults it to USD, while
custom Coinbase-format hosts may omit the query when they default to BTC), and
`mempool` quote all seven fiat currencies; `bitstamp`
(`/api/v2/ticker/`) quotes `USD`, `EUR`, and `GBP` only. A single Kraken
`?pair=<PAIR>` or Bitstamp `/api/v2/ticker/btc<ccy>/` URL quotes one fiat;
command admission, background sampling, bind-time quoting, and `GET /health`
`fx_source.currencies` all use that URL-specific set (not the provider-wide
default). Commands or Paykit binds outside the resolved set are refused upfront
with `422 INVALID_COMMAND`, `reason: "currency_unsupported"`, including when
fresh pre-seeded `fx_rate_samples` rows exist for an unsupported fiat. When
`EU_FENCE_ENABLED=true` or any EU-27 country resolves to `nuanced` in
`REGION_MODES`, C4 annual limits and private-seller EU re-confirmation convert
non-EUR orders and paid history through `eur_cents_reference`, so startup
requires `EUR` in the feed's supported currencies (blocked-only and open-only
region modes do not require `EUR`). `BTC` and `SAT` stay supported on every
provider. Each quoted currency is guarded independently, so a stale JPY trade
or ticker never blocks a USD quote:

- unambiguous pair match for `BTC` against `<CCY>`;
- age at most 300 s, at most 60 s in the future (from the payload timestamp on
  Blocktank, Mempool and Bitstamp, from the selected trade execution `time` on
  Kraken `/0/public/Trades`, or from a required single HTTP `Date` header on
  Coinbase; responses with an `Age` header over 300 s or malformed `Age`/`Date`
  fail closed);
- strictly positive, exact decimal precision up to 18 fractional digits
  (`Decimal::is_valid_rate`, stored without truncation in PostgreSQL `NUMERIC`,
  with no hardcoded market-price ceilings or floors and fail-closed rejection
  of excess non-zero digits);
- within 5% (`MAX_DEVIATION_BPS`, inclusive) of the median of that currency's
  own persisted samples of the last hour, with at least three samples.

The `fx_sampler` task takes one snapshot per minute across the fiat currencies
the resolved feed URL quotes (one HTTP request for Blocktank, Coinbase, Mempool,
Bitstamp's all-pairs array, or a single-pair Bitstamp ticker; concurrent pair
requests under the shared 3-second deadline for Kraken when no single `pair` is
configured), checks each against its own median, and drops a rejected currency
for that minute only. A currency with fewer than three samples is refused with
`fx_reference_unavailable` and no feed fetch. After the first deploy of 0061
the non-USD references fill in about three minutes.

## Rounding

All integer arithmetic. Every conversion rounds up, so the buyer pays the exact
price or less than one unit of the settlement asset more, never less.

- Fiat to sats (legacy Paykit contract, `amount_sats`):
  `ceil(total_minor * 10^8 / (10^exponent * BTC<CCY>))`, positive satoshis with
  checked `i64` overflow safety and no artificial minimum-satoshi floor or
  one-bitcoin cap. `SAT` and `BTC` totals are already sats and never read the
  feed.
- Fiat to USD cents (multi-asset Paykit contract, `fx::usd_cents_for`):
  `ceil(total_minor * 100 * BTCUSD / (10^exponent * BTC<CCY>))`. The cross
  rate is the ratio of two tickers of one snapshot, each guarded against its
  own reference. A positive price is at least one cent. A USD price is the
  identity.

## The two Paykit contracts

- **Legacy (`PAYKIT_MARKETPLACE_CONTRACT=legacy`, the fork, rc16, and the
  legacy `bitcoin` bind on every deployment):** the service converts the fiat
  price to sats with `BTC<CCY>` and sends `amount_sats`.
- **Multi-asset (`multi_asset`, the `paykit` bind):** every fiat price is
  requested as `{amount: <USD cents>, asset: "USD"}`. A non-USD price is
  converted with the cross rate of one snapshot. That cross leg needs
  `BTCUSD` from the same FX snapshot, so `PAYKIT_MARKETPLACE_CONTRACT=multi_asset`
  requires USD among the feed's supported currencies (a multi-currency feed or
  a USD single-pair Kraken/Bitstamp URL). A non-USD single-pair feed is valid
  only with the legacy contract / Paykit fork. USDT is then exactly
  `cents x 10_000` at parity, and paykit-server fixes the Bitcoin rate for
  its own conversion exactly as for a USD listing, so paykit-server needs no
  change. The service keeps its guard: the Bitcoin amount paykit-server fixes
  must be positive and sit within 5% of the service's own direct `BTC<CCY>`
  quote of the price, else the bind is refused with `fx_deviation_exceeded` and
  the prepared invoice is voided. When a seller accepts Bitcoin (alone or
  alongside USDT) and FX lookup fails, the bind is refused with the typed FX
  error rather than silently stripping Bitcoin to offer USDT only.
  A seller with only the USDT consent has no Bitcoin leg to guard; a non-USD
  price still needs the feed for its USD amount, so an unreadable feed
  refuses that bind (`fx_reference_unavailable`) instead of narrowing it. A
  USD price for a USDT-only seller needs no feed for USDT at parity and still
  binds with the feed down.

A USDT refund of a non-USD order is recorded in USDT0 millionths and capped at
the USDT paid (`refund_destination::received_refund_amount_minor` and
`refund_terms` treat every non-USD order that way). A USD order records
its USDT refund in cents, capped at the USDT received in whole cents, not at
the order total: a transfer for another amount than quoted is refunded as it
arrived, an overpayment included. A received amount between two cents is the
lower cent, both as the cap and as the manual-review refunded resolution. Less
than one cent is a cap of 0: nothing can be recorded, and the resolution
refuses with `refund_amount_unrecordable`
(`refund_destination::recordable_refund_amount_minor`). A Bitcoin refund is
recorded in sats and capped at the sats received, payment code included,
whatever the price currency (`refund_destination::BitcoinPayment`). The
record names its unit (`external_refund.amount_unit`: `order`, `sat` or
`usdt0`), so a reader never infers it from the order columns, which a later
settlement may change.

## What the order records

The existing quote columns (migration 0024) hold the direct quote, projected
as `order.bitcoin_quote`:

| Field | Value |
| --- | --- |
| `pair` | `BTC<CCY>` (present only once a quote exists) |
| `rate` | units of the listing currency per bitcoin, exact decimal string |
| `source` | FX provider (`blocktank`, or in single-seller mode `kraken`, `coinbase`, `mempool`, or `bitstamp`) |
| `fetched_at` | the ticker's own timestamp |
| `quoted_sats` | the amount asked of Paykit (legacy) or the guard amount (multi-asset) |
| `currency`, `exponent` | the listing currency (the order's `total` is the listing-currency total) |
| `expires_at`, `spread_bps` | as before |

A multi-asset bind of a non-USD price also records the conversion in
`orders.usd_conversion_*` (migration 0061) and projects it as
`order.fx_conversion`, present only then:

```json
{
  "pair": "EURUSD",
  "rate": "1.161188911117",
  "usd_rate": "77197",
  "currency_rate": "66481",
  "source": "blocktank",
  "fetched_at": "2026-10-15T10:00:00.000Z",
  "from": { "amount_minor": 13700, "currency": "EUR", "exponent": 2 },
  "to": { "amount_minor": 15909, "currency": "USD", "exponent": 2 }
}
```

`rate` is informational (USD per one major unit, 12 places, rounded half up);
the cents charged come from the two exact rates. `fetched_at` is the older of
the two ticker times. The record is cleared on a re-bind and kept with a
released attempt in `paykit_superseded_attempts`.

## PayPal

PayPal charges in the listing currency: `currency_code` is the order's code and
`amount` has exactly `exponent` fraction digits, with no decimal point for JPY
(`13700`, never `13700.0`). The IPN check compares `mc_currency` with the order
currency and `mc_gross` strictly at the order exponent (`137.00` for EUR,
`13700` for JPY). A refund or reversal notification is matched in the same
currency, and the recorded external refund is in that currency's minor units.

## Refunds and receipts

Amounts are in the paid asset's units (sats for a Bitcoin payment, USDT0
millionths for USDT on a non-USD order, the listing currency for PayPal).
