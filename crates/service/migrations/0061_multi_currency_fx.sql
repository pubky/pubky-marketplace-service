-- Multi-currency prices and fiat-at-bind quotes: additive, idempotent,
-- directly rerunnable. There is NO DOWN migration.
--
-- Numbered after 0060 (the single Paykit bind, #129), which this change is
-- stacked on. It reads and writes nothing 0060 creates, so it also applies
-- on a database without it.
--
-- 1. Reference samples per currency. 0024 admitted only the BTCUSD ticker.
--    The sampler now stores one sample per fiat currency of the Blocktank
--    snapshot (USD, EUR, CHF, GBP, JPY, CAD, AUD), each with its own
--    sanity band. The bands repeat `crates/service/src/fx.rs`
--    `rate_bounds` (units of the currency per one bitcoin), and
--    `multi_currency_test::rate_bounds_match_the_table_check` keeps them
--    equal. Existing rows are all USD inside the USD band, so both
--    constraints hold on the rows already there and the table is small (the
--    sampler keeps about an hour of rows).
--
-- 2. The USD conversion of a non-USD price. The multi-asset Paykit request
--    is denominated in USD cents, so a EUR (or any non-USD fiat) price is
--    converted at bind with the cross rate BTCUSD / BTC<CCY> of one
--    snapshot, rounded up to the cent. The order records the conversion
--    next to its bind-time Bitcoin quote (`bitcoin_quote_*`, 0024):
--
--      usd_conversion_pair           e.g. `EURUSD`: USD per one unit of the
--                                    listing currency
--      usd_conversion_source         the feed (`blocktank`)
--      usd_conversion_usd_rate       BTCUSD of the snapshot
--      usd_conversion_currency_rate  BTC<CCY> of the snapshot
--      usd_conversion_fetched_at     the older of the two ticker times
--      usd_conversion_cents          the amount requested, USD cents
--
--    The listing-currency total is the order's own `total_minor` with its
--    `currency` and `exponent`. All six are set together or all NULL; NULL
--    is every order bound without a conversion (USD, BTC and SAT prices,
--    and every order on the legacy `amount_sats` request, whose quote is
--    the direct BTC<CCY> one in `bitcoin_quote_*`). A released Paykit
--    attempt keeps its conversion beside its other pins in
--    `paykit_superseded_attempts`.
--
-- Constraints are added NOT VALID and then validated, so a rerun never
-- holds a long lock.

SET LOCAL lock_timeout = '10s';

ALTER TABLE fx_rate_samples DROP CONSTRAINT IF EXISTS fx_rate_samples_currency_check;
ALTER TABLE fx_rate_samples ADD CONSTRAINT fx_rate_samples_currency_check
    CHECK (currency IN ('USD', 'EUR', 'CHF', 'GBP', 'JPY', 'CAD', 'AUD')) NOT VALID;
ALTER TABLE fx_rate_samples VALIDATE CONSTRAINT fx_rate_samples_currency_check;

ALTER TABLE fx_rate_samples DROP CONSTRAINT IF EXISTS fx_rate_samples_rate_check;
ALTER TABLE fx_rate_samples ADD CONSTRAINT fx_rate_samples_rate_check CHECK (
    (currency = 'USD' AND rate BETWEEN 10000 AND 1000000)
    OR (currency = 'EUR' AND rate BETWEEN 7000 AND 1000000)
    OR (currency = 'CHF' AND rate BETWEEN 7000 AND 1000000)
    OR (currency = 'GBP' AND rate BETWEEN 6000 AND 900000)
    OR (currency = 'JPY' AND rate BETWEEN 1000000 AND 200000000)
    OR (currency = 'CAD' AND rate BETWEEN 10000 AND 1500000)
    OR (currency = 'AUD' AND rate BETWEEN 10000 AND 1700000)
) NOT VALID;
ALTER TABLE fx_rate_samples VALIDATE CONSTRAINT fx_rate_samples_rate_check;

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS usd_conversion_pair TEXT,
    ADD COLUMN IF NOT EXISTS usd_conversion_source TEXT,
    ADD COLUMN IF NOT EXISTS usd_conversion_usd_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS usd_conversion_currency_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS usd_conversion_fetched_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS usd_conversion_cents BIGINT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_usd_conversion_check;
ALTER TABLE orders ADD CONSTRAINT orders_usd_conversion_check CHECK (
    (usd_conversion_pair IS NULL AND usd_conversion_source IS NULL
     AND usd_conversion_usd_rate IS NULL AND usd_conversion_currency_rate IS NULL
     AND usd_conversion_fetched_at IS NULL AND usd_conversion_cents IS NULL)
    OR (currency <> 'USD'
        AND usd_conversion_pair IS NOT NULL AND usd_conversion_source IS NOT NULL
        AND usd_conversion_usd_rate IS NOT NULL AND usd_conversion_currency_rate IS NOT NULL
        AND usd_conversion_fetched_at IS NOT NULL AND usd_conversion_cents IS NOT NULL
        AND usd_conversion_pair = currency || 'USD'
        AND usd_conversion_source = 'blocktank'
        AND usd_conversion_usd_rate > 0 AND usd_conversion_currency_rate > 0
        AND usd_conversion_cents > 0)
) NOT VALID;
ALTER TABLE orders VALIDATE CONSTRAINT orders_usd_conversion_check;

ALTER TABLE paykit_superseded_attempts
    ADD COLUMN IF NOT EXISTS usd_conversion_pair TEXT,
    ADD COLUMN IF NOT EXISTS usd_conversion_source TEXT,
    ADD COLUMN IF NOT EXISTS usd_conversion_usd_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS usd_conversion_currency_rate NUMERIC,
    ADD COLUMN IF NOT EXISTS usd_conversion_fetched_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS usd_conversion_cents BIGINT;

ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_usd_conversion_check;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_usd_conversion_check CHECK (
    (usd_conversion_pair IS NULL AND usd_conversion_source IS NULL
     AND usd_conversion_usd_rate IS NULL AND usd_conversion_currency_rate IS NULL
     AND usd_conversion_fetched_at IS NULL AND usd_conversion_cents IS NULL)
    OR (usd_conversion_pair IS NOT NULL AND usd_conversion_source IS NOT NULL
        AND usd_conversion_usd_rate IS NOT NULL AND usd_conversion_currency_rate IS NOT NULL
        AND usd_conversion_fetched_at IS NOT NULL AND usd_conversion_cents IS NOT NULL
        AND usd_conversion_pair ~ '^[A-Z]{3}USD$' AND usd_conversion_pair <> 'USDUSD'
        AND usd_conversion_source = 'blocktank'
        AND usd_conversion_usd_rate > 0 AND usd_conversion_currency_rate > 0
        AND usd_conversion_cents > 0)
) NOT VALID;
ALTER TABLE paykit_superseded_attempts
    VALIDATE CONSTRAINT paykit_superseded_attempts_usd_conversion_check;
