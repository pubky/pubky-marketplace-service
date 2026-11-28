-- no-transaction
-- Remove static FX rate ceilings/floors and the 1.0 BTC fiat-quote cap,
-- and admit built-in single-seller FX feed providers alongside Blocktank.
-- Additive, idempotent, directly rerunnable. There is NO DOWN migration.
--
-- 1. `fx_rate_samples`: replace per-currency hardcoded market-price bands
--    (`fx_rate_samples_rate_check`, 0024/0061) with a finite positive rate
--    check (`rate > 0 AND rate < 'Infinity'::numeric`; PostgreSQL orders
--    NaN above Infinity, so this excludes 0, negatives, +/-Infinity and NaN).
--    Widen `fx_rate_samples_source_check` to admit the built-in single-seller
--    exchange providers (`kraken`, `coinbase`, `mempool`, `bitstamp`) beside
--    `blocktank`.
-- 2. `orders`: drop `bitcoin_quoted_sats <= 100000000` from
--    `orders_fx_quote_check`, keep finite positive rate and positive sats,
--    and admit the same FX source vocabulary on `orders_fx_quote_check`,
--    `orders_usd_conversion_check`, and
--    `paykit_superseded_attempts_usd_conversion_check`.
--
-- Runs with `-- no-transaction` so each DROP + ADD NOT VALID commits and
-- releases ACCESS EXCLUSIVE before VALIDATE CONSTRAINT scans the table under
-- SHARE UPDATE EXCLUSIVE, with lock_timeout = '10s' on every short block.

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE fx_rate_samples DROP CONSTRAINT IF EXISTS fx_rate_samples_rate_check;
ALTER TABLE fx_rate_samples ADD CONSTRAINT fx_rate_samples_rate_check
    CHECK (rate > 0 AND rate < 'Infinity'::numeric) NOT VALID;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE fx_rate_samples VALIDATE CONSTRAINT fx_rate_samples_rate_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE fx_rate_samples DROP CONSTRAINT IF EXISTS fx_rate_samples_source_check;
ALTER TABLE fx_rate_samples ADD CONSTRAINT fx_rate_samples_source_check
    CHECK (source IN ('blocktank', 'kraken', 'coinbase', 'mempool', 'bitstamp')) NOT VALID;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE fx_rate_samples VALIDATE CONSTRAINT fx_rate_samples_source_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_fx_quote_check;
ALTER TABLE orders ADD CONSTRAINT orders_fx_quote_check CHECK (
    (bitcoin_quote_rate IS NULL AND bitcoin_quote_source IS NULL
     AND bitcoin_quote_fetched_at IS NULL AND bitcoin_quoted_sats IS NULL
     AND bitcoin_quote_expires_at IS NULL AND bitcoin_quote_currency IS NULL
     AND bitcoin_quote_exponent IS NULL AND bitcoin_quote_spread_bps IS NULL)
    OR (bitcoin_quote_rate > 0 AND bitcoin_quote_rate < 'Infinity'::numeric
        AND bitcoin_quote_source IN ('blocktank', 'kraken', 'coinbase', 'mempool', 'bitstamp')
        AND bitcoin_quote_fetched_at IS NOT NULL AND bitcoin_quoted_sats > 0
        AND bitcoin_quote_expires_at = paykit_expires_at
        AND bitcoin_quote_currency = currency
        AND bitcoin_quote_exponent = exponent
        AND bitcoin_quote_spread_bps >= 0)
) NOT VALID;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE orders VALIDATE CONSTRAINT orders_fx_quote_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
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
        AND usd_conversion_source IN ('blocktank', 'kraken', 'coinbase', 'mempool', 'bitstamp')
        AND usd_conversion_usd_rate > 0 AND usd_conversion_usd_rate < 'Infinity'::numeric
        AND usd_conversion_currency_rate > 0 AND usd_conversion_currency_rate < 'Infinity'::numeric
        AND usd_conversion_cents > 0)
) NOT VALID;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE orders VALIDATE CONSTRAINT orders_usd_conversion_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
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
        AND usd_conversion_source IN ('blocktank', 'kraken', 'coinbase', 'mempool', 'bitstamp')
        AND usd_conversion_usd_rate > 0 AND usd_conversion_usd_rate < 'Infinity'::numeric
        AND usd_conversion_currency_rate > 0 AND usd_conversion_currency_rate < 'Infinity'::numeric
        AND usd_conversion_cents > 0)
) NOT VALID;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE paykit_superseded_attempts
    VALIDATE CONSTRAINT paykit_superseded_attempts_usd_conversion_check;
COMMIT;
