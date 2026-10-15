-- One Paykit bind (`paykit`) for every asset a seller accepts: additive,
-- idempotent, directly rerunnable.
--
-- `orders.payment_method` gains `paykit`: how the buyer pays (through Paykit
-- and a Paykit wallet). What was offered and what was actually paid are
-- separate facts:
--
--   paykit_amount_minor      the request amount in `paykit_asset`'s base
--                            units (cents for USD, satoshis for BTC). With
--                            `paykit_asset` this is the denomination the
--                            upstream request was prepared and activated in.
--   paykit_accepted_assets   the assets the upstream request offers, as
--                            paykit-server derived them (`BTC`, `USDT`).
--   paykit_payment_amounts   what each offered asset costs, in the asset's
--                            base units: `{"BTC": 50000, "USDT": 25000000}`.
--
-- The three are set together or all NULL. NULL is every order bound through
-- the legacy `amount_sats` request (the fork, rc16 upstream, and the legacy
-- `bitcoin` bind), which activates by `paykit_total_sats` as before.
--
-- The rail the buyer actually paid lands in the existing S1 columns
-- (`payment_asset`, `payment_network`, `payment_amount_minor`,
-- `payment_exponent`, `payment_quote_basis`, 0053) when the payment settles;
-- they stay NULL until then.
--
-- `payment_finality` is the S3 projection of a USDT payment: `pending` from
-- verified inclusion, `final` once paykit-server reports the transfer
-- finalized, `reverted` when an included transfer disappears before it is
-- final. It gates physical fulfilment, never whether the order is paid.
-- `payment_finality_at` is its last change.
--
-- `paykit_second_rail_at` is when money was first seen on the rail that did
-- not pay the order (a buyer who paid both BTC and USDT).
--
-- A USDT-only request has no satoshi total: `paykit_superseded_attempts`
-- keeps the released attempt of such an order, so its `total_sats` may be
-- NULL when the attempt carries no Bitcoin amount.
--
-- Both orders constraints are NOT VALID like 0053's: enforced for every new
-- and updated row without scanning the table under a lock. There is NO DOWN
-- migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_payment_method_check;
ALTER TABLE orders ADD CONSTRAINT orders_payment_method_check
    CHECK (payment_method IN ('bitcoin', 'stripe', 'paypal', 'usdt', 'paykit')) NOT VALID;

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS paykit_amount_minor BIGINT,
    ADD COLUMN IF NOT EXISTS paykit_accepted_assets TEXT[],
    ADD COLUMN IF NOT EXISTS paykit_payment_amounts JSONB,
    ADD COLUMN IF NOT EXISTS payment_finality TEXT,
    ADD COLUMN IF NOT EXISTS payment_finality_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paykit_second_rail_at TIMESTAMPTZ;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_paykit_multi_asset_terms_check;
ALTER TABLE orders ADD CONSTRAINT orders_paykit_multi_asset_terms_check
    CHECK (
        (paykit_amount_minor IS NULL) = (paykit_accepted_assets IS NULL)
        AND (paykit_amount_minor IS NULL) = (paykit_payment_amounts IS NULL)
        AND (paykit_amount_minor IS NULL OR paykit_amount_minor > 0)
        AND (paykit_accepted_assets IS NULL OR cardinality(paykit_accepted_assets) > 0)
    ) NOT VALID;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_payment_finality_check;
ALTER TABLE orders ADD CONSTRAINT orders_payment_finality_check
    CHECK (
        (payment_finality IS NULL OR payment_finality IN ('pending', 'final', 'reverted'))
        AND (payment_finality IS NULL) = (payment_finality_at IS NULL)
    ) NOT VALID;

ALTER TABLE paykit_superseded_attempts ALTER COLUMN total_sats DROP NOT NULL;
ALTER TABLE paykit_superseded_attempts
    DROP CONSTRAINT IF EXISTS paykit_superseded_attempts_total_sats_check;
ALTER TABLE paykit_superseded_attempts
    ADD CONSTRAINT paykit_superseded_attempts_total_sats_check
    CHECK (total_sats IS NULL OR total_sats > 0);
