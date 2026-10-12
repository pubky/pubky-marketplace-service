-- Seller-set post-purchase periods, captured per order, and an optional
-- guarantee policy version.
--
-- * `orders.delivery_assume_days` / `orders.auto_complete_days` freeze, on
--   the order, the periods the seller's shop record set when the order
--   started its payment (the first payment lock point).
--   `delivery_terms_captured_at` records that the capture happened, once:
--   a later lock point on the same order never reads the record again. The
--   delivery assumption and auto-complete sweeps read the two periods per
--   order, so a later change to the shop record never moves an open order's
--   clock. A NULL period means the seller left it unset (or the order
--   predates this migration, or never reached a payment lock point): the
--   sweeps use the configured `DELIVERY_ASSUME_DAYS` / `AUTO_COMPLETE_DAYS`
--   for it. Both are bounded to the same 1-60 days the shop record allows.
-- * `orders.guarantee_policy_version` is no longer required: a checkout no
--   longer has to carry it. Existing rows keep their value.
--
-- Additive and rerunnable. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_assume_days INTEGER
    CONSTRAINT orders_delivery_assume_days_range
    CHECK (delivery_assume_days IS NULL OR delivery_assume_days BETWEEN 1 AND 60);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS auto_complete_days INTEGER
    CONSTRAINT orders_auto_complete_days_range
    CHECK (auto_complete_days IS NULL OR auto_complete_days BETWEEN 1 AND 60);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_terms_captured_at TIMESTAMPTZ;

ALTER TABLE orders ALTER COLUMN guarantee_policy_version DROP NOT NULL;
