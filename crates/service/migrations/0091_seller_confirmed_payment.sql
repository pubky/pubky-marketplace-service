-- Seller-confirmed payment (PAYMENT_CONFIRMATION): who marks an order paid,
-- frozen on the order at its payment start, and the PayPal notification that
-- now only informs the seller. Additive and directly rerunnable. There is NO
-- DOWN migration; to back the columns out by hand, see "Backing out 0091" in
-- docs/migrations.md.
--
-- * `orders.payment_confirmation` is `seller` when the deployment ran
--   `PAYMENT_CONFIRMATION=seller` at the order's first payment start (the
--   seller's attestation is the only way Bitcoin, USDT and PayPal money pays
--   the order) and `automatic` when it ran `automatic` (the earlier
--   behaviour, the seller's own Bitcoin opt-in in `seller_confirms_bitcoin`
--   included). NULL for an order whose payment had not started before 0091,
--   which reads as `automatic`.
-- * `orders.gateway_payment_seen_at` is when a postback-verified PayPal
--   `Completed` notification that matched the seller, currency and amount was
--   received for a `seller` order. It is a hint for the seller, never a
--   transition input. NULL otherwise.
--
-- Both columns are nullable with no default (no rewrite) and the CHECK treats
-- NULL as valid. A binary from before 0091 never sets or reads them.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_confirmation TEXT
    CONSTRAINT orders_payment_confirmation_values
    CHECK (payment_confirmation IS NULL OR payment_confirmation IN ('seller', 'automatic'));

ALTER TABLE orders ADD COLUMN IF NOT EXISTS gateway_payment_seen_at TIMESTAMPTZ;
