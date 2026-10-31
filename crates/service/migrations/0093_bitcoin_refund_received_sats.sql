-- A Bitcoin refund is recorded in the sats the order received: the observed
-- amount frozen at settlement (`paykit_observed_sats`), which an overpaid
-- order holds above its payable (`paykit_total_sats`). The refunded review
-- resolution has always recorded it, and `refund.record_external` now caps
-- at it, so `orders_external_refund_within_total` also admits it.
--
-- The bound only widens, so every existing row still satisfies it, and it is
-- added NOT VALID like 0060's: enforced for every new and updated row without
-- scanning the table under a lock. A replica still running the previous
-- release writes nothing the wider bound refuses, so the migration is safe
-- to apply before or during a rolling deploy. Additive and directly
-- rerunnable. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_external_refund_within_total;
ALTER TABLE orders ADD CONSTRAINT orders_external_refund_within_total CHECK (
    external_refund IS NULL
    OR (external_refund ->> 'amount_minor')::bigint
        BETWEEN 1 AND GREATEST(
            total_minor,
            COALESCE(paykit_total_sats, total_minor),
            COALESCE(payment_amount_minor, total_minor),
            COALESCE(paykit_observed_sats, total_minor)
        )
) NOT VALID;

-- Every refund now names the unit of its `amount_minor` as `amount_unit`:
-- `order` (the order currency's minor units), `sat`, or `usdt0` (USDT0 base
-- units). A refund recorded before this release named none, and the order
-- columns cannot tell its unit apart from the new rule's, so it is named here
-- the way its writer recorded it: the refunded review resolution (one
-- `paykit_manual_resolutions` row per order, written in the refund's own
-- transaction) recorded a Bitcoin order's frozen observed sats, every other
-- writer the order total's units, and both recorded a USDT-paid order priced
-- in anything but USD in USDT0 units. `RefundUnit::unnamed` reads a record a
-- replica of the previous release writes during the rollout the same way.
-- Only `amount_unit` is added: the amount, transaction id and `recorded_at`
-- that identify a mirrored refund are unchanged, and a rerun skips a named
-- record.
UPDATE orders o
SET external_refund = o.external_refund || jsonb_build_object(
    'amount_unit',
    CASE
        WHEN (o.payment_method = 'usdt' OR o.payment_asset = 'USDT')
            AND o.currency <> 'USD' THEN 'usdt0'
        WHEN o.payment_method = 'usdt' OR o.payment_asset = 'USDT' THEN 'order'
        WHEN EXISTS (
            SELECT 1 FROM paykit_manual_resolutions r
            WHERE r.order_id = o.id AND r.outcome = 'refunded'
        ) THEN 'sat'
        ELSE 'order'
    END
)
WHERE o.external_refund IS NOT NULL
  AND jsonb_typeof(o.external_refund) = 'object'
  AND NOT (o.external_refund ? 'amount_unit');
