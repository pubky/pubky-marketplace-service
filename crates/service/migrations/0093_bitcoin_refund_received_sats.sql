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
