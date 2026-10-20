-- Legal launch plan C4 (LG18): the private seller's re-confirmation date,
-- frozen on the order at its payment start with the seller's declaration.
-- Additive and directly rerunnable. There is NO DOWN migration; to back the
-- column out by hand, see "Backing out 0080" in docs/migrations.md.
--
-- `orders.seller_status_confirmed_at` is the shop record's
-- `sellerStatusConfirmedAt` as read when the order's payment started: set when
-- the record carried a valid RFC 3339 timestamp that was not in the future,
-- NULL when it carried none, the record was unreadable, or the order's terms
-- were frozen before this migration. The payment-time re-check of the private
-- seller's re-confirmation rule counts sales from this date (or from the start
-- of the year when NULL), so a payment never reads the homeserver. One nullable
-- column without a default: no rewrite, no index. A binary from before 0080
-- never sets or reads it.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_status_confirmed_at TIMESTAMPTZ;
