-- Legal launch plan B4: the seller's Bitcoin confirmation option, frozen on
-- the order at its payment start with the seller's delivery periods.
-- Additive and directly rerunnable. There is NO DOWN migration; to back the
-- column out by hand, see "Backing out 0079" in docs/migrations.md.
--
-- `orders.seller_confirms_bitcoin` is true when the seller's shop record said
-- `bitcoinConfirmation: "seller"` when the order's payment started, false when
-- it did not (or the seller has no shop record), and NULL for an order that
-- predates this migration or whose shop record was unreadable at the time.
-- The Paykit poll reads NULL as not opted in. One nullable column without a
-- default: no rewrite, no index. A binary from before 0079 never sets or
-- reads it.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_confirms_bitcoin BOOLEAN;
