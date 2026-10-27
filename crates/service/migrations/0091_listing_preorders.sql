-- Preorders: a fixed-price physical listing that sells before stock exists
-- and ships on or after a date. Additive and directly rerunnable. There is NO
-- DOWN migration; to back the columns out by hand, see "Backing out 0091" in
-- docs/migrations.md.
--
-- `listings.preorder_ships_from` / `preorder_grace_days` are the registered
-- record's `preorder` (both NULL for any other listing). The order columns
-- are the same pair frozen when the order's payment starts (the latest
-- deadline among its lines), and `orders.preorder_overdue_at` is when the
-- preorder_overdue sweep found it still unshipped after that deadline. Nullable
-- columns without a default: no rewrite, no index. A binary from before 0091
-- never sets or reads them.

SET LOCAL lock_timeout = '10s';

ALTER TABLE listings ADD COLUMN IF NOT EXISTS preorder_ships_from DATE;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS preorder_grace_days INTEGER;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS preorder_ships_from DATE;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS preorder_grace_days INTEGER;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS preorder_overdue_at TIMESTAMPTZ;
