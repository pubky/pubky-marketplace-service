-- The listings' return policy frozen on the order at its payment start, with
-- the seller's delivery terms. Additive and directly rerunnable. There is NO
-- DOWN migration; to back the column out by hand, see "Backing out 0081" in
-- docs/migrations.md.
--
-- `orders.return_policy` is `{accepts_returns, return_window_days}` read from
-- the order's listing records when its payment started (the most generous of
-- its lines); NULL when a record was unreadable or carried no policy, or the
-- order's terms were frozen before this migration. `return.request` refuses a
-- return on a final-sale order and after the window; NULL keeps the old
-- behaviour. One nullable column without a default: no rewrite, no index. A
-- binary from before 0081 never sets or reads it.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS return_policy JSONB;
