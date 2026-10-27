-- no-transaction
-- The preorder_overdue worker's scan: paid preorders not yet marked overdue.
-- Partial, so it holds only those orders.
CREATE INDEX CONCURRENTLY orders_preorder_overdue_due_idx
    ON orders (preorder_ships_from)
    WHERE state = 'paid' AND preorder_ships_from IS NOT NULL AND preorder_overdue_at IS NULL
