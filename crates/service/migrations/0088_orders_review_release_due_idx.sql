-- no-transaction
-- The review_release worker's scan: orders whose window is stamped and whose
-- reviews are still withheld (SV2). Partial, so it holds only the pending
-- orders.
CREATE INDEX CONCURRENTLY orders_review_release_due_idx
    ON orders (review_window_ends_at)
    WHERE review_window_ends_at IS NOT NULL AND reviews_released_at IS NULL
