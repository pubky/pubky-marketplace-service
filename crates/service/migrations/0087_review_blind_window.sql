-- Double-blind seller-to-buyer reviews (Pubky Market reviews, SV2).
-- `review_window_ends_at` is stamped at an order's first review when
-- REVIEW_BLIND_WINDOW_DAYS is above 0: until `reviews_released_at` is set
-- the counterparty's review stays withheld and the reviewer's attestations
-- stay unserved. Both columns are NULL on every order reviewed while the
-- window was off, and an order with no window is never withheld. Additive
-- and directly rerunnable.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS review_window_ends_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS reviews_released_at TIMESTAMPTZ;

-- Reviews that exist today were public from the moment they were written,
-- so their orders are released as of the first review.
UPDATE orders o
SET reviews_released_at = first_review.created_at
FROM (
    SELECT order_id, MIN(created_at) AS created_at
    FROM reviews
    GROUP BY order_id
) AS first_review
WHERE first_review.order_id = o.id
  AND o.reviews_released_at IS NULL;

-- One notification intent per order, kind and recipient: `review_waiting`,
-- `review_released` and, while the window is on, `review_received`. The
-- primary key decides who enqueues it, so a replayed or racing release or
-- create never doubles one.
CREATE TABLE IF NOT EXISTS review_notifications (
    order_id UUID NOT NULL REFERENCES orders (id),
    kind TEXT NOT NULL CHECK (kind IN ('review_waiting', 'review_released', 'review_received')),
    recipient_pubky TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, kind, recipient_pubky)
);
