-- Per-item review keys (SV1): the per-order keys give way to the per-listing
-- indexes 0084 and 0085 built concurrently. Attaching an existing index
-- builds nothing; each swap holds its table's lock only briefly. Rerunnable:
-- each swap runs only while its new constraint is missing.

SET LOCAL lock_timeout = '10s';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'reviews'::regclass
          AND conname = 'reviews_one_per_order_role_listing'
    ) THEN
        ALTER TABLE reviews
            DROP CONSTRAINT IF EXISTS reviews_one_per_order_role,
            ADD CONSTRAINT reviews_one_per_order_role_listing
                UNIQUE USING INDEX reviews_one_per_order_role_listing;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'review_attestations'::regclass
          AND conname = 'review_attestations_item_key'
    ) THEN
        ALTER TABLE review_attestations
            DROP CONSTRAINT IF EXISTS review_attestations_pkey,
            ADD CONSTRAINT review_attestations_item_key
                PRIMARY KEY USING INDEX review_attestations_item_key;
    END IF;
END
$$;
