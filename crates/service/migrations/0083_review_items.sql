-- Per-item reviews and seller ratings (Pubky Market reviews, SV1). A buyer
-- reviews each listing of an order (one row and one purchase attestation per
-- listing); a seller's review of the buyer stays one row per order, keyed to
-- the first line. Additive and directly rerunnable. 0084 and 0085 build the
-- new unique indexes concurrently and 0086 swaps the keys onto them.
--
-- Existing rows rated the order's first line (the only listing the service
-- attested), so the backfill keys them there, and an existing attestation's
-- listing is the one its claims bind. The BEFORE INSERT triggers fill the
-- same values for a binary from before 0083 during a rolling deploy: it
-- writes neither column, and its v1 reviews rate the first line.

SET LOCAL lock_timeout = '10s';

-- === reviews ===================================================================

ALTER TABLE reviews
    ADD COLUMN IF NOT EXISTS listing_aggregate_id TEXT,
    ADD COLUMN IF NOT EXISTS shipping INTEGER,
    ADD COLUMN IF NOT EXISTS communication INTEGER;

-- Seller ratings are the buyer's, set together, 1 to 5 each.
ALTER TABLE reviews DROP CONSTRAINT IF EXISTS reviews_seller_ratings_check;
ALTER TABLE reviews
    ADD CONSTRAINT reviews_seller_ratings_check CHECK (
        (shipping IS NULL AND communication IS NULL)
        OR (
            reviewer_role = 'buyer'
            AND shipping IS NOT NULL
            AND communication IS NOT NULL
            AND shipping BETWEEN 1 AND 5
            AND communication BETWEEN 1 AND 5
        )
    );

CREATE OR REPLACE FUNCTION reviews_default_listing() RETURNS trigger AS $$
BEGIN
    IF NEW.listing_aggregate_id IS NULL THEN
        SELECT lines -> 0 ->> 'listing_aggregate_id'
          INTO NEW.listing_aggregate_id
          FROM orders WHERE id = NEW.order_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS reviews_default_listing ON reviews;
CREATE TRIGGER reviews_default_listing
    BEFORE INSERT ON reviews
    FOR EACH ROW EXECUTE FUNCTION reviews_default_listing();

UPDATE reviews r
SET listing_aggregate_id = o.lines -> 0 ->> 'listing_aggregate_id'
FROM orders o
WHERE o.id = r.order_id AND r.listing_aggregate_id IS NULL;

ALTER TABLE reviews ALTER COLUMN listing_aggregate_id SET NOT NULL;

-- === review_attestations ======================================================
-- The table is append-only (0007's trigger forbids UPDATE and DELETE). The
-- trigger is lifted for the backfill and re-created immediately after, as
-- 0018 does for attestation_annotations; drop-then-create keeps this
-- rerunnable.

ALTER TABLE review_attestations ADD COLUMN IF NOT EXISTS listing_uri TEXT;

DROP TRIGGER IF EXISTS review_attestations_append_only ON review_attestations;

UPDATE review_attestations
SET listing_uri = claims ->> 'listing'
WHERE listing_uri IS NULL;

CREATE TRIGGER review_attestations_append_only
    BEFORE UPDATE OR DELETE ON review_attestations
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();

CREATE OR REPLACE FUNCTION review_attestations_default_listing() RETURNS trigger AS $$
BEGIN
    IF NEW.listing_uri IS NULL THEN
        NEW.listing_uri := NEW.claims ->> 'listing';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS review_attestations_default_listing ON review_attestations;
CREATE TRIGGER review_attestations_default_listing
    BEFORE INSERT ON review_attestations
    FOR EACH ROW EXECUTE FUNCTION review_attestations_default_listing();

ALTER TABLE review_attestations ALTER COLUMN listing_uri SET NOT NULL;

-- The column is the listing the signed claims bind, never another.
ALTER TABLE review_attestations
    DROP CONSTRAINT IF EXISTS review_attestations_listing_uri_check;
ALTER TABLE review_attestations
    ADD CONSTRAINT review_attestations_listing_uri_check
    CHECK (listing_uri = claims ->> 'listing');
