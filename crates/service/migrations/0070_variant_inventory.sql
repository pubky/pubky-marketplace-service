-- Per-variant inventory: additive, idempotent, directly rerunnable. There is
-- NO DOWN migration.
--
-- Until now a listing tracked one stock total, the sum of the quantities of
-- the variants in the seller's record, so a buyer could buy a size that was
-- sold out while another size still had stock. `listing_variant_inventory`
-- holds one row per variant with the same balance rule as `listings`
-- (available + reserved + sold = total). A listing that sells one variant
-- keeps a single IMPLICIT row (`variant_id = ''`) equal to its own totals, so
-- its behaviour is unchanged. A listing whose record has two or more enabled
-- variants carries one row per variant, keyed by the record's variant id.
--
-- The listing's own quantity columns are the SUM of its rows, kept by
-- triggers in the same statement and transaction as every row change, so no
-- writer can leave the two apart:
--
--   * `listing_variant_inventory_roll_up` (rows -> listing): after a row is
--     inserted, updated or deleted it rewrites the listing's four quantity
--     columns and `variant_availability` (the public per-variant view) from
--     the rows. `listings_quantity_balance` therefore keeps holding.
--   * `listings_variant_rows_follow` (listing -> implicit row): a direct
--     write to a listing's quantity columns, which is what a binary from
--     before this migration issues during a rolling replacement, is copied
--     into the implicit row so both agree. On a listing that tracks several
--     variants the write is refused (the old binary cannot say which variant
--     moved), which fails that old replica's transaction closed instead of
--     letting it sell stock the variant rows do not know about.
--   * `listings_implicit_variant_row` (new listing -> its implicit row):
--     every listing has at least one row from the moment it exists.
--
-- Backfill: one implicit row per existing listing, equal to its current
-- totals. The service converts a listing to per-variant rows the first time
-- the seller's record is read with two or more enabled variants and the
-- listing's committed stock can be attributed exactly (see docs/api.md).
--
-- `reservations.variant_id` names the variant row a reservation holds
-- (`inventory.reserve`, an accepted offer, an auction win). NULL is the
-- implicit row, which is every reservation that exists today.
--
-- `listing_variant_conversion_blocks` is the operator-visible marker of a
-- listing whose record has two or more enabled variants but that cannot convert
-- yet: `open_holds` (units reserved), `unattributable_sales` (units sold that
-- the paid orders' lines do not attribute to the record's variants),
-- `adjusted_total` (the record is unchanged but the seller's `inventory.adjust`
-- moved the listing total, so converting from the record would create or
-- destroy units) or `below_committed` (the record gives a variant fewer units
-- than the paid orders attribute to it). A sync
-- upserts the row while the listing is blocked and deletes it when the listing
-- converts (or its record stops listing several variants).
-- `scripts/variant-conversion-sweep.sh` reads it; `listing.sync_many` retries
-- the listings and reports the state per item.
--
-- No index is built on an existing table; the new table's primary key is
-- built on an empty table. The `ALTER TABLE ... ADD COLUMN` statements add
-- nullable columns without a default, so they rewrite nothing.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS listing_variant_inventory (
    listing_aggregate_id TEXT NOT NULL REFERENCES listings (aggregate_id) ON DELETE CASCADE,
    variant_id TEXT NOT NULL,
    position INTEGER NOT NULL DEFAULT 0 CHECK (position >= 0),
    sku TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    total_quantity BIGINT NOT NULL CHECK (total_quantity >= 0),
    available_quantity BIGINT NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity BIGINT NOT NULL CHECK (reserved_quantity >= 0),
    sold_quantity BIGINT NOT NULL CHECK (sold_quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (listing_aggregate_id, variant_id),
    CONSTRAINT listing_variant_inventory_balance
        CHECK (available_quantity + reserved_quantity + sold_quantity = total_quantity),
    CONSTRAINT listing_variant_inventory_implicit_shape
        CHECK (variant_id <> '' OR (sku IS NULL AND enabled AND position = 0))
);

CREATE TABLE IF NOT EXISTS listing_variant_conversion_blocks (
    listing_aggregate_id TEXT PRIMARY KEY REFERENCES listings (aggregate_id) ON DELETE CASCADE,
    reason TEXT NOT NULL CHECK (reason IN ('open_holds', 'unattributable_sales', 'adjusted_total', 'below_committed')),
    enabled_variants INTEGER NOT NULL CHECK (enabled_variants >= 2),
    reserved_quantity BIGINT NOT NULL CHECK (reserved_quantity >= 0),
    sold_quantity BIGINT NOT NULL CHECK (sold_quantity >= 0),
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    first_blocked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE listings ADD COLUMN IF NOT EXISTS variant_availability JSONB;
ALTER TABLE reservations ADD COLUMN IF NOT EXISTS variant_id TEXT;

CREATE OR REPLACE FUNCTION listing_variant_inventory_roll_up() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    target TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        target := OLD.listing_aggregate_id;
    ELSE
        target := NEW.listing_aggregate_id;
    END IF;
    UPDATE listings AS l SET
        total_quantity = s.total,
        available_quantity = s.available,
        reserved_quantity = s.reserved,
        sold_quantity = s.sold,
        variant_availability = s.availability
    FROM (
        SELECT
            COALESCE(SUM(total_quantity), 0) AS total,
            COALESCE(SUM(available_quantity), 0) AS available,
            COALESCE(SUM(reserved_quantity), 0) AS reserved,
            COALESCE(SUM(sold_quantity), 0) AS sold,
            CASE
                WHEN COUNT(*) FILTER (WHERE variant_id <> '') = 0 THEN NULL
                ELSE COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'variant_id', variant_id,
                            'available_quantity', available_quantity,
                            'sold_out', available_quantity = 0
                        )
                        ORDER BY position, variant_id
                    ) FILTER (WHERE enabled),
                    '[]'::jsonb
                )
            END AS availability
        FROM listing_variant_inventory
        WHERE listing_aggregate_id = target
    ) AS s
    WHERE l.aggregate_id = target
      AND (l.total_quantity, l.available_quantity, l.reserved_quantity, l.sold_quantity,
           l.variant_availability)
          IS DISTINCT FROM (s.total, s.available, s.reserved, s.sold, s.availability);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS listing_variant_inventory_roll_up ON listing_variant_inventory;
CREATE TRIGGER listing_variant_inventory_roll_up
    AFTER INSERT OR UPDATE OR DELETE ON listing_variant_inventory
    FOR EACH ROW
    EXECUTE FUNCTION listing_variant_inventory_roll_up();

CREATE OR REPLACE FUNCTION listings_variant_rows_follow() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    rows_found INTEGER;
    only_implicit BOOLEAN;
BEGIN
    -- The roll-up above rewrites these columns from the rows; that nested
    -- write is already consistent.
    IF pg_trigger_depth() > 1 THEN
        RETURN NULL;
    END IF;
    SELECT COUNT(*), COALESCE(BOOL_AND(variant_id = ''), false)
    INTO rows_found, only_implicit
    FROM listing_variant_inventory
    WHERE listing_aggregate_id = NEW.aggregate_id;
    IF rows_found = 0 THEN
        INSERT INTO listing_variant_inventory
            (listing_aggregate_id, variant_id, total_quantity, available_quantity,
             reserved_quantity, sold_quantity)
        VALUES (NEW.aggregate_id, '', NEW.total_quantity, NEW.available_quantity,
                NEW.reserved_quantity, NEW.sold_quantity);
    ELSIF rows_found = 1 AND only_implicit THEN
        UPDATE listing_variant_inventory SET
            total_quantity = NEW.total_quantity,
            available_quantity = NEW.available_quantity,
            reserved_quantity = NEW.reserved_quantity,
            sold_quantity = NEW.sold_quantity,
            updated_at = now()
        WHERE listing_aggregate_id = NEW.aggregate_id AND variant_id = '';
    ELSE
        RAISE EXCEPTION 'listing % tracks stock per variant; change its variant rows, not its totals',
            NEW.aggregate_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS listings_variant_rows_follow ON listings;
CREATE TRIGGER listings_variant_rows_follow
    AFTER UPDATE OF total_quantity, available_quantity, reserved_quantity, sold_quantity ON listings
    FOR EACH ROW
    WHEN (OLD.total_quantity IS DISTINCT FROM NEW.total_quantity
          OR OLD.available_quantity IS DISTINCT FROM NEW.available_quantity
          OR OLD.reserved_quantity IS DISTINCT FROM NEW.reserved_quantity
          OR OLD.sold_quantity IS DISTINCT FROM NEW.sold_quantity)
    EXECUTE FUNCTION listings_variant_rows_follow();

CREATE OR REPLACE FUNCTION listings_implicit_variant_row() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO listing_variant_inventory
        (listing_aggregate_id, variant_id, total_quantity, available_quantity,
         reserved_quantity, sold_quantity)
    VALUES (NEW.aggregate_id, '', NEW.total_quantity, NEW.available_quantity,
            NEW.reserved_quantity, NEW.sold_quantity)
    ON CONFLICT (listing_aggregate_id, variant_id) DO NOTHING;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS listings_implicit_variant_row ON listings;
CREATE TRIGGER listings_implicit_variant_row
    AFTER INSERT ON listings
    FOR EACH ROW
    EXECUTE FUNCTION listings_implicit_variant_row();

INSERT INTO listing_variant_inventory
    (listing_aggregate_id, variant_id, total_quantity, available_quantity,
     reserved_quantity, sold_quantity)
SELECT aggregate_id, '', total_quantity, available_quantity, reserved_quantity, sold_quantity
FROM listings
WHERE NOT EXISTS (
    SELECT 1 FROM listing_variant_inventory
    WHERE listing_variant_inventory.listing_aggregate_id = listings.aggregate_id
)
ON CONFLICT (listing_aggregate_id, variant_id) DO NOTHING;
