-- Made-to-order stock. Additive and directly rerunnable. There is NO DOWN
-- migration.
--
-- A made-to-order variant row (`listing_variant_inventory.made_to_order`)
-- sells without a stock count: a store that does not track the variant's
-- inventory, or that keeps selling it at zero (Shopify `tracked: false` or
-- `inventoryPolicy: CONTINUE`, typical for print-on-demand), is the source of
-- truth that it is always available. The row keeps the balance rule
-- (available + reserved + sold = total) with `available_quantity` fixed at 0:
-- a claim adds its units to the total as it reserves them, and a release
-- takes them out of the total again, so no unit is ever invented and holds,
-- payment confirmation, cancels and expiry move units exactly as for a
-- tracked row. Only the connector worker sets or clears the flag, from the
-- store's own stock settings; clearing it puts the store's count on sale.
--
-- `listings.made_to_order` is the roll-up: true while any enabled variant row
-- of the listing is made to order. The roll-up trigger from 0070 is replaced
-- so it also maintains the flag and adds `"made_to_order": true` to the
-- `variant_availability` entry of a made-to-order variant, which is never
-- `sold_out` (entries of counted variants are unchanged).
--
-- The two columns are `NOT NULL DEFAULT false`, a constant default, so adding
-- them rewrites no table. The CHECK is `NOT VALID` (every existing row is
-- `false`, and new and updated rows are checked); no index is built.

SET LOCAL lock_timeout = '10s';

ALTER TABLE listing_variant_inventory
    ADD COLUMN IF NOT EXISTS made_to_order BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE listings
    ADD COLUMN IF NOT EXISTS made_to_order BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE listing_variant_inventory
    DROP CONSTRAINT IF EXISTS listing_variant_inventory_made_to_order_uncounted;
ALTER TABLE listing_variant_inventory
    ADD CONSTRAINT listing_variant_inventory_made_to_order_uncounted
    CHECK (NOT made_to_order OR available_quantity = 0) NOT VALID;

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
        variant_availability = s.availability,
        made_to_order = s.made_to_order
    FROM (
        SELECT
            COALESCE(SUM(total_quantity), 0) AS total,
            COALESCE(SUM(available_quantity), 0) AS available,
            COALESCE(SUM(reserved_quantity), 0) AS reserved,
            COALESCE(SUM(sold_quantity), 0) AS sold,
            COALESCE(BOOL_OR(made_to_order) FILTER (WHERE enabled), false) AS made_to_order,
            CASE
                WHEN COUNT(*) FILTER (WHERE variant_id <> '') = 0 THEN NULL
                ELSE COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'variant_id', variant_id,
                            'available_quantity', available_quantity,
                            'sold_out', available_quantity = 0 AND NOT made_to_order
                        ) || CASE WHEN made_to_order
                                  THEN '{"made_to_order": true}'::jsonb
                                  ELSE '{}'::jsonb END
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
           l.variant_availability, l.made_to_order)
          IS DISTINCT FROM (s.total, s.available, s.reserved, s.sold, s.availability,
                            s.made_to_order);
    RETURN NULL;
END;
$$;
