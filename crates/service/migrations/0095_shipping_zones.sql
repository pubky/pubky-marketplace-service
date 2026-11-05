-- Zone-priced shipping: additive, idempotent, directly rerunnable. There is NO
-- DOWN migration.
--
-- A listing record may reference a seller's shipping profile
-- (`shippingProfileId`) or carry its own zones (`shippingZones`). Until now the
-- service kept one flat `shipping_minor` per listing. These columns hold what
-- the registration reads out of the record so checkout can price an order by
-- the buyer's destination country and rate:
--
-- * `listings.shipping_profile_id` / `listings.shipping_zones`: at most one is
--   set. `shipping_minor` stays, as the fallback the record's legacy
--   `shippingOptions` entry derives, and keeps pricing every listing that has
--   neither column set.
-- * `orders.destination_country` and `orders.shipping_quotes`: the country the
--   order ships to, in clear text beside the sealed address, and the quote of
--   each shipping group (profile or listing, zone, rate, amount, the profile
--   record's SHA-256) the order was priced with. `shipping_quotes` is NULL for
--   an order priced by the flat rate; `destination_country` is NULL unless the
--   buyer sent one.
-- * `offers.destination_country`, `offers.shipping_choice` and
--   `offers.accepted_shipping_quotes`: the buyer's destination and rate,
--   re-priced at accept for the accepted quantity.
-- * `bids.destination_country` and `bids.shipping_quote`: the bidder's
--   destination and the quote the bid was placed with; the winning bid's quote
--   prices the order.
--
-- Nullable columns with no default rewrite no table and build no index. Every
-- check is added NOT VALID, like 0053's and 0060's: enforced for each new and
-- changed row, with no scan of the table under the ALTER's lock. Every
-- existing row has NULL in the new columns, so it satisfies each check.

SET LOCAL lock_timeout = '10s';

ALTER TABLE listings ADD COLUMN IF NOT EXISTS shipping_profile_id TEXT;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS shipping_zones JSONB;
ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_shipping_profile_or_zones;
ALTER TABLE listings ADD CONSTRAINT listings_shipping_profile_or_zones
    CHECK (shipping_profile_id IS NULL OR shipping_zones IS NULL) NOT VALID;

ALTER TABLE orders ADD COLUMN IF NOT EXISTS destination_country TEXT;
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_destination_country_format;
ALTER TABLE orders ADD CONSTRAINT orders_destination_country_format
    CHECK (destination_country IS NULL OR destination_country ~ '^[A-Z]{2}$') NOT VALID;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_quotes JSONB;

ALTER TABLE offers ADD COLUMN IF NOT EXISTS destination_country TEXT;
ALTER TABLE offers DROP CONSTRAINT IF EXISTS offers_destination_country_format;
ALTER TABLE offers ADD CONSTRAINT offers_destination_country_format
    CHECK (destination_country IS NULL OR destination_country ~ '^[A-Z]{2}$') NOT VALID;
ALTER TABLE offers ADD COLUMN IF NOT EXISTS shipping_choice JSONB;
ALTER TABLE offers ADD COLUMN IF NOT EXISTS accepted_shipping_quotes JSONB;

ALTER TABLE bids ADD COLUMN IF NOT EXISTS destination_country TEXT;
ALTER TABLE bids DROP CONSTRAINT IF EXISTS bids_destination_country_format;
ALTER TABLE bids ADD CONSTRAINT bids_destination_country_format
    CHECK (destination_country IS NULL OR destination_country ~ '^[A-Z]{2}$') NOT VALID;
ALTER TABLE bids ADD COLUMN IF NOT EXISTS shipping_quote JSONB;
