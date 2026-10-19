-- Legal launch plan C1-C4: what the service records about who sold to whom.
-- Additive and directly rerunnable. There is NO DOWN migration; to back the
-- columns out by hand, see "Backing out 0076" in docs/migrations.md.
--
-- * `orders.buyer_country` is the buyer's country (ISO 3166-1 alpha-2) as the
--   buyer's client sent it, in clear text, with `checkout.create` or
--   `offer.checkout`. The delivery address itself stays sealed to the
--   seller. `offers.buyer_country` and `bids.buyer_country` carry the same
--   field from `offer.create` and `auction.place_bid`, so the order an offer
--   or an auction later becomes can inherit it. NULL for a command whose
--   client sent none (old clients) and for every existing row.
-- * `orders.seller_status` and `orders.seller_country` freeze, with the
--   seller's delivery periods, what the seller's shop record declared when
--   the order's payment started: `sellerStatus` ("private", "trader" or
--   "undeclared" for a record without a usable declaration) and
--   `location.countryCode`. NULL until the order reaches a payment lock
--   point, and for every existing row.
--
-- Every column is nullable with no default (no rewrite) and bounded by a
-- CHECK that treats NULL as valid, so existing rows pass without a scan that
-- blocks writers for long. A binary from before 0076 never sets or reads
-- these columns.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS buyer_country TEXT
    CONSTRAINT orders_buyer_country_format
    CHECK (buyer_country IS NULL OR buyer_country ~ '^[A-Z]{2}$');

ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_status TEXT
    CONSTRAINT orders_seller_status_values
    CHECK (seller_status IS NULL OR seller_status IN ('private', 'trader', 'undeclared'));

ALTER TABLE orders ADD COLUMN IF NOT EXISTS seller_country TEXT
    CONSTRAINT orders_seller_country_format
    CHECK (seller_country IS NULL OR seller_country ~ '^[A-Z]{2}$');

ALTER TABLE offers ADD COLUMN IF NOT EXISTS buyer_country TEXT
    CONSTRAINT offers_buyer_country_format
    CHECK (buyer_country IS NULL OR buyer_country ~ '^[A-Z]{2}$');

ALTER TABLE bids ADD COLUMN IF NOT EXISTS buyer_country TEXT
    CONSTRAINT bids_buyer_country_format
    CHECK (buyer_country IS NULL OR buyer_country ~ '^[A-Z]{2}$');
