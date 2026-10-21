-- no-transaction
-- One review per order, reviewer role and listing (SV1). 0086 attaches it as
-- the table's uniqueness constraint in place of `reviews_one_per_order_role`.
CREATE UNIQUE INDEX CONCURRENTLY reviews_one_per_order_role_listing
    ON reviews (order_id, reviewer_role, listing_aggregate_id)
