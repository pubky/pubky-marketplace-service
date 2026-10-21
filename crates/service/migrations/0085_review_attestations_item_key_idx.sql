-- no-transaction
-- One purchase attestation per order, reviewer and listing (SV1). 0086
-- makes it the table's primary key in place of (order_id, reviewer_pubky).
CREATE UNIQUE INDEX CONCURRENTLY review_attestations_item_key
    ON review_attestations (order_id, reviewer_pubky, listing_uri)
