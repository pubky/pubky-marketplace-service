-- Read-only operator query: every listing whose record lists two or more
-- enabled variants but that still sells from one shared total because it
-- cannot convert to per-variant stock yet (migration 0070,
-- `listing_variant_conversion_blocks`). NOT a sqlx migration; it writes
-- nothing.
--
-- Columns (tab-separated, no header): aggregate_id, seller_pubky, listing_id,
-- reason (`open_holds` or `unattributable_sales`), enabled_variants,
-- reserved_quantity, sold_quantity, attempts, first_blocked_at,
-- last_attempt_at (UTC). Seller pubkeys are public; no buyer data is read.
SELECT l.aggregate_id, l.seller_pubky, l.listing_id, b.reason, b.enabled_variants,
       b.reserved_quantity, b.sold_quantity, b.attempts,
       to_char(b.first_blocked_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
       to_char(b.last_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
FROM listing_variant_conversion_blocks b
JOIN listings l ON l.aggregate_id = b.listing_aggregate_id
WHERE l.deleted_at IS NULL
ORDER BY b.first_blocked_at, l.aggregate_id;
