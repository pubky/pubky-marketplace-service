-- Read-only release-lane query: every listing the service would still sell or
-- take payment on, for scripts/listing-record-sweep.sh to probe on the
-- homeserver. NOT a sqlx migration; it writes nothing.
--
-- Columns (tab-separated, no header): aggregate_id, seller_pubky, listing_id,
-- sale_format, state. Seller pubkeys are public; no buyer data is read.
SELECT aggregate_id, seller_pubky, listing_id, sale_format, state
FROM listings
WHERE deleted_at IS NULL
  AND (
    (sale_format = 'fixed_price'
      AND state IN ('available', 'reserved')
      AND (available_quantity > 0 OR reserved_quantity > 0))
    OR (sale_format = 'auction' AND auction->>'status' = 'active')
  )
ORDER BY seller_pubky, listing_id;
