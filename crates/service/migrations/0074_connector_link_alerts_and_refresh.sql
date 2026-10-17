-- A per-connection request to read the store's stock now, and the
-- `listing_not_linked` connector alert. Additive and directly rerunnable.
-- There is NO DOWN migration.
--
-- Saving a connection's mappings sets `stock_refresh_requested_at`; the
-- connector worker reads that connection's catalog on its next tick and
-- applies the stock of its mapped variants (counts, and made-to-order for a
-- store variant sold without a limit), then clears the mark. A listing just
-- linked to its store product (published from the catalog, or a CSV-imported
-- listing matched by SKU) therefore shows the store's stock within seconds
-- instead of at the next periodic poll.
--
-- `connector_alerts_kind` gains `listing_not_linked`: a paid order whose
-- listings are not linked to any product of the seller's connected store was
-- not sent to the store, and the seller is told so on the order. The table is
-- small, so the constraint is replaced in one pass, like 0067.
--
-- One nullable column without a default: no rewrite, no index. A binary from
-- before 0074 never sets or reads it, and never writes the new alert kind.

SET LOCAL lock_timeout = '10s';

ALTER TABLE connector_connections
    ADD COLUMN IF NOT EXISTS stock_refresh_requested_at TIMESTAMPTZ;

ALTER TABLE connector_alerts DROP CONSTRAINT IF EXISTS connector_alerts_kind;
ALTER TABLE connector_alerts
    ADD CONSTRAINT connector_alerts_kind CHECK (
        kind IN (
            'push_quarantined',
            'action_quarantined',
            'merchant_cancelled',
            'merchant_refunded',
            'store_uninstalled',
            'credentials_rejected',
            'inbound_failed',
            'refund_reversed_after_mirror',
            'listings_unmapped',
            'variant_mapping_error',
            'push_total_mismatch',
            'listing_not_linked'
        )
    );
