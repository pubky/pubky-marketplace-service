-- A per-connection request to read the store's stock now. Additive and
-- directly rerunnable. There is NO DOWN migration.
--
-- Saving a connection's mappings sets `stock_refresh_requested_at`; the
-- connector worker reads that connection's catalog on its next tick and
-- applies the stock of its mapped variants (counts, and made-to-order for a
-- store variant sold without a limit), then clears the mark. A listing just
-- linked to its store product (published from the catalog, or a CSV-imported
-- listing matched by SKU) therefore shows the store's stock within seconds
-- instead of at the next periodic poll.
--
-- One nullable column without a default: no rewrite, no index. A binary from
-- before 0074 never sets or reads it.

SET LOCAL lock_timeout = '10s';

ALTER TABLE connector_connections
    ADD COLUMN IF NOT EXISTS stock_refresh_requested_at TIMESTAMPTZ;
