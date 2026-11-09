-- Store product changes queued for the seller's Shop to review and apply.
--
--   connector_sync_settings    the sync direction of a field group for a
--                              connection; no row is the default
--                              (`from_store`).
--   connector_sync_overrides   the same for one linked listing; it wins over
--                              the connection's setting.
--   connector_product_snapshots the base of each linked store product: per
--                              field group, the store's value as last applied
--                              or first seen. A NULL group is rebased (taken as
--                              is) at the next observation.
--   connector_product_changes  a field group whose store value differs from
--                              its base. At most one pending row per product
--                              and group (a newer store value replaces its
--                              proposal) and the latest resolution, which a
--                              later observation is compared with. Withdrawn
--                              changes are deleted.
--
-- The service only queues. Listing records live on the seller's homeserver
-- and only the seller's Shop writes them; it reads the pending changes,
-- applies them, and resolves each one here.

CREATE TABLE IF NOT EXISTS connector_sync_settings (
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    field_group TEXT NOT NULL
        CONSTRAINT connector_sync_settings_group CHECK (field_group IN ('content', 'price', 'variants')),
    direction TEXT NOT NULL
        CONSTRAINT connector_sync_settings_direction CHECK (direction IN ('from_store', 'off')),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, field_group)
);

CREATE TABLE IF NOT EXISTS connector_sync_overrides (
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    listing_id TEXT NOT NULL
        CONSTRAINT connector_sync_overrides_listing_len CHECK (char_length(listing_id) BETWEEN 1 AND 128),
    field_group TEXT NOT NULL
        CONSTRAINT connector_sync_overrides_group CHECK (field_group IN ('content', 'price', 'variants')),
    direction TEXT NOT NULL
        CONSTRAINT connector_sync_overrides_direction CHECK (direction IN ('from_store', 'off')),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, listing_id, field_group)
);

CREATE TABLE IF NOT EXISTS connector_product_snapshots (
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    provider_product_id TEXT NOT NULL
        CONSTRAINT connector_product_snapshots_product_len CHECK (char_length(provider_product_id) BETWEEN 1 AND 256),
    content JSONB,
    price JSONB,
    variants JSONB,
    status JSONB,
    checked_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, provider_product_id)
);

CREATE TABLE IF NOT EXISTS connector_product_changes (
    id UUID PRIMARY KEY,
    -- Creation order. A product and group has at most one live change at a
    -- time, so this is also the order of their resolutions.
    seq BIGINT GENERATED ALWAYS AS IDENTITY,
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    provider_product_id TEXT NOT NULL
        CONSTRAINT connector_product_changes_product_len CHECK (char_length(provider_product_id) BETWEEN 1 AND 256),
    field_group TEXT NOT NULL
        CONSTRAINT connector_product_changes_group CHECK (field_group IN ('content', 'price', 'variants', 'status')),
    base JSONB,
    proposed JSONB NOT NULL,
    proposed_hash BYTEA NOT NULL
        CONSTRAINT connector_product_changes_hash_len CHECK (octet_length(proposed_hash) = 32),
    state TEXT NOT NULL
        CONSTRAINT connector_product_changes_state
            CHECK (state IN ('pending', 'applied', 'dismissed')),
    detected_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    CONSTRAINT connector_product_changes_resolved
        CHECK ((state = 'pending') = (resolved_at IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS connector_product_changes_one_pending
    ON connector_product_changes (connection_id, provider_product_id, field_group)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS connector_product_changes_by_connection
    ON connector_product_changes (connection_id, state, detected_at);

CREATE INDEX IF NOT EXISTS connector_product_changes_resolved_by_product
    ON connector_product_changes (connection_id, provider_product_id, field_group, seq DESC)
    WHERE state IN ('applied', 'dismissed');
