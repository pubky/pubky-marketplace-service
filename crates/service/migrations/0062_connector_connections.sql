-- Commerce connectors (Shopify, WooCommerce): the connection, its sealed
-- credential, its sealed address-recipient key, the listing <-> store-product
-- mapping and the seller's product selection. Owned by the connector worker
-- and the /v1/connectors routes; no existing table is altered and nothing
-- here depends on another pending migration number. Additive and rerunnable.
--
--   connector_connections      one row per (seller, provider, shop). A row is
--                              `pending` from the seller's connect request
--                              until the store's callback (a one-time state
--                              hash, 30 minutes). A disconnected row stays so
--                              the push ledger keeps its owner; its
--                              credential, key and mappings are gone. A store
--                              has at most one live (connected or error)
--                              connection, so a webhook names one seller.
--                              `app_key` names which configured provider app
--                              (SHOPIFY_APPS) the store installed.
--   connector_credentials      the provider secret, sealed with the shared
--                              seal construction under
--                              CONNECTOR_CREDENTIAL_ENCRYPTION_KEY. Associated
--                              data: connector-cred|v1|{seller}|{provider}|{shop}.
--   connector_recipient_keys   the X25519 key buyers seal an opted-in delivery
--                              address to. The public half is published in the
--                              seller's connector status; the secret half is
--                              sealed like the credential, with associated
--                              data connector-recipient|v1|{seller}|{provider}|
--                              {shop}|{key_id}.
--   connector_variant_map      Pubky listing/variant <-> store product/variant,
--                              unique in both directions per connection. An
--                              empty variant_id (or provider_variant_id) is a
--                              listing (or product) without variants.
--   connector_product_selection  the store products the seller chose to
--                              publish on the Shop.

CREATE TABLE IF NOT EXISTS connector_connections (
    id UUID PRIMARY KEY,
    seller_pubky TEXT NOT NULL
        CONSTRAINT connector_connections_seller_len CHECK (char_length(seller_pubky) BETWEEN 1 AND 128),
    provider TEXT NOT NULL
        CONSTRAINT connector_connections_provider CHECK (provider IN ('shopify', 'woocommerce')),
    shop TEXT NOT NULL
        CONSTRAINT connector_connections_shop_shape CHECK (
            char_length(shop) BETWEEN 1 AND 255 AND shop !~ '[|[:cntrl:]]'
        ),
    app_key TEXT
        CONSTRAINT connector_connections_app_key_shape CHECK (
            app_key IS NULL OR app_key ~ '^[a-z0-9][a-z0-9-]{0,31}$'
        ),
    status TEXT NOT NULL
        CONSTRAINT connector_connections_status CHECK (
            status IN ('pending', 'connected', 'error', 'disconnected')
        ),
    scopes TEXT[] NOT NULL DEFAULT '{}',
    connect_state_hash BYTEA
        CONSTRAINT connector_connections_state_len CHECK (
            connect_state_hash IS NULL OR octet_length(connect_state_hash) = 32
        ),
    connect_expires_at TIMESTAMPTZ,
    connected_at TIMESTAMPTZ,
    last_error TEXT
        CONSTRAINT connector_connections_error_len CHECK (
            last_error IS NULL OR char_length(last_error) <= 500
        ),
    last_error_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT connector_connections_one_per_shop UNIQUE (seller_pubky, provider, shop),
    CONSTRAINT connector_connections_state_only_pending CHECK (
        connect_state_hash IS NULL OR status = 'pending'
    ),
    CONSTRAINT connector_connections_connected_stamp CHECK (
        status NOT IN ('connected', 'error') OR connected_at IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS connector_connections_seller_idx
    ON connector_connections (seller_pubky, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS connector_connections_state_idx
    ON connector_connections (connect_state_hash)
    WHERE connect_state_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS connector_connections_live_idx
    ON connector_connections (provider, shop)
    WHERE status IN ('connected', 'error');

CREATE INDEX IF NOT EXISTS connector_connections_shop_idx
    ON connector_connections (provider, shop, updated_at DESC);

CREATE TABLE IF NOT EXISTS connector_credentials (
    connection_id UUID PRIMARY KEY REFERENCES connector_connections (id) ON DELETE CASCADE,
    sealed BYTEA NOT NULL
        CONSTRAINT connector_credentials_sealed_len CHECK (octet_length(sealed) > 24),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_recipient_keys (
    connection_id UUID PRIMARY KEY REFERENCES connector_connections (id) ON DELETE CASCADE,
    key_id TEXT NOT NULL
        CONSTRAINT connector_recipient_keys_id_shape CHECK (key_id ~ '^[0-9a-f]{32}$'),
    public_key BYTEA NOT NULL
        CONSTRAINT connector_recipient_keys_public_len CHECK (octet_length(public_key) = 32),
    sealed_secret BYTEA NOT NULL
        CONSTRAINT connector_recipient_keys_sealed_len CHECK (octet_length(sealed_secret) > 24),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_variant_map (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    listing_id TEXT NOT NULL
        CONSTRAINT connector_variant_map_listing_len CHECK (char_length(listing_id) BETWEEN 1 AND 128),
    variant_id TEXT NOT NULL DEFAULT ''
        CONSTRAINT connector_variant_map_variant_len CHECK (char_length(variant_id) <= 128),
    provider_product_id TEXT NOT NULL
        CONSTRAINT connector_variant_map_product_len CHECK (char_length(provider_product_id) BETWEEN 1 AND 256),
    provider_variant_id TEXT NOT NULL DEFAULT ''
        CONSTRAINT connector_variant_map_pvariant_len CHECK (char_length(provider_variant_id) <= 256),
    sku TEXT
        CONSTRAINT connector_variant_map_sku_len CHECK (sku IS NULL OR char_length(sku) <= 128),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT connector_variant_map_pubky_side UNIQUE (connection_id, listing_id, variant_id),
    CONSTRAINT connector_variant_map_provider_side
        UNIQUE (connection_id, provider_product_id, provider_variant_id)
);

CREATE TABLE IF NOT EXISTS connector_product_selection (
    connection_id UUID NOT NULL REFERENCES connector_connections (id) ON DELETE CASCADE,
    provider_product_id TEXT NOT NULL
        CONSTRAINT connector_product_selection_product_len CHECK (
            char_length(provider_product_id) BETWEEN 1 AND 256
        ),
    selected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (connection_id, provider_product_id)
);
