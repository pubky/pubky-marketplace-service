-- Commerce connectors (Shopify, WooCommerce): the connection, its sealed
-- credential, its sealed address-recipient key, the listing <-> store-product
-- mapping and the seller's product selection. Owned by the connector worker
-- and the /v1/connectors routes; no existing table is altered and nothing
-- here depends on another pending migration number. Additive and rerunnable.
--
--   connector_connections      one row per (seller, provider, shop). A row is
--                              a `draft` from the seller's connect request
--                              (POST /v1/connectors) until the store's
--                              install completes; a draft that is never
--                              completed expires after 24 hours. A connected
--                              store whose grant lacks a required scope is
--                              `needs_attention` with the missing names. A
--                              disconnected row stays so the push ledger keeps
--                              its owner; its credential, key and mappings are
--                              gone. A store has at most one live (connected,
--                              needs_attention or error) connection, so a
--                              webhook names one seller. `app_kind` says whose
--                              app the store installed: `none` (WooCommerce),
--                              `custom` (the merchant's own Shopify custom
--                              app, its client id and secret in
--                              connector_app_credentials) or `public` (the
--                              reserved slot for Pubky's own public app).
--   connector_app_credentials  a merchant's own app client id and secret, one
--                              pair per connection, sealed with the shared seal
--                              construction under
--                              CONNECTOR_CREDENTIAL_ENCRYPTION_KEY. Associated
--                              data: connector-cred|v1|{seller}|{provider}|
--                              {shop}|{connection_id}. They verify the launch,
--                              OAuth and webhook signatures of that connection
--                              only. After a disconnect or an uninstall the pair
--                              is kept until `retain_until` (30 days) or until
--                              Shopify's `shop/redact` is handled, so the
--                              privacy webhooks that arrive after an uninstall
--                              can still be verified; it is deleted at once for
--                              a draft that was never installed.
--   connector_credentials      the store's access secret (an offline token, an
--                              API key pair), sealed under the same key.
--                              Associated data: connector-cred|v1|{seller}|
--                              {provider}|{shop}. Deleted on disconnect.
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
    app_kind TEXT NOT NULL DEFAULT 'none'
        CONSTRAINT connector_connections_app_kind CHECK (app_kind IN ('none', 'custom', 'public')),
    status TEXT NOT NULL
        CONSTRAINT connector_connections_status CHECK (
            status IN ('draft', 'connected', 'needs_attention', 'error', 'disconnected')
        ),
    scopes TEXT[] NOT NULL DEFAULT '{}',
    missing_scopes TEXT[] NOT NULL DEFAULT '{}',
    connect_state_hash BYTEA
        CONSTRAINT connector_connections_state_len CHECK (
            connect_state_hash IS NULL OR octet_length(connect_state_hash) = 32
        ),
    state_expires_at TIMESTAMPTZ,
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
    CONSTRAINT connector_connections_draft_expiry CHECK (
        status <> 'draft' OR connect_expires_at IS NOT NULL
    ),
    CONSTRAINT connector_connections_connected_stamp CHECK (
        status NOT IN ('connected', 'needs_attention', 'error') OR connected_at IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS connector_connections_seller_idx
    ON connector_connections (seller_pubky, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS connector_connections_state_idx
    ON connector_connections (connect_state_hash)
    WHERE connect_state_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS connector_connections_live_idx
    ON connector_connections (provider, shop)
    WHERE status IN ('connected', 'needs_attention', 'error');

CREATE INDEX IF NOT EXISTS connector_connections_draft_idx
    ON connector_connections (connect_expires_at)
    WHERE status = 'draft';

CREATE INDEX IF NOT EXISTS connector_connections_shop_idx
    ON connector_connections (provider, shop, updated_at DESC);

CREATE TABLE IF NOT EXISTS connector_app_credentials (
    connection_id UUID PRIMARY KEY REFERENCES connector_connections (id) ON DELETE CASCADE,
    sealed BYTEA NOT NULL
        CONSTRAINT connector_app_credentials_sealed_len CHECK (octet_length(sealed) > 24),
    retain_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS connector_app_credentials_retain_idx
    ON connector_app_credentials (retain_until)
    WHERE retain_until IS NOT NULL;

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
