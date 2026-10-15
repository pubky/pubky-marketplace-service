-- Commerce connectors: the order-push ledger, the downstream order actions,
-- the durable event cursor, the inbound webhook dedupe table and the seller
-- alerts. Needs only 0062 (connector_connections) and the events table.
-- Additive and rerunnable.
--
--   connector_order_push      one row per (seller, Pubky order, provider).
--                             pending -> sending -> created(external_id)
--                             | quarantined(reason). `sending` is written
--                             before the provider is called; a `sending`
--                             row whose lease lapsed is an unknown outcome,
--                             so the worker looks the order up in the store
--                             before it pushes again.
--   connector_order_actions   cancel and refund mirrors of a created push,
--                             one per (push, kind, action_key).
--   connector_event_cursor    the worker's durable position in `events`,
--                             started at the newest event when this file
--                             runs, so orders paid before the connectors
--                             existed are never pushed.
--   connector_inbound_events  provider webhooks deduplicated by the
--                             provider's own event id. Holds the normalised
--                             event, never the raw body.
--   connector_alerts          what a seller must look at: a quarantined
--                             push, a cancel or refund made in the store, an
--                             uninstalled app.

CREATE TABLE IF NOT EXISTS connector_order_push (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connector_connections (id),
    seller_pubky TEXT NOT NULL,
    pubky_order_id UUID NOT NULL,
    provider TEXT NOT NULL
        CONSTRAINT connector_order_push_provider CHECK (provider IN ('shopify', 'woocommerce')),
    state TEXT NOT NULL
        CONSTRAINT connector_order_push_state CHECK (
            state IN ('pending', 'sending', 'created', 'quarantined')
        ),
    external_id TEXT
        CONSTRAINT connector_order_push_external_len CHECK (
            external_id IS NULL OR char_length(external_id) BETWEEN 1 AND 256
        ),
    quarantine_reason TEXT
        CONSTRAINT connector_order_push_reason_len CHECK (
            quarantine_reason IS NULL OR char_length(quarantine_reason) BETWEEN 1 AND 64
        ),
    attempts INTEGER NOT NULL DEFAULT 0
        CONSTRAINT connector_order_push_attempts CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    last_error TEXT
        CONSTRAINT connector_order_push_error_len CHECK (
            last_error IS NULL OR char_length(last_error) <= 500
        ),
    source_event_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT connector_order_push_once UNIQUE (seller_pubky, pubky_order_id, provider),
    CONSTRAINT connector_order_push_shape CHECK (
        (state = 'created') = (external_id IS NOT NULL)
        AND (state = 'quarantined') = (quarantine_reason IS NOT NULL)
        AND (state = 'sending') = (lease_until IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS connector_order_push_due_idx
    ON connector_order_push (next_attempt_at)
    WHERE state IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS connector_order_push_connection_idx
    ON connector_order_push (connection_id, created_at DESC, id);

CREATE INDEX IF NOT EXISTS connector_order_push_external_idx
    ON connector_order_push (connection_id, external_id)
    WHERE external_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS connector_order_actions (
    id UUID PRIMARY KEY,
    push_id UUID NOT NULL REFERENCES connector_order_push (id),
    kind TEXT NOT NULL
        CONSTRAINT connector_order_actions_kind CHECK (kind IN ('cancel', 'refund')),
    action_key TEXT NOT NULL
        CONSTRAINT connector_order_actions_key_len CHECK (char_length(action_key) BETWEEN 1 AND 128),
    state TEXT NOT NULL
        CONSTRAINT connector_order_actions_state CHECK (
            state IN ('pending', 'sending', 'done', 'quarantined')
        ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempts INTEGER NOT NULL DEFAULT 0
        CONSTRAINT connector_order_actions_attempts CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    last_error TEXT
        CONSTRAINT connector_order_actions_error_len CHECK (
            last_error IS NULL OR char_length(last_error) <= 500
        ),
    quarantine_reason TEXT
        CONSTRAINT connector_order_actions_reason_len CHECK (
            quarantine_reason IS NULL OR char_length(quarantine_reason) BETWEEN 1 AND 64
        ),
    source_event_id UUID,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT connector_order_actions_once UNIQUE (push_id, kind, action_key),
    CONSTRAINT connector_order_actions_shape CHECK (
        (state = 'quarantined') = (quarantine_reason IS NOT NULL)
        AND (state = 'sending') = (lease_until IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS connector_order_actions_due_idx
    ON connector_order_actions (next_attempt_at)
    WHERE state IN ('pending', 'sending');

CREATE TABLE IF NOT EXISTS connector_event_cursor (
    name TEXT PRIMARY KEY,
    sequence BIGINT NOT NULL CONSTRAINT connector_event_cursor_sequence CHECK (sequence >= 0),
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO connector_event_cursor (name, sequence, updated_at)
SELECT 'events', COALESCE(MAX(sequence), 0), now() FROM events
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS connector_inbound_events (
    id UUID PRIMARY KEY,
    connection_id UUID NOT NULL REFERENCES connector_connections (id),
    provider TEXT NOT NULL
        CONSTRAINT connector_inbound_events_provider CHECK (provider IN ('shopify', 'woocommerce')),
    provider_event_id TEXT NOT NULL
        CONSTRAINT connector_inbound_events_event_len CHECK (
            char_length(provider_event_id) BETWEEN 1 AND 256
        ),
    topic TEXT NOT NULL
        CONSTRAINT connector_inbound_events_topic_len CHECK (char_length(topic) BETWEEN 1 AND 128),
    event JSONB NOT NULL,
    state TEXT NOT NULL
        CONSTRAINT connector_inbound_events_state CHECK (
            state IN ('pending', 'applied', 'ignored', 'failed')
        ),
    outcome TEXT
        CONSTRAINT connector_inbound_events_outcome_len CHECK (
            outcome IS NULL OR char_length(outcome) <= 500
        ),
    attempts INTEGER NOT NULL DEFAULT 0
        CONSTRAINT connector_inbound_events_attempts CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    CONSTRAINT connector_inbound_events_once UNIQUE (connection_id, provider_event_id)
);

CREATE INDEX IF NOT EXISTS connector_inbound_events_due_idx
    ON connector_inbound_events (next_attempt_at)
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS connector_alerts (
    id UUID PRIMARY KEY,
    seller_pubky TEXT NOT NULL,
    connection_id UUID NOT NULL REFERENCES connector_connections (id),
    kind TEXT NOT NULL
        CONSTRAINT connector_alerts_kind CHECK (
            kind IN (
                'push_quarantined',
                'action_quarantined',
                'merchant_cancelled',
                'merchant_refunded',
                'store_uninstalled',
                'credentials_rejected',
                'inbound_failed'
            )
        ),
    pubky_order_id UUID,
    detail TEXT NOT NULL
        CONSTRAINT connector_alerts_detail_len CHECK (char_length(detail) <= 500),
    dedupe_key TEXT NOT NULL
        CONSTRAINT connector_alerts_dedupe_len CHECK (char_length(dedupe_key) <= 300),
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    CONSTRAINT connector_alerts_once UNIQUE (dedupe_key)
);

CREATE INDEX IF NOT EXISTS connector_alerts_seller_idx
    ON connector_alerts (seller_pubky, created_at DESC, id);
