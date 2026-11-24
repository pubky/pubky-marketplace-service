-- Per-seller inventory rate buckets and a rate class for session creation.
--
-- `inventory_actor_rate_limits` holds one bucket per seller pubky and
-- inventory endpoint class, shared by all of that seller's sessions. It has
-- no foreign key: a seller's budget outlives any one session, so revoking or
-- expiring a session does not refill it. `inventory_rate_limits` (one bucket
-- per session) stays in place with its rows; a binary from before this
-- migration keeps using it until it drains, and nothing newer reads it.
--
-- `automation_rate_limits` admits one more endpoint class, `session.create`,
-- spent by `POST /v1/auth/sessions` and keyed by the verified pubky. The
-- widened CHECK is `NOT VALID`: every existing row holds one of the old
-- classes, a subset of the new list, and no table scan runs under the lock.
--
-- Builds no index on an existing table. Additive and rerunnable. There is NO
-- DOWN migration.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS inventory_actor_rate_limits (
    actor_pubky TEXT NOT NULL,
    endpoint_class TEXT NOT NULL CHECK (
        endpoint_class IN ('inventory.adjust', 'inventory.read')
    ),
    tokens DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (actor_pubky, endpoint_class)
);

ALTER TABLE automation_rate_limits
    DROP CONSTRAINT IF EXISTS automation_rate_limits_endpoint_class_check;
ALTER TABLE automation_rate_limits
    ADD CONSTRAINT automation_rate_limits_endpoint_class_check CHECK (
        endpoint_class IN (
            'session.admin', 'export.listings', 'export.orders',
            'events.read', 'listing.sync_many', 'webhook.admin',
            'grant_session.mint', 'session.create'
        )
    ) NOT VALID;
