-- Marketplace sessions minted from the Shop's own sign-in grant
-- (`POST /v1/auth/grant-sessions`).
--
-- `grant_session_challenges` records each challenge id the moment a session
-- is minted from it, in the same transaction, so a captured proof cannot mint
-- a second session. Rows are pruned once no proof naming them can still be
-- young enough to be accepted.
--
-- `auth_sessions.source_grant_id` and `source_client_id` record which grant
-- (and which app's grant) a session came from; NULL for every other session.
-- They let a revoked grant's sessions be found and revoked.
--
-- `automation_rate_limits` admits one more endpoint class,
-- `grant_session.mint`. The widened CHECK is `NOT VALID`: every existing row
-- holds one of the old classes, a subset of the new list, and no table scan
-- runs under the lock.
--
-- Builds no index on an existing table. Additive and rerunnable. There is NO
-- DOWN migration. A binary from before this migration still runs against it:
-- it never reads the new table and its inserts leave the new columns NULL.

SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS grant_session_challenges (
    challenge_id UUID PRIMARY KEY,
    pubky TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    used_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS grant_session_challenges_used_at_idx
    ON grant_session_challenges (used_at);

ALTER TABLE auth_sessions ADD COLUMN IF NOT EXISTS source_grant_id TEXT;
ALTER TABLE auth_sessions ADD COLUMN IF NOT EXISTS source_client_id TEXT;

ALTER TABLE automation_rate_limits
    DROP CONSTRAINT IF EXISTS automation_rate_limits_endpoint_class_check;
ALTER TABLE automation_rate_limits
    ADD CONSTRAINT automation_rate_limits_endpoint_class_check CHECK (
        endpoint_class IN (
            'session.admin', 'export.listings', 'export.orders',
            'events.read', 'listing.sync_many', 'webhook.admin',
            'grant_session.mint'
        )
    ) NOT VALID;
