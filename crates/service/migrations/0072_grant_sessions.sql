-- Marketplace sessions minted from the Shop's own sign-in grant
-- (`POST /v1/auth/grant-sessions`).
--
-- `grant_session_challenges` holds the challenges the service issues
-- (`POST /v1/auth/grant-session-challenges`), each valid for one proof age.
-- A mint burns its challenge (`used_at`, `grant_id`) in the same transaction
-- that inserts the session, so a captured proof cannot mint twice, and a
-- proof document cannot be staged for a challenge that does not exist yet.
-- Expired rows are pruned when a challenge is issued.
--
-- `auth_sessions.source_grant_id` and `source_client_id` record which grant
-- (and which app's grant) a session came from; NULL for every other session.
-- They are kept for audit; nothing revokes sessions by grant yet, so a
-- session minted before its grant is revoked lives until it expires.
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
    issued_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    grant_id TEXT,
    CHECK (expires_at > issued_at),
    CHECK ((used_at IS NULL) = (grant_id IS NULL))
);

CREATE INDEX IF NOT EXISTS grant_session_challenges_expires_at_idx
    ON grant_session_challenges (expires_at);

CREATE INDEX IF NOT EXISTS grant_session_challenges_live_idx
    ON grant_session_challenges (pubky)
    WHERE used_at IS NULL;

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
