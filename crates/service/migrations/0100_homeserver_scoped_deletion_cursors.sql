-- Listing deletion cursors name the homeserver they count on.
--
-- A homeserver numbers its events in a sequence of its own. Under
-- HOMESERVER_RESOLUTION=pkarr each seller is read from the homeserver their
-- Pkarr record names, which changes when the seller moves. A cursor from one
-- homeserver says nothing about another's events, so every stored cursor now
-- names its homeserver, and cursors are compared only on the same one:
--
-- * `listing_deletion_cursors.homeserver`: the homeserver `event_cursor`
--   counts on. When the follower reads a seller from another homeserver it
--   restarts that seller at the start of the new homeserver's stream, so no
--   delete there is skipped. The cursor still never moves backwards on one
--   homeserver; a write that changes `homeserver` may set any cursor.
-- * `listings.deleted_event_homeserver`: the homeserver of a tombstone's
--   `deleted_event_cursor`. The revival trigger moves it into the revival
--   marker and clears it.
-- * `listings.revived_from_homeserver`: the homeserver of
--   `revived_from_cursor`. A tombstone is refused for a delete that predates
--   the revival only when both are on the same homeserver. The marker never
--   moves backwards on one homeserver, and its homeserver changes only with
--   a revival, which records the delete it superseded, on its homeserver.
--
-- NULL is the one homeserver HOMESERVER_URL names
-- (HOMESERVER_RESOLUTION=fixed): every row that exists when this runs, and
-- every row a `fixed` deployment writes, so `fixed` compares cursors exactly
-- as before. Otherwise the value is the homeserver's pubky (52 characters of
-- z-base-32).
--
-- Additive and directly rerunnable. There is NO DOWN migration. A binary
-- from before this migration ignores the columns; its writes leave
-- `homeserver` as it is and its tombstones name no homeserver, which is what
-- `fixed` writes.
--
-- Nullable columns with no default rewrite no table and build no index. Every
-- check is added NOT VALID, like 0095's: enforced for each new and changed
-- row, with no scan of the table under the ALTER's lock. Every existing row
-- has NULL in the new columns, so it satisfies each check.

-- The column adds and trigger replacement lock `listings` briefly, as 0050.
SET LOCAL lock_timeout = '10s';

ALTER TABLE listing_deletion_cursors ADD COLUMN IF NOT EXISTS homeserver TEXT;
ALTER TABLE listing_deletion_cursors
    DROP CONSTRAINT IF EXISTS listing_deletion_cursors_homeserver_check;
ALTER TABLE listing_deletion_cursors ADD CONSTRAINT listing_deletion_cursors_homeserver_check
    CHECK (homeserver IS NULL OR homeserver ~ '^[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$')
    NOT VALID;

ALTER TABLE listings ADD COLUMN IF NOT EXISTS deleted_event_homeserver TEXT;
ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_deleted_event_homeserver_check;
ALTER TABLE listings ADD CONSTRAINT listings_deleted_event_homeserver_check
    CHECK (deleted_event_homeserver IS NULL
           OR deleted_event_homeserver ~ '^[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$')
    NOT VALID;

ALTER TABLE listings ADD COLUMN IF NOT EXISTS revived_from_homeserver TEXT;
ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_revived_from_homeserver_check;
ALTER TABLE listings ADD CONSTRAINT listings_revived_from_homeserver_check
    CHECK (revived_from_homeserver IS NULL
           OR revived_from_homeserver ~ '^[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$')
    NOT VALID;

CREATE OR REPLACE FUNCTION listings_deletion_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    revival BOOLEAN := OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NULL;
BEGIN
    IF revival THEN
        NEW.revived_from_homeserver := OLD.revived_from_homeserver;
    ELSIF NEW.revived_from_homeserver IS DISTINCT FROM OLD.revived_from_homeserver THEN
        RAISE EXCEPTION 'listing revival marker refused: its homeserver changes only with a revival';
    END IF;
    IF OLD.revived_from_cursor IS NOT NULL
        AND (NEW.revived_from_cursor IS NULL
             OR NEW.revived_from_cursor::numeric < OLD.revived_from_cursor::numeric) THEN
        RAISE EXCEPTION 'listing revival marker refused: it would move backwards';
    END IF;
    IF revival THEN
        IF OLD.deleted_event_cursor IS NOT NULL
            AND (NEW.revived_from_cursor IS NULL
                 OR OLD.deleted_event_homeserver IS DISTINCT FROM NEW.revived_from_homeserver
                 OR OLD.deleted_event_cursor::numeric > NEW.revived_from_cursor::numeric) THEN
            NEW.revived_from_cursor := OLD.deleted_event_cursor;
            NEW.revived_from_homeserver := OLD.deleted_event_homeserver;
        END IF;
        NEW.deleted_event_homeserver := NULL;
    ELSIF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        IF listing_deletion_authority() <> 'command'
            AND NOT listing_deletion_authority_holds_lease(listing_deletion_authority()) THEN
            RAISE EXCEPTION 'listing tombstone refused: no current deletion authority';
        END IF;
        IF listing_deletion_observed_epoch() <> OLD.record_epoch::text THEN
            RAISE EXCEPTION 'listing tombstone refused: the record changed after the delete was confirmed';
        END IF;
        IF NEW.revived_from_cursor IS NOT NULL
            AND NEW.deleted_event_homeserver IS NOT DISTINCT FROM NEW.revived_from_homeserver
            AND NEW.deleted_event_cursor ~ '^[0-9]{1,20}$'
            AND NEW.deleted_event_cursor::numeric < NEW.revived_from_cursor::numeric THEN
            RAISE EXCEPTION 'listing tombstone refused: its delete predates the revival';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS listings_deletion_guard ON listings;
CREATE TRIGGER listings_deletion_guard
    BEFORE UPDATE OF deleted_at, revived_from_cursor, revived_from_homeserver ON listings
    FOR EACH ROW
    WHEN (OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
          OR OLD.revived_from_cursor IS DISTINCT FROM NEW.revived_from_cursor
          OR OLD.revived_from_homeserver IS DISTINCT FROM NEW.revived_from_homeserver)
    EXECUTE FUNCTION listings_deletion_guard();

CREATE OR REPLACE FUNCTION listing_deletion_cursors_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'listing deletion cursor rows are never deleted';
    END IF;
    IF NOT listing_deletion_authority_holds_lease(listing_deletion_authority()) THEN
        RAISE EXCEPTION 'listing deletion cursor write refused: no current follower lease';
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.event_cursor IS NOT NULL
        AND NEW.homeserver IS NOT DISTINCT FROM OLD.homeserver
        AND (NEW.event_cursor IS NULL
             OR NEW.event_cursor::numeric < OLD.event_cursor::numeric) THEN
        RAISE EXCEPTION 'listing deletion cursor refused: it would move backwards';
    END IF;
    RETURN NEW;
END;
$$;
