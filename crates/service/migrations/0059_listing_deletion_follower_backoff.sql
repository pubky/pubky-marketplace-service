-- The listing deletion follower backs off per seller when the homeserver
-- event stream rejects or cannot answer a read.
--
-- `polled_at` is now the seller's last SUCCESSFUL poll; NULL means the
-- seller has never been polled successfully (a failed first poll inserts
-- the row so its backoff is kept). A failed poll counts in `failures`, holds
-- the seller back until `retry_not_before`, and a successful poll clears
-- both. `attempted_at` is the last poll of either kind: due sellers are
-- served least recently attempted first, so a seller that keeps failing
-- cannot crowd the others out of a pass.
--
-- Additive and rerunnable. There is NO DOWN migration. A binary from before
-- this migration still runs against it: it ignores the new columns and its
-- inserts take their defaults.

ALTER TABLE listing_deletion_cursors ALTER COLUMN polled_at DROP NOT NULL;
ALTER TABLE listing_deletion_cursors
    ADD COLUMN IF NOT EXISTS attempted_at TIMESTAMPTZ;
ALTER TABLE listing_deletion_cursors
    ADD COLUMN IF NOT EXISTS failures INTEGER NOT NULL DEFAULT 0
        CONSTRAINT listing_deletion_cursors_failures_check CHECK (failures >= 0);
ALTER TABLE listing_deletion_cursors
    ADD COLUMN IF NOT EXISTS retry_not_before TIMESTAMPTZ;
