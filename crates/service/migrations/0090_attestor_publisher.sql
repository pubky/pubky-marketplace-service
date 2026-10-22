-- Attestor publisher (Pubky Market reviews, SV3): what the service has written
-- to the attestor's own homeserver, and the `returned` annotation outcome.
-- Additive and directly rerunnable.

SET LOCAL lock_timeout = '10s';

-- One row per record the publisher has written successfully, keyed by the
-- attestor that owns it: the attestor's profile and one annotation per
-- attested order. `body_hash` covers the record's content without its
-- revision and proof, so an unchanged record is never written again and a
-- changed one is written at the next revision. The attestor is part of the key
-- because a rotated attestor key publishes to a new account, where nothing has
-- been written yet.
CREATE TABLE IF NOT EXISTS attestor_publications (
    attestor_pubky TEXT NOT NULL,
    path TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    body_hash TEXT NOT NULL CHECK (body_hash ~ '^[0-9a-f]{64}$'),
    published_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (attestor_pubky, path)
);

-- The write in flight or last failed, recorded before the PUT so a crash
-- mid-write and a failed write look the same on the next pass: the revision is
-- not reused for different content, and the backoff holds across a restart.
-- Deleted when the write is recorded in `attestor_publications`.
CREATE TABLE IF NOT EXISTS attestor_publish_attempts (
    attestor_pubky TEXT NOT NULL,
    path TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    body_hash TEXT NOT NULL CHECK (body_hash ~ '^[0-9a-f]{64}$'),
    attempts INTEGER NOT NULL CHECK (attempts > 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (attestor_pubky, path)
);

-- `returned`: written when the seller receives the returned goods. Widens the
-- outcome list 0039 set, so no existing row can violate it; NOT VALID then
-- VALIDATE keeps the table lock short. The rows themselves stay append-only
-- (0007's trigger).
ALTER TABLE attestation_annotations
    DROP CONSTRAINT IF EXISTS attestation_annotations_outcome_check;
ALTER TABLE attestation_annotations
    ADD CONSTRAINT attestation_annotations_outcome_check
    CHECK (outcome IN ('refunded', 'refund_reversal_cancelled', 'returned')) NOT VALID;
ALTER TABLE attestation_annotations
    VALIDATE CONSTRAINT attestation_annotations_outcome_check;
