-- The lifecycle claim generation of a pending Locks correlation. Additive and
-- directly rerunnable. There is NO DOWN migration; to back the column out by
-- hand, see "Backing out 0104" in docs/migrations.md.
--
--   claim_generation
--       incremented by every lifecycle claim of the row and returned to the
--       claiming pass. Each outcome the pass writes for the row (a status, a
--       terminal state, a refusal, a parked or confirmed completion, a retry
--       stamp) requires the row to still carry the generation its claim
--       took. A pass whose lease expired while it was still working through
--       its batch can no longer overwrite what a newer claim of the same row
--       recorded: its late response is dropped, and the newer claim (or the
--       next one) records the task's state.
--
-- A constant default adds the column without rewriting the table. Existing
-- rows start at 0; the next claim takes 1. A binary from before 0104 never
-- reads or increments it.

SET LOCAL lock_timeout = '10s';

ALTER TABLE payment_locks_correlations
    ADD COLUMN IF NOT EXISTS claim_generation BIGINT NOT NULL DEFAULT 0;
