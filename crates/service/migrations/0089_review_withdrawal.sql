-- Review withdrawal (Pubky Market reviews): `review.withdraw` records that a
-- reviewer removed their public review, so every device the reviewer signs
-- in on reads the removal from the service instead of republishing the
-- review. `public_removed_at` is NULL on every review that is published,
-- which is every review that exists today; `review.update` clears it again.
-- Nullable with no default, so the ALTER rewrites nothing. Additive and
-- directly rerunnable.

SET LOCAL lock_timeout = '10s';

ALTER TABLE reviews
    ADD COLUMN IF NOT EXISTS public_removed_at TIMESTAMPTZ;

-- review.withdraw joins the closed refusal-audit command-kind catalog. The
-- catalog is append-only (0032's trigger forbids UPDATE and DELETE); the
-- source enum and this table keep exact parity.
INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (42, 'withdraw_review')
ON CONFLICT (id) DO NOTHING;

-- ON CONFLICT DO NOTHING keeps whatever row already holds id 42. Fail loudly
-- if that row is not this command kind, so source and catalog cannot drift
-- apart silently.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_command_kinds
        WHERE id = 42 AND name = 'withdraw_review'
    ) THEN
        RAISE EXCEPTION
            'command_refusal_command_kinds id 42 is not withdraw_review';
    END IF;
END
$$;
