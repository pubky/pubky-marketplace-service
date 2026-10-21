-- return.cancel joins the closed refusal-audit command-kind catalog. The
-- catalog is append-only (0032's trigger forbids UPDATE and DELETE); the
-- source enum and this table keep exact parity.

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (41, 'cancel_return')
ON CONFLICT (id) DO NOTHING;

-- ON CONFLICT DO NOTHING keeps whatever row already holds id 41. Fail loudly
-- if that row is not this command kind, so source and catalog cannot drift
-- apart silently.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_command_kinds
        WHERE id = 41 AND name = 'cancel_return'
    ) THEN
        RAISE EXCEPTION
            'command_refusal_command_kinds id 41 is not cancel_return';
    END IF;
END
$$;
