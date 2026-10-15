-- fulfillment.update_tracking joins the closed refusal-audit command-kind
-- catalog. The catalog is append-only (0032's trigger forbids UPDATE and
-- DELETE); the source enum and this table keep exact parity.

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (40, 'update_tracking')
ON CONFLICT (id) DO NOTHING;

-- ON CONFLICT DO NOTHING keeps whatever row already holds id 40. Fail loudly
-- if that row is not this command kind, so source and catalog cannot drift
-- apart silently.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_command_kinds
        WHERE id = 40 AND name = 'update_tracking'
    ) THEN
        RAISE EXCEPTION
            'command_refusal_command_kinds id 40 is not update_tracking';
    END IF;
END
$$;
