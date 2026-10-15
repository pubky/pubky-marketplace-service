-- fulfillment.update_tracking joins the closed refusal-audit command-kind
-- catalog. The catalog is append-only (0032's trigger forbids UPDATE and
-- DELETE); the source enum and this table keep exact parity.

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (40, 'update_tracking')
ON CONFLICT (id) DO NOTHING;
