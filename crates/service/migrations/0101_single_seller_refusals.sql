-- Single-seller mode (STORE_SELLER_PUBKY): a command or route that acts for a
-- seller the deployment does not serve is refused and audited. The three
-- closed refusal-audit catalogs each gain one row (append-only since 0032;
-- the source enums and these tables keep exact parity):
--
-- * refusal kind 41 `seller_not_served`, the refusal itself;
-- * surface 3 `seller_route`, a refused HTTP route outside `/v1/commands`;
-- * command kind 44 `seller_route`, the command kind such a route records.
--
-- A refused command keeps its own surface (`v1_command`) and command kind.
-- An image from before 0101 never writes these ids; the rows are harmless to
-- it. Directly rerunnable.

INSERT INTO command_refusal_kinds (id, name) VALUES
  (41, 'seller_not_served')
ON CONFLICT (id) DO NOTHING;

INSERT INTO command_refusal_surface_kinds (id, name) VALUES
  (3, 'seller_route')
ON CONFLICT (id) DO NOTHING;

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (44, 'seller_route')
ON CONFLICT (id) DO NOTHING;

-- ON CONFLICT DO NOTHING keeps whatever row already holds each id. Fail
-- loudly if that row is not this one, so source and catalog cannot drift
-- apart silently.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_kinds
        WHERE id = 41 AND name = 'seller_not_served'
    ) THEN
        RAISE EXCEPTION 'command_refusal_kinds id 41 is not seller_not_served';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_surface_kinds
        WHERE id = 3 AND name = 'seller_route'
    ) THEN
        RAISE EXCEPTION 'command_refusal_surface_kinds id 3 is not seller_route';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_command_kinds
        WHERE id = 44 AND name = 'seller_route'
    ) THEN
        RAISE EXCEPTION 'command_refusal_command_kinds id 44 is not seller_route';
    END IF;
END
$$;
