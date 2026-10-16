-- Commerce connectors, stage 2: how a connection's store variants relate to
-- Pubky listings.
--
--   native  a store product's variants are the variants of one Pubky listing
--           (the default).
--   split   each store variant maps 1:1 to its own Pubky listing. The mapping
--           is checked for it (a listing appears once), and stock then syncs
--           per listing, which the service already tracks, even for a store
--           whose products are mostly variable.
--
-- Additive and rerunnable: one column with a constant default on a small
-- table, so no rewrite. Existing connections stay `native`.

SET LOCAL lock_timeout = '10s';

ALTER TABLE connector_connections
    ADD COLUMN IF NOT EXISTS variant_mode TEXT NOT NULL DEFAULT 'native'
        CONSTRAINT connector_connections_variant_mode CHECK (variant_mode IN ('native', 'split'));
