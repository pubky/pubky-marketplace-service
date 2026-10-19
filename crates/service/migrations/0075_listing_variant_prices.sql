-- Per-variant prices: additive, idempotent, directly rerunnable. There is NO
-- DOWN migration.
--
-- A listing record may give an enabled variant its own `priceOverride`. Until
-- now the service kept only the listing's `unit_price`, so checkout charged
-- the listing price for every variant while the Shop showed the variant's own
-- price in the cart.
--
-- `variant_prices` holds the enabled variants that sell at their own price as
-- `[{"variant_id": "...", "unit_price": {amount_minor, currency, exponent}}]`,
-- or NULL when every variant sells at `unit_price`. It is written by the
-- registration (`listing.register`, `listing.sync`) that carries the record's
-- overrides, read by `checkout.create` under the listing row lock, and served
-- on the listing projection so a client can tell a row that predates the
-- column from the record. An equal-revision `listing.sync` fills it in for a
-- listing registered before this migration.
--
-- A nullable column with no default rewrites no table and builds no index.

SET LOCAL lock_timeout = '10s';

ALTER TABLE listings ADD COLUMN IF NOT EXISTS variant_prices JSONB;
