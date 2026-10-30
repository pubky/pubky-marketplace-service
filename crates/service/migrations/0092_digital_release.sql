-- Digital release split from payment (DIGITAL_RELEASE): who releases an
-- order's instant digital lines (file, link, text) to the buyer, when that
-- happened, and the seller's `fulfillment.release_digital` command. Additive
-- and directly rerunnable. There is NO DOWN migration; to back it out by hand,
-- see "Backing out 0092" in docs/migrations.md.
--
-- * `orders.digital_release` is frozen when an order with an instant line is
--   paid: `payment` when the payment confirmation released the lines (the
--   earlier behaviour), `seller` when they wait for the seller's release. NULL
--   for every other order, and for every order paid before 0092 or by a binary
--   from before it, which released at payment: NULL reads as `payment`, so no
--   existing purchase is withheld and no backfill is needed.
-- * `orders.digital_released_at` is when the lines were released: the
--   payment under `payment`, the seller's release under `seller`. NULL while a
--   `seller` order waits for its seller.
-- * `release_digital` joins the closed refusal-audit command-kind catalog
--   (append-only since 0032; the source enum and this table keep parity).
--
-- Both columns are nullable with no default (no rewrite) and the CHECK treats
-- NULL as valid.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_release TEXT
    CONSTRAINT orders_digital_release_values
    CHECK (digital_release IS NULL OR digital_release IN ('seller', 'payment'));

ALTER TABLE orders ADD COLUMN IF NOT EXISTS digital_released_at TIMESTAMPTZ;

INSERT INTO command_refusal_command_kinds (id, name) VALUES
  (43, 'release_digital')
ON CONFLICT (id) DO NOTHING;

-- ON CONFLICT DO NOTHING keeps whatever row already holds id 43. Fail loudly
-- if that row is not this command kind, so source and catalog cannot drift
-- apart silently.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM command_refusal_command_kinds
        WHERE id = 43 AND name = 'release_digital'
    ) THEN
        RAISE EXCEPTION
            'command_refusal_command_kinds id 43 is not release_digital';
    END IF;
END
$$;
