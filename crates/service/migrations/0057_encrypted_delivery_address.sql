-- Delivery addresses sealed to the seller, and the end of plaintext ones.
--
-- * `orders.delivery_address_envelope` holds the buyer's delivery address
--   sealed in the buyer's client to the seller's published delivery key (RFC
--   9180 HPKE, exactly 2,096 bytes). The service cannot open it: it holds no
--   key, only checks the length, and returns it to the seller untouched.
--   `orders.delivery_key_id` names the key it was sealed to, so the seller's
--   client finds the matching private key. They are set together.
-- * An order never holds a plaintext address and an envelope at once: a
--   checkout carries one or the other.
-- * A one-time sweep nulls the plaintext `delivery_address` on every order
--   that is already in a final state (completed, cancelled, refunded_external,
--   closed). The purge worker keeps doing this for orders that reach a final
--   state later.
--
-- Constraints are added NOT VALID and then validated, so a rerun never holds
-- a long lock and a legacy row that violates one stops the deploy loudly
-- instead of slipping through.
--
-- Additive and rerunnable. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_address_envelope BYTEA;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_key_id TEXT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_delivery_envelope_shape;
ALTER TABLE orders
    ADD CONSTRAINT orders_delivery_envelope_shape CHECK (
        (delivery_address_envelope IS NULL) = (delivery_key_id IS NULL)
        AND (delivery_address_envelope IS NULL OR octet_length(delivery_address_envelope) = 2096)
        AND (delivery_key_id IS NULL OR delivery_key_id ~ '^[0-9a-f]{32}$')
    ) NOT VALID;
ALTER TABLE orders VALIDATE CONSTRAINT orders_delivery_envelope_shape;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_delivery_address_xor_envelope;
ALTER TABLE orders
    ADD CONSTRAINT orders_delivery_address_xor_envelope CHECK (
        delivery_address IS NULL OR delivery_address_envelope IS NULL
    ) NOT VALID;
ALTER TABLE orders VALIDATE CONSTRAINT orders_delivery_address_xor_envelope;

UPDATE orders
SET delivery_address = NULL
WHERE delivery_address IS NOT NULL
  AND state IN ('completed', 'cancelled', 'refunded_external', 'closed');
