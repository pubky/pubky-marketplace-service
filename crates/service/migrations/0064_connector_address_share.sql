-- Buyer opt-in address sharing with a connected store (LG5).
--
-- A buyer who ticks "share my shipping address with <shop>" at checkout seals
-- the address a second time, to the connection's recipient key
-- (connector_recipient_keys), with the same construction as the delivery
-- envelope (RFC 9180 HPKE, base mode, 2,096 bytes). The order keeps that
-- envelope until the connector worker has created the store order, then the
-- worker deletes it. The consent travels with the order:
--
--   connector_share_domain           the store the buyer agreed to share with
--   connector_share_consent_version  the version of the consent copy shown
--   connector_share_consented_at     when the checkout recorded the consent
--   connector_address_envelope       the sealed address, deleted after the push
--   connector_key_id                 the recipient key it was sealed to
--
-- The consent columns stay after the envelope is gone: they are the record
-- that the buyer opted in. The three consent columns are set together, and
-- the envelope and its key id are set together and only with consent.
--
-- The constraint is NOT VALID: enforced for every new and updated row without
-- scanning the table, and existing rows (all NULL) satisfy it. Nullable
-- columns without defaults: no rewrite, and a binary built before this file
-- ignores them. There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS connector_share_domain TEXT,
    ADD COLUMN IF NOT EXISTS connector_share_consent_version TEXT,
    ADD COLUMN IF NOT EXISTS connector_share_consented_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS connector_address_envelope BYTEA,
    ADD COLUMN IF NOT EXISTS connector_key_id TEXT;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_connector_share_shape;
ALTER TABLE orders
    ADD CONSTRAINT orders_connector_share_shape CHECK (
        (connector_share_domain IS NULL) = (connector_share_consent_version IS NULL)
        AND (connector_share_domain IS NULL) = (connector_share_consented_at IS NULL)
        AND (connector_address_envelope IS NULL) = (connector_key_id IS NULL)
        AND (connector_address_envelope IS NULL OR connector_share_domain IS NOT NULL)
        AND (connector_address_envelope IS NULL OR octet_length(connector_address_envelope) = 2096)
    ) NOT VALID;
