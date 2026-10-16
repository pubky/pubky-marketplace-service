-- Three more connector alert kinds: `listings_unmapped` (switching a
-- connection to split variant mode unmapped listings that combined several
-- store variants), `variant_mapping_error` (a store stock change named a
-- variant the service cannot address: the mapping is stale) and
-- `push_total_mismatch` (the store order's total is not what the buyer paid).
-- Widens the alert kind check from 0066. Additive and rerunnable; the table is
-- small, so the constraint is replaced in one pass under a short lock timeout.
-- There is NO DOWN migration.

SET LOCAL lock_timeout = '10s';

ALTER TABLE connector_alerts DROP CONSTRAINT IF EXISTS connector_alerts_kind;
ALTER TABLE connector_alerts
    ADD CONSTRAINT connector_alerts_kind CHECK (
        kind IN (
            'push_quarantined',
            'action_quarantined',
            'merchant_cancelled',
            'merchant_refunded',
            'store_uninstalled',
            'credentials_rejected',
            'inbound_failed',
            'refund_reversed_after_mirror',
            'listings_unmapped',
            'variant_mapping_error',
            'push_total_mismatch'
        )
    );
