-- A refund that Pubky mirrored to a connected store and later reversed or
-- reduced cannot be retracted from the store; the seller is told with a
-- `refund_reversed_after_mirror` alert. Widens the alert kind check from 0063
-- by one value. Additive and rerunnable; the table is small and new, so the
-- constraint is added and validated in one pass under a short lock timeout.
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
            'refund_reversed_after_mirror'
        )
    );
