-- Seller wallet Receipt confirmation: under PAYMENT_CONFIRMATION=seller, the
-- Receipt the seller's wallet issued for an upstream Paykit request (read
-- from paykit-server's seller Receipt route) confirms the order the way the
-- seller's own tap does. Additive and directly rerunnable. There is NO DOWN
-- migration; to back it out by hand, see "Backing out 0098" in
-- docs/migrations.md.
--
-- * `paykit_seller_confirmations.confirmation_basis` also allows
--   `seller_wallet_receipt`: the seller's wallet, not the seller's tap,
--   attested the payment.
-- * `paykit_seller_confirmations.wallet_receipt` is that Receipt as
--   paykit-server reported it (ids, issuing App, Payment Reference, endpoint,
--   amount, metadata, issued and stored times). NULL for a tap.
--
-- A binary from before 0098 writes only `seller_attestation` rows and never
-- reads the column.

SET LOCAL lock_timeout = '10s';

ALTER TABLE paykit_seller_confirmations
    DROP CONSTRAINT IF EXISTS paykit_seller_confirmations_confirmation_basis_check;
ALTER TABLE paykit_seller_confirmations
    ADD CONSTRAINT paykit_seller_confirmations_confirmation_basis_check
    CHECK (confirmation_basis IN ('seller_attestation', 'seller_wallet_receipt'));

ALTER TABLE paykit_seller_confirmations ADD COLUMN IF NOT EXISTS wallet_receipt JSONB;

ALTER TABLE paykit_seller_confirmations
    DROP CONSTRAINT IF EXISTS paykit_seller_confirmations_wallet_receipt_basis;
ALTER TABLE paykit_seller_confirmations
    ADD CONSTRAINT paykit_seller_confirmations_wallet_receipt_basis
    CHECK ((confirmation_basis = 'seller_wallet_receipt') = (wallet_receipt IS NOT NULL));
