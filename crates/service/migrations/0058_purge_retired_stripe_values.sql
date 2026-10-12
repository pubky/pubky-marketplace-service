-- The Stripe rail is gone: no code reads or writes a seller's Stripe payment
-- link or sealed restricted key. A restricted key is a bearer credential,
-- and a sealed copy of it should not outlive the feature it served, so this
-- clears every stored value. The two columns stay (0009 created them, and
-- dropping a column is not additive); both are unused and always NULL.
--
-- Orders and payments bound to Stripe before the removal are history and
-- are not touched. Rerunnable: a second run matches no row.
-- There is NO DOWN migration.

UPDATE seller_payment_configs
SET stripe_payment_link = NULL,
    stripe_restricted_key_ciphertext = NULL
WHERE stripe_payment_link IS NOT NULL
   OR stripe_restricted_key_ciphertext IS NOT NULL
