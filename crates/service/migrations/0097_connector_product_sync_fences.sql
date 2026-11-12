-- Read fences of a linked store product's snapshot, per field group and
-- across an unlink.
--
--   content_rebased_at, price_rebased_at, variants_rebased_at
--       when the group's direction last changed for the product. Its NULL
--       base is taken only by a read that started at or after that instant.
--       Each group has its own, so a direction change in one group never
--       voids a read for another; status has no direction and no fence.
--   unlinked_at
--       when the product was last found linked to no listing. The snapshot
--       row exists from the link on and outlives it, emptied, as this fence:
--       a read that started before it belongs to a previous link and is
--       dropped whole, also after the product is linked again.
--
-- They replace `rebased_at`, which one group's change set for every group.
-- The new binary neither reads nor writes it; it stays for an image from
-- before this migration. Each group takes it as its own fence, which is only
-- consulted while the group's base is NULL. Directly rerunnable.

ALTER TABLE connector_product_snapshots
    ADD COLUMN IF NOT EXISTS content_rebased_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS price_rebased_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS variants_rebased_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS unlinked_at TIMESTAMPTZ;

UPDATE connector_product_snapshots SET
    content_rebased_at = COALESCE(content_rebased_at, rebased_at),
    price_rebased_at = COALESCE(price_rebased_at, rebased_at),
    variants_rebased_at = COALESCE(variants_rebased_at, rebased_at)
WHERE rebased_at IS NOT NULL;

-- Every linked product has a snapshot row from its link on, so its unlink
-- can be fenced even while its first read is in flight. An empty row (no
-- base, never read) is what a first read would create.
INSERT INTO connector_product_snapshots
    (connection_id, provider_product_id, checked_at, created_at)
SELECT DISTINCT connection_id, provider_product_id, 'epoch'::timestamptz, now()
FROM connector_variant_map
ON CONFLICT (connection_id, provider_product_id) DO NOTHING;
