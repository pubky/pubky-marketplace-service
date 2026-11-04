-- Moderation decisions: the statement of reasons a moderator records when a
-- listing, a shop or a review is restricted, delivered to the affected user
-- as a `moderation_decision` notification (docs/moderation-decisions.md).
-- One row per decision, written once by `moderation-decision-admin` in the
-- same transaction as its event and notification intent, and never changed:
-- lifting a restriction is a moderator action on the index, not an edit here.
-- A new table only; nothing existing is touched. Directly rerunnable. There
-- is NO DOWN migration; to back it out by hand, see "Backing out 0094" in
-- docs/migrations.md.
--
-- * `id` is derived from the statement itself (moderation_decisions.rs), so
--   recording the same statement twice is one decision.
-- * `recipient_pubky` is the owner of `target_uri`: the seller of a listing
--   or shop, the author of a review.
-- * `restriction` names what the decision does: `listing_removed` (out of the
--   catalogue, and it cannot be bought, bid on or offered on),
--   `shop_suspended` (the seller's shop and every listing), and
--   `review_text_hidden` / `review_excluded` (text hidden; excluded also
--   leaves every rating). `target_uri` must name the record it applies to.
-- * `ends_at` NULL means until the restriction is lifted.
-- * `automated_means` describes the automated detection or decision, and is
--   required when either is used.
-- * `ground` is `illegal_content` (with the law in `ground_reference`) or
--   `terms` (with the Terms clause).
-- * `decided_by` is the operator who recorded it; it is not shown to the
--   recipient.
-- The checks hold the same rules as the command's validation, so a row
-- written by hand cannot hold a statement the command would refuse.

CREATE TABLE IF NOT EXISTS moderation_decisions (
    id UUID PRIMARY KEY,
    recipient_pubky TEXT NOT NULL CHECK (recipient_pubky ~ '^[ybndrfg8ejkmcpqxot1uwisza345h769]{52}$'),
    restriction TEXT NOT NULL CHECK (
        restriction IN ('listing_removed', 'shop_suspended', 'review_text_hidden', 'review_excluded')
    ),
    target_uri TEXT NOT NULL CHECK (char_length(target_uri) <= 512),
    territorial_scope TEXT NOT NULL CHECK (char_length(btrim(territorial_scope)) BETWEEN 1 AND 200),
    ends_at TIMESTAMPTZ,
    decided_at TIMESTAMPTZ NOT NULL,
    basis TEXT NOT NULL CHECK (basis IN ('notice', 'own_initiative')),
    facts TEXT NOT NULL CHECK (char_length(btrim(facts)) BETWEEN 1 AND 2000),
    automated_detection BOOLEAN NOT NULL,
    automated_decision BOOLEAN NOT NULL,
    automated_means TEXT CHECK (
        automated_means IS NULL OR char_length(btrim(automated_means)) BETWEEN 1 AND 1000
    ),
    ground TEXT NOT NULL CHECK (ground IN ('illegal_content', 'terms')),
    ground_reference TEXT NOT NULL CHECK (char_length(btrim(ground_reference)) BETWEEN 1 AND 500),
    explanation TEXT NOT NULL CHECK (char_length(btrim(explanation)) BETWEEN 1 AND 2000),
    decided_by TEXT NOT NULL CHECK (char_length(btrim(decided_by)) BETWEEN 1 AND 200),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT moderation_decisions_target_matches_restriction CHECK (
        target_uri ~ (
            '^pubky://' || recipient_pubky || CASE restriction
                WHEN 'listing_removed' THEN '/pub/pubky\.app/marketplace/v1/listings/[A-Za-z0-9_-]{1,128}$'
                WHEN 'shop_suspended' THEN '/pub/pubky\.app/profile\.json$'
                ELSE '/pub/pubky\.app/marketplace/v1/reviews/[A-Za-z0-9_-]{1,128}$'
            END
        )
    ),
    CONSTRAINT moderation_decisions_automated_means_described CHECK (
        (automated_detection OR automated_decision) = (automated_means IS NOT NULL)
    ),
    CONSTRAINT moderation_decisions_ends_after_decision CHECK (
        ends_at IS NULL OR ends_at > decided_at
    )
);

CREATE INDEX IF NOT EXISTS moderation_decisions_recipient_idx
    ON moderation_decisions (recipient_pubky, decided_at DESC);

DROP TRIGGER IF EXISTS moderation_decisions_append_only ON moderation_decisions;
CREATE TRIGGER moderation_decisions_append_only
    BEFORE UPDATE OR DELETE ON moderation_decisions
    FOR EACH ROW EXECUTE FUNCTION forbid_event_mutation();
