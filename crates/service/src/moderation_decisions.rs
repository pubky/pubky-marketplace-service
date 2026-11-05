//! Moderation decisions: the statement of reasons a moderator records when a
//! listing, a shop or a review is restricted, and its delivery to the
//! affected user (runbook: `docs/moderation-decisions.md`).
//!
//! The restriction itself is the moderator's tag in the marketplace index
//! (`listing_moderation.rs`); this module only records why. A decision is
//! written once, by the `moderation-decision-admin` operator command, in one
//! transaction with its event and a `moderation_decision` notification intent
//! to the owner of the target. `GET /v1/notifications` carries the decision on
//! that notification, to its recipient only.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use marketplace_domain::commands::is_valid_entity_id;
use marketplace_domain::pubky::is_valid_pubky;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::insert_notification_intent;
use crate::workers::SYSTEM_ACTOR;

pub const NOTIFICATION_TYPE: &str = "moderation_decision";
pub const AGGREGATE_PREFIX: &str = "moderation_decision:";
const EVENT_KIND: &str = "moderation.decision_recorded";

/// How far ahead of the clock `decided_at` may be, for operator clock skew.
const DECIDED_AT_SKEW: chrono::Duration = chrono::Duration::minutes(5);

const MAX_TARGET_URI: usize = 512;
const MAX_SCOPE: usize = 200;
const MAX_FACTS: usize = 2000;
const MAX_AUTOMATED_MEANS: usize = 1000;
const MAX_GROUND_REFERENCE: usize = 500;
const MAX_EXPLANATION: usize = 2000;
const MAX_DECIDED_BY: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restriction {
    ListingRemoved,
    ShopSuspended,
    ReviewTextHidden,
    ReviewExcluded,
}

impl Restriction {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "listing_removed" => Some(Self::ListingRemoved),
            "shop_suspended" => Some(Self::ShopSuspended),
            "review_text_hidden" => Some(Self::ReviewTextHidden),
            "review_excluded" => Some(Self::ReviewExcluded),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ListingRemoved => "listing_removed",
            Self::ShopSuspended => "shop_suspended",
            Self::ReviewTextHidden => "review_text_hidden",
            Self::ReviewExcluded => "review_excluded",
        }
    }

    /// Whether `path` (after `pubky://<owner>`) names the record this
    /// restriction applies to.
    fn targets(self, path: &str) -> bool {
        let record_id = |prefix: &str| path.strip_prefix(prefix).is_some_and(is_valid_entity_id);
        match self {
            Self::ListingRemoved => record_id("/pub/pubky.app/marketplace/v1/listings/"),
            Self::ShopSuspended => path == "/pub/pubky.app/profile.json",
            Self::ReviewTextHidden | Self::ReviewExcluded => {
                record_id("/pub/pubky.app/marketplace/v1/reviews/")
            }
        }
    }
}

/// The operator's input, as JSON. Unknown fields are refused so a misspelt
/// field is never silently dropped from a statement of reasons.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionInput {
    pub restriction: String,
    pub target_uri: String,
    pub territorial_scope: String,
    #[serde(default)]
    pub ends_at: Option<DateTime<Utc>>,
    pub decided_at: DateTime<Utc>,
    pub basis: String,
    pub facts: String,
    pub automated_detection: bool,
    pub automated_decision: bool,
    #[serde(default)]
    pub automated_means: Option<String>,
    pub ground: String,
    pub ground_reference: String,
    pub explanation: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidDecision {
    #[error("restriction must be listing_removed, shop_suspended, review_text_hidden or review_excluded")]
    Restriction,
    #[error(
        "target_uri must be pubky://<owner>/pub/... naming the record the restriction applies to"
    )]
    TargetUri,
    #[error("{0} must be between 1 and {1} characters")]
    Length(&'static str, usize),
    #[error("basis must be notice or own_initiative")]
    Basis,
    #[error("ground must be illegal_content or terms")]
    Ground,
    #[error("automated_means is required when automated_detection or automated_decision is true, and only then")]
    AutomatedMeans,
    #[error("decided_at must not be in the future")]
    DecidedInFuture,
    #[error("ends_at must be after decided_at")]
    EndsBeforeDecision,
}

/// A decision that passed validation, with the recipient taken from the
/// target's owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub recipient_pubky: String,
    pub restriction: Restriction,
    pub target_uri: String,
    pub territorial_scope: String,
    pub ends_at: Option<DateTime<Utc>>,
    pub decided_at: DateTime<Utc>,
    pub basis: &'static str,
    pub facts: String,
    pub automated_detection: bool,
    pub automated_decision: bool,
    pub automated_means: Option<String>,
    pub ground: &'static str,
    pub ground_reference: String,
    pub explanation: String,
}

fn bounded(field: &'static str, value: &str, max: usize) -> Result<String, InvalidDecision> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.chars().count() > max {
        return Err(InvalidDecision::Length(field, max));
    }
    Ok(trimmed.to_string())
}

/// A timestamp at the input's full precision, so two statements that differ
/// only below a millisecond are still two decisions.
fn full_precision(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// The owner of a `pubky://<owner>/pub/...` URI and its path, or `None`.
fn split_target(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("pubky://")?;
    let slash = rest.find('/')?;
    let (owner, path) = rest.split_at(slash);
    (is_valid_pubky(owner) && path.starts_with("/pub/")).then_some((owner, path))
}

impl Decision {
    pub fn validate(input: DecisionInput, now: DateTime<Utc>) -> Result<Self, InvalidDecision> {
        let restriction =
            Restriction::parse(input.restriction.trim()).ok_or(InvalidDecision::Restriction)?;
        let target_uri = input.target_uri.trim();
        if target_uri.chars().count() > MAX_TARGET_URI {
            return Err(InvalidDecision::TargetUri);
        }
        let (owner, path) = split_target(target_uri).ok_or(InvalidDecision::TargetUri)?;
        if !restriction.targets(path) {
            return Err(InvalidDecision::TargetUri);
        }
        let basis = match input.basis.trim() {
            "notice" => "notice",
            "own_initiative" => "own_initiative",
            _ => return Err(InvalidDecision::Basis),
        };
        let ground = match input.ground.trim() {
            "illegal_content" => "illegal_content",
            "terms" => "terms",
            _ => return Err(InvalidDecision::Ground),
        };
        let automated = input.automated_detection || input.automated_decision;
        let automated_means = match (automated, input.automated_means.as_deref()) {
            (true, Some(means)) => Some(bounded("automated_means", means, MAX_AUTOMATED_MEANS)?),
            (false, None) => None,
            _ => return Err(InvalidDecision::AutomatedMeans),
        };
        if input.decided_at > now + DECIDED_AT_SKEW {
            return Err(InvalidDecision::DecidedInFuture);
        }
        if input
            .ends_at
            .is_some_and(|ends_at| ends_at <= input.decided_at)
        {
            return Err(InvalidDecision::EndsBeforeDecision);
        }
        Ok(Self {
            recipient_pubky: owner.to_string(),
            restriction,
            target_uri: target_uri.to_string(),
            territorial_scope: bounded("territorial_scope", &input.territorial_scope, MAX_SCOPE)?,
            ends_at: input.ends_at,
            decided_at: input.decided_at,
            basis,
            facts: bounded("facts", &input.facts, MAX_FACTS)?,
            automated_detection: input.automated_detection,
            automated_decision: input.automated_decision,
            automated_means,
            ground,
            ground_reference: bounded(
                "ground_reference",
                &input.ground_reference,
                MAX_GROUND_REFERENCE,
            )?,
            explanation: bounded("explanation", &input.explanation, MAX_EXPLANATION)?,
        })
    }
}

impl Decision {
    /// The decision id: a UUID (version 8) from the SHA-256 of the statement.
    /// Recording the same statement again names the same decision, so a
    /// retried command never sends a second notice.
    pub fn id(&self) -> Uuid {
        let statement = json!([
            self.recipient_pubky,
            self.restriction.as_str(),
            self.target_uri,
            self.territorial_scope,
            self.ends_at.map(full_precision),
            full_precision(self.decided_at),
            self.basis,
            self.facts,
            self.automated_detection,
            self.automated_decision,
            self.automated_means,
            self.ground,
            self.ground_reference,
            self.explanation,
        ]);
        let digest = Sha256::digest(statement.to_string().as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error(transparent)]
    Invalid(#[from] InvalidDecision),
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    /// The decision id, also the reference the recipient quotes in an appeal.
    pub id: Uuid,
    /// False when this exact statement was already recorded: nothing new
    /// was written and no second notice was queued.
    pub created: bool,
}

/// Records `decision` and queues its notification to the recipient, in one
/// transaction.
pub async fn record(
    pool: &PgPool,
    decision: &Decision,
    decided_by: &str,
    now: DateTime<Utc>,
) -> Result<Recorded, RecordError> {
    let decided_by = bounded("decided_by", decided_by, MAX_DECIDED_BY)?;
    let id = decision.id();
    let aggregate_id = format!("{AGGREGATE_PREFIX}{id}");
    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO moderation_decisions (id, recipient_pubky, restriction, target_uri, \
         territorial_scope, ends_at, decided_at, basis, facts, automated_detection, \
         automated_decision, automated_means, ground, ground_reference, explanation, \
         decided_by, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .bind(&decision.recipient_pubky)
    .bind(decision.restriction.as_str())
    .bind(&decision.target_uri)
    .bind(&decision.territorial_scope)
    .bind(decision.ends_at)
    .bind(decision.decided_at)
    .bind(decision.basis)
    .bind(&decision.facts)
    .bind(decision.automated_detection)
    .bind(decision.automated_decision)
    .bind(&decision.automated_means)
    .bind(decision.ground)
    .bind(&decision.ground_reference)
    .bind(&decision.explanation)
    .bind(&decided_by)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(Recorded { id, created: false });
    }
    let event_id =
        insert_event(&mut tx, id, &aggregate_id, 1, SYSTEM_ACTOR, EVENT_KIND, now).await?;
    insert_notification_intent(
        &mut tx,
        event_id,
        NOTIFICATION_TYPE,
        &decision.recipient_pubky,
        SYSTEM_ACTOR,
        &aggregate_id,
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    Ok(Recorded { id, created: true })
}

/// A recorded decision as its recipient sees it. `decided_by` is not read.
#[derive(Debug, Clone, FromRow)]
pub struct DecisionRow {
    pub id: Uuid,
    pub recipient_pubky: String,
    pub restriction: String,
    pub target_uri: String,
    pub territorial_scope: String,
    pub ends_at: Option<DateTime<Utc>>,
    pub decided_at: DateTime<Utc>,
    pub basis: String,
    pub facts: String,
    pub automated_detection: bool,
    pub automated_decision: bool,
    pub automated_means: Option<String>,
    pub ground: String,
    pub ground_reference: String,
    pub explanation: String,
}

const DECISION_COLUMNS: &str = "id, recipient_pubky, restriction, target_uri, territorial_scope, \
     ends_at, decided_at, basis, facts, automated_detection, automated_decision, automated_means, \
     ground, ground_reference, explanation";

impl DecisionRow {
    pub fn view(&self) -> Value {
        json!({
            "id": self.id,
            "restriction": self.restriction,
            "target_uri": self.target_uri,
            "territorial_scope": self.territorial_scope,
            "ends_at": self.ends_at.map(format_timestamp),
            "decided_at": format_timestamp(self.decided_at),
            "basis": self.basis,
            "facts": self.facts,
            "automated_detection": self.automated_detection,
            "automated_decision": self.automated_decision,
            "automated_means": self.automated_means,
            "ground": self.ground,
            "ground_reference": self.ground_reference,
            "explanation": self.explanation,
        })
    }
}

/// The decision a notification aggregate names, if it names one.
pub fn decision_id(aggregate_id: &str) -> Option<Uuid> {
    aggregate_id
        .strip_prefix(AGGREGATE_PREFIX)
        .and_then(|id| Uuid::parse_str(id).ok())
}

/// The decisions among `ids` whose recipient is `recipient`, by id.
pub async fn views_for_recipient(
    pool: &PgPool,
    recipient: &str,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Value>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<DecisionRow> = sqlx::query_as(&format!(
        "SELECT {DECISION_COLUMNS} FROM moderation_decisions \
         WHERE recipient_pubky = $1 AND id = ANY($2)"
    ))
    .bind(recipient)
    .bind(ids)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|row| (row.id, row.view())).collect())
}

/// One decision by id, for the operator command.
pub async fn find(pool: &PgPool, id: Uuid) -> Result<Option<DecisionRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "SELECT {DECISION_COLUMNS} FROM moderation_decisions WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: &str = "pxnu33x7jtpx9ar1ytsi4yxbp6a5o36gwhffs8zoxmbuptici1jy";
    const OTHER: &str = "8um71us3fyw6h8wbcxb5ar3rwusy1a6u49956ikzojg3gcwd1dty";

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-04T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn input() -> DecisionInput {
        DecisionInput {
            restriction: "listing_removed".into(),
            target_uri: format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/listings/abc_123"),
            territorial_scope: "Every region where Pubky Market is offered".into(),
            ends_at: None,
            decided_at: now() - chrono::Duration::hours(1),
            basis: "notice".into(),
            facts: "A report said the listing offers a prescription-only medicine; the listing \
                    text and photos confirm it."
                .into(),
            automated_detection: false,
            automated_decision: false,
            automated_means: None,
            ground: "terms".into(),
            ground_reference: "Pubky Terms of Service, Prohibited goods".into(),
            explanation: "The Terms prohibit listing anything that is illegal to offer.".into(),
        }
    }

    #[test]
    fn a_complete_decision_names_the_owner_as_recipient() {
        let decision = Decision::validate(input(), now()).expect("valid");
        assert_eq!(decision.recipient_pubky, OWNER);
        assert_eq!(decision.restriction, Restriction::ListingRemoved);
        assert_eq!(decision.basis, "notice");
        assert_eq!(decision.ground, "terms");
        assert_eq!(decision.automated_means, None);
    }

    #[test]
    fn the_target_must_be_the_record_the_restriction_applies_to() {
        let cases = [
            (
                "listing_removed",
                format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/reviews/r1"),
            ),
            (
                "listing_removed",
                format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/listings/"),
            ),
            (
                "listing_removed",
                format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/listings/a/b"),
            ),
            (
                "listing_removed",
                "pubky://not-a-pubky/pub/pubky.app/marketplace/v1/listings/a".into(),
            ),
            (
                "listing_removed",
                format!("https://{OWNER}/pub/pubky.app/marketplace/v1/listings/a"),
            ),
            (
                "shop_suspended",
                format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/listings/a"),
            ),
            (
                "review_excluded",
                format!("pubky://{OWNER}/pub/pubky.app/profile.json"),
            ),
        ];
        for (restriction, target_uri) in cases {
            let mut input = input();
            input.restriction = restriction.into();
            input.target_uri = target_uri.clone();
            assert_eq!(
                Decision::validate(input, now()),
                Err(InvalidDecision::TargetUri),
                "{restriction} {target_uri}"
            );
        }
        for (restriction, target_uri) in [
            (
                "shop_suspended",
                format!("pubky://{OTHER}/pub/pubky.app/profile.json"),
            ),
            (
                "review_text_hidden",
                format!("pubky://{OTHER}/pub/pubky.app/marketplace/v1/reviews/r-1"),
            ),
        ] {
            let mut input = input();
            input.restriction = restriction.into();
            input.target_uri = target_uri;
            assert_eq!(
                Decision::validate(input, now())
                    .expect(restriction)
                    .recipient_pubky,
                OTHER
            );
        }
    }

    #[test]
    fn automated_means_is_described_exactly_when_automation_was_used() {
        let mut detected = input();
        detected.automated_detection = true;
        assert_eq!(
            Decision::validate(detected.clone(), now()),
            Err(InvalidDecision::AutomatedMeans)
        );
        detected.automated_means = Some("An image classifier flagged the photos.".into());
        assert!(Decision::validate(detected, now()).is_ok());

        let mut manual = input();
        manual.automated_means = Some("unused".into());
        assert_eq!(
            Decision::validate(manual, now()),
            Err(InvalidDecision::AutomatedMeans)
        );
    }

    #[test]
    fn closed_vocabularies_and_lengths_are_enforced() {
        let mut bad = input();
        bad.restriction = "account_banned".into();
        assert_eq!(
            Decision::validate(bad, now()),
            Err(InvalidDecision::Restriction)
        );
        let mut bad = input();
        bad.basis = "report".into();
        assert_eq!(Decision::validate(bad, now()), Err(InvalidDecision::Basis));
        let mut bad = input();
        bad.ground = "policy".into();
        assert_eq!(Decision::validate(bad, now()), Err(InvalidDecision::Ground));
        let mut bad = input();
        bad.facts = "   ".into();
        assert_eq!(
            Decision::validate(bad, now()),
            Err(InvalidDecision::Length("facts", MAX_FACTS))
        );
        let mut bad = input();
        bad.explanation = "x".repeat(MAX_EXPLANATION + 1);
        assert_eq!(
            Decision::validate(bad, now()),
            Err(InvalidDecision::Length("explanation", MAX_EXPLANATION))
        );
    }

    #[test]
    fn decision_and_end_times_are_ordered() {
        let mut future = input();
        future.decided_at = now() + chrono::Duration::hours(1);
        assert_eq!(
            Decision::validate(future, now()),
            Err(InvalidDecision::DecidedInFuture)
        );
        let mut ends = input();
        ends.ends_at = Some(ends.decided_at);
        assert_eq!(
            Decision::validate(ends, now()),
            Err(InvalidDecision::EndsBeforeDecision)
        );
        let mut ends = input();
        ends.ends_at = Some(now() + chrono::Duration::days(30));
        assert!(Decision::validate(ends, now()).is_ok());
    }

    #[test]
    fn unknown_input_fields_are_refused() {
        let mut value = serde_json::to_value(json!({
            "restriction": "listing_removed",
            "target_uri": format!("pubky://{OWNER}/pub/pubky.app/marketplace/v1/listings/a"),
            "territorial_scope": "All regions",
            "decided_at": "2026-11-04T11:00:00Z",
            "basis": "own_initiative",
            "facts": "f",
            "automated_detection": false,
            "automated_decision": false,
            "ground": "terms",
            "ground_reference": "g",
            "explanation": "e"
        }))
        .unwrap();
        assert!(serde_json::from_value::<DecisionInput>(value.clone()).is_ok());
        value["notifier"] = json!("someone");
        assert!(serde_json::from_value::<DecisionInput>(value).is_err());
    }

    #[test]
    fn the_id_is_the_statement() {
        let decision = Decision::validate(input(), now()).expect("valid");
        let id = decision.id();
        assert_eq!(id, Decision::validate(input(), now()).unwrap().id());
        assert_eq!(id.get_version_num(), 8);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
        let mut other = input();
        other.facts.push('.');
        assert_ne!(id, Decision::validate(other, now()).unwrap().id());
        let mut padded = input();
        padded.facts = format!("  {}  ", padded.facts);
        assert_eq!(
            id,
            Decision::validate(padded, now()).unwrap().id(),
            "the stored, trimmed statement names the decision"
        );
        let mut later = input();
        later.decided_at += chrono::Duration::microseconds(1);
        assert_ne!(
            id,
            Decision::validate(later, now()).unwrap().id(),
            "a sub-millisecond difference is a different decision"
        );
    }

    #[test]
    fn only_decision_aggregates_name_a_decision() {
        let id = Uuid::new_v4();
        assert_eq!(decision_id(&format!("moderation_decision:{id}")), Some(id));
        assert_eq!(decision_id(&format!("order:{id}")), None);
        assert_eq!(decision_id("moderation_decision:nope"), None);
    }
}
