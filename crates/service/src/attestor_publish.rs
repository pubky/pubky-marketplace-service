//! The attestor publisher (Pubky Market reviews, S11): writes the attestor's
//! profile and one annotation per attested order to the attestor's own
//! homeserver, so a refund or a return shows next to the review it concerns and
//! a key rotation can be announced. The index (`marketplace-nexus`) verifies the
//! records and serves them; see [`crate::attestor_records`] for their shape and
//! proof.
//!
//! The publisher is off unless `ATTESTOR_PUBLISH_ENABLED` is true. Off, no task
//! runs and no network call is made.
//!
//! # What is written
//!
//! - **The profile** (`/pub/pubky.app/marketplace/v1/attestor/profile`): the
//!   operator, the key list and successor from configuration, and the issuance
//!   policy from what this service does (`REVIEW_BLIND_WINDOW_DAYS`, the edit
//!   window, and whether purchase attestations carry a price decade). It is
//!   written again when any of that changes, and not otherwise.
//! - **One annotation per attested order**
//!   (`.../attestor/annotations/{order_ref}`): the order's `refunded` and
//!   `returned` outcomes, only for an `order_ref` this attestor issued a
//!   purchase attestation for. An annotation with no outcome is never written
//!   first; one whose outcomes shrank to none (a refund superseded by a
//!   cancelled PayPal reversal) is written again, empty, so the public record
//!   stops claiming the refund.
//!
//! # Revisions and idempotency
//!
//! `attestor_publications` records what was written: its revision and a hash of
//! its content without the revision and proof. A record whose content hash is
//! the recorded one is not written again. A changed one is written at the
//! recorded revision plus one; the index replaces an annotation by a higher
//! revision. The hash recorded is that of the content the pass read before it
//! wrote, so an outcome added while a write is in flight is still different
//! from what was recorded, and the next pass writes it at the next revision.
//!
//! # One publisher, retries
//!
//! The pass holds the `attestor_publish` worker lease, renewed before every
//! write, and stops when it cannot renew. Before each write the pass records the
//! attempt in `attestor_publish_attempts` with the time the next attempt is
//! due, so a failure and a crash mid-write look alike: the path waits out its
//! backoff (doubling from 15 seconds to an hour, across restarts and instances),
//! and a revision is never reused for different content. A failed write ends the
//! pass; the other records wait for the next one. A duplicate write after a
//! crash between the write and its record is byte-identical to the first (the
//! signature is deterministic), and the index treats an equal revision as a
//! replacement.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use marketplace_domain::pubky::is_valid_pubky;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::attestor::{Attestor, ISSUES_PRICE_DECADE};
use crate::attestor_records::{
    annotation_path, Annotation, IssuancePolicy, Operator, Outcome, OutcomeKind, Profile,
    ProfileKey, AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT, MAX_ANNOTATION_OUTCOMES,
    PRICE_DECADE_POLICY_ALWAYS, PRICE_DECADE_POLICY_NEVER, PROFILE_PATH,
};
use crate::handlers::reviews::REVIEW_EDIT_WINDOW_SECONDS;
use crate::workers::{release_fenced_lease, try_acquire_fenced_lease};
use crate::AppState;

pub const TASK_ATTESTOR_PUBLISH: &str = "attestor_publish";

pub const ENV_ATTESTOR_PUBLISH_ENABLED: &str = "ATTESTOR_PUBLISH_ENABLED";
pub const ENV_ATTESTOR_HOMESERVER: &str = "ATTESTOR_HOMESERVER";
pub const ENV_ATTESTOR_OPERATOR_NAME: &str = "ATTESTOR_OPERATOR_NAME";
pub const ENV_ATTESTOR_OPERATOR_URL: &str = "ATTESTOR_OPERATOR_URL";
pub const ENV_ATTESTOR_POLICY_URL: &str = "ATTESTOR_POLICY_URL";
pub const ENV_ATTESTOR_KEYS: &str = "ATTESTOR_KEYS";
pub const ENV_ATTESTOR_SUCCESSOR: &str = "ATTESTOR_SUCCESSOR";

pub const DEFAULT_OPERATOR_NAME: &str = "Pubky Market";

/// First wait after a failed write; it doubles per failure up to
/// [`RETRY_MAX_SECONDS`].
pub const RETRY_BASE_SECONDS: i64 = 15;
pub const RETRY_MAX_SECONDS: i64 = 3_600;

/// Longest a sign-in plus one write may take. Below the default worker lease
/// (30 seconds), which the pass renews before each write.
const HOMESERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_ID: &str = "market.pubky.attestor";
const MAX_ERROR_LENGTH: usize = 500;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct PublishError(pub String);

/// Writes a file on the attestor's homeserver, signed in as the attestor.
#[async_trait]
pub trait AttestorHomeserver: Send + Sync {
    /// Creates or replaces the file at the absolute `path`.
    async fn put(&self, path: &str, body: &[u8]) -> Result<(), PublishError>;
}

/// What the profile says that configuration decides: who operates the key, the
/// keys it has issued with, and the successor.
#[derive(Debug, Clone)]
pub struct ProfileSettings {
    pub operator: Operator,
    pub keys: Vec<ProfileKey>,
    pub successor: Option<String>,
}

/// The validated publisher settings.
#[derive(Debug, Clone)]
pub struct PublishConfig {
    /// The homeserver the attestor's account lives on (its z-base-32 pubky).
    pub homeserver: String,
    pub profile: ProfileSettings,
}

pub struct AttestorPublisher {
    profile: ProfileSettings,
    homeserver: Arc<dyn AttestorHomeserver>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfiguredKey {
    pubky: String,
    valid_from: String,
    #[serde(default)]
    valid_until: Option<String>,
    #[serde(default)]
    compromised_from: Option<String>,
}

impl PublishConfig {
    /// Reads the publisher settings from a variable lookup. `Ok(None)` when
    /// `ATTESTOR_PUBLISH_ENABLED` is not true: nothing else is read, so the
    /// service starts and behaves as it did before the publisher existed.
    /// Enabled, every setting is required and the attestor key must be
    /// configured: a partial configuration refuses to start rather than run
    /// without publishing.
    pub fn from_lookup(
        get: &dyn Fn(&str) -> Option<String>,
        attestor: Option<&Attestor>,
    ) -> anyhow::Result<Option<Self>> {
        let enabled = match get(ENV_ATTESTOR_PUBLISH_ENABLED).as_deref().map(str::trim) {
            None => false,
            Some("true" | "1") => true,
            Some("false" | "0") => false,
            Some(_) => anyhow::bail!("{ENV_ATTESTOR_PUBLISH_ENABLED} must be true, false, 1, or 0"),
        };
        if !enabled {
            return Ok(None);
        }
        let Some(attestor) = attestor else {
            anyhow::bail!(
                "{ENV_ATTESTOR_PUBLISH_ENABLED} needs the attestor: set \
                 ATTESTOR_SECRET_KEY and ATTESTOR_ORDER_SALT"
            );
        };
        let required = |name: &str| -> anyhow::Result<String> {
            get(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{name} is required when {ENV_ATTESTOR_PUBLISH_ENABLED} is true"
                    )
                })
        };

        let homeserver = required(ENV_ATTESTOR_HOMESERVER)?;
        if !is_valid_pubky(&homeserver) {
            anyhow::bail!(
                "{ENV_ATTESTOR_HOMESERVER} must be the homeserver's 52-character z-base-32 pubky"
            );
        }
        let operator = Operator {
            name: get(ENV_ATTESTOR_OPERATOR_NAME)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| DEFAULT_OPERATOR_NAME.to_string()),
            url: required(ENV_ATTESTOR_OPERATOR_URL)?,
            policy_url: required(ENV_ATTESTOR_POLICY_URL)?,
        };
        let configured: Vec<ConfiguredKey> = serde_json::from_str(&required(ENV_ATTESTOR_KEYS)?)
            .map_err(|error| {
                anyhow::anyhow!(
                    "{ENV_ATTESTOR_KEYS} must be a JSON array of \
                     {{\"pubky\", \"validFrom\", \"validUntil\"?, \"compromisedFrom\"?}}: {error}"
                )
            })?;
        let keys: Vec<ProfileKey> = configured
            .into_iter()
            .map(|key| ProfileKey {
                pubky: key.pubky,
                valid_from: key.valid_from,
                valid_until: key.valid_until,
                compromised_from: key.compromised_from,
            })
            .collect();
        if !keys.iter().any(|key| key.pubky == attestor.pubky()) {
            anyhow::bail!(
                "{ENV_ATTESTOR_KEYS} must list the attestor's own key ({})",
                attestor.pubky()
            );
        }
        let successor = get(ENV_ATTESTOR_SUCCESSOR)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        let profile = ProfileSettings {
            operator,
            keys,
            successor,
        };
        build_profile(&profile, 0)
            .sign(&attestor.secret_key_bytes())
            .map_err(|error| {
                anyhow::anyhow!("the attestor profile settings are invalid: {error}")
            })?;
        Ok(Some(Self {
            homeserver,
            profile,
        }))
    }
}

impl AttestorPublisher {
    pub fn new(profile: ProfileSettings, homeserver: Arc<dyn AttestorHomeserver>) -> Self {
        Self {
            profile,
            homeserver,
        }
    }

    /// The publisher for the process environment: `Ok(None)` when publishing
    /// is not enabled. Enabled, it signs in to the real homeserver.
    pub fn from_env(attestor: Option<&Attestor>) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(config) = PublishConfig::from_lookup(&|name| std::env::var(name).ok(), attestor)?
        else {
            return Ok(None);
        };
        let attestor = attestor.expect("enabled publishing has an attestor");
        let homeserver = PubkyHomeserver::new(&attestor.secret_key_bytes(), &config.homeserver)?;
        Ok(Some(Arc::new(Self::new(
            config.profile,
            Arc::new(homeserver),
        ))))
    }
}

/// The attestor profile for the settings and the current blind window.
pub fn build_profile(settings: &ProfileSettings, blind_window_days: i64) -> Profile {
    Profile {
        operator: settings.operator.clone(),
        keys: settings.keys.clone(),
        successor: settings.successor.clone(),
        issuance_policy: IssuancePolicy {
            edit_window_hours: REVIEW_EDIT_WINDOW_SECONDS / 3_600,
            blind_window_days,
            amount_band: AMOUNT_BAND_POLICY_BOTH_SIDES_CONSENT.to_string(),
            price_decade: if ISSUES_PRICE_DECADE {
                PRICE_DECADE_POLICY_ALWAYS
            } else {
                PRICE_DECADE_POLICY_NEVER
            }
            .to_string(),
            min_order_value: None,
        },
        proof: String::new(),
    }
}

/// One annotation row of an attested order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationEvent {
    pub outcome: String,
    pub at: DateTime<Utc>,
}

/// The outcomes the rows say, oldest first. `refunded` and `returned` are
/// outcomes. `refund_reversal_cancelled` is not: it records that PayPal
/// cancelled a reversal and the order went back to the state the refund had
/// replaced, so it takes back the latest `refunded` rather than adding an
/// outcome of its own. Beyond [`MAX_ANNOTATION_OUTCOMES`] the oldest go.
pub fn fold_outcomes(events: &[AnnotationEvent]) -> Vec<Outcome> {
    let mut outcomes: Vec<Outcome> = Vec::new();
    for event in events {
        let on = event.at.format("%Y-%m-%d").to_string();
        match event.outcome.as_str() {
            "refunded" => outcomes.push(Outcome {
                kind: OutcomeKind::Refunded,
                on,
            }),
            "returned" => outcomes.push(Outcome {
                kind: OutcomeKind::Returned,
                on,
            }),
            "refund_reversal_cancelled" => {
                if let Some(index) = outcomes
                    .iter()
                    .rposition(|outcome| outcome.kind == OutcomeKind::Refunded)
                {
                    outcomes.remove(index);
                }
            }
            _ => {}
        }
    }
    if outcomes.len() > MAX_ANNOTATION_OUTCOMES {
        outcomes.drain(..outcomes.len() - MAX_ANNOTATION_OUTCOMES);
    }
    outcomes
}

#[derive(sqlx::FromRow)]
struct AnnotationRow {
    order_ref: String,
    outcome: String,
    annotated_at: DateTime<Utc>,
}

/// The annotation rows of every order this attestor issued a purchase
/// attestation for, grouped by `order_ref` in path order. An order the
/// attestor issued nothing for is never read: nothing about it is published.
async fn attested_annotations(
    pool: &PgPool,
    attestor_pubky: &str,
) -> Result<Vec<(String, Vec<AnnotationEvent>)>, sqlx::Error> {
    let rows: Vec<AnnotationRow> = sqlx::query_as(
        "SELECT a.order_ref, a.outcome, a.annotated_at FROM attestation_annotations a \
         WHERE EXISTS (SELECT 1 FROM review_attestations r \
                       WHERE r.order_ref = a.order_ref AND r.claims ->> 'iss' = $1) \
         ORDER BY a.order_ref, a.annotated_at, \
                  CASE a.outcome WHEN 'returned' THEN 0 WHEN 'refunded' THEN 1 ELSE 2 END, a.id",
    )
    .bind(attestor_pubky)
    .fetch_all(pool)
    .await?;
    let mut grouped: Vec<(String, Vec<AnnotationEvent>)> = Vec::new();
    for row in rows {
        let event = AnnotationEvent {
            outcome: row.outcome,
            at: row.annotated_at,
        };
        match grouped.last_mut() {
            Some((order_ref, events)) if *order_ref == row.order_ref => events.push(event),
            _ => grouped.push((row.order_ref, vec![event])),
        }
    }
    Ok(grouped)
}

#[derive(sqlx::FromRow)]
struct PublicationRow {
    path: String,
    revision: i64,
    body_hash: String,
}

#[derive(sqlx::FromRow, Clone)]
struct AttemptRow {
    path: String,
    revision: i64,
    body_hash: String,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
}

enum Content {
    Profile(Box<Profile>),
    Annotation {
        order_ref: String,
        outcomes: Vec<Outcome>,
    },
}

struct Desired {
    path: String,
    hash: String,
    content: Content,
}

impl Desired {
    fn is_empty_annotation(&self) -> bool {
        matches!(&self.content, Content::Annotation { outcomes, .. } if outcomes.is_empty())
    }

    fn signed(
        &self,
        revision: i64,
        secret: &[u8; 32],
    ) -> Result<String, crate::attestor_records::RecordError> {
        match &self.content {
            Content::Profile(profile) => profile.clone().sign(secret)?.to_canonical_json(),
            Content::Annotation {
                order_ref,
                outcomes,
            } => Annotation {
                order_ref: order_ref.clone(),
                outcomes: outcomes.clone(),
                revision,
                proof: String::new(),
            }
            .sign(secret)?
            .to_canonical_json(),
        }
    }
}

/// Seconds the path waits after its `attempts`-th consecutive failed or
/// interrupted write.
pub fn retry_delay_seconds(attempts: i32) -> i64 {
    let doublings = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(20);
    RETRY_BASE_SECONDS
        .saturating_mul(1_i64 << doublings)
        .min(RETRY_MAX_SECONDS)
}

async fn desired_records(
    state: &AppState,
    publisher: &AttestorPublisher,
    attestor: &Attestor,
    published: &HashMap<String, PublicationRow>,
) -> anyhow::Result<Vec<Desired>> {
    let profile = build_profile(&publisher.profile, state.config.review_blind_window_days);
    let mut desired = vec![Desired {
        path: PROFILE_PATH.to_string(),
        hash: profile.content_hash()?,
        content: Content::Profile(Box::new(profile)),
    }];
    for (order_ref, events) in attested_annotations(&state.pool, attestor.pubky()).await? {
        let outcomes = fold_outcomes(&events);
        let path = annotation_path(&order_ref);
        let annotation = Annotation {
            order_ref: order_ref.clone(),
            outcomes: outcomes.clone(),
            revision: 1,
            proof: String::new(),
        };
        let record = Desired {
            path,
            hash: annotation.content_hash()?,
            content: Content::Annotation {
                order_ref,
                outcomes,
            },
        };
        if record.is_empty_annotation() && !published.contains_key(&record.path) {
            continue;
        }
        desired.push(record);
    }
    Ok(desired)
}

fn truncate(message: &str) -> String {
    message.chars().take(MAX_ERROR_LENGTH).collect()
}

/// The pass's lease: the worker lease row for [`TASK_ATTESTOR_PUBLISH`], with
/// the fence of the latest acquisition.
struct Lease<'a> {
    pool: &'a PgPool,
    holder: Uuid,
    seconds: i64,
    fence: i64,
}

impl Lease<'_> {
    /// Renews the lease; false when another instance holds it now.
    async fn renew(&mut self, now: DateTime<Utc>) -> Result<bool, sqlx::Error> {
        match try_acquire_fenced_lease(
            self.pool,
            TASK_ATTESTOR_PUBLISH,
            self.holder,
            now,
            self.seconds,
        )
        .await?
        {
            Some(fence) => {
                self.fence = fence;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

/// One publisher pass: takes the lease, writes every due record that changed
/// (at most one batch), releases the lease. A failure is logged and retried by
/// a later pass under the path's backoff; it never fails the worker tick.
/// Returns the number of records written.
pub async fn run_pass(state: &AppState, holder: Uuid, now: DateTime<Utc>) -> u64 {
    let (Some(publisher), Some(attestor)) = (&state.attestor_publisher, &state.attestor) else {
        return 0;
    };
    let lease_seconds = state.config.worker_lease_seconds;
    let fence = match try_acquire_fenced_lease(
        &state.pool,
        TASK_ATTESTOR_PUBLISH,
        holder,
        now,
        lease_seconds,
    )
    .await
    {
        Ok(Some(fence)) => fence,
        Ok(None) => return 0,
        Err(error) => {
            tracing::error!(error = %error, "attestor publish could not take its lease");
            return 0;
        }
    };
    let mut lease = Lease {
        pool: &state.pool,
        holder,
        seconds: lease_seconds,
        fence,
    };
    let result = publish_due(state, publisher, attestor, &mut lease).await;
    if let Err(error) = release_fenced_lease(
        &state.pool,
        TASK_ATTESTOR_PUBLISH,
        holder,
        lease.fence,
        state.clock.now(),
    )
    .await
    {
        tracing::error!(error = %error, "attestor publish could not release its lease");
    }
    match result {
        Ok(published) => published,
        Err(error) => {
            tracing::error!(error = %error, "attestor publish pass failed; retrying next tick");
            0
        }
    }
}

async fn publish_due(
    state: &AppState,
    publisher: &AttestorPublisher,
    attestor: &Attestor,
    lease: &mut Lease<'_>,
) -> anyhow::Result<u64> {
    let pool = &state.pool;
    let pubky = attestor.pubky();
    let published: HashMap<String, PublicationRow> = sqlx::query_as::<_, PublicationRow>(
        "SELECT path, revision, body_hash FROM attestor_publications WHERE attestor_pubky = $1",
    )
    .bind(pubky)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.path.clone(), row))
    .collect();
    let mut attempts: HashMap<String, AttemptRow> = sqlx::query_as::<_, AttemptRow>(
        "SELECT path, revision, body_hash, attempts, next_attempt_at \
         FROM attestor_publish_attempts WHERE attestor_pubky = $1",
    )
    .bind(pubky)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|row| (row.path.clone(), row))
    .collect();

    let secret = attestor.secret_key_bytes();
    let batch = usize::try_from(state.config.delivery_sweep_batch_size.max(1)).unwrap_or(1);
    let mut written = 0_u64;
    for record in desired_records(state, publisher, attestor, &published).await? {
        if written as usize >= batch {
            break;
        }
        let recorded = published.get(&record.path);
        if recorded.is_some_and(|row| row.body_hash == record.hash) {
            if attempts.remove(&record.path).is_some() {
                clear_attempt(pool, pubky, &record.path).await?;
            }
            continue;
        }
        let previous_attempt = attempts.get(&record.path).cloned();
        if previous_attempt
            .as_ref()
            .is_some_and(|attempt| attempt.next_attempt_at > state.clock.now())
        {
            continue;
        }
        let recorded_revision = recorded.map_or(0, |row| row.revision);
        let revision = match &previous_attempt {
            Some(attempt)
                if attempt.body_hash == record.hash && attempt.revision > recorded_revision =>
            {
                attempt.revision
            }
            Some(attempt) => recorded_revision.max(attempt.revision) + 1,
            None => recorded_revision + 1,
        };
        let body = record.signed(revision, &secret)?;

        if !lease.renew(state.clock.now()).await? {
            tracing::info!("attestor publish lost its lease; stopping the pass");
            return Ok(written);
        }
        let attempt_count = previous_attempt.map_or(0, |attempt| attempt.attempts) + 1;
        let attempted_at = state.clock.now();
        sqlx::query(
            "INSERT INTO attestor_publish_attempts \
             (attestor_pubky, path, revision, body_hash, attempts, next_attempt_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (attestor_pubky, path) DO UPDATE SET revision = EXCLUDED.revision, \
             body_hash = EXCLUDED.body_hash, attempts = EXCLUDED.attempts, \
             next_attempt_at = EXCLUDED.next_attempt_at, updated_at = EXCLUDED.updated_at",
        )
        .bind(pubky)
        .bind(&record.path)
        .bind(revision)
        .bind(&record.hash)
        .bind(attempt_count)
        .bind(attempted_at + chrono::Duration::seconds(retry_delay_seconds(attempt_count)))
        .bind(attempted_at)
        .execute(pool)
        .await?;

        match publisher
            .homeserver
            .put(&record.path, body.as_bytes())
            .await
        {
            Ok(()) => {
                let mut tx = pool.begin().await?;
                sqlx::query(
                    "INSERT INTO attestor_publications \
                     (attestor_pubky, path, revision, body_hash, published_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (attestor_pubky, path) DO UPDATE SET \
                     revision = EXCLUDED.revision, body_hash = EXCLUDED.body_hash, \
                     published_at = EXCLUDED.published_at \
                     WHERE attestor_publications.revision < EXCLUDED.revision",
                )
                .bind(pubky)
                .bind(&record.path)
                .bind(revision)
                .bind(&record.hash)
                .bind(state.clock.now())
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "DELETE FROM attestor_publish_attempts \
                     WHERE attestor_pubky = $1 AND path = $2",
                )
                .bind(pubky)
                .bind(&record.path)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                attempts.remove(&record.path);
                written += 1;
                match &record.content {
                    Content::Profile(_) => tracing::info!(revision, "attestor profile published"),
                    Content::Annotation {
                        order_ref,
                        outcomes,
                    } => tracing::info!(
                        order_ref = %order_ref,
                        revision,
                        outcomes = outcomes.len(),
                        "attestor annotation published"
                    ),
                }
            }
            Err(error) => {
                let message = truncate(&error.to_string());
                sqlx::query(
                    "UPDATE attestor_publish_attempts SET last_error = $3, updated_at = $4 \
                     WHERE attestor_pubky = $1 AND path = $2",
                )
                .bind(pubky)
                .bind(&record.path)
                .bind(&message)
                .bind(state.clock.now())
                .execute(pool)
                .await?;
                tracing::warn!(
                    path = %record.path,
                    attempts = attempt_count,
                    error = %message,
                    "attestor publish failed; the path backs off and the pass ends"
                );
                return Ok(written);
            }
        }
    }
    Ok(written)
}

async fn clear_attempt(pool: &PgPool, pubky: &str, path: &str) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM attestor_publish_attempts WHERE attestor_pubky = $1 AND path = $2")
        .bind(pubky)
        .bind(path)
        .execute(pool)
        .await?;
    Ok(())
}

/// The real homeserver: signs in as the attestor and writes with the account's
/// session. The attestor's PKARR record must point at the configured
/// homeserver; anything else is refused, since the index reads the attestor's
/// records only from the homeserver it polls.
pub struct PubkyHomeserver {
    pubky: pubky::Pubky,
    keypair: pubky::Keypair,
    homeserver: pubky::PublicKey,
    session: Mutex<Option<pubky::PubkySession>>,
}

impl PubkyHomeserver {
    pub fn new(secret_key: &[u8; 32], homeserver: &str) -> anyhow::Result<Self> {
        let homeserver = pubky::PublicKey::try_from(homeserver).map_err(|error| {
            anyhow::anyhow!("{ENV_ATTESTOR_HOMESERVER} is not a pubky: {error}")
        })?;
        Ok(Self {
            pubky: pubky::Pubky::new()
                .map_err(|error| anyhow::anyhow!("homeserver client: {error}"))?,
            keypair: pubky::Keypair::from_secret(secret_key),
            homeserver,
            session: Mutex::new(None),
        })
    }

    async fn sign_in(&self) -> Result<pubky::PubkySession, PublishError> {
        let signer = self.pubky.signer(self.keypair.clone());
        let resolved = signer.pkdns().get_homeserver().await.map_err(|error| {
            PublishError(format!("resolving the attestor's homeserver: {error}"))
        })?;
        if resolved.as_ref() != Some(&self.homeserver) {
            return Err(PublishError(format!(
                "the attestor's PKARR record points at {}, not at {ENV_ATTESTOR_HOMESERVER}",
                resolved.map_or_else(|| "no homeserver".to_string(), |host| host.to_string())
            )));
        }
        let client_id = pubky::ClientId::new(CLIENT_ID)
            .map_err(|error| PublishError(format!("client id: {error}")))?;
        signer
            .signin(client_id)
            .await
            .map_err(|error| PublishError(format!("signing in as the attestor: {error}")))
    }
}

#[async_trait]
impl AttestorHomeserver for PubkyHomeserver {
    async fn put(&self, path: &str, body: &[u8]) -> Result<(), PublishError> {
        let write = async {
            let mut cached = self.session.lock().await;
            let session = match cached.as_ref() {
                Some(session) => session.clone(),
                None => {
                    let session = self.sign_in().await?;
                    *cached = Some(session.clone());
                    session
                }
            };
            let result = session
                .storage()
                .put(path, body.to_vec())
                .await
                .map(|_| ())
                .map_err(|error| PublishError(format!("writing {path}: {error}")));
            if result.is_err() {
                *cached = None;
            }
            result
        };
        tokio::time::timeout(HOMESERVER_WRITE_TIMEOUT, write)
            .await
            .unwrap_or_else(|_| {
                Err(PublishError(format!(
                    "writing {path} timed out after {} seconds",
                    HOMESERVER_WRITE_TIMEOUT.as_secs()
                )))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(outcome: &str, at: &str) -> AnnotationEvent {
        AnnotationEvent {
            outcome: outcome.to_string(),
            at: at.parse().expect("instant"),
        }
    }

    #[test]
    fn outcomes_follow_the_rows_in_order() {
        let outcomes = fold_outcomes(&[
            event("returned", "2026-10-12T09:00:00Z"),
            event("refunded", "2026-10-14T23:59:59Z"),
        ]);
        assert_eq!(
            outcomes,
            vec![
                Outcome {
                    kind: OutcomeKind::Returned,
                    on: "2026-10-12".into()
                },
                Outcome {
                    kind: OutcomeKind::Refunded,
                    on: "2026-10-14".into()
                },
            ]
        );
    }

    #[test]
    fn a_cancelled_reversal_takes_back_the_latest_refund_and_is_not_an_outcome() {
        let outcomes = fold_outcomes(&[
            event("refunded", "2026-10-12T09:00:00Z"),
            event("refund_reversal_cancelled", "2026-10-13T09:00:00Z"),
        ]);
        assert!(outcomes.is_empty());

        let outcomes = fold_outcomes(&[
            event("refunded", "2026-10-12T09:00:00Z"),
            event("refund_reversal_cancelled", "2026-10-13T09:00:00Z"),
            event("refunded", "2026-10-15T09:00:00Z"),
        ]);
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].on, "2026-10-15");

        assert!(
            fold_outcomes(&[event("refund_reversal_cancelled", "2026-10-13T09:00:00Z")]).is_empty()
        );
    }

    #[test]
    fn the_oldest_outcomes_go_past_the_record_limit() {
        let events: Vec<AnnotationEvent> = (1..=20)
            .map(|day| event("returned", &format!("2026-10-{day:02}T00:00:00Z")))
            .collect();
        let outcomes = fold_outcomes(&events);
        assert_eq!(outcomes.len(), MAX_ANNOTATION_OUTCOMES);
        assert_eq!(outcomes[0].on, "2026-10-05");
        assert_eq!(outcomes[MAX_ANNOTATION_OUTCOMES - 1].on, "2026-10-20");
    }

    #[test]
    fn the_retry_delay_doubles_to_the_cap() {
        let delays: Vec<i64> = (1..=10).map(retry_delay_seconds).collect();
        assert_eq!(delays[..4], [15, 30, 60, 120]);
        assert_eq!(delays[9], RETRY_MAX_SECONDS);
        assert_eq!(retry_delay_seconds(i32::MAX), RETRY_MAX_SECONDS);
    }
}
