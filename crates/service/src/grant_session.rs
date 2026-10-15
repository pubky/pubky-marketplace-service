//! Marketplace sessions minted from the Shop's own sign-in grant.
//!
//! The Shop signs in with a root-signed grant whose `cnf` key lives in the
//! browser. Instead of a second signer approval, the browser proves to this
//! service that it holds that grant:
//!
//! 1. `POST /v1/auth/grant-session-challenges` with a custom proof over
//!    `{aud, purpose: CHALLENGE_PURPOSE}` returns a random `challenge_id` the
//!    service stores for `pubky`, valid for the proof age (120 s by default).
//! 2. It writes a proof document to its own homeserver under
//!    [`PROOF_DOCUMENTS_PATH`], keyed by that `challenge_id`. Only a live
//!    homeserver session can write it, and the id exists only from step 1, so
//!    a document cannot be staged before the grant is revoked and used after
//!    (beyond one challenge lifetime), even with a stolen client key.
//! 3. It signs a pubky custom proof (`createCustomPop`) over
//!    `{aud, challenge_id, purpose}` with the grant's client key.
//! 4. `POST /v1/auth/grant-sessions` carries `{grant, pop}`.
//!
//! The service verifies both signatures offline with the SDK, then checks the
//! audience, purpose, proof age, client id and capabilities, requires an
//! issued, unexpired and unused challenge for that pubky, reads the proof
//! document, and in one transaction burns the challenge and inserts the
//! session. The session's capabilities are the grant's, narrowed to the
//! marketplace scopes; the Shop's wider storage grant is never stored.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use pubky::custom_pop::{
    verify_custom_grant_pop, CustomPop, CustomPopVerificationError, DEFAULT_CUSTOM_POP_CLOCK_SKEW,
};
use pubky::{Capabilities, Capability};
use pubky_common::capabilities::Action;
use pubky_common::StoragePath;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

use crate::auth::{self, INVENTORY_SERVICE_SCOPE};
use crate::clock::format_timestamp;
use crate::homeserver::HomeserverFetchOutcome;
use crate::logging;
use crate::priv_keys::PRIV_APP_SCOPE;
use crate::AppState;

pub const ENV_ENABLED: &str = "MARKETPLACE_GRANT_SESSION_ENABLED";
pub const ENV_CLIENT_IDS: &str = "MARKETPLACE_GRANT_SESSION_CLIENT_IDS";
pub const ENV_MAX_PROOF_AGE_SECONDS: &str = "MARKETPLACE_GRANT_SESSION_MAX_PROOF_AGE_SECONDS";

/// Signed `data.purpose` of the custom proof that mints a session.
pub const PROOF_PURPOSE: &str = "marketplace-service/grant-session/v1";
/// Signed `data.purpose` of the custom proof that requests a challenge.
pub const CHALLENGE_PURPOSE: &str = "marketplace-service/grant-session-challenge/v1";
/// Unused, unexpired challenges one pubky may hold at once.
pub const MAX_LIVE_CHALLENGES: i64 = 5;
/// `domain` of the homeserver proof document.
pub const PROOF_DOCUMENT_DOMAIN: &str = "marketplace-service/grant-session-proof/v1";
/// Where the Shop writes the proof document, followed by the challenge id.
pub const PROOF_DOCUMENTS_PATH: &str = "/pub/pubky.app/marketplace/v1/session-proofs/";

const DEFAULT_MAX_PROOF_AGE_SECONDS: i64 = 120;
const MAX_PROOF_AGE_SECONDS_LIMIT: i64 = 600;
pub const MAX_REQUEST_BYTES: usize = 16 * 1024;
const RATE_CLASS: &str = "grant_session.mint";

#[derive(Clone, Debug)]
pub struct GrantSessionConfig {
    /// The service origin the proof must name (`PUBLIC_SERVICE_ORIGIN`).
    pub audience: String,
    /// Grant `client_id`s whose sign-in may mint a marketplace session.
    pub client_ids: Vec<String>,
    pub max_proof_age_seconds: i64,
}

impl GrantSessionConfig {
    /// `None` while `MARKETPLACE_GRANT_SESSION_ENABLED` is off (the default).
    /// When on, a missing audience or client-id list refuses startup.
    pub fn from_env(public_service_origin: Option<&str>) -> anyhow::Result<Option<Arc<Self>>> {
        let enabled = match std::env::var(ENV_ENABLED) {
            Ok(value) => match value.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => anyhow::bail!("{ENV_ENABLED} must be true, false, 1, or 0"),
            },
            Err(_) => false,
        };
        if !enabled {
            return Ok(None);
        }
        let origin = public_service_origin.ok_or_else(|| {
            anyhow::anyhow!("{ENV_ENABLED} requires PUBLIC_SERVICE_ORIGIN as the proof audience")
        })?;
        let client_ids = std::env::var(ENV_CLIENT_IDS).unwrap_or_default();
        let max_proof_age_seconds = match std::env::var(ENV_MAX_PROOF_AGE_SECONDS) {
            Ok(raw) => raw
                .parse::<i64>()
                .map_err(|_| anyhow::anyhow!("{ENV_MAX_PROOF_AGE_SECONDS} must be an integer"))?,
            Err(_) => DEFAULT_MAX_PROOF_AGE_SECONDS,
        };
        Self::new(origin, &client_ids, max_proof_age_seconds).map(|config| Some(Arc::new(config)))
    }

    pub fn new(
        service_origin: &str,
        client_ids: &str,
        max_proof_age_seconds: i64,
    ) -> anyhow::Result<Self> {
        let audience = audience_from_origin(service_origin)?;
        let client_ids = client_ids
            .split(',')
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(|id| {
                pubky::ClientId::new(id)
                    .map(|id| id.as_str().to_string())
                    .map_err(|_| anyhow::anyhow!("{ENV_CLIENT_IDS} has an invalid client id"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if client_ids.is_empty() {
            anyhow::bail!("{ENV_CLIENT_IDS} must name at least one grant client id");
        }
        if !(1..=MAX_PROOF_AGE_SECONDS_LIMIT).contains(&max_proof_age_seconds) {
            anyhow::bail!(
                "{ENV_MAX_PROOF_AGE_SECONDS} must be between 1 and {MAX_PROOF_AGE_SECONDS_LIMIT}"
            );
        }
        Ok(Self {
            audience,
            client_ids,
            max_proof_age_seconds,
        })
    }
}

/// The scheme, host and port of `origin`, as a browser's `URL.origin` prints it.
fn audience_from_origin(origin: &str) -> anyhow::Result<String> {
    let url =
        Url::parse(origin).map_err(|_| anyhow::anyhow!("PUBLIC_SERVICE_ORIGIN must be a URL"))?;
    let audience = url.origin().ascii_serialization();
    if audience == "null" {
        anyhow::bail!("PUBLIC_SERVICE_ORIGIN must be an http(s) origin");
    }
    Ok(audience)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantSessionRequest {
    grant: String,
    pop: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChallengeRequestData {
    aud: String,
    purpose: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProofData {
    aud: String,
    challenge_id: String,
    purpose: String,
}

/// The document the Shop writes at `PROOF_DOCUMENTS_PATH{challenge_id}`.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProofDocument {
    pub aud: String,
    pub challenge_id: String,
    pub domain: String,
    pub pubky: String,
}

impl ProofDocument {
    pub fn expected(audience: &str, challenge_id: Uuid, pubky: &str) -> Self {
        Self {
            aud: audience.to_string(),
            challenge_id: challenge_id.to_string(),
            domain: PROOF_DOCUMENT_DOMAIN.to_string(),
            pubky: pubky.to_string(),
        }
    }
}

/// Why a grant session was refused. Each maps to one status and code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantSessionRefusal {
    /// Malformed body, signatures, binding, audience, purpose or age.
    ProofInvalid,
    /// The grant expired or is not yet valid.
    GrantExpired,
    /// The grant was issued to an app this service does not accept.
    ClientNotAllowed,
    /// The grant does not cover the marketplace inventory scope with `rw`.
    CapabilityRequired,
}

/// Why a mint was refused after the proof itself verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChallengeRefusal {
    /// Never issued to this pubky, expired, or already used.
    Invalid,
}

impl GrantSessionRefusal {
    fn status_and_code(self) -> (StatusCode, &'static str) {
        match self {
            Self::ProofInvalid => (StatusCode::UNAUTHORIZED, "grant_session_invalid"),
            Self::GrantExpired => (StatusCode::UNAUTHORIZED, "grant_expired"),
            Self::ClientNotAllowed => (StatusCode::FORBIDDEN, "grant_client_not_allowed"),
            Self::CapabilityRequired => (StatusCode::FORBIDDEN, "capability_required"),
        }
    }
}

/// The facts of an accepted proof that the session is minted from.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedGrantSession {
    pub pubky: String,
    pub client_id: String,
    pub grant_id: String,
    pub capabilities: String,
    pub challenge_id: Uuid,
    pub grant_expires_at: DateTime<Utc>,
}

fn scope_covered_rw(capabilities: &Capabilities, scope: &str) -> bool {
    let path = StoragePath::new(scope).expect("marketplace scopes are canonical");
    capabilities.iter().any(|capability| {
        capability.scope_covers_path(&path)
            && capability.actions().contains(&Action::Read)
            && capability.actions().contains(&Action::Write)
    })
}

/// The grant's capabilities narrowed to the marketplace scopes: the
/// inventory scope (required) and `/priv/pubky.app/` when the grant covers
/// it. `None` when the inventory scope is not covered with read and write.
pub fn marketplace_capabilities(granted: &[Capability]) -> Option<String> {
    let granted = Capabilities::from(granted.to_vec());
    if !scope_covered_rw(&granted, INVENTORY_SERVICE_SCOPE) {
        return None;
    }
    let mut narrowed = format!("{INVENTORY_SERVICE_SCOPE}:rw");
    if scope_covered_rw(&granted, PRIV_APP_SCOPE) {
        narrowed.push_str(&format!(",{PRIV_APP_SCOPE}:rw"));
    }
    let parsed = narrowed
        .parse::<Capabilities>()
        .expect("marketplace capabilities are canonical");
    Some(parsed.normalize().to_string())
}

/// Offline checks of a custom proof. Signatures and grant validity come from
/// the SDK (which reads the system clock); the proof age is bounded against
/// `now`, the service clock.
/// The grant facts shared by a challenge request and a mint.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedGrant {
    pub pubky: String,
    pub client_id: String,
    pub grant_id: String,
    pub capabilities: String,
    pub grant_expires_at: DateTime<Utc>,
}

/// Verifies a challenge request: a fresh proof over exactly
/// `{aud, purpose: CHALLENGE_PURPOSE}` from an allowed, sufficient grant.
pub fn verify_challenge_request(
    grant: &str,
    pop: &str,
    config: &GrantSessionConfig,
    now: DateTime<Utc>,
) -> Result<VerifiedGrant, GrantSessionRefusal> {
    let (verified, data) = verify_signed(grant, pop, config, now)?;
    let data: ChallengeRequestData =
        serde_json::from_value(data).map_err(|_| GrantSessionRefusal::ProofInvalid)?;
    if data.purpose != CHALLENGE_PURPOSE || data.aud != config.audience {
        return Err(GrantSessionRefusal::ProofInvalid);
    }
    Ok(verified)
}

pub fn verify_grant_session_proof(
    grant: &str,
    pop: &str,
    config: &GrantSessionConfig,
    now: DateTime<Utc>,
) -> Result<VerifiedGrantSession, GrantSessionRefusal> {
    let (verified, data) = verify_signed(grant, pop, config, now)?;
    let data: ProofData =
        serde_json::from_value(data).map_err(|_| GrantSessionRefusal::ProofInvalid)?;
    if data.purpose != PROOF_PURPOSE || data.aud != config.audience {
        return Err(GrantSessionRefusal::ProofInvalid);
    }
    let challenge_id =
        Uuid::parse_str(&data.challenge_id).map_err(|_| GrantSessionRefusal::ProofInvalid)?;
    // The id becomes a homeserver path segment: only the canonical form.
    if challenge_id.to_string() != data.challenge_id {
        return Err(GrantSessionRefusal::ProofInvalid);
    }
    Ok(VerifiedGrantSession {
        pubky: verified.pubky,
        client_id: verified.client_id,
        grant_id: verified.grant_id,
        capabilities: verified.capabilities,
        challenge_id,
        grant_expires_at: verified.grant_expires_at,
    })
}

/// Signatures, grant validity, proof age, client id and capabilities; the
/// signed `data` is returned for the caller's purpose-specific checks.
fn verify_signed(
    grant: &str,
    pop: &str,
    config: &GrantSessionConfig,
    now: DateTime<Utc>,
) -> Result<(VerifiedGrant, Value), GrantSessionRefusal> {
    let credentials = CustomPop {
        grant: grant.to_string(),
        pop: pop.to_string(),
    };
    let verified =
        verify_custom_grant_pop(&credentials, DEFAULT_CUSTOM_POP_CLOCK_SKEW).map_err(|error| {
            match error {
                CustomPopVerificationError::GrantExpired
                | CustomPopVerificationError::GrantNotYetValid => GrantSessionRefusal::GrantExpired,
                _ => GrantSessionRefusal::ProofInvalid,
            }
        })?;

    let issued_at = i64::try_from(verified.iat()).map_err(|_| GrantSessionRefusal::ProofInvalid)?;
    let now_seconds = now.timestamp();
    let skew = i64::try_from(DEFAULT_CUSTOM_POP_CLOCK_SKEW.as_secs()).unwrap_or(0);
    if issued_at > now_seconds + skew || now_seconds - issued_at > config.max_proof_age_seconds {
        return Err(GrantSessionRefusal::ProofInvalid);
    }

    let claims = verified.grant_claims();
    if !config
        .client_ids
        .iter()
        .any(|allowed| allowed == claims.client_id.as_str())
    {
        return Err(GrantSessionRefusal::ClientNotAllowed);
    }
    let capabilities =
        marketplace_capabilities(&claims.caps).ok_or(GrantSessionRefusal::CapabilityRequired)?;
    let grant_expires_at = i64::try_from(claims.exp)
        .ok()
        .and_then(|exp| DateTime::<Utc>::from_timestamp(exp, 0))
        .ok_or(GrantSessionRefusal::ProofInvalid)?;

    Ok((
        VerifiedGrant {
            pubky: verified.identity().z32(),
            client_id: claims.client_id.as_str().to_string(),
            grant_id: claims.jti.as_str().to_string(),
            capabilities,
            grant_expires_at,
        },
        verified.data().clone(),
    ))
}

fn error_response(status: StatusCode, code: &'static str) -> Response {
    no_store((status, Json(json!({ "error": code }))).into_response())
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn internal_error() -> Response {
    error_response(StatusCode::INTERNAL_SERVER_ERROR, "session_unavailable")
}

fn rate_limited(retry_after: i64) -> Response {
    let mut response = error_response(StatusCode::TOO_MANY_REQUESTS, "retry_later");
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from_str(&retry_after.to_string()).expect("retry-after is a safe integer"),
    );
    response
}

fn parse_request(
    state: &AppState,
    body: &Bytes,
) -> Result<(Arc<GrantSessionConfig>, GrantSessionRequest), Box<Response>> {
    let Some(config) = state.grant_session.clone() else {
        return Err(Box::new(error_response(
            StatusCode::NOT_FOUND,
            "grant_session_unavailable",
        )));
    };
    // Bodies over MAX_REQUEST_BYTES never get here: the router's
    // DefaultBodyLimit answers them with a plain-text 413.
    let request = serde_json::from_slice::<GrantSessionRequest>(body)
        .map_err(|_| Box::new(error_response(StatusCode::BAD_REQUEST, "invalid_request")))?;
    Ok((config, request))
}

fn refused(refusal: GrantSessionRefusal) -> Response {
    tracing::info!(refusal = ?refusal, "refused grant session proof");
    let (status, code) = refusal.status_and_code();
    error_response(status, code)
}

/// `POST /v1/auth/grant-session-challenges`.
pub async fn create_challenge(State(state): State<AppState>, body: Bytes) -> Response {
    let (config, request) = match parse_request(&state, &body) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    let now = state.clock.now();
    let verified = match verify_challenge_request(&request.grant, &request.pop, &config, now) {
        Ok(verified) => verified,
        Err(refusal) => return refused(refusal),
    };
    // A pubky at its live cap gets 429 without spending a rate token, so a
    // client waiting for a slot cannot lock itself out of the bucket.
    // `issue_challenge` re-checks under the per-pubky lock.
    match live_challenges(&state, &verified.pubky, now).await {
        Ok(live) if live >= MAX_LIVE_CHALLENGES => {
            return error_response(StatusCode::TOO_MANY_REQUESTS, "too_many_challenges");
        }
        Ok(_) => {}
        Err(error) => {
            tracing::error!(error = %error, "grant session challenge count failed");
            return internal_error();
        }
    }
    match crate::automation::consume_automation_rate(&state, &verified.pubky, RATE_CLASS).await {
        Ok(None) => {}
        Ok(Some(retry_after)) => return rate_limited(retry_after),
        Err(error) => {
            tracing::error!(error = %error, "grant session rate limit failed");
            return internal_error();
        }
    }
    match issue_challenge(&state, &config, &verified.pubky, now).await {
        Ok(Some((challenge_id, expires_at))) => no_store(
            (
                StatusCode::CREATED,
                Json(json!({
                    "challenge_id": challenge_id,
                    "expires_at": format_timestamp(expires_at),
                })),
            )
                .into_response(),
        ),
        Ok(None) => error_response(StatusCode::TOO_MANY_REQUESTS, "too_many_challenges"),
        Err(error) => {
            tracing::error!(error = %error, "failed to issue grant session challenge");
            internal_error()
        }
    }
}

async fn live_challenges(state: &AppState, pubky: &str, now: DateTime<Utc>) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM grant_session_challenges \
         WHERE pubky = $1 AND used_at IS NULL AND expires_at > $2",
    )
    .bind(pubky)
    .bind(now)
    .fetch_one(&state.pool)
    .await
}

/// Stores a fresh challenge for `pubky`, valid for one proof age. `None`
/// when the pubky already holds [`MAX_LIVE_CHALLENGES`] live ones.
async fn issue_challenge(
    state: &AppState,
    config: &GrantSessionConfig,
    pubky: &str,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<(Uuid, DateTime<Utc>)>> {
    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 6371))")
        .bind(pubky)
        .execute(&mut *tx)
        .await?;
    // An expired challenge can never mint again, used or not.
    sqlx::query("DELETE FROM grant_session_challenges WHERE expires_at <= $1")
        .bind(now)
        .execute(&mut *tx)
        .await?;
    let live: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM grant_session_challenges WHERE pubky = $1 AND used_at IS NULL",
    )
    .bind(pubky)
    .fetch_one(&mut *tx)
    .await?;
    if live >= MAX_LIVE_CHALLENGES {
        tx.rollback().await?;
        return Ok(None);
    }
    let challenge_id = Uuid::new_v4();
    let expires_at = now + Duration::seconds(config.max_proof_age_seconds);
    sqlx::query(
        "INSERT INTO grant_session_challenges (challenge_id, pubky, issued_at, expires_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(challenge_id)
    .bind(pubky)
    .bind(now)
    .bind(expires_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some((challenge_id, expires_at)))
}

async fn challenge_is_live(
    state: &AppState,
    verified: &VerifiedGrantSession,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM grant_session_challenges \
         WHERE challenge_id = $1 AND pubky = $2 AND used_at IS NULL AND expires_at > $3)",
    )
    .bind(verified.challenge_id)
    .bind(&verified.pubky)
    .bind(now)
    .fetch_one(&state.pool)
    .await
}

/// `POST /v1/auth/grant-sessions`.
pub async fn create_grant_session(State(state): State<AppState>, body: Bytes) -> Response {
    let (config, request) = match parse_request(&state, &body) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };
    let now = state.clock.now();
    let verified = match verify_grant_session_proof(&request.grant, &request.pop, &config, now) {
        Ok(verified) => verified,
        Err(refusal) => return refused(refusal),
    };
    // Checked before the homeserver read so a replayed or unissued challenge
    // costs one indexed lookup; the burn below re-checks under the update.
    match challenge_is_live(&state, &verified, now).await {
        Ok(true) => {}
        Ok(false) => return challenge_refused(ChallengeRefusal::Invalid),
        Err(error) => {
            tracing::error!(error = %error, "grant session challenge lookup failed");
            return internal_error();
        }
    }

    let Some(homeserver) = state.homeserver.clone() else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "homeserver_unavailable");
    };
    let expected =
        ProofDocument::expected(&config.audience, verified.challenge_id, &verified.pubky);
    match homeserver
        .fetch_grant_session_proof(&verified.pubky, &expected.challenge_id)
        .await
    {
        HomeserverFetchOutcome::Found(document) => {
            let matches = serde_json::from_value::<ProofDocument>(document)
                .is_ok_and(|document| document == expected);
            if !matches {
                return error_response(StatusCode::UNAUTHORIZED, "homeserver_proof_invalid");
            }
        }
        HomeserverFetchOutcome::NotFound => {
            return error_response(StatusCode::UNAUTHORIZED, "homeserver_proof_invalid");
        }
        HomeserverFetchOutcome::Unavailable => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "homeserver_unavailable");
        }
    }

    match mint(&state, &verified, now).await {
        Ok(Some(response)) => response,
        Ok(None) => challenge_refused(ChallengeRefusal::Invalid),
        Err(error) => {
            tracing::error!(error = %error, "failed to mint grant session");
            internal_error()
        }
    }
}

fn challenge_refused(refusal: ChallengeRefusal) -> Response {
    tracing::info!(refusal = ?refusal, "refused grant session challenge");
    error_response(StatusCode::UNAUTHORIZED, "grant_session_challenge_invalid")
}

/// Burns the challenge and inserts the session in one transaction. `None`
/// when the challenge was used, expired or never issued to this pubky.
async fn mint(
    state: &AppState,
    verified: &VerifiedGrantSession,
    now: DateTime<Utc>,
) -> anyhow::Result<Option<Response>> {
    let mut tx = state.pool.begin().await?;
    let burned = sqlx::query(
        "UPDATE grant_session_challenges SET used_at = $3, grant_id = $4 \
         WHERE challenge_id = $1 AND pubky = $2 AND used_at IS NULL AND expires_at > $3",
    )
    .bind(verified.challenge_id)
    .bind(&verified.pubky)
    .bind(now)
    .bind(&verified.grant_id)
    .execute(&mut *tx)
    .await?;
    if burned.rows_affected() == 0 {
        tx.rollback().await?;
        return Ok(None);
    }

    let mut token = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut token);
    let expires_at =
        (now + Duration::seconds(state.config.session_ttl_seconds)).min(verified.grant_expires_at);
    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO auth_sessions \
         (token_hash, session_id, pubky, capabilities, created_at, expires_at, last_used_at, \
          source_grant_id, source_client_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $5, $7, $8)",
    )
    .bind(auth::hash_token(&token))
    .bind(session_id)
    .bind(&verified.pubky)
    .bind(&verified.capabilities)
    .bind(now)
    .bind(expires_at)
    .bind(&verified.grant_id)
    .bind(&verified.client_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    tracing::info!(
        actor_prefix = logging::actor_prefix(&verified.pubky),
        "issued grant session"
    );
    Ok(Some(no_store(
        (
            StatusCode::CREATED,
            Json(json!({
                "token": URL_SAFE_NO_PAD.encode(token),
                "session_id": session_id,
                "pubky": verified.pubky,
                "capabilities": verified.capabilities,
                "expires_at": format_timestamp(expires_at),
            })),
        )
            .into_response(),
    )))
}

/// Exposed for tests that build proofs: the exact `data` a challenge request signs.
pub fn challenge_request_data(audience: &str) -> Value {
    json!({ "aud": audience, "purpose": CHALLENGE_PURPOSE })
}

/// Exposed for tests that build proofs: the exact `data` a valid proof signs.
pub fn proof_data(audience: &str, challenge_id: Uuid) -> Value {
    json!({
        "aud": audience,
        "challenge_id": challenge_id.to_string(),
        "purpose": PROOF_PURPOSE,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky::{ClientId, GrantClaims, GrantId, Keypair, PopNonce, GRANT_JWS_TYP};
    use pubky_common::auth::jws::sign_jws;

    const AUDIENCE: &str = "https://svc.test";
    const CLIENT: &str = "shop.pubky.app";

    fn config() -> GrantSessionConfig {
        GrantSessionConfig::new(AUDIENCE, CLIENT, 120).expect("test config")
    }

    struct Fixture {
        root: Keypair,
        client: Keypair,
        claims: GrantClaims,
    }

    impl Fixture {
        fn new(caps: &str, client_id: &str, exp_offset: i64) -> Self {
            let root = Keypair::random();
            let client = Keypair::random();
            let now = Utc::now().timestamp() as u64;
            let claims = GrantClaims {
                iss: root.public_key(),
                client_id: ClientId::new(client_id).expect("client id"),
                caps: caps
                    .parse::<Capabilities>()
                    .expect("caps")
                    .iter()
                    .cloned()
                    .collect(),
                cnf: client.public_key(),
                jti: GrantId::generate(),
                iat: now - 3600,
                exp: (now as i64 + exp_offset) as u64,
            };
            Self {
                root,
                client,
                claims,
            }
        }

        fn grant(&self) -> String {
            self.claims.sign(&self.root, GRANT_JWS_TYP)
        }

        fn pop(&self, signer: &Keypair, data: Value, iat: i64) -> String {
            sign_jws(
                signer,
                pubky::custom_pop::CUSTOM_POP_JWS_TYP,
                &json!({
                    "gid": self.claims.jti,
                    "iat": iat,
                    "nonce": PopNonce::generate(),
                    "data": data,
                }),
            )
        }

        fn valid_pop(&self, challenge_id: Uuid) -> String {
            self.pop(
                &self.client,
                proof_data(AUDIENCE, challenge_id),
                Utc::now().timestamp(),
            )
        }
    }

    const SHOP_CAPS: &str =
        "/pub/pubky.app/:rw,/pub/paykit/:rw,/priv/pubky.app/:rw,/pub/chat/v0/:rw,/priv/chat/v0/:rw";

    #[test]
    fn a_shop_grant_proof_mints_the_narrowed_marketplace_session() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let challenge = Uuid::new_v4();
        let verified = verify_grant_session_proof(
            &fixture.grant(),
            &fixture.valid_pop(challenge),
            &config(),
            Utc::now(),
        )
        .expect("valid proof");
        assert_eq!(verified.pubky, fixture.root.public_key().z32());
        assert_eq!(verified.client_id, CLIENT);
        assert_eq!(verified.grant_id, fixture.claims.jti.as_str());
        assert_eq!(verified.challenge_id, challenge);
        assert_eq!(
            verified.capabilities,
            "/pub/pubky.app/marketplace-service/v1/:rw,/priv/pubky.app/:rw"
        );
    }

    #[test]
    fn capabilities_are_narrowed_and_inventory_is_required() {
        let parse = |raw: &str| -> Vec<Capability> {
            raw.parse::<Capabilities>()
                .expect("caps")
                .iter()
                .cloned()
                .collect()
        };
        assert_eq!(
            marketplace_capabilities(&parse("/:rw")).as_deref(),
            Some("/pub/pubky.app/marketplace-service/v1/:rw,/priv/pubky.app/:rw")
        );
        assert_eq!(
            marketplace_capabilities(&parse("/pub/pubky.app/:rw")).as_deref(),
            Some("/pub/pubky.app/marketplace-service/v1/:rw")
        );
        assert_eq!(marketplace_capabilities(&parse("/pub/pubky.app/:r")), None);
        assert_eq!(
            marketplace_capabilities(&parse("/priv/pubky.app/:rw")),
            None
        );
        assert_eq!(marketplace_capabilities(&parse("/pub/paykit/:rw")), None);
        assert_eq!(marketplace_capabilities(&[]), None);
    }

    #[test]
    fn a_proof_signed_by_any_key_but_the_grants_client_key_is_refused() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let pop = fixture.pop(
            &Keypair::random(),
            proof_data(AUDIENCE, Uuid::new_v4()),
            Utc::now().timestamp(),
        );
        assert_eq!(
            verify_grant_session_proof(&fixture.grant(), &pop, &config(), Utc::now()),
            Err(GrantSessionRefusal::ProofInvalid)
        );
        // The root key signing the proof is still not the client key.
        let pop = fixture.pop(
            &fixture.root,
            proof_data(AUDIENCE, Uuid::new_v4()),
            Utc::now().timestamp(),
        );
        assert_eq!(
            verify_grant_session_proof(&fixture.grant(), &pop, &config(), Utc::now()),
            Err(GrantSessionRefusal::ProofInvalid)
        );
    }

    #[test]
    fn a_grant_not_signed_by_its_issuer_is_refused() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let forged = fixture.claims.sign(&Keypair::random(), GRANT_JWS_TYP);
        assert_eq!(
            verify_grant_session_proof(
                &forged,
                &fixture.valid_pop(Uuid::new_v4()),
                &config(),
                Utc::now()
            ),
            Err(GrantSessionRefusal::ProofInvalid)
        );
    }

    #[test]
    fn audience_purpose_and_shape_of_the_signed_data_are_enforced() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let now = Utc::now().timestamp();
        let challenge = Uuid::new_v4().to_string();
        for data in [
            json!({"aud": "https://other.test", "challenge_id": challenge, "purpose": PROOF_PURPOSE}),
            json!({"aud": AUDIENCE, "challenge_id": challenge, "purpose": "paykit/session/v1"}),
            json!({"aud": AUDIENCE, "challenge_id": "not-a-uuid", "purpose": PROOF_PURPOSE}),
            json!({"aud": AUDIENCE, "challenge_id": challenge.to_uppercase(), "purpose": PROOF_PURPOSE}),
            json!({"aud": AUDIENCE, "challenge_id": challenge, "purpose": PROOF_PURPOSE, "extra": 1}),
            json!({"aud": AUDIENCE, "purpose": PROOF_PURPOSE}),
            json!(null),
        ] {
            let pop = fixture.pop(&fixture.client, data.clone(), now);
            assert_eq!(
                verify_grant_session_proof(&fixture.grant(), &pop, &config(), Utc::now()),
                Err(GrantSessionRefusal::ProofInvalid),
                "{data}"
            );
        }
    }

    #[test]
    fn stale_and_future_proofs_are_refused() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let now = Utc::now();
        let data = || proof_data(AUDIENCE, Uuid::new_v4());
        let stale = fixture.pop(&fixture.client, data(), now.timestamp() - 121);
        assert_eq!(
            verify_grant_session_proof(&fixture.grant(), &stale, &config(), now),
            Err(GrantSessionRefusal::ProofInvalid)
        );
        let fresh_enough = fixture.pop(&fixture.client, data(), now.timestamp() - 119);
        assert!(
            verify_grant_session_proof(&fixture.grant(), &fresh_enough, &config(), now).is_ok()
        );
        // The service clock bounds age: a proof valid by system time is stale
        // once the service clock has moved past the window.
        let current = fixture.pop(&fixture.client, data(), now.timestamp());
        assert_eq!(
            verify_grant_session_proof(
                &fixture.grant(),
                &current,
                &config(),
                now + Duration::seconds(121)
            ),
            Err(GrantSessionRefusal::ProofInvalid)
        );
        assert_eq!(
            verify_grant_session_proof(
                &fixture.grant(),
                &current,
                &config(),
                now - Duration::seconds(31)
            ),
            Err(GrantSessionRefusal::ProofInvalid)
        );
    }

    #[test]
    fn expired_grants_unlisted_clients_and_narrow_grants_are_refused() {
        let expired = Fixture::new(SHOP_CAPS, CLIENT, -1);
        assert_eq!(
            verify_grant_session_proof(
                &expired.grant(),
                &expired.valid_pop(Uuid::new_v4()),
                &config(),
                Utc::now()
            ),
            Err(GrantSessionRefusal::GrantExpired)
        );
        let other_app = Fixture::new(SHOP_CAPS, "franky.pubky.app", 3600);
        assert_eq!(
            verify_grant_session_proof(
                &other_app.grant(),
                &other_app.valid_pop(Uuid::new_v4()),
                &config(),
                Utc::now()
            ),
            Err(GrantSessionRefusal::ClientNotAllowed)
        );
        let narrow = Fixture::new("/pub/paykit/:rw,/priv/pubky.app/:rw", CLIENT, 3600);
        assert_eq!(
            verify_grant_session_proof(
                &narrow.grant(),
                &narrow.valid_pop(Uuid::new_v4()),
                &config(),
                Utc::now()
            ),
            Err(GrantSessionRefusal::CapabilityRequired)
        );
    }

    #[test]
    fn a_challenge_request_signs_only_audience_and_its_own_purpose() {
        let fixture = Fixture::new(SHOP_CAPS, CLIENT, 3600);
        let now = Utc::now();
        let request = fixture.pop(
            &fixture.client,
            challenge_request_data(AUDIENCE),
            now.timestamp(),
        );
        let verified = verify_challenge_request(&fixture.grant(), &request, &config(), now)
            .expect("valid challenge request");
        assert_eq!(verified.pubky, fixture.root.public_key().z32());
        assert_eq!(
            verified.capabilities,
            "/pub/pubky.app/marketplace-service/v1/:rw,/priv/pubky.app/:rw"
        );
        for data in [
            proof_data(AUDIENCE, Uuid::new_v4()),
            json!({"aud": "https://other.test", "purpose": CHALLENGE_PURPOSE}),
            json!({"aud": AUDIENCE, "purpose": CHALLENGE_PURPOSE, "challenge_id": "x"}),
        ] {
            let pop = fixture.pop(&fixture.client, data.clone(), now.timestamp());
            assert_eq!(
                verify_challenge_request(&fixture.grant(), &pop, &config(), now),
                Err(GrantSessionRefusal::ProofInvalid),
                "{data}"
            );
        }
        // A challenge request is not a mint proof either.
        assert_eq!(
            verify_grant_session_proof(&fixture.grant(), &request, &config(), now),
            Err(GrantSessionRefusal::ProofInvalid)
        );
        let stale = fixture.pop(
            &fixture.client,
            challenge_request_data(AUDIENCE),
            now.timestamp() - 121,
        );
        assert_eq!(
            verify_challenge_request(&fixture.grant(), &stale, &config(), now),
            Err(GrantSessionRefusal::ProofInvalid)
        );
    }

    #[test]
    fn config_requires_an_audience_and_client_ids_and_bounds_the_age() {
        assert_eq!(
            GrantSessionConfig::new("https://svc.test/", CLIENT, 120)
                .expect("config")
                .audience,
            "https://svc.test"
        );
        assert_eq!(
            GrantSessionConfig::new("https://svc.test:8443/base", "a.test, b.test", 60)
                .expect("config")
                .client_ids,
            vec!["a.test".to_string(), "b.test".to_string()]
        );
        assert!(GrantSessionConfig::new("https://svc.test", " , ", 120).is_err());
        assert!(GrantSessionConfig::new("https://svc.test", CLIENT, 0).is_err());
        assert!(GrantSessionConfig::new("https://svc.test", CLIENT, 601).is_err());
        assert!(GrantSessionConfig::new("not a url", CLIENT, 120).is_err());
    }
}
