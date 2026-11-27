//! First-boot single-seller claim: setup-code normalization and digests,
//! per-address buckets, and the public state and authenticated claim routes.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::address_search::{
    resolve_client_address, Bucket, CLIENT_TABLE_CAPACITY, ENV_CLIENT_IP_HEADER,
};
use crate::auth::{capability_covers_inventory_service, AuthSession};
use crate::clock::format_timestamp;
use crate::listing_availability::ENV_CLIENT_IP_HEADER_OVERRIDE;
use crate::result::CommandFailure;
use crate::store_seller::{
    failure_response, other_seller_rows_on, take_binding_lock, Active, StoreMode,
    STORE_ALREADY_CLAIMED_MESSAGE,
};
use crate::AppState;
use marketplace_domain::ErrorCode;

pub const ENV_CLAIM_TOKEN: &str = "STORE_CLAIM_TOKEN";
pub const ENV_CLAIM_SESSION_MAX_AGE_SECONDS: &str = "STORE_CLAIM_SESSION_MAX_AGE_SECONDS";
pub const DEFAULT_CLAIM_SESSION_MAX_AGE_SECONDS: i64 = 900;
pub const MAX_CLAIM_SESSION_MAX_AGE_SECONDS: i64 = 3600;
pub const CLAIM_BODY_LIMIT: usize = 1024;
pub const CLAIM_CODE_SHAPE_MESSAGE: &str = "STORE_CLAIM_TOKEN must be 32 Crockford Base32 characters (hyphens and spaces allowed) or 64 hex characters";
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Only the normalized code's SHA-256 digest survives configuration parsing.
#[derive(Clone)]
pub struct ClaimCode([u8; 32]);

impl std::fmt::Debug for ClaimCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ClaimCode(<digest>)")
    }
}

impl ClaimCode {
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        let normalized = normalize(raw);
        let crockford = normalized.len() == 32
            && normalized
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
        let hex = normalized.len() == 64 && normalized.bytes().all(|byte| byte.is_ascii_hexdigit());
        let distinct = normalized.bytes().collect::<HashSet<_>>().len();
        if !(crockford || hex) || distinct < 10 {
            anyhow::bail!(CLAIM_CODE_SHAPE_MESSAGE);
        }
        Ok(Self(Sha256::digest(normalized.as_bytes()).into()))
    }

    pub fn matches(&self, supplied: &str) -> bool {
        let normalized = normalize(supplied);
        let digest = Sha256::digest(normalized.as_bytes());
        self.0.ct_eq(digest.as_slice()).into()
    }

    /// 160 bits from the operating system CSPRNG. The returned display string
    /// is consumed by the single boot INFO line and is not retained.
    pub fn ephemeral() -> (Self, String) {
        let mut entropy = [0u8; 20];
        rand::rngs::OsRng.fill_bytes(&mut entropy);
        let mut normalized = String::with_capacity(32);
        for index in 0..32 {
            let bit = index * 5;
            let byte = bit / 8;
            let offset = bit % 8;
            let window = (u16::from(entropy[byte]) << 8)
                | u16::from(entropy.get(byte + 1).copied().unwrap_or(0));
            let shift = 11usize.saturating_sub(offset);
            let value = ((window >> shift) & 31) as usize;
            normalized.push(CROCKFORD[value] as char);
        }
        let display = normalized
            .as_bytes()
            .chunks(4)
            .map(|group| std::str::from_utf8(group).expect("ASCII code"))
            .collect::<Vec<_>>()
            .join("-");
        (
            Self::parse(&normalized).expect("generated code has the accepted shape"),
            display,
        )
    }
}

pub fn normalize(raw: &str) -> String {
    raw.chars()
        .filter(|character| !character.is_whitespace() && *character != '-')
        .map(|character| match character.to_ascii_uppercase() {
            'O' => '0',
            'I' | 'L' => '1',
            other => other,
        })
        .collect()
}

#[derive(Debug)]
struct ClientLimiter {
    header: Option<HeaderName>,
    salt: [u8; 16],
    per_second: f64,
    capacity: f64,
    clients: Mutex<HashMap<[u8; 16], Bucket>>,
}

impl ClientLimiter {
    fn new(header: Option<HeaderName>, per_second: f64, capacity: f64) -> Self {
        let mut salt = [0u8; 16];
        rand::rngs::OsRng.fill_bytes(&mut salt);
        Self {
            header,
            salt,
            per_second,
            capacity,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn key(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> [u8; 16] {
        let address = resolve_client_address(self.header.as_ref(), headers, peer);
        let digest = Sha256::new()
            .chain_update(self.salt)
            .chain_update(address.as_bytes())
            .finalize();
        digest[..16].try_into().expect("a 16-byte client key")
    }

    fn bucket<'a>(
        clients: &'a mut HashMap<[u8; 16], Bucket>,
        key: [u8; 16],
        now: DateTime<Utc>,
        capacity: f64,
    ) -> &'a mut Bucket {
        clients
            .entry(key)
            .or_insert_with(|| Bucket::full(capacity, now))
    }

    fn admit(
        &self,
        headers: &HeaderMap,
        peer: Option<IpAddr>,
        now: DateTime<Utc>,
    ) -> Result<(), u64> {
        let key = self.key(headers, peer);
        let mut clients = self.clients.lock().expect("client limiter lock");
        if clients.len() >= CLIENT_TABLE_CAPACITY && !clients.contains_key(&key) {
            return Err(60);
        }
        Self::bucket(&mut clients, key, now, self.capacity).take(
            now,
            self.per_second,
            self.capacity,
        )
    }

    fn check_failure_budget(&self, key: [u8; 16], now: DateTime<Utc>) -> Result<(), u64> {
        let mut clients = self.clients.lock().expect("claim limiter lock");
        let bucket = Self::bucket(&mut clients, key, now, self.capacity);
        bucket.refill(now, self.per_second, self.capacity);
        if bucket.tokens() >= 1.0 {
            Ok(())
        } else {
            Err((((1.0 - bucket.tokens()) / self.per_second).ceil() as u64).max(1))
        }
    }

    fn record_failure(&self, key: [u8; 16], now: DateTime<Utc>) {
        let mut clients = self.clients.lock().expect("claim limiter lock");
        let _ = Self::bucket(&mut clients, key, now, self.capacity).take(
            now,
            self.per_second,
            self.capacity,
        );
    }
}

#[derive(Debug)]
pub struct StoreLimiters {
    read: ClientLimiter,
    claim_failures: ClientLimiter,
}

impl StoreLimiters {
    pub fn from_env() -> anyhow::Result<Self> {
        let selected = [ENV_CLIENT_IP_HEADER_OVERRIDE, ENV_CLIENT_IP_HEADER]
            .into_iter()
            .find_map(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| (name, value))
            });
        let header = selected
            .map(|(name, value)| {
                HeaderName::from_bytes(value.trim().as_bytes())
                    .map_err(|_| anyhow::anyhow!("{name} is not a header name"))
            })
            .transpose()?;
        Ok(Self::new(header))
    }

    pub fn new(header: Option<HeaderName>) -> Self {
        Self {
            read: ClientLimiter::new(header.clone(), 1.0, 30.0),
            claim_failures: ClientLimiter::new(header, 10.0 / 900.0, 10.0),
        }
    }

    fn claim_key(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> [u8; 16] {
        self.claim_failures.key(headers, peer)
    }
}

impl Default for StoreLimiters {
    fn default() -> Self {
        Self::new(None)
    }
}

fn peer(request: &Request) -> Option<IpAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(address)| address.ip())
}

fn rate_limited(retry_after: u64) -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "ok": false,
            "error": { "code": "rate_limited", "message": "The request rate limit was exceeded." }
        })),
    )
        .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        HeaderValue::from_str(&retry_after.to_string()).expect("safe retry-after"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn cached(mut response: Response, value: &'static str) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(value));
    response
}

fn store_body(active: &Active) -> Value {
    match active {
        Active::Off => json!({ "mode": "marketplace", "seller": null }),
        Active::Unclaimed => {
            json!({ "mode": "single_seller", "state": "unclaimed", "seller": null })
        }
        Active::Claimed(seller) => {
            json!({ "mode": "single_seller", "state": "claimed", "seller": seller })
        }
    }
}

pub async fn get_store(State(state): State<AppState>, request: Request) -> Response {
    if matches!(state.config.store_mode, StoreMode::Marketplace) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let active = match state.store.refresh(&state.pool, false).await {
        Ok(active) => active,
        Err(error) => return internal(error),
    };
    if let Err(retry_after) =
        state
            .store_limiters
            .read
            .admit(request.headers(), peer(&request), state.clock.now())
    {
        return rate_limited(retry_after);
    }
    let cache = if matches!(active, Active::Unclaimed) {
        "no-store"
    } else {
        "public, max-age=30"
    };
    cached(Json(store_body(&active)).into_response(), cache)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaimRequest {
    claim_code: String,
}

#[derive(Debug, sqlx::FromRow)]
struct ClaimSessionFacts {
    session_id: Uuid,
    created_at: DateTime<Utc>,
    source_grant_id: Option<String>,
    source_client_id: Option<String>,
}

fn internal(error: sqlx::Error) -> Response {
    tracing::error!(error = %error, "store claim request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "ok": false, "error": { "code": "INTERNAL", "message": "The request could not be processed." } })),
    )
        .into_response()
}

fn already_claimed(seller: &str, client: [u8; 16]) -> Response {
    reject_log("already_claimed", client);
    (
        StatusCode::CONFLICT,
        Json(json!({
            "ok": false,
            "error": { "code": ErrorCode::StoreAlreadyClaimed, "message": STORE_ALREADY_CLAIMED_MESSAGE },
            "seller": seller,
            "store": store_body(&Active::Claimed(seller.to_string())),
        })),
    )
        .into_response()
}

fn reject_log(reason: &'static str, client: [u8; 16]) {
    tracing::info!(reason, client = hex::encode(client), "store.claim.rejected");
}

fn session_rejection(reason: &'static str, client: [u8; 16]) -> Response {
    reject_log(reason, client);
    failure_response(
        &CommandFailure::unaudited(
            ErrorCode::Unauthorized,
            "The claim session is not authorized.",
        )
        .with_reason(reason),
    )
}

pub async fn claim_store(State(state): State<AppState>, request: Request) -> Response {
    // require_session has already run. The actor and token hash are verified
    // session facts, never client-supplied identity fields.
    let session = request
        .extensions()
        .get::<AuthSession>()
        .cloned()
        .expect("claim route is layered with require_session");
    if matches!(state.config.store_mode, StoreMode::Marketplace) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if matches!(
        state.config.store_mode,
        StoreMode::SingleSeller { pinned: Some(_) }
    ) {
        return already_claimed(
            state.config.store_mode.pinned().expect("pinned seller"),
            state
                .store_limiters
                .claim_key(request.headers(), peer(&request)),
        );
    }
    let now = state.clock.now();
    let client = state
        .store_limiters
        .claim_key(request.headers(), peer(&request));
    if let Err(retry_after) = state
        .store_limiters
        .claim_failures
        .check_failure_budget(client, now)
    {
        return rate_limited(retry_after);
    }
    let active = match state.store.refresh(&state.pool, true).await {
        Ok(active) => active,
        Err(error) => return internal(error),
    };
    if let Active::Claimed(seller) = active {
        return already_claimed(&seller, client);
    }

    let facts = match sqlx::query_as::<_, ClaimSessionFacts>(
        "SELECT session_id, created_at, source_grant_id, source_client_id \
         FROM auth_sessions WHERE token_hash = $1",
    )
    .bind(&session.token_hash)
    .fetch_optional(&state.pool)
    .await
    {
        Ok(Some(facts)) => facts,
        Ok(None) => return session_rejection("claim_session_source", client),
        Err(error) => return internal(error),
    };
    if facts.source_grant_id.is_none() || facts.source_client_id.is_none() {
        return session_rejection("claim_session_source", client);
    }
    if !capability_covers_inventory_service(&session.capabilities) {
        return session_rejection("claim_session_capability", client);
    }
    let age = (now - facts.created_at).num_seconds();
    if age < 0 || age > state.config.store_claim_session_max_age_seconds {
        return session_rejection("claim_session_stale", client);
    }

    let body = match axum::body::to_bytes(request.into_body(), CLAIM_BODY_LIMIT).await {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "ok": false, "error": { "code": "INVALID_COMMAND", "message": "The claim body is too large." } })),
            )
                .into_response()
        }
    };
    let claim: ClaimRequest = match serde_json::from_slice(&body) {
        Ok(claim) => claim,
        Err(_) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "ok": false, "error": { "code": "INVALID_COMMAND", "message": "The claim request is invalid." } })),
            )
                .into_response()
        }
    };
    if !state.store.claim_code_matches(&claim.claim_code) {
        state
            .store_limiters
            .claim_failures
            .record_failure(client, now);
        return session_rejection("claim_code_invalid", client);
    }

    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(error) => return internal(error),
    };
    if let Err(error) = take_binding_lock(&mut tx).await {
        return internal(error);
    }
    match other_seller_rows_on(&mut tx, "").await {
        Ok(rows) if rows.is_empty() => {}
        Ok(_) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "ok": false, "error": { "code": ErrorCode::StoreAlreadyClaimed, "message": "The database is not empty; the store cannot be claimed." } })),
            )
                .into_response()
        }
        Err(error) => return internal(error),
    }
    let inserted: Option<(String, DateTime<Utc>)> = match sqlx::query_as(
        "INSERT INTO store_seller_binding \
         (singleton, seller_pubky, bound_by, claimed_at, claim_session_id, service_commit) \
         VALUES (TRUE, $1, 'claim', now(), $2, $3) \
         ON CONFLICT (singleton) DO NOTHING RETURNING seller_pubky, claimed_at",
    )
    .bind(&session.actor.0)
    .bind(facts.session_id)
    .bind(crate::http::build_commit())
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(inserted) => inserted,
        Err(error) => return internal(error),
    };
    let Some((seller, claimed_at)) = inserted else {
        let binding = match crate::store_seller::read_binding(&state.pool).await {
            Ok(Some(binding)) => binding.seller_pubky,
            Ok(None) => session.actor.0.clone(),
            Err(error) => return internal(error),
        };
        return already_claimed(&binding, client);
    };
    if let Err(error) = tx.commit().await {
        return internal(error);
    }
    state.store.mark_claimed(seller.clone());
    tracing::info!(
        seller = crate::logging::actor_prefix(&seller),
        by = "claim",
        "store.claimed"
    );
    Json(json!({
        "ok": true,
        "store": {
            "mode": "single_seller",
            "state": "claimed",
            "seller": seller,
            "claimed_at": format_timestamp(claimed_at),
        }
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_shapes_and_normalization_are_exact() {
        let crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let grouped = format!("{}-{}", &crockford[..16], &crockford[16..]);
        assert!(ClaimCode::parse(&grouped).is_ok());
        let hex = "0123456789abcdef".repeat(4);
        assert!(ClaimCode::parse(&hex).is_ok());
        let normalized = ClaimCode::parse(&hex.to_uppercase()).expect("hex");
        assert!(normalized.matches(&hex));
        for bad in [
            "A".repeat(26),
            "a".repeat(40),
            "A".repeat(32),
            format!("{}!", &crockford[..31]),
        ] {
            assert_eq!(
                ClaimCode::parse(&bad).expect_err("weak code").to_string(),
                CLAIM_CODE_SHAPE_MESSAGE
            );
        }
        assert_eq!(normalize("oi-l abc"), "011ABC");
    }

    #[test]
    fn ephemeral_codes_have_160_bits_and_pass_their_own_gate() {
        let (code, display) = ClaimCode::ephemeral();
        assert_eq!(display.len(), 39);
        assert!(code.matches(&display));
    }
}
