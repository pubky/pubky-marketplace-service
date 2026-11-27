//! First-boot single-seller claim: setup-code normalization and digests,
//! per-address buckets, and the public state and authenticated claim routes.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
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
use crate::clock::{format_timestamp, retry_after_seconds};
use crate::listing_availability::ENV_CLIENT_IP_HEADER_OVERRIDE;
use crate::result::CommandFailure;
use crate::store_seller::{
    failure_response, other_seller_rows_on, read_binding_on, take_binding_lock, Active, StoreMode,
    STORE_ALREADY_CLAIMED_MESSAGE,
};
use crate::AppState;
use marketplace_domain::ErrorCode;

pub const ENV_CLAIM_TOKEN: &str = "STORE_CLAIM_TOKEN";
pub const ENV_CLAIM_SESSION_MAX_AGE_SECONDS: &str = "STORE_CLAIM_SESSION_MAX_AGE_SECONDS";
pub const DEFAULT_CLAIM_SESSION_MAX_AGE_SECONDS: i64 = 900;
pub const MAX_CLAIM_SESSION_MAX_AGE_SECONDS: i64 = 3600;
pub const CLAIM_BODY_LIMIT: usize = 1024;
pub const CLAIM_BODY_TIMEOUT: Duration = Duration::from_secs(10);
pub const CLAIM_ROUTE_TIMEOUT: Duration = Duration::from_secs(30);
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

/// Reads the setup code through the same lookup used by `Config::from_env`.
/// Keeping this boundary separate lets boot-shape tests exercise the actual
/// environment parsing without mutating process-global variables.
pub fn claim_code_from_lookup(
    mode: &StoreMode,
    get: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<Option<ClaimCode>> {
    match get(ENV_CLAIM_TOKEN) {
        Some(raw) if mode.is_claim_mode() && !raw.is_empty() => ClaimCode::parse(&raw).map(Some),
        Some(_) if !mode.is_claim_mode() => {
            tracing::warn!(
                "STORE_CLAIM_TOKEN is read only in single-seller claim mode; ignoring it"
            );
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Default)]
struct ClientTable {
    clients: HashMap<[u8; 16], Bucket>,
    in_flight: HashMap<[u8; 16], usize>,
}

#[derive(Debug)]
struct ClientLimiter {
    header: Option<HeaderName>,
    salt: [u8; 16],
    per_second: f64,
    capacity: f64,
    client_capacity: usize,
    table: Mutex<ClientTable>,
}

#[derive(Debug)]
struct ClaimFailureGuard {
    limiter: Arc<ClientLimiter>,
    key: [u8; 16],
    active: bool,
}

impl ClaimFailureGuard {
    fn record_failure(mut self, now: DateTime<Utc>) {
        self.active = false;
        self.limiter.finish_failure(self.key, now);
    }
}

impl Drop for ClaimFailureGuard {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.limiter.release_in_flight(self.key);
        }
    }
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
            client_capacity: CLIENT_TABLE_CAPACITY,
            table: Mutex::new(ClientTable::default()),
        }
    }

    #[cfg(test)]
    fn with_client_capacity(mut self, client_capacity: usize) -> Self {
        self.client_capacity = client_capacity;
        self
    }

    fn key(&self, headers: &HeaderMap, peer: Option<IpAddr>) -> [u8; 16] {
        let address = resolve_client_address(self.header.as_ref(), headers, peer);
        let digest = Sha256::new()
            .chain_update(self.salt)
            .chain_update(address.as_bytes())
            .finalize();
        digest[..16].try_into().expect("a 16-byte client key")
    }

    fn bucket(
        clients: &mut HashMap<[u8; 16], Bucket>,
        key: [u8; 16],
        now: DateTime<Utc>,
        capacity: f64,
    ) -> &mut Bucket {
        clients
            .entry(key)
            .or_insert_with(|| Bucket::full(capacity, now))
    }

    fn decrement_in_flight(in_flight: &mut HashMap<[u8; 16], usize>, key: &[u8; 16]) {
        if let Some(count) = in_flight.get_mut(key) {
            if *count <= 1 {
                in_flight.remove(key);
            } else {
                *count -= 1;
            }
        }
    }

    fn evict_if_at_capacity(&self, table: &mut ClientTable, now: DateTime<Utc>) {
        if table.clients.len() < self.client_capacity {
            return;
        }
        let per_second = self.per_second;
        let capacity = self.capacity;
        table.clients.retain(|_, bucket| {
            bucket.refill(now, per_second, capacity);
            bucket.tokens() < capacity
        });
        // Never globally lock out newcomers: if every tracked bucket is still
        // active after pruning idle entries, evict the most-nearly-recovered
        // bucket (counting active in-flight reservations) so active repeat
        // offenders remain tracked while memory stays bounded at `client_capacity`.
        while table.clients.len() >= self.client_capacity {
            let Some(evict) = table
                .clients
                .iter()
                .max_by(|(key_a, bucket_a), (key_b, bucket_b)| {
                    let eff_a = bucket_a.tokens()
                        - table.in_flight.get(*key_a).copied().unwrap_or(0) as f64;
                    let eff_b = bucket_b.tokens()
                        - table.in_flight.get(*key_b).copied().unwrap_or(0) as f64;
                    eff_a.total_cmp(&eff_b).then_with(|| key_a.cmp(key_b))
                })
                .map(|(key, _)| *key)
            else {
                break;
            };
            table.clients.remove(&evict);
        }
    }

    fn admit(
        &self,
        headers: &HeaderMap,
        peer: Option<IpAddr>,
        now: DateTime<Utc>,
    ) -> Result<(), u64> {
        let key = self.key(headers, peer);
        let mut table = self.table.lock().expect("client limiter lock");
        if !table.clients.contains_key(&key) {
            self.evict_if_at_capacity(&mut table, now);
            if table.clients.len() >= self.client_capacity {
                return Err(60);
            }
        }
        Self::bucket(&mut table.clients, key, now, self.capacity).take(
            now,
            self.per_second,
            self.capacity,
        )
    }

    fn reserve_failure_slot(
        self: &Arc<Self>,
        key: [u8; 16],
        now: DateTime<Utc>,
    ) -> Result<ClaimFailureGuard, u64> {
        let mut table = self.table.lock().expect("claim limiter lock");
        let (available, effective) = match table.clients.get_mut(&key) {
            Some(bucket) => {
                bucket.refill(now, self.per_second, self.capacity);
                let tokens = bucket.tokens();
                if tokens >= self.capacity {
                    table.clients.remove(&key);
                    (self.capacity, now)
                } else {
                    (tokens, bucket.updated())
                }
            }
            None => (self.capacity, now),
        };
        let active = table.in_flight.get(&key).copied().unwrap_or(0);
        let remaining = available - active as f64;
        if remaining >= 1.0 {
            *table.in_flight.entry(key).or_insert(0) += 1;
            Ok(ClaimFailureGuard {
                limiter: Arc::clone(self),
                key,
                active: true,
            })
        } else {
            Err(retry_after_seconds(
                remaining.max(0.0),
                self.per_second * 60.0,
                self.capacity,
                effective,
                now,
            )
            .max(1) as u64)
        }
    }

    fn release_in_flight(&self, key: [u8; 16]) {
        let mut table = self.table.lock().expect("claim limiter lock");
        Self::decrement_in_flight(&mut table.in_flight, &key);
    }

    fn finish_failure(&self, key: [u8; 16], now: DateTime<Utc>) {
        let mut table = self.table.lock().expect("claim limiter lock");
        Self::decrement_in_flight(&mut table.in_flight, &key);
        if !table.clients.contains_key(&key) {
            self.evict_if_at_capacity(&mut table, now);
            if table.clients.len() >= self.client_capacity {
                return;
            }
        }
        let _ = Self::bucket(&mut table.clients, key, now, self.capacity).take(
            now,
            self.per_second,
            self.capacity,
        );
    }
}

#[derive(Debug)]
pub struct StoreLimiters {
    read: ClientLimiter,
    claim_failures: Arc<ClientLimiter>,
    claim_body_timeout: Duration,
    claim_route_timeout: Duration,
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
            claim_failures: Arc::new(ClientLimiter::new(header, 10.0 / 900.0, 10.0)),
            claim_body_timeout: CLAIM_BODY_TIMEOUT,
            claim_route_timeout: CLAIM_ROUTE_TIMEOUT,
        }
    }

    #[cfg(any(test, feature = "test-faults"))]
    pub fn with_claim_timeouts(mut self, body: Duration, route: Duration) -> Self {
        self.claim_body_timeout = body;
        self.claim_route_timeout = route;
        self
    }

    #[cfg(any(test, feature = "test-faults"))]
    pub fn claim_in_flight_total(&self) -> usize {
        self.claim_failures
            .table
            .lock()
            .expect("claim limiter lock")
            .in_flight
            .values()
            .sum()
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

fn timeout_response(message: &'static str) -> Response {
    cached(
        (
            StatusCode::REQUEST_TIMEOUT,
            Json(json!({
                "ok": false,
                "error": { "code": "REQUEST_TIMEOUT", "message": message }
            })),
        )
            .into_response(),
        "no-store",
    )
}

pub async fn bound_claim_route_timeout(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(state.store_limiters.claim_route_timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => timeout_response("The claim request timed out."),
    }
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
    let client = state
        .store_limiters
        .claim_key(request.headers(), peer(&request));
    let failure_guard = match state
        .store_limiters
        .claim_failures
        .reserve_failure_slot(client, state.clock.now())
    {
        Ok(guard) => guard,
        Err(retry_after) => return rate_limited(retry_after),
    };
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
    let age = (state.clock.now() - facts.created_at).num_seconds();
    if age < 0 || age > state.config.store_claim_session_max_age_seconds {
        return session_rejection("claim_session_stale", client);
    }

    let body = match tokio::time::timeout(
        state.store_limiters.claim_body_timeout,
        axum::body::to_bytes(request.into_body(), CLAIM_BODY_LIMIT),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "ok": false, "error": { "code": "INVALID_COMMAND", "message": "The claim body is too large." } })),
            )
                .into_response()
        }
        Err(_) => return timeout_response("Timed out reading the claim request body."),
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
        // A parallel winner may have committed and dropped the digest after
        // this request's first state check. It wins the race response too:
        // claimed stores never disclose whether an in-flight guess was right.
        match state.store.refresh(&state.pool, true).await {
            Ok(Active::Claimed(seller)) => return already_claimed(&seller, client),
            Ok(_) => {}
            Err(error) => return internal(error),
        }
        failure_guard.record_failure(state.clock.now());
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
        let binding = match read_binding_on(&mut tx).await {
            Ok(Some(binding)) => binding.seller_pubky,
            Ok(None) => return internal(sqlx::Error::RowNotFound),
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
    fn claim_failure_clients_stay_bounded_across_distinct_addresses() {
        let limiter =
            Arc::new(ClientLimiter::new(None, 10.0 / 900.0, 10.0).with_client_capacity(4));
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("test time")
            .with_timezone(&Utc);
        let key = |last| limiter.key(&HeaderMap::new(), Some(IpAddr::from([127, 0, 0, last])));

        // Budget reservations on unseen addresses are read-only against
        // `clients`, release their in-flight slot on drop, and never trigger a
        // capacity lockout.
        for last in 1..=20 {
            let guard = limiter
                .reserve_failure_slot(key(last), now)
                .expect("unseen address admitted");
            drop(guard);
        }
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 0);
            assert_eq!(table.in_flight.len(), 0);
        }

        // One active brute-force caller exhausts all 10 failure tokens while
        // three other callers fail once each, filling the 4-slot table.
        let attacker = key(1);
        for _ in 0..10 {
            limiter
                .reserve_failure_slot(attacker, now)
                .expect("attacker budget available")
                .record_failure(now);
        }
        assert_eq!(limiter.reserve_failure_slot(attacker, now).unwrap_err(), 90);
        for last in 2..=4 {
            limiter
                .reserve_failure_slot(key(last), now)
                .expect("caller admitted")
                .record_failure(now);
        }
        assert_eq!(limiter.table.lock().expect("table").clients.len(), 4);

        // Newcomers at capacity are never locked out, and non-failure outcomes
        // (dropping the guard) do not grow or mutate `clients`.
        let merchant = key(5);
        drop(
            limiter
                .reserve_failure_slot(merchant, now)
                .expect("merchant admitted"),
        );
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 4);
            assert!(!table.clients.contains_key(&merchant));
            assert!(table.in_flight.is_empty());
        }

        // Additional failing addresses evict highest-token entries, keeping
        // table size bounded and the active brute-force caller rate-limited.
        for last in 6..=20 {
            limiter
                .reserve_failure_slot(key(last), now)
                .expect("newcomer admitted")
                .record_failure(now);
            assert!(limiter.table.lock().expect("table").clients.len() <= 4);
            assert_eq!(limiter.reserve_failure_slot(attacker, now).unwrap_err(), 90);
        }
        drop(
            limiter
                .reserve_failure_slot(merchant, now)
                .expect("merchant still admitted"),
        );

        // Partial recovery: when single-failure buckets refill to capacity
        // while a recent brute-force caller is still empty, recording a new
        // failure prunes the refilled buckets and keeps the brute-force caller.
        let recent_attacker = key(30);
        let t_recent = now + chrono::Duration::seconds(30);
        for _ in 0..10 {
            limiter
                .reserve_failure_slot(recent_attacker, t_recent)
                .expect("recent attacker admitted")
                .record_failure(t_recent);
        }
        let t_partial = now + chrono::Duration::seconds(90);
        assert!(limiter
            .reserve_failure_slot(recent_attacker, t_partial)
            .is_err());
        limiter
            .reserve_failure_slot(key(31), t_partial)
            .expect("newcomer admitted")
            .record_failure(t_partial);
        {
            let table = limiter.table.lock().expect("table");
            assert!(table.clients.contains_key(&recent_attacker));
            assert!(table.clients.contains_key(&key(31)));
            assert_eq!(table.clients.len(), 3);
        }
        assert!(limiter
            .reserve_failure_slot(recent_attacker, t_partial)
            .is_err());

        // Advancing past 900 s lets every tracked bucket refill to capacity:
        // reserve_failure_slot drops refilled hits, and record_failure prunes
        // all remaining stale buckets before inserting.
        let later = t_partial + chrono::Duration::seconds(901);
        let len_before_drop = limiter.table.lock().expect("table").clients.len();
        drop(
            limiter
                .reserve_failure_slot(recent_attacker, later)
                .expect("refilled attacker admitted"),
        );
        {
            let table = limiter.table.lock().expect("table");
            assert!(!table.clients.contains_key(&recent_attacker));
            assert_eq!(table.clients.len(), len_before_drop - 1);
        }

        // Refill all 4 slots at `later`, then advance another 901 s and prove
        // `record_failure` prunes every idle bucket down to only the newcomer.
        for last in 40..=43 {
            limiter
                .reserve_failure_slot(key(last), later)
                .expect("slot filled")
                .record_failure(later);
        }
        assert_eq!(limiter.table.lock().expect("table").clients.len(), 4);
        let much_later = later + chrono::Duration::seconds(901);
        let fresh = key(44);
        limiter
            .reserve_failure_slot(fresh, much_later)
            .expect("fresh caller admitted")
            .record_failure(much_later);
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 1);
            assert!(table.clients.contains_key(&fresh));
            assert!(table.in_flight.is_empty());
        }
    }

    #[test]
    fn stale_timestamps_never_rewind_buckets_or_unrelated_entries_at_capacity() {
        let limiter =
            Arc::new(ClientLimiter::new(None, 10.0 / 900.0, 10.0).with_client_capacity(4));
        let t_old = DateTime::parse_from_rfc3339("2026-01-01T09:58:20Z")
            .expect("test time")
            .with_timezone(&Utc);
        let t_now = DateTime::parse_from_rfc3339("2026-01-01T10:00:00Z")
            .expect("test time")
            .with_timezone(&Utc);
        let key = |last| limiter.key(&HeaderMap::new(), Some(IpAddr::from([192, 0, 2, last])));

        // Caller A exhausts all 10 failures at 10:00:00; three other callers
        // fail once each at 10:00:00 to fill the 4-slot table.
        let attacker = key(1);
        for _ in 0..10 {
            limiter
                .reserve_failure_slot(attacker, t_now)
                .expect("attacker admitted")
                .record_failure(t_now);
        }
        for last in 2..=4 {
            limiter
                .reserve_failure_slot(key(last), t_now)
                .expect("caller admitted")
                .record_failure(t_now);
        }
        assert_eq!(limiter.table.lock().expect("table").clients.len(), 4);

        // An out-of-order failure against the same attacker with an older
        // timestamp must neither rewind `updated` nor credit refill time.
        limiter.finish_failure(attacker, t_old);
        {
            let table = limiter.table.lock().expect("table");
            let bucket = table.clients.get(&attacker).expect("attacker retained");
            assert_eq!(bucket.tokens(), 0.0);
            assert_eq!(bucket.updated(), t_now);
        }
        assert_eq!(
            limiter.reserve_failure_slot(attacker, t_old).unwrap_err(),
            190
        );
        assert_eq!(
            limiter.reserve_failure_slot(attacker, t_now).unwrap_err(),
            90
        );

        // A slow newcomer finishing at capacity with `t_old` (100 s earlier)
        // triggers `evict_if_at_capacity` across every retained bucket: it must
        // not rewind attacker A's timestamp or allow immediate refill at `t_now`.
        let slow_newcomer = key(5);
        let guard = limiter
            .reserve_failure_slot(slow_newcomer, t_old)
            .expect("newcomer admitted");
        guard.record_failure(t_old);
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 4);
            let bucket = table
                .clients
                .get(&attacker)
                .expect("attacker still retained");
            assert_eq!(bucket.tokens(), 0.0);
            assert_eq!(bucket.updated(), t_now);
        }
        assert_eq!(
            limiter.reserve_failure_slot(attacker, t_now).unwrap_err(),
            90
        );
    }

    #[test]
    fn concurrent_in_flight_reservations_enforce_failure_budget_and_release_on_drop() {
        let limiter =
            Arc::new(ClientLimiter::new(None, 10.0 / 900.0, 10.0).with_client_capacity(4));
        let now = DateTime::parse_from_rfc3339("2026-01-01T10:00:00Z")
            .expect("test time")
            .with_timezone(&Utc);
        let key = |last| limiter.key(&HeaderMap::new(), Some(IpAddr::from([203, 0, 113, last])));
        let attacker = key(1);
        let other = key(2);

        // 10 concurrent in-flight reservations saturate the 10-failure budget
        // without allocating into `clients`.
        let mut guards = Vec::new();
        for _ in 0..10 {
            guards.push(
                limiter
                    .reserve_failure_slot(attacker, now)
                    .expect("within 10 in-flight slots"),
            );
        }
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 0);
            assert_eq!(table.in_flight.get(&attacker).copied(), Some(10));
        }

        // Attempts 11..=15 from the same address are rejected immediately with
        // Retry-After = 90 while another address stays unaffected.
        for _ in 0..5 {
            assert_eq!(limiter.reserve_failure_slot(attacker, now).unwrap_err(), 90);
        }
        drop(
            limiter
                .reserve_failure_slot(other, now)
                .expect("other address unaffected"),
        );

        // Dropping 4 guards (e.g., 200 / 409 / 403 session error / 408 / cancel)
        // releases those 4 slots cleanly without mutating `clients`.
        guards.truncate(6);
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 0);
            assert_eq!(table.in_flight.get(&attacker).copied(), Some(6));
        }
        for _ in 0..4 {
            guards.push(
                limiter
                    .reserve_failure_slot(attacker, now)
                    .expect("released slot reusable"),
            );
        }
        assert_eq!(limiter.reserve_failure_slot(attacker, now).unwrap_err(), 90);

        // Consuming all 10 guards via `record_failure` moves the reservation
        // into `clients` atomically with zero gap and clears `in_flight`.
        for guard in guards {
            guard.record_failure(now);
            assert_eq!(limiter.reserve_failure_slot(attacker, now).unwrap_err(), 90);
        }
        {
            let table = limiter.table.lock().expect("table");
            assert!(table.in_flight.is_empty());
            assert_eq!(table.clients.get(&attacker).expect("tracked").tokens(), 0.0);
        }

        // Mixed balance: 6 recorded failures leave 4 tokens, so only 4 concurrent
        // reservations succeed and dropping them restores all 4 slots.
        let mixed = key(3);
        for _ in 0..6 {
            limiter
                .reserve_failure_slot(mixed, now)
                .expect("mixed admitted")
                .record_failure(now);
        }
        let mut mixed_guards = Vec::new();
        for _ in 0..4 {
            mixed_guards.push(
                limiter
                    .reserve_failure_slot(mixed, now)
                    .expect("remaining 4 slots"),
            );
        }
        assert_eq!(limiter.reserve_failure_slot(mixed, now).unwrap_err(), 90);
        drop(mixed_guards);
        {
            let table = limiter.table.lock().expect("table");
            assert!(table.in_flight.is_empty());
            assert_eq!(table.clients.get(&mixed).expect("mixed").tokens(), 4.0);
        }

        // In-flight reservations also count toward effective tokens during
        // at-capacity eviction: a bucket with 5 tokens and 5 in-flight
        // (effective 0.0) is retained ahead of a bucket with 4 tokens and 0
        // in-flight.
        let active_busy = key(10);
        for _ in 0..5 {
            limiter
                .reserve_failure_slot(active_busy, now)
                .expect("busy admitted")
                .record_failure(now);
        }
        let busy_guards: Vec<_> = (0..5)
            .map(|_| {
                limiter
                    .reserve_failure_slot(active_busy, now)
                    .expect("busy in-flight")
            })
            .collect();
        let low_idle = key(11);
        for _ in 0..6 {
            limiter
                .reserve_failure_slot(low_idle, now)
                .expect("low_idle admitted")
                .record_failure(now);
        }
        // Table currently holds 4 entries (attacker: 0, mixed: 4, active_busy:
        // 5-5=0, low_idle: 4). A newcomer must evict one of the 4-token idle
        // buckets rather than `attacker` or `active_busy`.
        limiter
            .reserve_failure_slot(key(12), now)
            .expect("newcomer admitted")
            .record_failure(now);
        {
            let table = limiter.table.lock().expect("table");
            assert_eq!(table.clients.len(), 4);
            assert!(table.clients.contains_key(&attacker));
            assert!(table.clients.contains_key(&active_busy));
        }
        drop(busy_guards);
    }

    #[test]
    fn store_read_clients_prune_idle_buckets_and_never_lock_out_newcomers() {
        let limiter = ClientLimiter::new(None, 1.0, 30.0).with_client_capacity(4);
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("test time")
            .with_timezone(&Utc);
        let headers = HeaderMap::new();
        let peer = |last| Some(IpAddr::from([198, 51, 100, last]));

        // One hammering visitor exhausts the 30-request burst.
        for _ in 0..30 {
            assert_eq!(limiter.admit(&headers, peer(1), now), Ok(()));
        }
        assert_eq!(limiter.admit(&headers, peer(1), now), Err(1));

        // Three more distinct visitors fill the 4-slot table.
        for last in 2..=4 {
            assert_eq!(limiter.admit(&headers, peer(last), now), Ok(()));
        }
        assert_eq!(limiter.table.lock().expect("table").clients.len(), 4);

        // Burst of distinct visitors at the same timestamp evicts the
        // highest-token buckets so newcomers are admitted while the hammering
        // visitor remains rate-limited.
        for last in 5..=25 {
            assert_eq!(limiter.admit(&headers, peer(last), now), Ok(()));
            assert!(limiter.table.lock().expect("table").clients.len() <= 4);
            assert_eq!(limiter.admit(&headers, peer(1), now), Err(1));
        }

        // Advancing past the full refill window prunes all idle buckets on the
        // next visitor request, so lifetime distinct visitors never permanently
        // 429 GET /v1/store.
        let later = now + chrono::Duration::seconds(31);
        assert_eq!(limiter.admit(&headers, peer(50), later), Ok(()));
        assert_eq!(limiter.table.lock().expect("table").clients.len(), 1);
        assert_eq!(limiter.admit(&headers, peer(1), later), Ok(()));

        // Sequential visitors spread over time beyond 900 s stay bounded and
        // every visitor is admitted.
        let far_later = later + chrono::Duration::seconds(901);
        for offset in 0..20u8 {
            let at = far_later + chrono::Duration::seconds(i64::from(offset) * 10);
            assert_eq!(limiter.admit(&headers, peer(100 + offset), at), Ok(()));
            assert!(limiter.table.lock().expect("table").clients.len() <= 4);
        }
    }

    #[test]
    fn ephemeral_codes_have_160_bits_and_pass_their_own_gate() {
        let (code, display) = ClaimCode::ephemeral();
        assert_eq!(display.len(), 39);
        assert!(code.matches(&display));
    }
}
