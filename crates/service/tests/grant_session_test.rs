//! `POST /v1/auth/grant-sessions`: marketplace sessions minted from the
//! Shop's own sign-in grant, with genuine grant and custom-proof JWS signed by
//! `pubky-common` keys and a stand-in homeserver holding the proof documents.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Duration, Utc};
use marketplace_service::clock::{AdjustableClock, Clock};
use marketplace_service::config::Config;
use marketplace_service::grant_session::{
    challenge_request_data, proof_data, GrantSessionConfig, ProofDocument, MAX_LIVE_CHALLENGES,
};
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use marketplace_service::http::build_router;
use marketplace_service::AppState;
use pubky::custom_pop::CUSTOM_POP_JWS_TYP;
use pubky::{Capabilities, ClientId, GrantClaims, GrantId, Keypair, PopNonce, GRANT_JWS_TYP};
use pubky_common::auth::jws::sign_jws;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::util::ServiceExt;
use uuid::Uuid;

use common::{send, TestApp};

const AUDIENCE: &str = "https://svc.test";
const CLIENT_ID: &str = "shop.pubky.app";
const SHOP_CAPS: &str =
    "/pub/pubky.app/:rw,/pub/paykit/:rw,/priv/pubky.app/:rw,/pub/chat/v0/:rw,/priv/chat/v0/:rw";
const MARKETPLACE_CAPS: &str = "/pub/pubky.app/marketplace-service/v1/:rw,/priv/pubky.app/:rw";

#[derive(Default)]
struct ProofHomeserver {
    documents: Mutex<HashMap<(String, String), Value>>,
    unavailable: AtomicBool,
}

impl ProofHomeserver {
    fn write(&self, pubky: &str, challenge_id: Uuid, document: Value) {
        self.documents
            .lock()
            .expect("documents lock")
            .insert((pubky.to_string(), challenge_id.to_string()), document);
    }
}

impl HomeserverListingClient for ProofHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }

    fn fetch_grant_session_proof<'a>(
        &'a self,
        pubky: &'a str,
        challenge_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            if self.unavailable.load(Ordering::SeqCst) {
                return HomeserverFetchOutcome::Unavailable;
            }
            match self
                .documents
                .lock()
                .expect("documents lock")
                .get(&(pubky.to_string(), challenge_id.to_string()))
            {
                Some(document) => HomeserverFetchOutcome::Found(document.clone()),
                None => HomeserverFetchOutcome::NotFound,
            }
        })
    }
}

/// A Shop sign-in grant: the root key signs it, the browser key proves it.
struct ShopGrant {
    root: Keypair,
    client: Keypair,
    claims: GrantClaims,
}

impl ShopGrant {
    fn new(expires_in: Duration) -> Self {
        let root = Keypair::random();
        let client = Keypair::random();
        let now = Utc::now().timestamp();
        let claims = GrantClaims {
            iss: root.public_key(),
            client_id: ClientId::new(CLIENT_ID).expect("client id"),
            caps: SHOP_CAPS
                .parse::<Capabilities>()
                .expect("caps")
                .iter()
                .cloned()
                .collect(),
            cnf: client.public_key(),
            jti: GrantId::generate(),
            iat: (now - 3600) as u64,
            exp: (now + expires_in.num_seconds()) as u64,
        };
        Self {
            root,
            client,
            claims,
        }
    }

    fn pubky(&self) -> String {
        self.root.public_key().z32()
    }

    fn expires_at(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(self.claims.exp as i64, 0).expect("grant expiry")
    }

    /// Signs `data` as issued at `at`; tests that move the service clock
    /// sign at the service's time so only the behaviour under test differs.
    fn signed_at(&self, data: Value, at: DateTime<Utc>) -> Value {
        let pop = sign_jws(
            &self.client,
            CUSTOM_POP_JWS_TYP,
            &json!({
                "gid": self.claims.jti,
                "iat": at.timestamp(),
                "nonce": PopNonce::generate(),
                "data": data,
            }),
        );
        json!({ "grant": self.claims.sign(&self.root, GRANT_JWS_TYP), "pop": pop })
    }

    fn body(&self, challenge_id: Uuid) -> Value {
        self.signed_at(proof_data(AUDIENCE, challenge_id), Utc::now())
    }

    fn challenge_request(&self) -> Value {
        self.challenge_request_at(Utc::now())
    }

    fn challenge_request_at(&self, at: DateTime<Utc>) -> Value {
        self.signed_at(challenge_request_data(AUDIENCE), at)
    }

    fn document(&self, challenge_id: Uuid) -> Value {
        serde_json::to_value(ProofDocument::expected(
            AUDIENCE,
            challenge_id,
            &self.pubky(),
        ))
        .expect("document serializes")
    }
}

async fn grant_session_app(pool: PgPool, enabled: bool) -> (TestApp, Arc<ProofHomeserver>) {
    grant_session_app_with(pool, enabled, Config::for_tests()).await
}

async fn grant_session_app_with(
    pool: PgPool,
    enabled: bool,
    service_config: Config,
) -> (TestApp, Arc<ProofHomeserver>) {
    let homeserver = Arc::new(ProofHomeserver::default());
    // Grant validity is checked against system time by the SDK.
    let clock = Arc::new(AdjustableClock::new(Utc::now()));
    let config = enabled.then(|| {
        Arc::new(GrantSessionConfig::new(AUDIENCE, CLIENT_ID, 120).expect("grant session config"))
    });
    let state = AppState::new(pool.clone(), clock.clone(), service_config)
        .with_homeserver(Some(homeserver.clone()))
        .with_grant_session(config);
    (
        TestApp {
            router: build_router(state.clone()),
            pool,
            clock,
            state,
        },
        homeserver,
    )
}

async fn post(app: &TestApp, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-sessions",
        None,
        body,
    )
    .await
}

async fn session_count(pool: &PgPool, pubky: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM auth_sessions WHERE pubky = $1")
        .bind(pubky)
        .fetch_one(pool)
        .await
        .expect("session count")
}

/// Requests a challenge signed at the service clock's current time.
async fn request_challenge(app: &TestApp, grant: &ShopGrant) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-session-challenges",
        None,
        &grant.challenge_request_at(app.clock.now()),
    )
    .await
}

/// Issues a challenge and writes its proof document, as the Shop does.
async fn issue(app: &TestApp, homeserver: &ProofHomeserver, grant: &ShopGrant) -> Uuid {
    let (status, body) = request_challenge(app, grant).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let challenge: Uuid = body["challenge_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("challenge id");
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
    challenge
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn both_routes_are_unavailable_while_the_flag_is_off(pool: PgPool) {
    let (app, _) = grant_session_app(pool, false).await;
    let grant = ShopGrant::new(Duration::hours(1));

    let (challenge_status, challenge_body) = request_challenge(&app, &grant).await;
    let (status, body) = post(&app, &grant.body(Uuid::new_v4())).await;

    assert_eq!(challenge_status, StatusCode::NOT_FOUND);
    assert_eq!(
        challenge_body,
        json!({"error": "grant_session_unavailable"})
    );
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error": "grant_session_unavailable"}));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_shop_grant_proof_mints_a_narrowed_session_without_an_approval(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::days(365));
    let challenge = issue(&app, &homeserver, &grant).await;

    let (status, session) = post(&app, &grant.body(challenge)).await;

    assert_eq!(status, StatusCode::CREATED, "{session}");
    assert_eq!(session["pubky"], json!(grant.pubky()));
    assert_eq!(session["capabilities"], json!(MARKETPLACE_CAPS));
    let token = session["token"].as_str().expect("token");
    assert_eq!(token.len(), 43);
    let (capabilities, grant_id, client_id, expires_at): (
        String,
        Option<String>,
        Option<String>,
        DateTime<Utc>,
    ) = sqlx::query_as(
        "SELECT capabilities, source_grant_id, source_client_id, expires_at \
         FROM auth_sessions WHERE pubky = $1",
    )
    .bind(grant.pubky())
    .fetch_one(&app.pool)
    .await
    .expect("session row");
    assert_eq!(capabilities, MARKETPLACE_CAPS);
    assert_eq!(grant_id.as_deref(), Some(grant.claims.jti.as_str()));
    assert_eq!(client_id.as_deref(), Some(CLIENT_ID));
    assert_eq!(
        expires_at.timestamp(),
        (app.clock.now() + Duration::seconds(86_400)).timestamp()
    );
    let burned: (Option<DateTime<Utc>>, Option<String>) = sqlx::query_as(
        "SELECT used_at, grant_id FROM grant_session_challenges WHERE challenge_id = $1",
    )
    .bind(challenge)
    .fetch_one(&app.pool)
    .await
    .expect("challenge row");
    assert!(burned.0.is_some());
    assert_eq!(burned.1.as_deref(), Some(grant.claims.jti.as_str()));

    // The bearer passes the inventory capability gate.
    let (status, listed) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert_eq!(
        listed["sessions"][0]["capabilities"],
        json!(MARKETPLACE_CAPS)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_session_never_outlives_the_grant(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::minutes(10));
    let challenge = issue(&app, &homeserver, &grant).await;

    let (status, session) = post(&app, &grant.body(challenge)).await;

    assert_eq!(status, StatusCode::CREATED, "{session}");
    let expires_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT expires_at FROM auth_sessions WHERE pubky = $1")
            .bind(grant.pubky())
            .fetch_one(&app.pool)
            .await
            .expect("session row");
    assert_eq!(expires_at, grant.expires_at());
}

/// Kimi #137 P1: a stolen client key used while the grant is live cannot
/// stage proof documents for later. A challenge id the service never issued
/// mints nothing, even with a correct document on the homeserver.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pre_staged_document_for_an_unissued_challenge_mints_nothing(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::days(365));
    let staged = Uuid::new_v4();
    homeserver.write(&grant.pubky(), staged, grant.document(staged));

    let (status, body) = post(&app, &grant.body(staged)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "grant_session_challenge_invalid"}));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

/// Kimi #137 P1, the rest of the attack: challenges and documents staged
/// while the grant is live are useless once their challenges expire, which
/// bounds the window after a revocation to one challenge lifetime.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_staged_challenge_expires_with_its_lifetime(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::days(365));
    let real_now = app.clock.now();
    app.clock.set(real_now - Duration::seconds(121));
    let staged = issue(&app, &homeserver, &grant).await;
    app.clock.set(real_now);

    let (status, body) = post(&app, &grant.body(staged)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "grant_session_challenge_invalid"}));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_challenge_belongs_to_the_pubky_it_was_issued_to(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let owner = ShopGrant::new(Duration::hours(1));
    let other = ShopGrant::new(Duration::hours(1));
    let challenge = issue(&app, &homeserver, &owner).await;
    homeserver.write(&other.pubky(), challenge, other.document(challenge));

    let (status, body) = post(&app, &other.body(challenge)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "grant_session_challenge_invalid"}));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_holds_a_bounded_number_of_live_challenges(pool: PgPool) {
    let (app, _) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let real_now = app.clock.now();
    app.clock.set(real_now - Duration::seconds(121));
    for _ in 0..MAX_LIVE_CHALLENGES {
        let (status, body) = request_challenge(&app, &grant).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(body["expires_at"].as_str().is_some());
    }
    let (status, body) = request_challenge(&app, &grant).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, json!({"error": "too_many_challenges"}));

    // Once they expire they stop counting and are pruned on the next request.
    app.clock.set(real_now);
    let (status, _) = request_challenge(&app, &grant).await;
    assert_eq!(status, StatusCode::CREATED);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM grant_session_challenges")
        .fetch_one(&app.pool)
        .await
        .expect("challenge rows");
    assert_eq!(rows, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_challenge_request_needs_its_own_signed_purpose(pool: PgPool) {
    let (app, _) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let other_client = ShopGrant::new(Duration::hours(1));

    // A mint proof is not a challenge request, and vice versa.
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-session-challenges",
        None,
        &grant.body(Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "grant_session_invalid"}));
    let (status, _) = post(&app, &grant.challenge_request()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A proof signed by a key other than the grant's client key.
    let mut forged = grant.challenge_request();
    forged["pop"] = other_client.challenge_request()["pop"].clone();
    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-session-challenges",
        None,
        &forged,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM grant_session_challenges")
        .fetch_one(&app.pool)
        .await
        .expect("challenge rows");
    assert_eq!(rows, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replayed_proof_mints_nothing(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = issue(&app, &homeserver, &grant).await;
    let body = grant.body(challenge);

    let (first, _) = post(&app, &body).await;
    let (second, refusal) = post(&app, &body).await;
    // A fresh proof over the same challenge id is a replay too.
    let (third, _) = post(&app, &grant.body(challenge)).await;

    assert_eq!(first, StatusCode::CREATED);
    assert_eq!(second, StatusCode::UNAUTHORIZED);
    assert_eq!(refusal, json!({"error": "grant_session_challenge_invalid"}));
    assert_eq!(third, StatusCode::UNAUTHORIZED);
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_uses_of_one_proof_mint_exactly_one_session(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = issue(&app, &homeserver, &grant).await;
    let body = grant.body(challenge);

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let router = app.router.clone();
        let body = body.clone();
        tasks.spawn(
            async move { send(router, "POST", "/v1/auth/grant-sessions", None, &body).await },
        );
    }
    let results = tasks.join_all().await;

    let created = results
        .iter()
        .filter(|(status, _)| *status == StatusCode::CREATED)
        .count();
    assert_eq!(created, 1, "{results:?}");
    assert!(results
        .iter()
        .all(|(status, body)| *status == StatusCode::CREATED
            || (*status == StatusCode::UNAUTHORIZED
                && *body == json!({"error": "grant_session_challenge_invalid"}))));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_the_homeserver_proof_document_nothing_is_minted_or_burned(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let (_, issued) = request_challenge(&app, &grant).await;
    let challenge: Uuid = issued["challenge_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("challenge id");

    // A revoked grant cannot write its homeserver: no document.
    let (status, body) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "homeserver_proof_invalid"}));

    // The challenge was not burned: once a live session writes it, it mints.
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
    let (status, _) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_proof_document_for_anything_else_is_refused(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let other = Keypair::random().public_key().z32();
    for (index, mutate) in [
        |document: &mut Value| document["aud"] = json!("https://other.test"),
        |document: &mut Value| document["pubky"] = json!("someone-else"),
        |document: &mut Value| document["domain"] = json!("shop-bff/cli-grant-homeserver-pop/v1"),
        |document: &mut Value| document["challenge_id"] = json!(Uuid::new_v4().to_string()),
        |document: &mut Value| document["extra"] = json!(true),
    ]
    .into_iter()
    .enumerate()
    {
        let challenge = issue(&app, &homeserver, &grant).await;
        let mut document = grant.document(challenge);
        mutate(&mut document);
        homeserver.write(&grant.pubky(), challenge, document);
        let (status, body) = post(&app, &grant.body(challenge)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "case {index}: {body}");
        assert_eq!(body, json!({"error": "homeserver_proof_invalid"}));
        // Refused documents burn nothing; free the slot for the next case.
        sqlx::query("DELETE FROM grant_session_challenges WHERE challenge_id = $1")
            .bind(challenge)
            .execute(&app.pool)
            .await
            .expect("drop the case's challenge");
    }
    // The right document under another user's pubky proves nothing.
    let (_, issued) = request_challenge(&app, &grant).await;
    let challenge: Uuid = issued["challenge_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("challenge id");
    homeserver.write(&other, challenge, grant.document(challenge));
    let (status, _) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreachable_homeserver_is_retriable(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = issue(&app, &homeserver, &grant).await;
    homeserver.unavailable.store(true, Ordering::SeqCst);

    let (status, body) = post(&app, &grant.body(challenge)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"error": "homeserver_unavailable"}));
    homeserver.unavailable.store(false, Ordering::SeqCst);
    let (status, _) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn invalid_proofs_are_refused_before_any_lookup(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    homeserver.unavailable.store(true, Ordering::SeqCst);
    let grant = ShopGrant::new(Duration::hours(1));
    let mut body = grant.body(Uuid::new_v4());
    body["pop"] = ShopGrant::new(Duration::hours(1)).body(Uuid::new_v4())["pop"].clone();

    let (status, refusal) = post(&app, &body).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(refusal, json!({"error": "grant_session_invalid"}));

    let (status, _) = post(&app, &json!({"grant": "x", "pop": "y", "extra": 1})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let oversized = serde_json::to_vec(&json!({"grant": "x".repeat(20_000), "pop": "y"}))
        .expect("body serializes");
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/auth/grant-sessions")
                .header("content-type", "application/json")
                .body(Body::from(oversized))
                .expect("request builds"),
        )
        .await
        .expect("request executes");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sign_out_revokes_a_grant_session(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = issue(&app, &homeserver, &grant).await;
    let (_, session) = post(&app, &grant.body(challenge)).await;
    let token = session["token"].as_str().expect("token");
    let id = session["session_id"].as_str().expect("session id");

    let (status, _) = send(
        app.router.clone(),
        "DELETE",
        &format!("/v1/auth/sessions/{id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(
        app.router.clone(),
        "GET",
        "/v1/auth/sessions",
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Kimi delta P3-3(a): issuers racing for the last live slot of one pubky
/// are serialized by the per-pubky lock, so exactly one wins.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn racing_issuers_cannot_exceed_the_live_cap(pool: PgPool) {
    let (app, _) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    for _ in 0..MAX_LIVE_CHALLENGES - 1 {
        let (status, _) = request_challenge(&app, &grant).await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let router = app.router.clone();
        let body = grant.challenge_request_at(app.clock.now());
        tasks.spawn(async move {
            send(
                router,
                "POST",
                "/v1/auth/grant-session-challenges",
                None,
                &body,
            )
            .await
        });
    }
    let results = tasks.join_all().await;

    let created = results
        .iter()
        .filter(|(status, _)| *status == StatusCode::CREATED)
        .count();
    assert_eq!(created, 1, "{results:?}");
    assert!(results
        .iter()
        .all(|(status, body)| *status == StatusCode::CREATED
            || (*status == StatusCode::TOO_MANY_REQUESTS
                && *body == json!({"error": "too_many_challenges"}))));
    let live: i64 =
        sqlx::query_scalar("SELECT count(*) FROM grant_session_challenges WHERE used_at IS NULL")
            .fetch_one(&app.pool)
            .await
            .expect("live challenges");
    assert_eq!(live, MAX_LIVE_CHALLENGES);
}

/// Kimi delta P3-3(b): issuing a challenge spends the `grant_session.mint`
/// bucket and answers `429 retry_later` with `Retry-After` when it is empty.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn challenge_issue_spends_the_rate_bucket(pool: PgPool) {
    let mut config = Config::for_tests();
    config.automation_rate_limit_per_minute = 1;
    config.automation_rate_limit_burst_multiplier = 1;
    let (app, _) = grant_session_app_with(pool, true, config).await;
    let grant = ShopGrant::new(Duration::hours(1));

    let (first, _) = request_challenge(&app, &grant).await;
    let (status, headers, body) = common::send_with_headers(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-session-challenges",
        None,
        &grant.challenge_request_at(app.clock.now()),
    )
    .await;

    assert_eq!(first, StatusCode::CREATED);
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body, json!({"error": "retry_later"}));
    assert!(headers.get("retry-after").is_some());
}

/// Kimi delta P3-2: a refusal at the live cap does not spend a rate token,
/// so a client waiting for a slot cannot drain its own bucket.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refusal_at_the_live_cap_spends_no_rate_token(pool: PgPool) {
    let (app, _) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    for _ in 0..MAX_LIVE_CHALLENGES {
        let (status, _) = request_challenge(&app, &grant).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let tokens = || async {
        sqlx::query_scalar::<_, f64>(
            "SELECT tokens FROM automation_rate_limits \
             WHERE seller_pubky = $1 AND endpoint_class = 'grant_session.mint'",
        )
        .bind(grant.pubky())
        .fetch_one(&app.pool)
        .await
        .expect("bucket")
    };
    let before = tokens().await;

    for _ in 0..3 {
        let (status, body) = request_challenge(&app, &grant).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body, json!({"error": "too_many_challenges"}));
    }

    assert_eq!(tokens().await, before);
}

/// Kimi delta P3-3(c): a challenge is live strictly before `expires_at`.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_mint_at_the_exact_expiry_instant_is_refused(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::days(1));
    let issued_at = app.clock.now() - Duration::seconds(120);
    app.clock.set(issued_at);
    let at_expiry = issue(&app, &homeserver, &grant).await;
    let just_before = issue(&app, &homeserver, &grant).await;

    app.clock.set(issued_at + Duration::seconds(120));
    let (status, body) = post(
        &app,
        &grant.signed_at(proof_data(AUDIENCE, at_expiry), app.clock.now()),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, json!({"error": "grant_session_challenge_invalid"}));

    app.clock.set(issued_at + Duration::seconds(119));
    let (status, body) = post(
        &app,
        &grant.signed_at(proof_data(AUDIENCE, just_before), app.clock.now()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}
