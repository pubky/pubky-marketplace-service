//! `POST /v1/auth/grant-sessions`: marketplace sessions minted from the
//! Shop's own sign-in grant, with genuine grant and custom-proof JWS signed by
//! `pubky-common` keys and a stand-in homeserver holding the proof documents.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use marketplace_service::clock::{AdjustableClock, Clock};
use marketplace_service::config::Config;
use marketplace_service::grant_session::{proof_data, GrantSessionConfig, ProofDocument};
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use marketplace_service::http::build_router;
use marketplace_service::AppState;
use pubky::custom_pop::CUSTOM_POP_JWS_TYP;
use pubky::{Capabilities, ClientId, GrantClaims, GrantId, Keypair, PopNonce, GRANT_JWS_TYP};
use pubky_common::auth::jws::sign_jws;
use serde_json::{json, Value};
use sqlx::PgPool;
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

    fn body(&self, challenge_id: Uuid) -> Value {
        let pop = sign_jws(
            &self.client,
            CUSTOM_POP_JWS_TYP,
            &json!({
                "gid": self.claims.jti,
                "iat": Utc::now().timestamp(),
                "nonce": PopNonce::generate(),
                "data": proof_data(AUDIENCE, challenge_id),
            }),
        );
        json!({ "grant": self.claims.sign(&self.root, GRANT_JWS_TYP), "pop": pop })
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
    let homeserver = Arc::new(ProofHomeserver::default());
    // Grant validity is checked against system time by the SDK.
    let clock = Arc::new(AdjustableClock::new(Utc::now()));
    let config = enabled.then(|| {
        Arc::new(GrantSessionConfig::new(AUDIENCE, CLIENT_ID, 120).expect("grant session config"))
    });
    let state = AppState::new(pool.clone(), clock.clone(), Config::for_tests())
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_route_is_unavailable_while_the_flag_is_off(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, false).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));

    let (status, body) = post(&app, &grant.body(challenge)).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error": "grant_session_unavailable"}));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_shop_grant_proof_mints_a_narrowed_session_without_an_approval(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::days(365));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));

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
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));

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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replayed_proof_mints_nothing(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
    let body = grant.body(challenge);

    let (first, _) = post(&app, &body).await;
    let (second, refusal) = post(&app, &body).await;
    // A fresh proof over the same challenge id is a replay too.
    let (third, _) = post(&app, &grant.body(challenge)).await;

    assert_eq!(first, StatusCode::CREATED);
    assert_eq!(second, StatusCode::UNAUTHORIZED);
    assert_eq!(refusal, json!({"error": "grant_session_replayed"}));
    assert_eq!(third, StatusCode::UNAUTHORIZED);
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_uses_of_one_proof_mint_exactly_one_session(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
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
                && *body == json!({"error": "grant_session_replayed"}))));
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_the_homeserver_proof_document_nothing_is_minted_or_burned(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();

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
        let challenge = Uuid::new_v4();
        let mut document = grant.document(challenge);
        mutate(&mut document);
        homeserver.write(&grant.pubky(), challenge, document);
        let (status, body) = post(&app, &grant.body(challenge)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "case {index}: {body}");
        assert_eq!(body, json!({"error": "homeserver_proof_invalid"}));
    }
    // The right document under another user's pubky proves nothing.
    let challenge = Uuid::new_v4();
    homeserver.write(&other, challenge, grant.document(challenge));
    let (status, _) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(session_count(&app.pool, &grant.pubky()).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreachable_homeserver_is_retriable(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
    homeserver.unavailable.store(true, Ordering::SeqCst);

    let (status, body) = post(&app, &grant.body(challenge)).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, json!({"error": "homeserver_unavailable"}));
    homeserver.unavailable.store(false, Ordering::SeqCst);
    let (status, _) = post(&app, &grant.body(challenge)).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn invalid_proofs_are_refused_before_any_homeserver_read(pool: PgPool) {
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
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn used_challenges_are_pruned_once_no_proof_can_name_them(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let first = Uuid::new_v4();
    homeserver.write(&grant.pubky(), first, grant.document(first));
    let (status, _) = post(&app, &grant.body(first)).await;
    assert_eq!(status, StatusCode::CREATED);
    sqlx::query("UPDATE grant_session_challenges SET used_at = used_at - INTERVAL '301 seconds'")
        .execute(&app.pool)
        .await
        .expect("age the record");

    let second = Uuid::new_v4();
    homeserver.write(&grant.pubky(), second, grant.document(second));
    let (status, _) = post(&app, &grant.body(second)).await;
    assert_eq!(status, StatusCode::CREATED);

    let remaining: Vec<Uuid> =
        sqlx::query_scalar("SELECT challenge_id FROM grant_session_challenges")
            .fetch_all(&app.pool)
            .await
            .expect("challenges");
    assert_eq!(remaining, vec![second]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sign_out_revokes_a_grant_session(pool: PgPool) {
    let (app, homeserver) = grant_session_app(pool, true).await;
    let grant = ShopGrant::new(Duration::hours(1));
    let challenge = Uuid::new_v4();
    homeserver.write(&grant.pubky(), challenge, grant.document(challenge));
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
