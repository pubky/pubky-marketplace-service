//! HTTP-level tests for Pubky AuthToken authentication, using genuine
//! tokens signed with `pubky-common` — the same library the service
//! verifies with — end to end.

mod common;

use axum::http::StatusCode;
use chrono::Utc;
use marketplace_service::clock::Clock;
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::Capability;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    auth_token_bytes, execute, new_actor, random_keypair, register_command, send, send_bytes,
    test_app, TestApp,
};

/// Posts raw AuthToken bytes with the test clock aligned to real system
/// time (tokens are stamped with real time by `AuthToken::sign`), restoring
/// the fixture instant afterwards.
async fn post_token(app: &TestApp, bytes: Vec<u8>) -> (StatusCode, Value) {
    let fixture_now = app.clock.now();
    app.clock.set(Utc::now());
    let result = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;
    app.clock.set(fixture_now);
    result
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_valid_auth_token_mints_a_session(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, pubky) = random_keypair();

    let (status, session) = post_token(&app, auth_token_bytes(&keypair)).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "session issue failed: {session}"
    );
    assert_eq!(session["pubky"], json!(pubky));
    assert_eq!(session["capabilities"], json!("/:rw"));
    assert!(session["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert!(session["expires_at"].as_str().is_some());

    let stored: String =
        sqlx::query_scalar("SELECT capabilities FROM auth_sessions WHERE pubky = $1")
            .bind(&pubky)
            .fetch_one(&app.pool)
            .await
            .expect("session row");
    assert_eq!(stored, "/:rw");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_session_from_a_valid_token_authorizes_commands(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn rejects_tampered_and_corrupted_tokens(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let genuine = auth_token_bytes(&keypair);

    // Tampered signature.
    let mut tampered_signature = genuine.clone();
    tampered_signature[0] ^= 0x01;
    let (status, body) = post_token(&app, tampered_signature).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "tampered signature accepted: {body}"
    );

    // Tampered payload (capabilities tail) breaks the signature.
    let mut tampered_payload = genuine.clone();
    let last = tampered_payload.len() - 1;
    tampered_payload[last] ^= 0x01;
    let (status, body) = post_token(&app, tampered_payload).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "tampered payload accepted: {body}"
    );

    // Truncated and garbage bytes.
    let (status, _) = post_token(&app, genuine[..genuine.len() - 1].to_vec()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_token(&app, vec![0u8; 16]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = post_token(&app, Vec::new()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // The genuine token still works: the rejections above were about the
    // corruption, not the token.
    let (status, _) = post_token(&app, genuine).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replayed_token_is_rejected(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    let (status, _) = post_token(&app, bytes.clone()).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = post_token(&app, bytes).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "replay accepted: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_expired_token_is_rejected_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    // The token was signed at real system time; the service clock is 121 s
    // later, past the 120 s acceptance window.
    app.clock.set(Utc::now() + chrono::Duration::seconds(121));
    let (status, body) = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "expired token accepted: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_token_from_the_future_is_rejected_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, _) = random_keypair();
    let bytes = auth_token_bytes(&keypair);

    // The service clock is 121 s before the token's signing time.
    app.clock.set(Utc::now() - chrono::Duration::seconds(121));
    let (status, body) = send_bytes(app.router.clone(), "POST", "/v1/auth/sessions", bytes).await;

    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "future token accepted: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_token_for_pubky_a_cannot_act_as_pubky_b(pool: PgPool) {
    let app = test_app(pool).await;
    let alice = new_actor(&app).await;
    let (_, bob_pubky) = random_keypair();

    // Alice's session is bound to the pubky inside her token; a command
    // claiming Bob as the seller is refused.
    let (status, body) = execute(&app, &alice.token, &register_command(&bob_pubky, 1)).await;

    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "cross-actor command accepted: {body}"
    );
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sessions_expire_on_server_time(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    // The session was minted at real system time (see `authenticate`); move
    // the clock one second past its 24 h TTL.
    app.clock
        .set(Utc::now() + chrono::Duration::seconds(86_401));
    let (status, _) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn commands_require_a_bearer_session(pool: PgPool) {
    let app = test_app(pool).await;
    let (_, pubky) = random_keypair();

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        None,
        &register_command(&pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some("bm90LWEtcmVhbC10b2tlbg"),
        &register_command(&pubky, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_challenge_endpoint_is_gone(pool: PgPool) {
    let app = test_app(pool).await;
    let (_, pubky) = random_keypair();

    let (status, _) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/challenges",
        None,
        &json!({ "pubky": pubky }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn verified_capabilities_are_returned_and_stored_canonically(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, pubky) = random_keypair();
    let presented = "/pub/pubky.app/:rw,/priv/pubky.app/:rw,/pub/paykit/:rw";
    let bytes = AuthToken::sign(
        &keypair,
        vec![
            Capability::read_write("/pub/pubky.app/").expect("scope"),
            Capability::read_write("/priv/pubky.app/").expect("scope"),
            Capability::read_write("/pub/paykit/").expect("scope"),
        ],
    )
    .serialize();

    let (status, session) = post_token(&app, bytes).await;

    assert_eq!(
        status,
        StatusCode::CREATED,
        "session issue failed: {session}"
    );
    assert_eq!(session["pubky"], json!(pubky));
    assert_eq!(session["capabilities"], json!(presented));

    let stored: String =
        sqlx::query_scalar("SELECT capabilities FROM auth_sessions WHERE pubky = $1")
            .bind(&pubky)
            .fetch_one(&app.pool)
            .await
            .expect("session row");
    assert_eq!(stored, presented);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn health_and_readiness_endpoints_respond(pool: PgPool) {
    let app = test_app(pool).await;

    let (status, body) = send(app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ok"));

    let (status, body) = send(app.router.clone(), "GET", "/ready", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], json!("ready"));
    assert!(body.get("warnings").is_none(), "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ready_returns_503_when_postgres_fails(pool: PgPool) {
    let app = test_app(pool.clone()).await;
    pool.close().await;

    let (status, body) = send(app.router.clone(), "GET", "/ready", None, &Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body, json!({ "status": "unavailable" }));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ready_returns_200_with_warnings_when_secondary_diagnostics_or_feeds_degrade(pool: PgPool) {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use marketplace_service::config::Config;
    use marketplace_service::http::build_router;
    use marketplace_service::listing_moderation::{
        ListingModerationClient, ListingModerationGate, ModerationLookup,
    };
    use marketplace_service::refusal_audit::{AuditKeys, RefusalAuditRuntime};

    struct UnavailableModeration;
    impl ListingModerationClient for UnavailableModeration {
        fn lookup<'a>(
            &'a self,
            _seller_pubky: &'a str,
            _listing_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
            Box::pin(async { ModerationLookup::Unavailable })
        }
    }

    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    config.fx_feed_url = "http://127.0.0.1:1/unreachable-fx".to_string();
    let app = common::test_app_with_config(pool.clone(), config).await;

    // Install an unverified refusal-audit writer (domain pool fails authority
    // probe so is_ready() is false) plus an unavailable moderation gate.
    let root = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [9u8; 32]);
    let keys = AuditKeys::parse(&root, "1", None, None).expect("keys");
    let audit = Arc::new(RefusalAuditRuntime::spawn(pool.clone(), keys));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!audit.is_ready());

    // Seed an ended order with an overdue unpurged buyer delivery email.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;
    let order_id = uuid::Uuid::parse_str(&order.order_id).expect("order uuid");
    let now = app.clock.now();
    sqlx::query("UPDATE orders SET state = 'cancelled' WHERE id = $1")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("cancel order");
    sqlx::query("UPDATE orders SET ended_at = $2 WHERE id = $1")
        .bind(order_id)
        .bind(now)
        .execute(&pool)
        .await
        .expect("stamp ended_at at app clock");
    sqlx::query(
        "INSERT INTO order_delivery_emails \
         (order_id, buyer_pubky, email_ciphertext, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, $4)",
    )
    .bind(order_id)
    .bind(&buyer.pubky)
    .bind(&[0_u8; 32][..])
    .bind(now)
    .execute(&pool)
    .await
    .expect("seed overdue email");
    app.clock.set(now + chrono::Duration::days(40));

    let state = app
        .state
        .clone()
        .with_refusal_audit(audit)
        .with_listing_moderation(Some(Arc::new(ListingModerationGate::new(Arc::new(
            UnavailableModeration,
        )))));
    let router = build_router(state);

    let (status, body) = send(router, "GET", "/ready", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("ready"));
    assert_eq!(
        body["warnings"],
        json!(["refusal_audit_not_ready", "delivery_email_purge_overdue"])
    );
}
