//! `POST /v1/auth/sessions` spends one `session.create` token per verified
//! AuthToken, keyed by the token's pubky. Requests drive the real router and
//! genuine AuthToken verification against PostgreSQL.

mod common;

use axum::http::StatusCode;
use chrono::{Duration, Utc};
use common::{
    auth_token_bytes, random_keypair, send_bytes, test_app, test_app_with_config, TestApp,
};
use marketplace_service::config::Config;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;

/// A configuration whose `session.create` bucket holds one token and refills
/// one per minute.
fn one_token_config() -> Config {
    let mut config = Config::for_tests();
    config.automation_rate_limit_per_minute = 1;
    config.automation_rate_limit_burst_multiplier = 1;
    config
}

async fn post_token(app: &TestApp, bytes: Vec<u8>) -> (StatusCode, axum::http::HeaderMap, Value) {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/auth/sessions")
        .body(axum::body::Body::from(bytes))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("request executes");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("body is JSON")
    };
    (status, headers, body)
}

async fn creation_tokens(app: &TestApp, pubky: &str) -> Option<f64> {
    sqlx::query_scalar(
        "SELECT tokens FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = 'session.create'",
    )
    .bind(pubky)
    .fetch_optional(&app.pool)
    .await
    .expect("bucket read")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn session_creation_is_limited_per_pubky(pool: PgPool) {
    let app = test_app_with_config(pool, one_token_config()).await;
    let (keypair, pubky) = random_keypair();
    app.clock.set(Utc::now());

    let (first, _, body) = post_token(&app, auth_token_bytes(&keypair)).await;
    assert_eq!(first, StatusCode::CREATED, "{body}");
    let (second, headers, body) = post_token(&app, auth_token_bytes(&keypair)).await;
    assert_eq!(
        second,
        StatusCode::TOO_MANY_REQUESTS,
        "a second session inside the window was accepted: {body}"
    );
    assert_eq!(
        body,
        json!({
            "ok": false,
            "error": {"code": "rate_limited", "message": "The request rate limit was exceeded."}
        })
    );
    let retry_after: i64 = headers
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .expect("Retry-After header is missing")
        .parse()
        .expect("Retry-After is an integer");
    assert!((1..=60).contains(&retry_after), "Retry-After {retry_after}");
    let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auth_sessions WHERE pubky = $1")
        .bind(&pubky)
        .fetch_one(&app.pool)
        .await
        .expect("session count");
    assert_eq!(sessions, 1, "a limited request stores no session");

    let (other_keypair, _) = random_keypair();
    let (status, body) = send_bytes(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        auth_token_bytes(&other_keypair),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "another pubky: {body}");

    app.clock.advance_seconds(61);
    let (status, _, body) = post_token(&app, auth_token_bytes(&keypair)).await;
    assert_eq!(status, StatusCode::CREATED, "after refill: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn invalid_session_token_spends_no_creation_budget(pool: PgPool) {
    let app = test_app_with_config(pool, one_token_config()).await;
    let (keypair, pubky) = random_keypair();

    // Signed now but checked an hour later: outside the acceptance window.
    let expired = auth_token_bytes(&keypair);
    app.clock.set(Utc::now() + Duration::hours(1));
    let (status, _, body) = post_token(&app, expired).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    app.clock.set(Utc::now());
    let mut tampered = auth_token_bytes(&keypair);
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    let (status, _, body) = post_token(&app, tampered).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let (status, _, body) = post_token(&app, b"not an auth token".to_vec()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    assert_eq!(
        creation_tokens(&app, &pubky).await,
        None,
        "refused tokens never touch the pubky's bucket"
    );
    let (status, _, body) = post_token(&app, auth_token_bytes(&keypair)).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "the one token is still there: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn replayed_token_is_refused(pool: PgPool) {
    let app = test_app(pool).await;
    let (keypair, pubky) = random_keypair();
    app.clock.set(Utc::now());
    let token = auth_token_bytes(&keypair);

    let (status, _, body) = post_token(&app, token.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let after_first = creation_tokens(&app, &pubky).await.expect("bucket row");
    let (status, _, body) = post_token(&app, token).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("The auth token has already been used.")
    );

    // Defaults: 120 per minute, burst 2. The clock is frozen, so each
    // verified request spends exactly one token, the replay included.
    assert_eq!(after_first, 239.0);
    assert_eq!(creation_tokens(&app, &pubky).await, Some(238.0));
}

async fn post_token_from(
    app: &TestApp,
    origin: &str,
    bytes: Vec<u8>,
) -> (StatusCode, axum::http::HeaderMap) {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/auth/sessions")
        .header(axum::http::header::ORIGIN, origin)
        .body(axum::body::Body::from(bytes))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("request executes");
    (response.status(), response.headers().clone())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn limited_creation_exposes_retry_after_to_allowed_origins_only(pool: PgPool) {
    let app = test_app_with_config(pool, one_token_config()).await;
    let (keypair, _) = random_keypair();
    app.clock.set(Utc::now());
    let allowed = "http://localhost:3000";

    let (status, _) = post_token_from(&app, allowed, auth_token_bytes(&keypair)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, headers) = post_token_from(&app, allowed, auth_token_bytes(&keypair)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(axum::http::header::RETRY_AFTER));
    assert_eq!(
        headers
            .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some(allowed)
    );
    let exposed = headers
        .get(axum::http::header::ACCESS_CONTROL_EXPOSE_HEADERS)
        .and_then(|value| value.to_str().ok())
        .expect("Access-Control-Expose-Headers is missing");
    assert!(
        exposed
            .split(',')
            .any(|name| name.trim().eq_ignore_ascii_case("retry-after")),
        "exposed headers: {exposed}"
    );

    let (status, headers) = post_token_from(
        &app,
        "https://elsewhere.example",
        auth_token_bytes(&keypair),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(axum::http::header::RETRY_AFTER));
    let cors: Vec<_> = headers
        .keys()
        .filter(|name| name.as_str().starts_with("access-control-"))
        .collect();
    assert!(
        cors.is_empty(),
        "a disallowed origin got CORS headers: {cors:?}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn replayed_token_with_an_empty_bucket_is_limited(pool: PgPool) {
    let app = test_app_with_config(pool, one_token_config()).await;
    let (keypair, _) = random_keypair();
    app.clock.set(Utc::now());
    let token = auth_token_bytes(&keypair);

    let (status, _, body) = post_token(&app, token.clone()).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, headers, body) = post_token(&app, token).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(headers.get(axum::http::header::RETRY_AFTER).is_some());
}
