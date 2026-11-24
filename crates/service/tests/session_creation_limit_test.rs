//! `POST /v1/auth/sessions` spends one `session.create` token per verified
//! AuthToken, keyed by the token's pubky. Requests drive the real router and
//! genuine AuthToken verification against PostgreSQL.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, SubsecRound, Utc};
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

async fn creation_bucket(app: &TestApp, pubky: &str) -> (f64, DateTime<Utc>) {
    sqlx::query_as(
        "SELECT tokens, updated_at FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = 'session.create'",
    )
    .bind(pubky)
    .fetch_one(&app.pool)
    .await
    .expect("bucket row")
}

async fn seed_creation_bucket(app: &TestApp, pubky: &str, tokens: f64, at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO automation_rate_limits (seller_pubky, endpoint_class, tokens, updated_at) \
         VALUES ($1, 'session.create', $2, $3)",
    )
    .bind(pubky)
    .bind(tokens)
    .bind(at)
    .execute(&app.pool)
    .await
    .expect("seed bucket");
}

async fn create_session_at(
    app: &TestApp,
    keypair: &pubky_common::crypto::Keypair,
    at: DateTime<Utc>,
) {
    app.clock.set(at);
    let (status, _, body) = post_token(app, auth_token_bytes(keypair)).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn creation_requests_in_reverse_clock_order_credit_refill_once(pool: PgPool) {
    // Defaults: 120 per minute (2 tokens a second), capacity 240.
    let app = test_app(pool).await;
    let start = Utc::now().trunc_subsecs(3);
    let early = start + Duration::seconds(10);
    let late = start + Duration::seconds(30);
    let (in_order_keys, in_order) = random_keypair();
    let (reversed_keys, reversed) = random_keypair();
    seed_creation_bucket(&app, &in_order, 100.0, start).await;
    seed_creation_bucket(&app, &reversed, 100.0, start).await;

    create_session_at(&app, &in_order_keys, early).await;
    create_session_at(&app, &in_order_keys, late).await;
    create_session_at(&app, &reversed_keys, late).await;
    create_session_at(&app, &reversed_keys, early).await;

    // 100 + 30 s of refill (60 tokens) - 2 spent.
    assert_eq!(creation_bucket(&app, &in_order).await, (158.0, late));
    assert_eq!(
        creation_bucket(&app, &reversed).await,
        (158.0, late),
        "the older request spends a token and leaves updated_at where it was"
    );

    create_session_at(&app, &in_order_keys, late).await;
    create_session_at(&app, &reversed_keys, late).await;
    assert_eq!(creation_bucket(&app, &in_order).await.0, 157.0);
    assert_eq!(
        creation_bucket(&app, &reversed).await.0,
        157.0,
        "no refill interval is credited twice"
    );
}

fn retry_after_seconds(headers: &axum::http::HeaderMap) -> i64 {
    headers
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .expect("Retry-After is an integer")
}

/// Defaults, with a token acceptance window wide enough for the clock moves
/// below.
fn wide_window_config() -> Config {
    let mut config = Config::for_tests();
    config.auth_token_window_seconds = 7_200;
    config
}

async fn post_token_at(
    app: &TestApp,
    keypair: &pubky_common::crypto::Keypair,
    at: DateTime<Utc>,
) -> (StatusCode, axum::http::HeaderMap) {
    app.clock.set(at);
    let (status, headers, body) = post_token(app, auth_token_bytes(keypair)).await;
    assert!(
        matches!(status, StatusCode::CREATED | StatusCode::TOO_MANY_REQUESTS),
        "{status}: {body}"
    );
    (status, headers)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn refusals_from_a_slower_clock_never_rewind_a_creation_bucket(pool: PgPool) {
    // Defaults: 120 per minute, so one token refills every 500 ms.
    let app = test_app_with_config(pool, wide_window_config()).await;
    let late = Utc::now().trunc_subsecs(3);
    let one_refill = Duration::milliseconds(500);
    // A second instance whose clock runs a minute behind.
    let behind = Duration::seconds(60);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 0.0, late).await;

    let mut admitted = 0;
    for cycle in 1..=5 {
        let previous = late + one_refill * (cycle - 1);
        let fast = late + one_refill * cycle;
        let (status, headers) = post_token_at(&app, &keypair, previous - behind).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        // 60 s until the slower clock reaches the bucket, then 500 ms.
        assert_eq!(retry_after_seconds(&headers), 61);
        assert_eq!(
            creation_bucket(&app, &pubky).await,
            (0.0, previous),
            "a refusal from the slower clock leaves the bucket alone"
        );
        for _ in 0..3 {
            if post_token_at(&app, &keypair, fast).await.0 == StatusCode::CREATED {
                admitted += 1;
            }
        }
        assert_eq!(creation_bucket(&app, &pubky).await, (0.0, fast));
    }
    assert_eq!(admitted, 5, "one admission per refill interval");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn far_future_empty_creation_bucket_resets_on_refusal(pool: PgPool) {
    // Defaults: 120 per minute, so one token refills every 500 ms.
    let app = test_app_with_config(pool, wide_window_config()).await;
    let start = Utc::now().trunc_subsecs(3);
    let one_refill = Duration::milliseconds(500);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 0.0, start + Duration::hours(1)).await;

    let (status, headers) = post_token_at(&app, &keypair, start).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(retry_after_seconds(&headers), 1);
    assert_eq!(
        creation_bucket(&app, &pubky).await,
        (0.0, start),
        "the refusal resets the timestamp to its own time and credits no refill"
    );
    let (status, _) = post_token_at(
        &app,
        &keypair,
        start + one_refill - Duration::milliseconds(1),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(creation_bucket(&app, &pubky).await, (0.0, start));
    create_session_at(&app, &keypair, start + one_refill).await;
    assert_eq!(
        creation_bucket(&app, &pubky).await,
        (0.0, start + one_refill)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn far_future_creation_bucket_with_tokens_resets_on_admission(pool: PgPool) {
    let app = test_app_with_config(pool, wide_window_config()).await;
    let start = Utc::now().trunc_subsecs(3);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 3.0, start + Duration::hours(1)).await;

    create_session_at(&app, &keypair, start).await;
    assert_eq!(
        creation_bucket(&app, &pubky).await,
        (2.0, start),
        "the admitted request writes its own time, credits no refill and spends one token"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn creation_bucket_ahead_inside_the_window_waits_for_the_clock(pool: PgPool) {
    // Defaults: 120 per minute, so one token refills every 500 ms.
    let app = test_app_with_config(pool, wide_window_config()).await;
    let start = Utc::now().trunc_subsecs(3);
    let one_refill = Duration::milliseconds(500);
    let ahead = start + Duration::minutes(2);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 0.0, ahead).await;

    let (status, headers) = post_token_at(&app, &keypair, start).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // 120 s until this clock reaches the bucket, then 500 ms for one token.
    assert_eq!(retry_after_seconds(&headers), 121);
    assert_eq!(
        creation_bucket(&app, &pubky).await,
        (0.0, ahead),
        "a timestamp inside the window is kept"
    );
    let (status, _) = post_token_at(
        &app,
        &keypair,
        ahead + one_refill - Duration::milliseconds(1),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "time before the stored timestamp is not credited as refill"
    );
    create_session_at(&app, &keypair, ahead + one_refill).await;
    assert_eq!(
        creation_bucket(&app, &pubky).await,
        (0.0, ahead + one_refill)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn creation_retry_after_rounds_up_a_sub_millisecond_wait(pool: PgPool) {
    let mut config = wide_window_config();
    // One token refills every second.
    config.automation_rate_limit_per_minute = 60;
    let app = test_app_with_config(pool, config).await;
    let start = Utc::now().trunc_subsecs(3);
    let ahead = start + Duration::microseconds(60_000_500);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 0.0, ahead).await;

    let (status, headers) = post_token_at(&app, &keypair, start).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // 60.0005 s until this clock reaches the bucket, then 1 s for one token.
    assert_eq!(retry_after_seconds(&headers), 62);
    assert_eq!(creation_bucket(&app, &pubky).await, (0.0, ahead));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn creation_retry_after_rounds_up_a_sub_microsecond_wait(pool: PgPool) {
    let mut config = wide_window_config();
    // One token refills every second.
    config.automation_rate_limit_per_minute = 60;
    let app = test_app_with_config(pool, config).await;
    let ahead = Utc::now().trunc_subsecs(6) + Duration::seconds(60);
    let start = ahead - Duration::seconds(60) - Duration::nanoseconds(1);
    let (keypair, pubky) = random_keypair();
    seed_creation_bucket(&app, &pubky, 0.0, ahead).await;

    let (status, headers) = post_token_at(&app, &keypair, start).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // 60 s and 1 ns until this clock reaches the bucket, then 1 s for one token.
    assert_eq!(retry_after_seconds(&headers), 62);
    assert_eq!(creation_bucket(&app, &pubky).await, (0.0, ahead));
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
