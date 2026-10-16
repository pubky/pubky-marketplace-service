//! The upstream `POST /setup/status` timeout and the seller's own
//! `bitcoin_setup` fallback to a recent `ready`. paykit-server takes 2 to
//! 3.5 s to answer `ready` for a set-up seller, so the call has its own
//! timeout (8 s by default) instead of the 3 s availability timeout, and one
//! failed ask within a minute of a `ready` does not tell a ready seller to
//! set up Bitkit.

mod common;

use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::*;
use marketplace_service::config::Config;
use marketplace_service::payments::{
    PaykitApi, PaykitClient, PaykitRequestError, DEFAULT_SETUP_STATUS_TIMEOUT,
};
use serde_json::{json, Value};
use sqlx::PgPool;

const OWN: &str = "/v0/sellers/me/payment-config";
/// Above the 3 s availability timeout `/setup/status` used to share, inside
/// the 8 s default.
const SLOW_READY: Duration = Duration::from_millis(3_500);

async fn bitcoin_setup(app: &TestApp, token: &str) -> Value {
    let (status, body) = send(app.router.clone(), "GET", OWN, Some(token), &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "own config read failed: {body}");
    body["payment_config"]["bitcoin_setup"].clone()
}

async fn upstream_app(pool: PgPool) -> (TestApp, FakePaykit) {
    test_app_with_paykit_api_config(pool, Config::for_tests(), PaykitApi::Upstream).await
}

async fn upstream_paykit() -> FakePaykit {
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    paykit
}

fn upstream_client(paykit: &FakePaykit) -> PaykitClient {
    PaykitClient::new(&paykit.base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(PaykitApi::Upstream)
}

#[test]
fn the_default_setup_status_timeout_is_eight_seconds() {
    assert_eq!(DEFAULT_SETUP_STATUS_TIMEOUT, Duration::from_secs(8));
}

#[tokio::test]
async fn a_slow_ready_inside_the_setup_status_timeout_is_ready() {
    let paykit = upstream_paykit().await;
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "ready");
    paykit.delay_setup_status(SLOW_READY);

    let started = Instant::now();
    assert_eq!(
        upstream_client(&paykit).seller_ready(&seller).await,
        Ok(true)
    );
    assert!(
        started.elapsed() >= SLOW_READY,
        "the answer really took longer than the old 3 s budget"
    );
}

#[tokio::test]
async fn an_answer_slower_than_the_setup_status_timeout_is_an_outage() {
    let paykit = upstream_paykit().await;
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "ready");
    paykit.delay_setup_status(Duration::from_secs(2));

    let started = Instant::now();
    let client = upstream_client(&paykit).with_setup_status_timeout(Duration::from_millis(300));
    assert_eq!(
        client.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the client gave up at its own timeout"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_ready_seller_answering_slowly_reads_ready(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    paykit.delay_setup_status(SLOW_READY);

    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_recent_ready_survives_one_failed_ask(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));

    paykit.fail_setup_status_with(503);
    app.clock.advance_seconds(59);
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("ready"),
        "a failed ask within a minute of ready stays ready"
    );

    paykit.clear_setup_status_failure();
    paykit.set_setup_status_body(json!({ "status": "unavailable" }));
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("ready"),
        "paykit-server answering unavailable is a failed ask too"
    );

    let other = new_actor(&app).await;
    assert_eq!(
        bitcoin_setup(&app, &other.token).await,
        json!("unavailable"),
        "the last ready is per seller"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_last_ready_expires_after_a_minute(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));

    paykit.fail_setup_status_with(503);
    app.clock.advance_seconds(60);
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_read_still_asks_and_setup_required_forgets_the_last_ready(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_setup_status(&seller.pubky, "ready");
    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));

    paykit.set_setup_status(&seller.pubky, "setup_required");
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("setup_required"),
        "a remembered ready never hides a fresh setup_required"
    );

    paykit.fail_setup_status_with(503);
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable"),
        "setup_required forgot the last ready"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unavailable_is_never_remembered(pool: PgPool) {
    let (app, paykit) = upstream_app(pool).await;
    let seller = new_actor(&app).await;

    paykit.set_setup_status(&seller.pubky, "unavailable");
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable")
    );

    paykit.set_setup_status(&seller.pubky, "ready");
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("ready"),
        "the next answer is read, not an earlier unavailable"
    );

    paykit.set_setup_status(&seller.pubky, "setup_required");
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("setup_required")
    );
    paykit.set_setup_status(&seller.pubky, "unavailable");
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable")
    );
    paykit.fail_setup_status_with(503);
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable"),
        "an unavailable answer left nothing behind to serve"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_fork_falls_back_to_a_recent_ready_too(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    paykit.set_claimed(&seller.pubky);
    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));

    paykit.fail_account_exists();
    assert_eq!(bitcoin_setup(&app, &seller.token).await, json!("ready"));
    app.clock.advance_seconds(60);
    assert_eq!(
        bitcoin_setup(&app, &seller.token).await,
        json!("unavailable")
    );
}
