//! `bitcoin_setup` on the seller's own payment config: paykit-server's
//! answer to "can this seller receive Bitcoin?", read on every own-config
//! GET and PUT whatever the USDT flag, so the Shop can confirm a Bitkit
//! setup without a Lock Server session.

mod common;

use axum::http::StatusCode;
use common::*;
use marketplace_service::config::Config;
use marketplace_service::payments::PaykitApi;
use serde_json::{json, Value};
use sqlx::PgPool;

const OWN: &str = "/v0/sellers/me/payment-config";
const EPOCH: &str = "1970-01-01T00:00:00.000Z";

async fn get_own(app: &TestApp, token: &str) -> Value {
    let (status, body) = send(app.router.clone(), "GET", OWN, Some(token), &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "own config read failed: {body}");
    body
}

async fn put_own(app: &TestApp, token: &str, bitcoin_enabled: bool) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        OWN,
        Some(token),
        &json!({ "bitcoin_enabled": bitcoin_enabled, "paypal_merchant_email": null }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "own config save failed: {body}");
    body
}

fn btc_setup_calls(paykit: &FakePaykit) -> usize {
    paykit
        .calls()
        .iter()
        .filter(|call| call.path == "/setup/status" && call.body["asset"] == json!("BTC"))
        .count()
}

async fn upstream_flag_off(pool: PgPool) -> (TestApp, FakePaykit) {
    test_app_with_paykit_api_config(pool, Config::for_tests(), PaykitApi::Upstream).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_who_never_saved_reads_the_empty_config_with_bitcoin_setup(pool: PgPool) {
    let (app, paykit) = upstream_flag_off(pool).await;
    let seller = new_actor(&app).await;

    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body,
        json!({
            "payment_config": {
                "bitcoin_enabled": false,
                "bitcoin_setup": "setup_required",
                "paypal_merchant_email": null,
                "updated_at": EPOCH,
            }
        })
    );
    assert_eq!(btc_setup_calls(&paykit), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn upstream_reports_ready_before_bitcoin_is_switched_on_and_never_caches(pool: PgPool) {
    let (app, paykit) = upstream_flag_off(pool).await;
    let seller = new_actor(&app).await;

    let saved = put_own(&app, &seller.token, false).await;
    assert_eq!(
        saved["payment_config"]["bitcoin_setup"],
        json!("setup_required")
    );

    paykit.set_setup_status(&seller.pubky, "ready");
    let body = get_own(&app, &seller.token).await;
    assert_eq!(body["payment_config"]["bitcoin_enabled"], json!(false));
    assert_eq!(body["payment_config"]["bitcoin_setup"], json!("ready"));

    paykit.set_setup_status(&seller.pubky, "setup_required");
    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body["payment_config"]["bitcoin_setup"],
        json!("setup_required"),
        "every read asks paykit-server again"
    );
    assert_eq!(btc_setup_calls(&paykit), 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paykit_outage_reads_as_unavailable_and_never_fails_the_read(pool: PgPool) {
    let (app, paykit) = upstream_flag_off(pool).await;
    let seller = new_actor(&app).await;
    put_own(&app, &seller.token, true).await;

    paykit.fail_setup_status_with(503);
    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body["payment_config"]["bitcoin_setup"],
        json!("unavailable")
    );
    assert_eq!(body["payment_config"]["bitcoin_enabled"], json!(true));

    paykit.clear_setup_status_failure();
    paykit.set_setup_status_body(json!({ "status": "ready", "extra": true }));
    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body["payment_config"]["bitcoin_setup"],
        json!("unavailable"),
        "an answer outside the closed contract is not ready"
    );

    paykit.set_setup_status_body(json!({ "status": "unavailable" }));
    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body["payment_config"]["bitcoin_setup"],
        json!("unavailable")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_fork_answers_from_the_claimed_watch_only_account(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;

    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body["payment_config"]["bitcoin_setup"],
        json!("setup_required")
    );

    paykit.set_claimed(&seller.pubky);
    let body = get_own(&app, &seller.token).await;
    assert_eq!(body["payment_config"]["bitcoin_setup"], json!("ready"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_paykit_the_read_still_answers_unavailable(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    let body = get_own(&app, &seller.token).await;
    assert_eq!(
        body,
        json!({
            "payment_config": {
                "bitcoin_enabled": false,
                "bitcoin_setup": "unavailable",
                "paypal_merchant_email": null,
                "updated_at": EPOCH,
            }
        })
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_usdt_flag_adds_its_fields_beside_bitcoin_setup(pool: PgPool) {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = true;
    let (app, paykit) = test_app_with_paykit_api_config(pool, config, PaykitApi::Upstream).await;
    let seller = new_actor(&app).await;
    paykit.set_claimed(&seller.pubky);

    let body = get_own(&app, &seller.token).await;
    let config = &body["payment_config"];
    assert_eq!(config["bitcoin_setup"], json!("ready"));
    for key in ["usdt_enabled", "usdt_setup", "usdt_setup_action"] {
        assert!(config.get(key).is_some(), "{key} missing: {config}");
    }
}
