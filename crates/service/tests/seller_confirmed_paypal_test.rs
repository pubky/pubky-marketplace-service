//! Seller-confirmed PayPal payments (`PAYMENT_CONFIRMATION=seller`): a
//! verified PayPal notification informs the seller and settles the PayPal
//! payment id, and only the seller's `confirm-received` pays the order. The
//! IPN bodies are the raw fixtures in `tests/fixtures/paypal_ipn`, posted
//! through the real router and the real postback verifier.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use common::paykit_review::delivered_notifications;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::config::{Config, PaymentConfirmation};
use marketplace_service::workers::expire_due_payment_windows;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const PAYMENT_TXN: &str = "7XP31449AB123456C";

type Fields = Vec<(String, String)>;

fn fixture(name: &str, order_id: &str) -> Fields {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    url::form_urlencoded::parse(text.trim_end().as_bytes())
        .map(|(name, value)| {
            let value = if value == "{{ORDER_ID}}" {
                order_id.to_string()
            } else {
                value.into_owned()
            };
            (name.into_owned(), value)
        })
        .collect()
}

fn encode(fields: &Fields) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in fields {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

async fn post_ipn(app: &TestApp, name: &str, order_id: &str) -> StatusCode {
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        encode(&fixture(name, order_id)).into_bytes(),
    )
    .await;
    status
}

async fn read_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

async fn confirm_received(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/confirm-received"),
        Some(token),
        &json!({}),
    )
    .await
}

async fn app_confirmed_by(pool: PgPool, mode: PaymentConfirmation) -> TestApp {
    let mut config = Config::for_tests();
    config.payment_confirmation = mode;
    let (app, _paykit, _ipn) = test_app_with_payments_config(pool, config).await;
    app
}

struct Order {
    seller: TestActor,
    buyer: TestActor,
    id: String,
}

/// A fresh seller with a PayPal email and a PayPal-bound pending order of
/// 137.00 USD.
async fn pending_paypal_order(app: &TestApp) -> Order {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({
            "bitcoin_enabled": false,
            "paypal_merchant_email": "merchant@example.com",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    Order {
        seller,
        buyer,
        id: order.order_id,
    }
}

#[derive(Debug, sqlx::FromRow)]
struct Facts {
    state: String,
    payment_state: String,
    payment_confirmation: Option<String>,
    gateway_payment_seen_at: Option<DateTime<Utc>>,
    paypal_txn_id: Option<String>,
    hold_expires_at: Option<DateTime<Utc>>,
    fiat_verified_by: Option<String>,
}

async fn facts(pool: &PgPool, order_id: &str) -> Facts {
    sqlx::query_as(
        "SELECT o.state, p.state AS payment_state, o.payment_confirmation, \
         o.gateway_payment_seen_at, o.paypal_txn_id, o.hold_expires_at, o.fiat_verified_by \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn confirmed_events(pool: &PgPool, order_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p ON e.aggregate_id = 'payment:' || p.id::text \
         WHERE p.order_id = $1 AND e.kind = 'payment.confirmed'",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_one(pool)
    .await
    .expect("event count")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_verified_paypal_payment_waits_for_the_seller_who_then_pays_the_order(pool: PgPool) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Seller).await;
    let order = pending_paypal_order(&app).await;
    assert_eq!(
        facts(&pool, &order.id)
            .await
            .payment_confirmation
            .as_deref(),
        Some("seller")
    );

    assert_eq!(
        post_ipn(&app, "completed.ipn", &order.id).await,
        StatusCode::OK
    );
    let stored = facts(&pool, &order.id).await;
    assert_eq!(
        stored.state, "pending_payment",
        "the notification alone pays nothing"
    );
    assert_eq!(stored.payment_state, "awaiting_entitlement");
    assert!(stored.gateway_payment_seen_at.is_some());
    assert_eq!(
        stored.paypal_txn_id.as_deref(),
        Some(PAYMENT_TXN),
        "the PayPal payment id still settles, so refunds keep matching"
    );
    let hold = stored.hold_expires_at.expect("held");
    assert!(
        hold >= app.clock.now() + Duration::hours(23),
        "the hold covers the seller window: {hold}"
    );
    assert_eq!(confirmed_events(&pool, &order.id).await, 0);
    let seen = delivered_notifications(&app, &order.seller.token, "paypal_payment_seen").await;
    assert_eq!(
        seen.len(),
        1,
        "the seller is told PayPal reported the payment"
    );
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["next_actor"], json!("seller"));
    assert!(view["gateway_payment_seen_at"].is_string(), "{view}");
    assert_eq!(view["payment_confirmation"], json!("seller"));

    // The buyer who paid can no longer cancel the order out from under it.
    let revision = view["revision"].as_i64().expect("revision");
    let (status, body) = execute(
        &app,
        &order.buyer.token,
        &order_command(
            "order.cancel_request",
            &order.id,
            revision,
            json!({ "reason": "Changed mind" }),
            7,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("This order can no longer be cancelled.")
    );

    // A repeated notification changes nothing and tells the seller once.
    assert_eq!(
        post_ipn(&app, "completed.ipn", &order.id).await,
        StatusCode::OK
    );
    assert_eq!(facts(&pool, &order.id).await.state, "pending_payment");
    assert!(
        delivered_notifications(&app, &order.seller.token, "paypal_payment_seen")
            .await
            .len()
            == 1
    );

    // The buyer cannot confirm; the seller can, once.
    let (status, _) = confirm_received(&app, &order.buyer.token, &order.id).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = confirm_received(&app, &order.seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = facts(&pool, &order.id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.fiat_verified_by.as_deref(), Some("seller"));
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["fiat_verification"], json!("seller-attested"));
    assert_eq!(confirmed_events(&pool, &order.id).await, 1);
    let paid = delivered_notifications(&app, &order.buyer.token, "payment_confirmed").await;
    assert_eq!(paid.len(), 1);
    let (status, _) = confirm_received(&app, &order.seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "a repeat is idempotent");
    assert_eq!(confirmed_events(&pool, &order.id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_after_the_sellers_confirmation_is_still_detected(pool: PgPool) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Seller).await;
    let order = pending_paypal_order(&app).await;
    post_ipn(&app, "completed.ipn", &order.id).await;
    let (status, body) = confirm_received(&app, &order.seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        post_ipn(&app, "refund-full.ipn", &order.id).await,
        StatusCode::OK
    );
    let view = read_order(&app, &order.buyer.token, &order.id).await;
    assert_eq!(view["state"], json!("refunded_external"), "{view}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_paypal_reports_refunded_cannot_be_confirmed_as_received(pool: PgPool) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Seller).await;
    let order = pending_paypal_order(&app).await;
    post_ipn(&app, "completed.ipn", &order.id).await;
    assert_eq!(
        post_ipn(&app, "refund-full.ipn", &order.id).await,
        StatusCode::OK
    );
    let (status, body) = confirm_received(&app, &order.seller.token, &order.id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("payment_taken_back"));
    assert_eq!(facts(&pool, &order.id).await.state, "pending_payment");
    assert_eq!(confirmed_events(&pool, &order.id).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn after_the_seller_window_the_order_expires_and_a_late_confirmation_takes_the_late_path(
    pool: PgPool,
) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Seller).await;
    let order = pending_paypal_order(&app).await;
    post_ipn(&app, "completed.ipn", &order.id).await;

    // The 10-minute PayPal window passes: the seller window keeps the hold.
    app.clock.advance_seconds(3_600);
    expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("sweep runs");
    assert_eq!(facts(&pool, &order.id).await.state, "pending_payment");

    // After the seller window the ordinary expiry applies.
    app.clock.advance_seconds(24 * 3_600);
    expire_due_payment_windows(&app.state, app.clock.now())
        .await
        .expect("sweep runs");
    let stored = facts(&pool, &order.id).await;
    assert_eq!(stored.state, "cancelled");
    assert_eq!(stored.payment_state, "expired");

    // The seller who then finds the money in PayPal still confirms it late.
    let (status, body) = confirm_received(&app, &order.seller.token, &order.id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(
        facts(&pool, &order.id).await.payment_state,
        "awaiting_entitlement",
        "the late confirmation is recorded, never dropped"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn automatic_confirmation_still_pays_on_the_verified_notification(pool: PgPool) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Automatic).await;
    let order = pending_paypal_order(&app).await;
    assert_eq!(
        facts(&pool, &order.id)
            .await
            .payment_confirmation
            .as_deref(),
        Some("automatic")
    );
    post_ipn(&app, "completed.ipn", &order.id).await;
    let stored = facts(&pool, &order.id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.fiat_verified_by.as_deref(), Some("gateway"));
    assert!(stored.gateway_payment_seen_at.is_none());
}

// --- interleaving (money path) ----------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_notifications_and_seller_confirmations_pay_exactly_once(pool: PgPool) {
    let app = app_confirmed_by(pool.clone(), PaymentConfirmation::Seller).await;
    let order = pending_paypal_order(&app).await;
    let (ipn_a, ipn_b, confirm_a, confirm_b) = tokio::join!(
        post_ipn(&app, "completed.ipn", &order.id),
        post_ipn(&app, "completed.ipn", &order.id),
        confirm_received(&app, &order.seller.token, &order.id),
        confirm_received(&app, &order.seller.token, &order.id),
    );
    assert_eq!(ipn_a, StatusCode::OK);
    assert_eq!(ipn_b, StatusCode::OK);
    for (status, body) in [&confirm_a, &confirm_b] {
        assert_eq!(*status, StatusCode::OK, "{body}");
    }
    let stored = facts(&pool, &order.id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.paypal_txn_id.as_deref(), Some(PAYMENT_TXN));
    assert!(stored.gateway_payment_seen_at.is_some());
    assert_eq!(confirmed_events(&pool, &order.id).await, 1);
    let receipts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM receipts WHERE order_id = $1::uuid")
            .bind(&order.id)
            .fetch_one(&pool)
            .await
            .expect("receipt count");
    assert_eq!(receipts, 1);
    assert!(
        delivered_notifications(&app, &order.seller.token, "paypal_payment_seen")
            .await
            .len()
            <= 1
    );
}
