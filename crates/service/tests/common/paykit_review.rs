use super::{
    bitcoin_status_v2, checkout_command_with_id, execute, indexed_command_id,
    register_listing_command, register_sat_command, send, FakePaykit, PendingOrder, TestActor,
    TestApp,
};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use marketplace_service::bitcoin_review::{
    route_due_seller_confirmation_windows, SELLER_CONFIRMATION_WINDOW_SECONDS,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{
    close_due_auctions, drain_outbox, expire_due_payment_windows, verify_due_paykit_payments,
};
use serde_json::{json, Value};
use tower::util::ServiceExt;
use uuid::Uuid;

pub const TOTAL_SATS: i64 = 51_637;
pub const OBSERVED_TXID: &str = "9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";

pub fn status_detected(mode: &str, confirmations: u32) -> Value {
    bitcoin_status_v2(
        "detected",
        true,
        mode,
        Some(OBSERVED_TXID),
        Some(TOTAL_SATS as u64),
        Some(confirmations),
    )
}

pub fn status_confirmed(mode: &str, amount_matched: bool, confirmations: u32) -> Value {
    bitcoin_status_v2(
        "confirmed",
        amount_matched,
        mode,
        Some(OBSERVED_TXID),
        Some(TOTAL_SATS as u64),
        Some(confirmations),
    )
}

pub async fn poll_now(app: &TestApp, now: DateTime<Utc>) -> u64 {
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client configured");
    verify_due_paykit_payments(&app.state, client, now)
        .await
        .expect("poll runs")
}

/// Delivers every pending outbox intent, then reads the caller's
/// notifications through `GET /v1/notifications`, filtered to `kind`.
pub async fn delivered_notifications(app: &TestApp, token: &str, kind: &str) -> Vec<Value> {
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (status, body) = send(
        app.router.clone(),
        "GET",
        "/v1/notifications",
        Some(token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "notifications read failed: {body}");
    body["notifications"]
        .as_array()
        .expect("notifications array")
        .iter()
        .filter(|notification| notification["type"] == json!(kind))
        .cloned()
        .collect()
}

pub async fn enable_bitcoin(app: &TestApp, paykit: &FakePaykit, seller: &TestActor) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    paykit.set_claimed(&seller.pubky);
}

pub async fn create_sat_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
) -> PendingOrder {
    let (status, body) =
        execute(app, &seller.token, &register_sat_command(&seller.pubky, 16)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", seller.pubky))
            .fetch_one(&app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id present")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id present")
            .to_string(),
    }
}

pub async fn bound_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
    mode: &str,
) -> (String, String, String) {
    paykit.set_allocation_mode(mode);
    enable_bitcoin(app, paykit, seller).await;
    let order = create_sat_order(app, seller, buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "bitcoin" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = attempt_reference(Uuid::parse_str(&order.order_id).expect("order uuid"), 1);
    (order.order_id, order.payment_id, reference)
}

pub async fn bound_shared_manual_order(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String, String) {
    bound_order(app, paykit, seller, buyer, "shared_manual").await
}

pub async fn into_awaiting_confirmation(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, payment_id, reference) =
        bound_shared_manual_order(app, paykit, seller, buyer).await;
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 2));
    assert!(poll_now(app, app.clock.now()).await >= 1);
    let (request_state, payment_state): (String, String) = sqlx::query_as(
        "SELECT o.paykit_request_state, p.state \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("target awaiting order");
    assert_eq!(request_state, "awaiting_seller_confirmation");
    assert_eq!(payment_state, "awaiting_entitlement");
    (order_id, payment_id)
}

pub async fn into_manual_review_held(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, payment_id) = into_awaiting_confirmation(app, paykit, seller, buyer).await;
    let routed = route_due_seller_confirmation_windows(
        &app.state,
        app.clock.now() + chrono::Duration::seconds(SELLER_CONFIRMATION_WINDOW_SECONDS),
    )
    .await
    .expect("seller-window reaper runs");
    assert!(routed >= 1);
    let (request_state, payment_state, stock_held): (String, String, bool) = sqlx::query_as(
        "SELECT o.paykit_request_state, p.state, o.stock_held \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("target held order");
    assert_eq!(request_state, "confirmed");
    assert_eq!(payment_state, "manual_review");
    assert!(stock_held);
    let manual_review_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p \
         ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = 'payment.manual_review'",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("target held audit count");
    assert_eq!(manual_review_events, 1);
    (order_id, payment_id)
}

pub async fn into_manual_review_late(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, payment_id, reference) =
        bound_shared_manual_order(app, paykit, seller, buyer).await;
    let after_window = app.clock.now() + chrono::Duration::seconds(7300);
    assert!(
        expire_due_payment_windows(&app.state, after_window)
            .await
            .expect("payment-window reaper runs")
            >= 1
    );
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 0, \
         sold_quantity = total_quantity, state = 'sold' \
         WHERE aggregate_id = $1",
    )
    .bind(format!("listing:{}_boots_01", seller.pubky))
    .execute(&app.pool)
    .await
    .expect("sold-out stock so late money cannot reacquire");
    let mut late_status = status_confirmed("shared_manual", true, 2);
    late_status["late_settlement"] = json!(true);
    paykit.set_status(&reference, late_status);
    assert!(poll_now(app, after_window + chrono::Duration::seconds(60)).await >= 1);
    let (order_state, payment_state, stock_held, review_reason): (
        String,
        String,
        bool,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT o.state, p.state, o.stock_held, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("target late order");
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert!(!stock_held);
    assert_eq!(review_reason.as_deref(), Some("refund_required"));
    let manual_review_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events e JOIN payments p \
         ON e.aggregate_id = ('payment:' || p.id::text) \
         WHERE p.order_id = $1 AND e.kind = 'payment.manual_review'",
    )
    .bind(Uuid::parse_str(&order_id).expect("order uuid"))
    .fetch_one(&app.pool)
    .await
    .expect("target late audit count");
    assert_eq!(manual_review_events, 1);
    (order_id, payment_id)
}

pub async fn into_manual_review_mismatch(
    app: &TestApp,
    paykit: &FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> (String, String) {
    let (order_id, payment_id, reference) =
        bound_order(app, paykit, seller, buyer, "exclusive").await;
    paykit.set_status(&reference, status_confirmed("exclusive", false, 2));
    assert_eq!(poll_now(app, app.clock.now()).await, 1);
    (order_id, payment_id)
}

pub async fn confirm_call(
    app: &TestApp,
    token: &str,
    order_id: &str,
    body: &Value,
) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/confirm-bitcoin-payment"),
        Some(token),
        body,
    )
    .await
}

pub async fn resolve_call(
    app: &TestApp,
    token: &str,
    order_id: &str,
    key: Option<Uuid>,
    body: &Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/v0/orders/{order_id}/bitcoin/resolve"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key.to_string());
    }
    let request = request
        .body(Body::from(
            serde_json::to_vec(body).expect("body serializes"),
        ))
        .expect("request builds");
    let response = app
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("request executes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

/// A SAT auction command fixture (the USD fixture's shape, priced in sats
/// so the winner can bind bitcoin). Each auction gets a distinct listing id
/// and command id, so several auctions can run in one test.
pub fn register_sat_auction_command(
    seller_pubky: &str,
    listing_id: &str,
    index: u64,
    starts_at: DateTime<Utc>,
) -> Value {
    let mut command = register_listing_command(seller_pubky, listing_id, 1, index + 100);
    command["payload"]["unit_price"] =
        json!({ "amount_minor": 45_000, "currency": "SAT", "exponent": 0 });
    command["payload"]["sale_format"] = json!("auction");
    command["payload"]["auction_terms"] = json!({
        "starts_at": marketplace_service::clock::format_timestamp(starts_at),
        "ends_at": marketplace_service::clock::format_timestamp(
            starts_at + chrono::Duration::seconds(600),
        ),
        "minimum_increment": { "amount_minor": 5_000, "currency": "SAT", "exponent": 0 },
        "anti_sniping_window_seconds": 60,
        "anti_sniping_extension_seconds": 120,
    });
    command["payload"]["auction_reserve"] = json!({
        "expected_record_revision": 0,
        "record_revision": 1,
        "reserve_price": { "amount_minor": 45_000, "currency": "SAT", "exponent": 0 },
    });
    command
}

pub fn auction_listing(seller_pubky: &str, index: u64) -> (String, String) {
    let listing_id = format!("auction_{index}");
    (
        listing_id.clone(),
        format!("listing:{seller_pubky}_{listing_id}"),
    )
}

/// Closes a SAT auction with one winning bid and returns the winner's
/// order id plus the listing aggregate id.
pub async fn auction_winning_order(
    app: &TestApp,
    seller: &TestActor,
    winner: &TestActor,
    index: u64,
) -> (String, String) {
    let (listing_id, aggregate) = auction_listing(&seller.pubky, index);
    let (status, body) = execute(
        app,
        &seller.token,
        &register_sat_auction_command(&seller.pubky, &listing_id, index, app.clock.now()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register auction: {body}");
    let bid = json!({
        "version": 1,
        "command_id": indexed_command_id(0x8001, index),
        "aggregate_id": aggregate,
        "expected_revision": 1,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "auction.place_bid",
        "payload": {
            "maximum_amount": { "amount_minor": 100_000, "currency": "SAT", "exponent": 0 },
        },
    });
    let (status, body) = execute(app, &winner.token, &bid).await;
    assert_eq!(status, StatusCode::OK, "bid: {body}");
    app.clock.advance_seconds(660);
    let closed = close_due_auctions(&app.pool, app.clock.now())
        .await
        .expect("close runs");
    assert_eq!(closed.closed, 1, "the auction closed");
    assert_eq!(closed.failed, 0, "the auction close did not fail");
    let (order_id,): (String,) = sqlx::query_as(
        "SELECT id::text FROM orders WHERE auction_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("winning order");
    (order_id, aggregate)
}
