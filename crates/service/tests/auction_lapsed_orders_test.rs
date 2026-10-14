//! An auction winner's order after its winning reservation lapses: the
//! expiry sweep returns the unit and the order sweep cancels the unpaid
//! order, but money can still arrive, either after the cancel (late money)
//! or on time and first seen after the reservation sweep. Either way the item
//! is sold exactly once and a payment that cannot complete the order carries a
//! named reason.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{
    auction_winning_order, enable_bitcoin, poll_now, resolve_call,
    status_confirmed as shared_status_confirmed, OBSERVED_TXID,
};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::expiry::expire_due_reservations;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{drain_outbox, expire_due_payment_windows};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

/// An auction winner's bind carries a sealed shipping address, which an
/// auction order cannot choose a payment method without.
fn winner_bind_body(method: &str) -> serde_json::Value {
    json!({
        "method": method,
        "delivery_address_envelope": {
            "key_id": TEST_DELIVERY_KEY_ID,
            "envelope": test_envelope(1),
        },
    })
}

struct Auction {
    order_id: Uuid,
    aggregate: String,
    seller: TestActor,
    winner: TestActor,
}

/// A SAT auction winner who bound bitcoin on the fork Paykit double and
/// whose request activated. Returns the order and its Paykit reference.
async fn bound_bitcoin_auction(app: &TestApp, paykit: &FakePaykit) -> (Auction, String) {
    let seller = new_actor(app).await;
    let winner = new_actor(app).await;
    let (order_id, aggregate) = auction_winning_order(app, &seller, &winner, 1).await;
    paykit.set_allocation_mode("exclusive");
    enable_bitcoin(app, paykit, &seller).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &winner_bind_body("bitcoin"),
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
    let order_id = Uuid::parse_str(&order_id).expect("order id");
    let reference = attempt_reference(order_id, 1);
    (
        Auction {
            order_id,
            aggregate,
            seller,
            winner,
        },
        reference,
    )
}

async fn reservation_expiry(pool: &PgPool, auction: &Auction) -> DateTime<Utc> {
    sqlx::query_scalar(
        "SELECT expires_at FROM reservations \
         WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&auction.aggregate)
    .bind(&auction.winner.pubky)
    .fetch_one(pool)
    .await
    .expect("reservation row")
}

async fn reservation_status(pool: &PgPool, auction: &Auction) -> String {
    sqlx::query_scalar(
        "SELECT status FROM reservations \
         WHERE listing_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&auction.aggregate)
    .bind(&auction.winner.pubky)
    .fetch_one(pool)
    .await
    .expect("reservation row")
}

async fn listing_quantities(pool: &PgPool, auction: &Auction) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(&auction.aggregate)
    .fetch_one(pool)
    .await
    .expect("listing row")
}

async fn order_payment(
    pool: &PgPool,
    auction: &Auction,
) -> (String, Option<String>, String, Option<String>) {
    sqlx::query_as(
        "SELECT o.state, o.cancellation_reason, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
    )
    .bind(auction.order_id)
    .fetch_one(pool)
    .await
    .expect("order and payment")
}

async fn receipts(pool: &PgPool, auction: &Auction) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM receipts WHERE order_id = $1")
        .bind(auction.order_id)
        .fetch_one(pool)
        .await
        .expect("receipt count")
}

fn paid_once_assertions(
    facts: &(String, Option<String>, String, Option<String>),
    quantities: (i64, i64, i64),
    reservation: &str,
    receipts: i64,
) {
    assert_eq!(facts.0, "paid", "the order completes: {facts:?}");
    assert_eq!(facts.2, "confirmed", "the payment confirms: {facts:?}");
    assert!(facts.3.is_none(), "no review reason: {facts:?}");
    assert_eq!(quantities, (0, 0, 1), "the unit is sold exactly once");
    assert_eq!(reservation, "converted");
    assert_eq!(receipts, 1, "one receipt");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_money_after_the_cancel_completes_the_auction_order_once(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let (auction, reference) = bound_bitcoin_auction(&app, &paykit).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);

    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);
    assert_eq!(
        expire_due_payment_windows(&app.state, after).await.unwrap(),
        1
    );
    assert_eq!(
        order_payment(&pool, &auction).await,
        (
            "cancelled".to_string(),
            Some("payment window elapsed".to_string()),
            "expired".to_string(),
            None
        )
    );
    assert_eq!(listing_quantities(&pool, &auction).await, (1, 0, 0));

    let mut late = shared_status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);
    paid_once_assertions(
        &order_payment(&pool, &auction).await,
        listing_quantities(&pool, &auction).await,
        &reservation_status(&pool, &auction).await,
        receipts(&pool, &auction).await,
    );

    assert_eq!(
        poll_now(&app, after + chrono::Duration::seconds(600)).await,
        0,
        "a settled payment is not polled again"
    );
    assert_eq!(listing_quantities(&pool, &auction).await, (0, 0, 1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_time_bitcoin_money_seen_after_the_reservation_sweep_keeps_the_unit(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let (auction, reference) = bound_bitcoin_auction(&app, &paykit).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);

    // The reservation sweep runs before the worker first sees the payment.
    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);
    assert_eq!(listing_quantities(&pool, &auction).await, (1, 0, 0));

    paykit.set_status(&reference, shared_status_confirmed("exclusive", true, 2));
    assert!(poll_now(&app, after + chrono::Duration::seconds(10)).await >= 1);
    paid_once_assertions(
        &order_payment(&pool, &auction).await,
        listing_quantities(&pool, &auction).await,
        &reservation_status(&pool, &auction).await,
        receipts(&pool, &auction).await,
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_time_bitcoin_money_for_a_unit_that_was_re_listed_names_refund_required(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let (auction, reference) = bound_bitcoin_auction(&app, &paykit).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);
    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);

    take_the_returned_unit(&pool, &auction).await;

    let total_sats: i64 = sqlx::query_scalar("SELECT paykit_total_sats FROM orders WHERE id = $1")
        .bind(auction.order_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    paykit.set_status(
        &reference,
        bitcoin_status_v2(
            "confirmed",
            true,
            "exclusive",
            Some(OBSERVED_TXID),
            Some(total_sats as u64),
            Some(2),
        ),
    );
    assert!(poll_now(&app, after + chrono::Duration::seconds(10)).await >= 1);
    let (order_state, _reason, payment_state, review_reason) = order_payment(&pool, &auction).await;
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(
        review_reason.as_deref(),
        Some("refund_required"),
        "the review carries a reason"
    );
    assert_eq!(
        listing_quantities(&pool, &auction).await,
        (0, 1, 0),
        "the other buyer's unit is untouched"
    );
    assert_eq!(receipts(&pool, &auction).await, 0);
    assert_no_reservation_left_over(&pool, &auction, after).await;

    // Recording the refund ends the review; the reservation sweep still
    // never returns the other buyer's unit.
    let (status, body) = resolve_call(
        &app,
        &auction.seller.token,
        &auction.order_id.to_string(),
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-refund-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund resolution: {body}");
    assert_no_reservation_left_over(&pool, &auction, after).await;
}

/// Another buyer took the unit the reservation sweep returned.
async fn take_the_returned_unit(pool: &PgPool, auction: &Auction) {
    sqlx::query(
        "UPDATE listings SET available_quantity = 0, reserved_quantity = 1, state = 'reserved' \
         WHERE aggregate_id = $1",
    )
    .bind(&auction.aggregate)
    .execute(pool)
    .await
    .unwrap();
}

/// A `refund_required` outcome re-takes nothing: the winning reservation
/// stays `expired`, so no later reservation sweep can return the other
/// buyer's unit.
async fn assert_no_reservation_left_over(pool: &PgPool, auction: &Auction, after: DateTime<Utc>) {
    assert_eq!(
        reservation_status(pool, auction).await,
        "expired",
        "refund_required leaves no reservation behind"
    );
    assert_eq!(
        expire_due_reservations(pool, after + chrono::Duration::hours(2))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        listing_quantities(pool, auction).await,
        (0, 1, 0),
        "the other buyer's unit is still held"
    );
}

fn ipn_body(order_id: Uuid, gross: &str) -> Vec<u8> {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in [
        ("payment_status", "Completed"),
        ("receiver_email", "merchant@example.com"),
        ("business", "merchant@example.com"),
        ("mc_gross", gross),
        ("mc_currency", "USD"),
        ("custom", &order_id.to_string()),
        ("txn_id", "7XP31449AB123456C"),
    ] {
        serializer.append_pair(name, value);
    }
    serializer.finish().into_bytes()
}

/// A USD auction winner who bound PayPal. Returns the order and the IPN
/// gross amount in major units.
async fn bound_paypal_auction(app: &TestApp) -> (Auction, String) {
    let seller = new_actor(app).await;
    let winner = new_actor(app).await;
    let runner_up = new_actor(app).await;
    execute(app, &seller.token, &register_auction_command(&seller.pubky)).await;
    execute(
        app,
        &winner.token,
        &place_bid_command(&seller.pubky, 1, 10_000, 1),
    )
    .await;
    execute(
        app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 2, 8_000, 2),
    )
    .await;
    app.clock.advance_seconds(11 * 60);
    let (status, closed) = execute(
        app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 951),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {closed}");
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config failed: {body}");
    let aggregate = listing_aggregate(&seller.pubky);
    let (order_id, total_minor): (Uuid, i64) = sqlx::query_as(
        "SELECT id, total_minor FROM orders WHERE auction_aggregate_id = $1 AND buyer_pubky = $2",
    )
    .bind(&aggregate)
    .bind(&winner.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("winning order");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&winner.token),
        &winner_bind_body("paypal"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "paypal bind failed: {body}");
    let gross = format!("{}.{:02}", total_minor / 100, total_minor % 100);
    (
        Auction {
            order_id,
            aggregate,
            seller,
            winner,
        },
        gross,
    )
}

async fn post_ipn(app: &TestApp, auction: &Auction, gross: &str) {
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        ipn_body(auction.order_id, gross),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paypal_money_after_the_cancel_completes_the_auction_order_once(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let (auction, gross) = bound_paypal_auction(&app).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);
    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);
    assert_eq!(
        expire_due_payment_windows(&app.state, after).await.unwrap(),
        1
    );
    let facts = order_payment(&pool, &auction).await;
    assert_eq!(
        (facts.0.as_str(), facts.2.as_str()),
        ("cancelled", "expired")
    );
    assert_eq!(listing_quantities(&pool, &auction).await, (1, 0, 0));

    post_ipn(&app, &auction, &gross).await;
    paid_once_assertions(
        &order_payment(&pool, &auction).await,
        listing_quantities(&pool, &auction).await,
        &reservation_status(&pool, &auction).await,
        receipts(&pool, &auction).await,
    );

    post_ipn(&app, &auction, &gross).await;
    assert_eq!(
        listing_quantities(&pool, &auction).await,
        (0, 0, 1),
        "a repeated IPN does not sell the unit again"
    );
    assert_eq!(receipts(&pool, &auction).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_time_paypal_money_seen_after_the_reservation_sweep_keeps_the_unit(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let (auction, gross) = bound_paypal_auction(&app).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);
    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);
    assert_eq!(listing_quantities(&pool, &auction).await, (1, 0, 0));

    post_ipn(&app, &auction, &gross).await;
    paid_once_assertions(
        &order_payment(&pool, &auction).await,
        listing_quantities(&pool, &auction).await,
        &reservation_status(&pool, &auction).await,
        receipts(&pool, &auction).await,
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_time_paypal_money_for_a_unit_that_was_re_listed_names_refund_required(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let (auction, gross) = bound_paypal_auction(&app).await;
    let after = reservation_expiry(&pool, &auction).await + chrono::Duration::seconds(1);
    assert_eq!(expire_due_reservations(&pool, after).await.unwrap(), 1);
    take_the_returned_unit(&pool, &auction).await;

    post_ipn(&app, &auction, &gross).await;
    let (order_state, _reason, payment_state, review_reason) = order_payment(&pool, &auction).await;
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(review_reason.as_deref(), Some("refund_required"));
    assert_eq!(receipts(&pool, &auction).await, 0);
    assert_no_reservation_left_over(&pool, &auction, after).await;

    post_ipn(&app, &auction, &gross).await;
    assert_eq!(
        listing_quantities(&pool, &auction).await,
        (0, 1, 0),
        "a repeated IPN does not take the unit"
    );
}
