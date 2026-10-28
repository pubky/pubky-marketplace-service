//! Digital release split from payment (`DIGITAL_RELEASE`): under `seller`
//! the payment confirmation pins the order's instant lines but does not
//! release them; the seller's `fulfillment.release_digital`, or a seller
//! confirmation that asks for it, is the release. Under `payment` the
//! confirmation releases them as before. Payments are confirmed through
//! verified PayPal IPN fixtures and the Paykit worker poll.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{confirm_call, poll_now, status_confirmed};
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::config::{Config, DigitalRelease, PaymentConfirmation};
use marketplace_service::http::build_router;
use marketplace_service::payments::attempt_reference;
use marketplace_service::workers::{assume_due_deliveries, drain_outbox};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const TEXT: &str = "LICENCE-RELEASE-SENTINEL";
const EMAIL: &str = "alice.example@example.com";
/// The gross of `tests/fixtures/paypal_ipn/completed.ipn`.
const PAYPAL_TOTAL_MINOR: i64 = 13_700;

static NUMBER: AtomicU64 = AtomicU64::new(1);

fn next() -> u64 {
    NUMBER.fetch_add(1, Ordering::Relaxed)
}

async fn app_with(
    pool: PgPool,
    payment: PaymentConfirmation,
    release: DigitalRelease,
) -> (TestApp, FakePaykit) {
    let mut config = Config::for_tests();
    config.payment_confirmation = payment;
    config.digital_release = release;
    let (app, paykit, _ipn) = test_app_with_payments_config(pool, config).await;
    let state = app.state.clone().with_digital(Some(test_digital_keys()));
    (
        TestApp {
            router: build_router(state.clone()),
            pool: app.pool,
            clock: app.clock,
            state,
        },
        paykit,
    )
}

fn aggregate(seller: &str, listing_id: &str) -> String {
    format!("listing:{seller}_{listing_id}")
}

async fn register(app: &TestApp, seller: &TestActor, listing_id: &str, unit_price: Value) {
    let mut command = register_command(&seller.pubky, 5);
    command["command_id"] = json!(indexed_command_id(0xe400, next()));
    command["aggregate_id"] = json!(aggregate(&seller.pubky, listing_id));
    command["payload"]["listing_id"] = json!(listing_id);
    command["payload"]["fulfillment_methods"] = json!(["digital"]);
    command["payload"]["unit_price"] = unit_price;
    let (status, body) = execute(app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
}

async fn set_delivery(app: &TestApp, seller: &TestActor, listing_id: &str, delivery: Value) {
    let (status, body) = execute(
        app,
        &seller.token,
        &json!({
            "version": 1,
            "command_id": indexed_command_id(0xe500, next()),
            "aggregate_id": aggregate(&seller.pubky, listing_id),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "digital_delivery.set",
            "payload": { "expected_version": 0, "delivery": delivery },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set failed: {body}");
}

async fn listing_revision(app: &TestApp, seller: &str, listing_id: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(aggregate(seller, listing_id))
        .fetch_one(&app.pool)
        .await
        .expect("listing row")
}

/// Checks out one digital order of `listing_ids` from `seller`.
async fn checkout(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listing_ids: &[&str],
    delivery_email: Option<&str>,
) -> String {
    let command_id = indexed_command_id(0xe600, next());
    let mut lines = Vec::new();
    for listing_id in listing_ids {
        lines.push(json!({
            "listing_aggregate_id": aggregate(&seller.pubky, listing_id),
            "expected_revision": listing_revision(app, &seller.pubky, listing_id).await,
            "quantity": 1,
            "fulfillment": "digital",
        }));
    }
    let mut payload = json!({ "lines": lines, "guarantee_policy_version": 1 });
    if let Some(email) = delivery_email {
        payload["delivery_email"] = json!(email);
    }
    let (status, body) = execute(
        app,
        &buyer.token,
        &json!({
            "version": 1,
            "command_id": command_id,
            "aggregate_id": format!("checkout:{command_id}"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "checkout.create",
            "payload": payload,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let orders = body["result"]["orders"].as_array().expect("orders");
    assert_eq!(orders.len(), 1, "one digital order");
    orders[0]["id"].as_str().expect("order id").to_string()
}

async fn enable_paypal(app: &TestApp, seller: &TestActor) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
}

async fn bind(app: &TestApp, buyer: &TestActor, order_id: &str, method: &str) {
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": method }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
}

/// A text listing, checked out and bound to PayPal.
async fn paypal_text_order(app: &TestApp) -> (TestActor, TestActor, String) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    enable_paypal(app, &seller).await;
    register(
        app,
        &seller,
        "guide_01",
        json!({ "amount_minor": PAYPAL_TOTAL_MINOR, "currency": "USD", "exponent": 2 }),
    )
    .await;
    set_delivery(
        app,
        &seller,
        "guide_01",
        json!({ "kind": "text", "text": TEXT }),
    )
    .await;
    let order_id = checkout(app, &seller, &buyer, &["guide_01"], None).await;
    bind(app, &buyer, &order_id, "paypal").await;
    (seller, buyer, order_id)
}

async fn post_completed_ipn(app: &TestApp, order_id: &str) -> StatusCode {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/completed.ipn",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (field, value) in url::form_urlencoded::parse(text.trim_end().as_bytes()) {
        let value = if value == "{{ORDER_ID}}" {
            order_id.into()
        } else {
            value
        };
        serializer.append_pair(&field, &value);
    }
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        serializer.finish().into_bytes(),
    )
    .await;
    status
}

#[derive(Debug, sqlx::FromRow)]
struct Facts {
    state: String,
    receipt: bool,
    digital_release: Option<String>,
    released: bool,
    revision: i64,
}

async fn facts(app: &TestApp, order_id: &str) -> Facts {
    sqlx::query_as(
        "SELECT state, receipt_id IS NOT NULL AS receipt, digital_release, \
         digital_released_at IS NOT NULL AS released, revision FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order facts")
}

async fn read_line(app: &TestApp, token: &str, order_id: &str, line: usize) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}/digital-delivery/{line}"),
        Some(token),
        &Value::Null,
    )
    .await
}

async fn release(app: &TestApp, token: &str, order_id: &str) -> (StatusCode, Value) {
    let revision = facts(app, order_id).await.revision;
    execute(
        app,
        token,
        &order_command(
            "fulfillment.release_digital",
            order_id,
            revision,
            json!({}),
            next(),
        ),
    )
    .await
}

async fn events(app: &TestApp, order_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:' || $1 AND kind = $2",
    )
    .bind(order_id)
    .bind(kind)
    .fetch_one(&app.pool)
    .await
    .expect("event count")
}

async fn buyer_notices(app: &TestApp, order_id: &str, buyer: &TestActor, kind: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.' || $3 \
         AND payload->>'aggregate_id' = 'order:' || $1 AND payload->>'recipient_pubky' = $2",
    )
    .bind(order_id)
    .bind(&buyer.pubky)
    .bind(kind)
    .fetch_one(&app.pool)
    .await
    .expect("notification count")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paid_order_waits_for_the_sellers_release(pool: PgPool) {
    let (app, _paykit) =
        app_with(pool, PaymentConfirmation::Automatic, DigitalRelease::Seller).await;
    let (seller, buyer, order_id) = paypal_text_order(&app).await;
    assert_eq!(post_completed_ipn(&app, &order_id).await, StatusCode::OK);

    let paid = facts(&app, &order_id).await;
    assert_eq!(paid.state, "paid", "paid, not delivered");
    assert!(paid.receipt);
    assert_eq!(paid.digital_release.as_deref(), Some("seller"));
    assert!(!paid.released);
    let (status, body) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("not_released"));
    assert_eq!(
        buyer_notices(&app, &order_id, &buyer, "order_delivered").await,
        0
    );

    // The delivered-assumption sweep never assumes an unreleased purchase.
    let later = app.clock.now() + chrono::Duration::days(90);
    assume_due_deliveries(&app.pool, later, 14, 100, 10)
        .await
        .expect("sweep runs");
    assert_eq!(facts(&app, &order_id).await.state, "paid");

    let (status, body) = release(&app, &seller.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("delivered"));
    assert!(
        body["result"]["order"]["digital_released_at"].is_string(),
        "{body}"
    );
    let released = facts(&app, &order_id).await;
    assert_eq!(
        (released.state.as_str(), released.released),
        ("delivered", true)
    );
    assert_eq!(
        events(&app, &order_id, "fulfillment.digital_delivered").await,
        1
    );
    assert_eq!(
        buyer_notices(&app, &order_id, &buyer, "order_delivered").await,
        1
    );

    let (status, body) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["lines"][0]["text"], json!(TEXT), "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_the_seller_releases_and_only_once(pool: PgPool) {
    let (app, _paykit) =
        app_with(pool, PaymentConfirmation::Automatic, DigitalRelease::Seller).await;
    let (seller, buyer, order_id) = paypal_text_order(&app).await;

    let (status, body) = release(&app, &seller.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "nothing is paid yet: {body}");

    post_completed_ipn(&app, &order_id).await;
    let (status, body) = release(&app, &buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = release(&app, &seller.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = release(&app, &seller.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        events(&app, &order_id, "fulfillment.digital_delivered").await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn payment_mode_releases_at_payment_as_before(pool: PgPool) {
    let (app, _paykit) = app_with(
        pool,
        PaymentConfirmation::Automatic,
        DigitalRelease::Payment,
    )
    .await;
    let (seller, buyer, order_id) = paypal_text_order(&app).await;
    post_completed_ipn(&app, &order_id).await;

    let paid = facts(&app, &order_id).await;
    assert_eq!(paid.state, "delivered");
    assert_eq!(paid.digital_release.as_deref(), Some("payment"));
    assert!(paid.released);
    let (status, _) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = release(&app, &seller.token, &order_id).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_paid_before_the_switch_existed_stays_released(pool: PgPool) {
    let (app, _paykit) = app_with(
        pool,
        PaymentConfirmation::Automatic,
        DigitalRelease::Payment,
    )
    .await;
    let (_seller, buyer, order_id) = paypal_text_order(&app).await;
    post_completed_ipn(&app, &order_id).await;
    sqlx::query(
        "UPDATE orders SET digital_release = NULL, digital_released_at = NULL WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .execute(&app.pool)
    .await
    .expect("present the order as pre-0092");
    let (status, body) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_sellers_paypal_confirmation_can_release_in_the_same_step(pool: PgPool) {
    let (app, _paykit) = app_with(pool, PaymentConfirmation::Seller, DigitalRelease::Seller).await;

    // Asked to release: paid and released, as two recorded steps.
    let (seller, buyer, order_id) = paypal_text_order(&app).await;
    post_completed_ipn(&app, &order_id).await;
    assert_eq!(facts(&app, &order_id).await.state, "pending_payment");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/confirm-received"),
        Some(&seller.token),
        &json!({ "release_digital": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let done = facts(&app, &order_id).await;
    assert_eq!((done.state.as_str(), done.released), ("delivered", true));
    assert_eq!(events(&app, &order_id, "receipt.issued").await, 1);
    assert_eq!(
        events(&app, &order_id, "fulfillment.digital_delivered").await,
        1
    );
    assert_eq!(
        buyer_notices(&app, &order_id, &buyer, "order_delivered").await,
        1
    );
    let (status, _) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::OK);

    // Not asked: paid, and the purchase still waits for the release.
    let (seller, buyer, order_id) = paypal_text_order(&app).await;
    post_completed_ipn(&app, &order_id).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/fiat/confirm-received"),
        Some(&seller.token),
        &json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let paid = facts(&app, &order_id).await;
    assert_eq!((paid.state.as_str(), paid.released), ("paid", false));
    let (status, _) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_sellers_bitcoin_confirmation_can_release_in_the_same_step(pool: PgPool) {
    let (app, paykit) =
        app_with(pool, PaymentConfirmation::Automatic, DigitalRelease::Seller).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    paykit.set_allocation_mode("shared_manual");
    common::paykit_review::enable_bitcoin(&app, &paykit, &seller).await;
    register(
        &app,
        &seller,
        "guide_01",
        json!({ "amount_minor": 50_000, "currency": "SAT", "exponent": 0 }),
    )
    .await;
    set_delivery(
        &app,
        &seller,
        "guide_01",
        json!({ "kind": "text", "text": TEXT }),
    )
    .await;
    let order_id = checkout(&app, &seller, &buyer, &["guide_01"], None).await;
    bind(&app, &buyer, &order_id, "bitcoin").await;
    let client = app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&app.pool, client, app.clock.now(), 30)
        .await
        .expect("activation delivers");
    let reference = attempt_reference(Uuid::parse_str(&order_id).expect("order uuid"), 1);
    paykit.set_status(&reference, status_confirmed("shared_manual", true, 2));
    assert!(poll_now(&app, app.clock.now()).await >= 1);
    assert_eq!(facts(&app, &order_id).await.state, "pending_payment");

    let (status, body) = confirm_call(
        &app,
        &seller.token,
        &order_id,
        &json!({ "release_digital": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("delivered"));
    let (status, read) = read_line(&app, &buyer.token, &order_id, 0).await;
    assert_eq!(status, StatusCode::OK, "{read}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_mixed_order_is_delivered_once_both_the_release_and_the_email_are_done(pool: PgPool) {
    let (app, _paykit) =
        app_with(pool, PaymentConfirmation::Automatic, DigitalRelease::Seller).await;
    for release_first in [true, false] {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        enable_paypal(&app, &seller).await;
        // Two lines of 68.50 USD: the order total is the IPN fixture's 137.00.
        let price =
            json!({ "amount_minor": PAYPAL_TOTAL_MINOR / 2, "currency": "USD", "exponent": 2 });
        register(&app, &seller, "guide_01", price.clone()).await;
        set_delivery(
            &app,
            &seller,
            "guide_01",
            json!({ "kind": "text", "text": TEXT }),
        )
        .await;
        register(&app, &seller, "guide_02", price).await;
        set_delivery(&app, &seller, "guide_02", json!({ "kind": "email" })).await;
        let order_id = checkout(
            &app,
            &seller,
            &buyer,
            &["guide_01", "guide_02"],
            Some(EMAIL),
        )
        .await;
        bind(&app, &buyer, &order_id, "paypal").await;
        assert_eq!(post_completed_ipn(&app, &order_id).await, StatusCode::OK);
        assert_eq!(facts(&app, &order_id).await.state, "paid");

        if release_first {
            let (status, body) = release(&app, &seller.token, &order_id).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                body["result"]["order"]["state"],
                json!("paid"),
                "the email is still due"
            );
            assert_eq!(
                buyer_notices(&app, &order_id, &buyer, "digital_released").await,
                1
            );
            let (status, body) = mark_emailed(&app, &seller, &order_id).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        } else {
            let (status, body) = mark_emailed(&app, &seller, &order_id).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(
                facts(&app, &order_id).await.state,
                "paid",
                "the instant line is still unreleased"
            );
            let (status, body) = release(&app, &seller.token, &order_id).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }
        assert_eq!(
            facts(&app, &order_id).await.state,
            "delivered",
            "release first: {release_first}"
        );
        assert_eq!(
            buyer_notices(&app, &order_id, &buyer, "order_delivered").await,
            1
        );
    }
}

async fn mark_emailed(app: &TestApp, seller: &TestActor, order_id: &str) -> (StatusCode, Value) {
    let revision = facts(app, order_id).await.revision;
    execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.deliver_digital",
            order_id,
            revision,
            json!({ "channel": "email" }),
            next(),
        ),
    )
    .await
}

// --- interleaving ------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_releases_and_a_cancel_request_have_one_winner(pool: PgPool) {
    let (app, _paykit) =
        app_with(pool, PaymentConfirmation::Automatic, DigitalRelease::Seller).await;
    for _ in 0..4 {
        let (seller, buyer, order_id) = paypal_text_order(&app).await;
        post_completed_ipn(&app, &order_id).await;
        let revision = facts(&app, &order_id).await.revision;
        let release_command = |number| {
            order_command(
                "fulfillment.release_digital",
                &order_id,
                revision,
                json!({}),
                number,
            )
        };
        let cancel = order_command(
            "order.cancel_request",
            &order_id,
            revision,
            json!({ "reason": "Changed mind" }),
            next(),
        );
        let (release_a, release_b) = (release_command(next()), release_command(next()));
        let (first, second, cancelled) = tokio::join!(
            execute(&app, &seller.token, &release_a),
            execute(&app, &seller.token, &release_b),
            execute(&app, &buyer.token, &cancel),
        );
        let winners = [&first, &second, &cancelled]
            .iter()
            .filter(|(status, _)| *status == StatusCode::OK)
            .count();
        assert_eq!(
            winners, 1,
            "one command wins the revision: {first:?} {second:?} {cancelled:?}"
        );
        let end = facts(&app, &order_id).await;
        let released = events(&app, &order_id, "fulfillment.digital_delivered").await;
        if cancelled.0 == StatusCode::OK {
            assert_eq!(end.state, "cancel_requested");
            assert!(!end.released);
            assert_eq!(released, 0);
            let (status, _) = read_line(&app, &buyer.token, &order_id, 0).await;
            assert_eq!(status, StatusCode::CONFLICT, "nothing was released");
        } else {
            assert_eq!(end.state, "delivered");
            assert!(end.released);
            assert_eq!(released, 1);
            assert_eq!(
                buyer_notices(&app, &order_id, &buyer, "order_delivered").await,
                1
            );
        }
    }
}
