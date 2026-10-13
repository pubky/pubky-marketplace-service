//! Seller-set post-purchase periods: the delivery-assumed and auto-complete
//! periods come from the seller's shop record (`deliveryAssumeDays`,
//! `autoCompleteDays`), are frozen on the order when its payment starts, and
//! drive the sweeps per order. The configured `DELIVERY_ASSUME_DAYS` /
//! `AUTO_COMPLETE_DAYS` apply only to a period the seller left unset. The
//! checkout `guarantee_policy_version` is retired: accepted, never stored or
//! projected.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use common::{
    captured_logs, checkout_command, checkout_command_with_id, count, create_pending_order,
    execute, indexed_command_id, install_log_capture, new_actor, order_command, payment_command,
    register_command, send, test_app_with_payments, test_app_with_shop_homeserver,
    with_shop_homeserver, FakeHomeserver, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::{
    DEFAULT_AUTO_COMPLETE_DAYS, DEFAULT_DELIVERY_ASSUME_DAYS, DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
};
use marketplace_service::seller_terms::capture_for_order;
use marketplace_service::workers::{
    assume_due_deliveries, complete_due_delivered_orders, DELIVERY_SWEEP_MAX_BATCHES,
};

struct PendingOrder {
    order_id: String,
    payment_id: String,
}

async fn pending_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> PendingOrder {
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    }
}

async fn confirm_payment(
    app: &TestApp,
    buyer: &TestActor,
    payment_id: &str,
) -> (StatusCode, Value) {
    execute(
        app,
        &buyer.token,
        &payment_command(payment_id, 1, "confirmed", 1, 1_050),
    )
    .await
}

async fn paid_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let pending = pending_order(app, seller, buyer).await;
    let (status, body) = confirm_payment(app, buyer, &pending.payment_id).await;
    assert_eq!(status, StatusCode::OK, "payment fixture failed: {body}");
    pending.order_id
}

async fn ship(app: &TestApp, seller: &TestActor, order_id: &str, command_number: u64) {
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-TERMS" }),
            command_number,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship fixture failed: {body}");
}

async fn terms_row(pool: &PgPool, order_id: &str) -> (Option<i32>, Option<i32>, bool) {
    sqlx::query_as(
        "SELECT delivery_assume_days, auto_complete_days, \
                delivery_terms_captured_at IS NOT NULL \
         FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn order_state(pool: &PgPool, order_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn assume_due(pool: &PgPool, at: DateTime<Utc>) -> u64 {
    assume_due_deliveries(
        pool,
        at,
        DEFAULT_DELIVERY_ASSUME_DAYS,
        DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        DELIVERY_SWEEP_MAX_BATCHES,
    )
    .await
    .expect("assume sweep runs")
}

async fn complete_due(pool: &PgPool, at: DateTime<Utc>) -> u64 {
    complete_due_delivered_orders(
        pool,
        at,
        DEFAULT_AUTO_COMPLETE_DAYS,
        DEFAULT_DELIVERY_SWEEP_BATCH_SIZE,
        DELIVERY_SWEEP_MAX_BATCHES,
    )
    .await
    .expect("complete sweep runs")
}

fn shop_record(seller: &TestActor, terms: Value) -> Value {
    let mut record = json!({
        "schemaVersion": 1,
        "recordType": "shop",
        "ownerPubky": seller.pubky,
        "name": "Terms Shop",
    });
    for (key, value) in terms.as_object().expect("terms object") {
        record[key] = value.clone();
    }
    record
}

fn days_after(app: &TestApp, days: i64) -> DateTime<Utc> {
    app.clock.now() + chrono::Duration::days(days)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_periods_freeze_on_the_order_and_drive_both_sweeps(pool: PgPool) {
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 3, "autoCompleteDays": 5 }),
        ),
    );
    let order_id = paid_order(&app, &seller, &buyer).await;
    assert_eq!(
        terms_row(&app.pool, &order_id).await,
        (Some(3), Some(5), true)
    );

    // A later change to the shop record never moves an open order.
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 40, "autoCompleteDays": 40 }),
        ),
    );
    assert_eq!(
        terms_row(&app.pool, &order_id).await,
        (Some(3), Some(5), true)
    );

    ship(&app, &seller, &order_id, 0x5601).await;
    let (status, projected) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(&buyer.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{projected}");
    assert_eq!(projected["delivery_assume_days"], json!(3));
    assert_eq!(projected["auto_complete_days"], json!(5));
    assert!(projected.get("guarantee_policy_version").is_none());

    // Delivery is assumed after the seller's 3 days, not the 14-day default.
    assert_eq!(assume_due(&app.pool, days_after(&app, 2)).await, 0);
    assert_eq!(order_state(&app.pool, &order_id).await, "shipped");
    let assumed_at = days_after(&app, 3);
    assert_eq!(assume_due(&app.pool, assumed_at).await, 1);
    assert_eq!(order_state(&app.pool, &order_id).await, "delivered");

    // The order completes 5 days after the (server-time) delivery.
    assert_eq!(
        complete_due(&app.pool, assumed_at + chrono::Duration::days(4)).await,
        0
    );
    assert_eq!(
        complete_due(&app.pool, assumed_at + chrono::Duration::days(5)).await,
        1
    );
    assert_eq!(order_state(&app.pool, &order_id).await, "completed");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_longer_seller_period_holds_an_order_past_the_default(pool: PgPool) {
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 30, "autoCompleteDays": 45 }),
        ),
    );
    let order_id = paid_order(&app, &seller, &buyer).await;
    ship(&app, &seller, &order_id, 0x5611).await;

    assert_eq!(
        assume_due(&app.pool, days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS)).await,
        0,
        "the seller's 30 days outrank the 14-day default"
    );
    assert_eq!(order_state(&app.pool, &order_id).await, "shipped");
    let assumed_at = days_after(&app, 30);
    assert_eq!(assume_due(&app.pool, assumed_at).await, 1);
    assert_eq!(
        complete_due(
            &app.pool,
            assumed_at + chrono::Duration::days(DEFAULT_AUTO_COMPLETE_DAYS)
        )
        .await,
        0
    );
    assert_eq!(
        complete_due(&app.pool, assumed_at + chrono::Duration::days(45)).await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unset_periods_fall_back_to_the_configured_defaults(pool: PgPool) {
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // The seller set only the completion period.
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(&seller, json!({ "autoCompleteDays": 2 })),
    );
    let order_id = paid_order(&app, &seller, &buyer).await;
    assert_eq!(terms_row(&app.pool, &order_id).await, (None, Some(2), true));
    ship(&app, &seller, &order_id, 0x5621).await;

    assert_eq!(
        assume_due(
            &app.pool,
            days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS - 1)
        )
        .await,
        0
    );
    let assumed_at = days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS);
    assert_eq!(assume_due(&app.pool, assumed_at).await, 1);
    assert_eq!(
        complete_due(&app.pool, assumed_at + chrono::Duration::days(1)).await,
        0
    );
    assert_eq!(
        complete_due(&app.pool, assumed_at + chrono::Duration::days(2)).await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_without_a_shop_record_gets_the_defaults(pool: PgPool) {
    let (app, _homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = paid_order(&app, &seller, &buyer).await;
    assert_eq!(terms_row(&app.pool, &order_id).await, (None, None, true));
    ship(&app, &seller, &order_id, 0x5631).await;

    assert_eq!(
        assume_due(
            &app.pool,
            days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS - 1)
        )
        .await,
        0
    );
    let assumed_at = days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS);
    assert_eq!(assume_due(&app.pool, assumed_at).await, 1);
    assert_eq!(
        complete_due(
            &app.pool,
            assumed_at + chrono::Duration::days(DEFAULT_AUTO_COMPLETE_DAYS - 1)
        )
        .await,
        0
    );
    assert_eq!(
        complete_due(
            &app.pool,
            assumed_at + chrono::Duration::days(DEFAULT_AUTO_COMPLETE_DAYS)
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn periods_outside_one_to_sixty_days_read_as_unset(pool: PgPool) {
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 0, "autoCompleteDays": 61 }),
        ),
    );
    let order_id = paid_order(&app, &seller, &buyer).await;
    assert_eq!(terms_row(&app.pool, &order_id).await, (None, None, true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_shop_record_does_not_block_the_payment_and_falls_back_to_defaults(
    pool: PgPool,
) {
    install_log_capture();
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 4, "autoCompleteDays": 6 }),
        ),
    );
    let pending = pending_order(&app, &seller, &buyer).await;
    homeserver.fail_shop_record(&seller.pubky);

    let (status, body) = confirm_payment(&app, &buyer, &pending.payment_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(order_state(&app.pool, &pending.order_id).await, "paid");
    assert_eq!(
        terms_row(&app.pool, &pending.order_id).await,
        (None, None, true),
        "the order freezes the configured defaults"
    );
    let alert = captured_logs()
        .lines()
        .find(|line| line.contains(&pending.order_id))
        .map(str::to_owned)
        .expect("an alert names the order");
    assert!(
        alert.contains("ALERT seller_terms_fallback_unreadable_shop_record"),
        "{alert}"
    );
    assert!(alert.contains(&seller.pubky), "{alert}");

    // The seller's record healing later never moves the frozen order.
    homeserver.heal_shop_record(&seller.pubky);
    assert_eq!(
        terms_row(&app.pool, &pending.order_id).await,
        (None, None, true)
    );

    ship(&app, &seller, &pending.order_id, 0x5641).await;
    assert_eq!(
        assume_due(
            &app.pool,
            days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS - 1)
        )
        .await,
        0,
        "the seller's 4 days were never read"
    );
    let assumed_at = days_after(&app, DEFAULT_DELIVERY_ASSUME_DAYS);
    assert_eq!(assume_due(&app.pool, assumed_at).await, 1);
    assert_eq!(
        complete_due(
            &app.pool,
            assumed_at + chrono::Duration::days(DEFAULT_AUTO_COMPLETE_DAYS - 1)
        )
        .await,
        0
    );
    assert_eq!(
        complete_due(
            &app.pool,
            assumed_at + chrono::Duration::days(DEFAULT_AUTO_COMPLETE_DAYS)
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_readable_shop_record_raises_no_fallback_alert(pool: PgPool) {
    install_log_capture();
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(&seller, json!({ "deliveryAssumeDays": 4 })),
    );
    let order_id = paid_order(&app, &seller, &buyer).await;
    assert!(
        !captured_logs().lines().any(|line| line.contains(&order_id)
            && line.contains("seller_terms_fallback_unreadable_shop_record")),
        "a readable record never raises the fallback alert"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn capture_happens_once_per_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let pending = pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&pending.order_id).expect("order uuid");
    let client = homeserver.client();

    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 7, "autoCompleteDays": 9 }),
        ),
    );
    let mut tx = app.pool.begin().await.expect("tx");
    capture_for_order(
        &mut tx,
        Some(client.as_ref()),
        order_id,
        &seller.pubky,
        app.clock.now(),
    )
    .await
    .expect("capture runs");
    tx.commit().await.expect("commit");

    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 20, "autoCompleteDays": 21 }),
        ),
    );
    let mut tx = app.pool.begin().await.expect("tx");
    capture_for_order(
        &mut tx,
        Some(client.as_ref()),
        order_id,
        &seller.pubky,
        app.clock.now(),
    )
    .await
    .expect("second capture runs");
    tx.commit().await.expect("commit");

    assert_eq!(
        terms_row(&app.pool, &pending.order_id).await,
        (Some(7), Some(9), true)
    );

    // An unreadable record does not matter once the order is captured.
    homeserver.fail_shop_record(&seller.pubky);
    let mut tx = app.pool.begin().await.expect("tx");
    capture_for_order(
        &mut tx,
        Some(client.as_ref()),
        order_id,
        &seller.pubky,
        app.clock.now(),
    )
    .await
    .expect("an already captured order never reads the record");
    tx.commit().await.expect("commit");
}

async fn bind_paypal(app: &TestApp, buyer: &TestActor, order_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await
}

async fn paypal_seller(app: &TestApp) -> TestActor {
    let seller = new_actor(app).await;
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
    assert_eq!(status, StatusCode::OK, "{body}");
    seller
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_payment_method_bind_captures_the_sellers_periods(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    let homeserver: FakeHomeserver = common::spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    let seller = paypal_seller(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 10, "autoCompleteDays": 12 }),
        ),
    );
    let order = create_pending_order(&app, &seller, &buyer).await;

    let (status, body) = bind_paypal(&app, &buyer, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        terms_row(&app.pool, &order.order_id).await,
        (Some(10), Some(12), true)
    );
    assert_eq!(body["order"]["delivery_assume_days"], json!(10));
    assert_eq!(body["order"]["auto_complete_days"], json!(12));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_payment_method_bind_proceeds_when_the_shop_record_is_unreadable(pool: PgPool) {
    install_log_capture();
    let (app, _paykit) = test_app_with_payments(pool).await;
    let homeserver: FakeHomeserver = common::spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    let seller = paypal_seller(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        shop_record(
            &seller,
            json!({ "deliveryAssumeDays": 10, "autoCompleteDays": 12 }),
        ),
    );
    let order = create_pending_order(&app, &seller, &buyer).await;

    homeserver.fail_shop_record(&seller.pubky);
    let (status, body) = bind_paypal(&app, &buyer, &order.order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        terms_row(&app.pool, &order.order_id).await,
        (None, None, true)
    );
    assert_eq!(body["order"]["delivery_assume_days"], Value::Null);
    assert_eq!(body["order"]["auto_complete_days"], Value::Null);
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM orders WHERE id = '{}'::uuid AND stock_held",
                order.order_id
            )
        )
        .await,
        1,
        "the bind still takes its hold"
    );
    assert!(
        captured_logs()
            .lines()
            .any(|line| line.contains(&order.order_id)
                && line.contains("ALERT seller_terms_fallback_unreadable_shop_record")
                && line.contains(&seller.pubky)),
        "the fallback is alerted with the seller and the order"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_accepts_but_neither_stores_nor_projects_a_guarantee_policy_version(pool: PgPool) {
    let (app, _homeserver) = test_app_with_shop_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Absent: accepted, and the order carries no policy version.
    let mut absent = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x5641, 1));
    absent["payload"]
        .as_object_mut()
        .expect("payload object")
        .remove("guarantee_policy_version");
    let (status, body) = execute(&app, &buyer.token, &absent).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["result"]["orders"][0]
        .get("guarantee_policy_version")
        .is_none());
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();
    let stored: Option<i32> =
        sqlx::query_scalar("SELECT guarantee_policy_version FROM orders WHERE id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(stored, None);

    // Present (an older Shop): still accepted, and still neither stored nor
    // projected.
    let present = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x5641, 2));
    let (status, body) = execute(&app, &buyer.token, &present).await;
    // Checkout does not hold stock, so a second checkout of the listing is
    // accepted too.
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["result"]["orders"][0]
        .get("guarantee_policy_version")
        .is_none());
    let stored: Option<i32> =
        sqlx::query_scalar("SELECT guarantee_policy_version FROM orders WHERE id = $1::uuid")
            .bind(
                body["result"]["orders"][0]["id"]
                    .as_str()
                    .expect("order id"),
            )
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(stored, None);

    // A version other than 1 is still refused.
    let mut unknown = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x5641, 3));
    unknown["payload"]["guarantee_policy_version"] = json!(2);
    let (status, body) = execute(&app, &buyer.token, &unknown).await;
    assert_ne!(status, StatusCode::OK, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
}
