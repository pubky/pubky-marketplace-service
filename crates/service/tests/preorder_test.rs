//! Preorders: a fixed-price physical listing that sells before stock exists.
//! The ships-from date is checked against server time when it is published,
//! the order freezes the preorder at its payment start, the overdue sweep
//! marks an unshipped order past its ship-by deadline, and from that deadline
//! the buyer cancels alone and the seller owes the refund.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command, count, execute, new_actor, order_command, payment_command, register_command,
    send, test_app, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::{drain_outbox, mark_overdue_preorders};

/// The test clock starts at 2026-08-19T22:00Z, so this date is 13 days out
/// and its ship-by deadline (seven grace days) is 2026-09-08T00:00Z.
const SHIPS_FROM: &str = "2026-09-01";

fn preorder_register_command(seller_pubky: &str, ships_from: &str) -> Value {
    let mut command = register_command(seller_pubky, 2);
    command["payload"]["preorder"] = json!({ "ships_from": ships_from, "grace_days": 7 });
    command
}

/// Registers a two-unit preorder listing and pays for one unit.
async fn paid_preorder(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    let (status, body) = execute(
        app,
        &seller.token,
        &preorder_register_command(&seller.pubky, SHIPS_FROM),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment failed: {body}");
    order_id
}

async fn order(app: &TestApp, order_id: &str, token: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

async fn notification_types(app: &TestApp, token: &str) -> Vec<String> {
    drain_outbox(&app.pool, None, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (_, body) = send(
        app.router.clone(),
        "GET",
        "/v1/notifications",
        Some(token),
        &json!(null),
    )
    .await;
    body["notifications"]
        .as_array()
        .expect("notifications array")
        .iter()
        .filter_map(|n| n["type"].as_str().map(str::to_string))
        .collect()
}

fn at(timestamp: &str) -> DateTime<Utc> {
    timestamp.parse().expect("valid timestamp")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_preorder_publishes_only_with_a_ships_from_date_in_the_next_90_days(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    for ships_from in ["2026-08-19", "2026-11-18"] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &preorder_register_command(&seller.pubky, ships_from),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{ships_from}: {body}"
        );
        assert_eq!(
            body["error"]["reason"],
            json!("preorder_ships_from_out_of_range")
        );
    }

    let (status, body) = execute(
        &app,
        &seller.token,
        &preorder_register_command(&seller.pubky, "2026-11-17"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    assert_eq!(
        body["result"]["listing"]["preorder"],
        json!({ "ships_from": "2026-11-17", "grace_days": 7 })
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_keeps_the_preorder_it_was_paid_under(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = paid_preorder(&app, &seller, &buyer).await;

    let mut edit = preorder_register_command(&seller.pubky, "2026-10-01");
    edit["command_id"] = json!("018f47d2-6a27-7c23-a49d-6b21bb770121");
    edit["expected_revision"] = json!(3);
    edit["payload"]["listing_revision"] = json!(2);
    let (status, body) = execute(&app, &seller.token, &edit).await;
    assert_eq!(status, StatusCode::OK, "edit failed: {body}");

    let order = order(&app, &order_id, &buyer.token).await;
    assert_eq!(
        order["preorder"],
        json!({
            "ships_from": SHIPS_FROM,
            "grace_days": 7,
            "ship_by": "2026-09-08T00:00:00.000Z",
            "overdue_at": null,
        })
    );
    assert_eq!(order["refund_due"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn before_the_deadline_a_cancel_waits_for_the_seller(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = paid_preorder(&app, &seller, &buyer).await;
    app.clock.set(at("2026-09-07T23:59:00Z"));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            2,
            json!({ "reason": "Too slow" }),
            1_400,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "request failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("cancel_requested"));
    assert_eq!(body["result"]["order"]["refund_due"], json!(false));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_preorder_is_the_buyers_to_cancel_and_the_sellers_to_refund(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = paid_preorder(&app, &seller, &buyer).await;

    // A minute before the deadline nothing is overdue yet.
    app.clock.set(at("2026-09-07T23:59:00Z"));
    assert_eq!(
        mark_overdue_preorders(&app.pool, app.clock.now(), 50, 4)
            .await
            .unwrap(),
        0
    );

    // At the deadline the sweep marks the unshipped order and tells both.
    app.clock.set(at("2026-09-08T00:00:00Z"));
    assert_eq!(
        mark_overdue_preorders(&app.pool, app.clock.now(), 50, 4)
            .await
            .unwrap(),
        1
    );
    let overdue = order(&app, &order_id, &seller.token).await;
    assert_eq!(overdue["state"], json!("paid"));
    assert_eq!(
        overdue["preorder"]["overdue_at"],
        json!("2026-09-08T00:00:00.000Z")
    );
    for token in [&buyer.token, &seller.token] {
        let types = notification_types(&app, token).await;
        assert!(
            types.contains(&"order_preorder_overdue".to_string()),
            "{types:?}"
        );
    }

    // The buyer cancels without the seller; the unit goes back on sale.
    let revision = overdue["revision"].as_i64().unwrap();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            revision,
            json!({ "reason": "Not shipped in time" }),
            1_402,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    let cancelled = &body["result"]["order"];
    assert_eq!(cancelled["state"], json!("cancelled"));
    assert_eq!(cancelled["cancellation_reason"], json!("preorder_late"));
    assert_eq!(cancelled["refund_due"], json!(true));
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'order.cancelled_preorder_late'"
        )
        .await,
        1
    );
    let (available, sold): (i64, i64) =
        sqlx::query_as("SELECT available_quantity, sold_quantity FROM listings")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!((available, sold), (2, 0));

    // The seller records the refund they sent; nothing is due after it.
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command(
            "refund.record_external",
            &order_id,
            revision + 1,
            json!({
                "amount_minor": cancelled["total"]["amount_minor"],
                "transaction_id": "usdt-refund-tx-0001",
            }),
            1_403,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("refunded_external"));
    assert_eq!(body["result"]["order"]["refund_due"], json!(false));
}
