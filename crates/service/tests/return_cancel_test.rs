//! `return.cancel`: the buyer withdraws a return the seller has not acted on
//! yet, and the order goes back to where the request came from.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{create_paid_order, execute, new_actor, order_command, test_app, TestActor, TestApp};

async fn delivered(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> (String, i64) {
    let order = create_paid_order(app, seller, buyer).await;
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order.order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-CANCEL" }),
            1_310,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship failed: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order.order_id,
            3,
            json!({}),
            1_311,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery failed: {body}");
    (order.order_id, order.total_minor)
}

async fn request_return(
    app: &TestApp,
    buyer: &TestActor,
    order_id: &str,
    revision: i64,
    total_minor: i64,
) -> Value {
    let (status, body) = execute(
        app,
        &buyer.token,
        &order_command(
            "return.request",
            order_id,
            revision,
            json!({ "reason": "Changed my mind", "requested_amount_minor": total_minor }),
            1_312 + revision as u64,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "return request failed: {body}");
    body
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_buyer_cancels_a_requested_return_and_may_request_again(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered(&app, &seller, &buyer).await;
    request_return(&app, &buyer, &order_id, 4, total_minor).await;

    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("return.cancel", &order_id, 5, json!({}), 1_320),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "seller cancelled: {body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command("return.cancel", &order_id, 5, json!({}), 1_321),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    let order = &body["result"]["order"];
    assert_eq!(order["state"], json!("delivered"));
    assert_eq!(order["return_request"], Value::Null);

    let again = request_return(&app, &buyer, &order_id, 6, total_minor).await;
    assert_eq!(again["result"]["order"]["state"], json!("return_requested"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_return_from_a_completed_order_goes_back_to_completed(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered(&app, &seller, &buyer).await;
    sqlx::query(
        "UPDATE orders SET state = 'completed', revision = revision + 1 WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .execute(&app.pool)
    .await
    .expect("complete");
    request_return(&app, &buyer, &order_id, 5, total_minor).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command("return.cancel", &order_id, 6, json!({}), 1_330),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {body}");
    assert_eq!(body["result"]["order"]["state"], json!("completed"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_approved_return_cannot_be_cancelled(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered(&app, &seller, &buyer).await;
    request_return(&app, &buyer, &order_id, 4, total_minor).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("return.approve", &order_id, 5, json!({}), 1_340),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "approve failed: {body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command("return.cancel", &order_id, 6, json!({}), 1_341),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "cancelled after approval: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("return_not_cancellable"));
}
