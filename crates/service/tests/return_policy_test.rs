//! The listing's return policy is frozen on the order when its payment starts
//! and `return.request` follows it: a final-sale order takes no return, and
//! none is taken after the return window. An order without a frozen policy
//! accepts a return as before.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    create_paid_order, execute, new_actor, order_command, test_app_with_homeserver_config,
    FakeHomeserver, TestActor, TestApp,
};
use marketplace_service::config::Config;

async fn delivered_order(
    app: &TestApp,
    homeserver: &FakeHomeserver,
    seller: &TestActor,
    buyer: &TestActor,
    return_policy: Option<Value>,
) -> (String, i64) {
    let mut record = json!({ "title": "Boots" });
    if let Some(policy) = return_policy {
        record["returnPolicy"] = policy;
    }
    homeserver.put_record(&seller.pubky, "boots_01", record);
    let order = create_paid_order(app, seller, buyer).await;
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order.order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-POLICY" }),
            1_210,
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
            1_211,
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
    total_minor: i64,
) -> (StatusCode, Value) {
    execute(
        app,
        &buyer.token,
        &order_command(
            "return.request",
            order_id,
            4,
            json!({ "reason": "Changed my mind", "requested_amount_minor": total_minor }),
            1_212,
        ),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn final_sale_order_refuses_a_return(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(json!({ "acceptsReturns": false, "buyerPaysReturnShipping": false })),
    )
    .await;

    let (status, body) = request_return(&app, &buyer, &order_id, total_minor).await;
    assert_eq!(status, StatusCode::CONFLICT, "final sale accepted: {body}");
    assert_eq!(
        body["error"]["reason"],
        json!("returns_not_accepted"),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn return_window_closes_after_its_days(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(json!({ "acceptsReturns": true, "returnWindowDays": 14, "buyerPaysReturnShipping": true })),
    )
    .await;
    let frozen: Option<Value> =
        sqlx::query_scalar("SELECT return_policy FROM orders WHERE id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(
        frozen,
        Some(json!({ "accepts_returns": true, "return_window_days": 14 }))
    );

    app.clock.advance_seconds(15 * 86_400);
    let (status, body) = request_return(&app, &buyer, &order_id, total_minor).await;
    assert_eq!(status, StatusCode::CONFLICT, "late return accepted: {body}");
    assert_eq!(
        body["error"]["reason"],
        json!("return_window_closed"),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn return_inside_the_window_is_accepted(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(json!({ "acceptsReturns": true, "returnWindowDays": 14, "buyerPaysReturnShipping": true })),
    )
    .await;

    app.clock.advance_seconds(13 * 86_400);
    let (status, body) = request_return(&app, &buyer, &order_id, total_minor).await;
    assert_eq!(status, StatusCode::OK, "return refused: {body}");
    assert_eq!(
        body["result"]["order"]["return_policy"],
        json!({ "accepts_returns": true, "return_window_days": 14 })
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn order_without_a_policy_accepts_a_return(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(&app, &homeserver, &seller, &buyer, None).await;

    let (status, body) = request_return(&app, &buyer, &order_id, total_minor).await;
    assert_eq!(status, StatusCode::OK, "return refused: {body}");
    assert_eq!(body["result"]["order"]["return_policy"], Value::Null);
}
