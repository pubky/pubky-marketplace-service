//! The listing's return policy is frozen on the order when its payment starts
//! and `return.request` follows it: a final-sale order takes no return, and
//! none is taken after the return window. An order without a frozen policy
//! accepts a return as before.
//!
//! The window runs from delivery and is independent of the auto-complete
//! sweep: a `completed` order still takes `return.request` until the window
//! closes, so the sweep completing an order inside its window loses no return.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    create_paid_order, execute, new_actor, order_command, test_app_with_homeserver_config,
    FakeHomeserver, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::workers::complete_due_delivered_orders;

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

const LONG_WINDOW_DAYS: i64 = 30;

fn long_window_policy() -> Value {
    json!({ "acceptsReturns": true, "returnWindowDays": LONG_WINDOW_DAYS, "buyerPaysReturnShipping": true })
}

async fn sweep(app: &TestApp) -> u64 {
    sweep_at_instant(app, app.clock.now()).await
}

async fn sweep_at_instant(app: &TestApp, now: chrono::DateTime<chrono::Utc>) -> u64 {
    complete_due_delivered_orders(&app.pool, now, app.state.config.auto_complete_days, 100, 10)
        .await
        .expect("auto-complete sweep")
}

async fn order_state(app: &TestApp, order_id: &str) -> (String, i64) {
    sqlx::query_as("SELECT state, revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row")
}

/// `return.request` for an order at `revision`.
async fn request_return_at(
    app: &TestApp,
    buyer: &TestActor,
    order_id: &str,
    total_minor: i64,
    revision: i64,
    command_number: u64,
) -> (StatusCode, Value) {
    execute(
        app,
        &buyer.token,
        &order_command(
            "return.request",
            order_id,
            revision,
            json!({ "reason": "Changed my mind", "requested_amount_minor": total_minor }),
            command_number,
        ),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn completed_order_inside_a_longer_window_still_takes_a_return(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let auto_complete_days = app.state.config.auto_complete_days;
    assert!(
        auto_complete_days < LONG_WINDOW_DAYS,
        "the window must outlast the auto-complete period"
    );
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(long_window_policy()),
    )
    .await;

    app.clock.advance_seconds((auto_complete_days + 1) * 86_400);
    assert_eq!(sweep(&app).await, 1);
    assert_eq!(order_state(&app, &order_id).await, ("completed".into(), 5));

    let (status, body) = request_return_at(&app, &buyer, &order_id, total_minor, 5, 1_212).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "return after completion refused: {body}"
    );
    assert_eq!(body["result"]["order"]["state"], json!("return_requested"));
    assert_eq!(
        body["result"]["order"]["return_request"]["requested_from"],
        json!("completed")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn completed_order_past_the_window_refuses_a_return(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(long_window_policy()),
    )
    .await;

    app.clock.advance_seconds((LONG_WINDOW_DAYS + 1) * 86_400);
    assert_eq!(sweep(&app).await, 1);
    assert_eq!(order_state(&app, &order_id).await.0, "completed");

    let (status, body) = request_return_at(&app, &buyer, &order_id, total_minor, 5, 1_212).await;
    assert_eq!(status, StatusCode::CONFLICT, "late return accepted: {body}");
    assert_eq!(
        body["error"]["reason"],
        json!("return_window_closed"),
        "{body}"
    );
    assert_eq!(order_state(&app, &order_id).await.0, "completed");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_open_return_keeps_the_order_out_of_the_sweep(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(long_window_policy()),
    )
    .await;
    let (status, body) = request_return(&app, &buyer, &order_id, total_minor).await;
    assert_eq!(status, StatusCode::OK, "return refused: {body}");

    let sweep_at =
        app.clock.now() + chrono::Duration::days(app.state.config.auto_complete_days + 1);
    assert_eq!(sweep_at_instant(&app, sweep_at).await, 0);
    assert_eq!(
        order_state(&app, &order_id).await,
        ("return_requested".into(), 5)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_return_built_before_the_sweep_completed_the_order_is_stale_then_retried(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, total_minor) = delivered_order(
        &app,
        &homeserver,
        &seller,
        &buyer,
        Some(long_window_policy()),
    )
    .await;
    let sweep_at =
        app.clock.now() + chrono::Duration::days(app.state.config.auto_complete_days + 1);
    assert_eq!(sweep_at_instant(&app, sweep_at).await, 1);

    let (status, body) = request_return_at(&app, &buyer, &order_id, total_minor, 4, 1_212).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "stale request accepted: {body}"
    );
    assert_eq!(order_state(&app, &order_id).await, ("completed".into(), 5));

    let (status, body) = request_return_at(&app, &buyer, &order_id, total_minor, 5, 1_213).await;
    assert_eq!(status, StatusCode::OK, "retry refused: {body}");
    assert_eq!(
        body["result"]["order"]["return_request"]["requested_from"],
        json!("completed")
    );
}

/// The auto-complete sweep and a buyer's `return.request` race on a delivered
/// order inside its return window. Whichever commits first, the return is never
/// lost: a request that lands first holds the order out of the sweep, and a
/// sweep that lands first completes the order, so the request (stale at the old
/// revision, then retried at the new one) is taken from `completed`.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_return_racing_the_auto_complete_sweep_is_never_lost(pool: PgPool) {
    const ROUNDS: u64 = 8;
    let (app, homeserver) = test_app_with_homeserver_config(pool, Config::for_tests()).await;
    let auto_complete_days = app.state.config.auto_complete_days;
    for round in 0..ROUNDS {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let (order_id, total_minor) = delivered_order(
            &app,
            &homeserver,
            &seller,
            &buyer,
            Some(long_window_policy()),
        )
        .await;
        let sweep_at = app.clock.now() + chrono::Duration::days(auto_complete_days + 1);

        // `join!` polls its futures in order, so alternating them lets either
        // side reach the database first.
        let request = request_return_at(&app, &buyer, &order_id, total_minor, 4, 2_000 + round);
        let sweeping = sweep_at_instant(&app, sweep_at);
        let (swept, (status, body)) = if round % 2 == 0 {
            tokio::join!(sweeping, request)
        } else {
            let (outcome, swept) = tokio::join!(request, sweeping);
            (swept, outcome)
        };
        let (state, revision) = order_state(&app, &order_id).await;
        if status == StatusCode::OK {
            assert_eq!(swept, 0, "the sweep completed an order with an open return");
            assert_eq!(state, "return_requested", "{body}");
            assert_eq!(
                body["result"]["order"]["return_request"]["requested_from"],
                json!("delivered")
            );
            continue;
        }
        assert_eq!(status, StatusCode::CONFLICT, "unexpected refusal: {body}");
        assert_eq!(swept, 1, "a stale request means the sweep won");
        assert_eq!(state, "completed");
        let (status, body) = request_return_at(
            &app,
            &buyer,
            &order_id,
            total_minor,
            revision,
            3_000 + round,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "retry after completion refused: {body}"
        );
        assert_eq!(
            body["result"]["order"]["return_request"]["requested_from"],
            json!("completed")
        );
        assert_eq!(order_state(&app, &order_id).await.0, "return_requested");
        assert_eq!(
            sweep_at_instant(&app, sweep_at).await,
            0,
            "an open return keeps the order out of the sweep"
        );
    }
}
