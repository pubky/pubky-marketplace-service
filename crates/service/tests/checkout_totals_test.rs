//! The largest order total a checkout accepts is `MAX_SAFE_INTEGER` (the
//! largest integer the TypeScript contracts carry exactly). A checkout whose
//! line subtotal, order subtotal or total would go above it is refused before
//! anything is written; one that lands exactly on it is charged exactly that.
//! Every request drives the real router, executor and PostgreSQL.

mod common;

use axum::http::StatusCode;
use common::{
    checkout_command, execute, new_actor, register_command, register_listing_command, test_app,
    TestActor, TestApp,
};
use marketplace_domain::money::MAX_SAFE_INTEGER;
use serde_json::{json, Value};

/// Units of each listing the seller holds: enough that a checkout of all of
/// them at `MAX_SAFE_INTEGER` each lies beyond even a 64-bit integer.
const STOCK: i64 = 2_049;

fn money(amount_minor: i64) -> Value {
    json!({ "amount_minor": amount_minor, "currency": "USD", "exponent": 2 })
}

/// A `listing.register` of `boots_01` selling `STOCK` units at `price` each,
/// shipped for `shipping`.
fn register_priced(seller: &str, price: i64, shipping: i64) -> Value {
    let mut command = register_command(seller, STOCK);
    command["payload"]["unit_price"] = money(price);
    command["payload"]["shipping_minor"] = json!(shipping);
    command
}

/// A shipped checkout of `quantity` units of the seller's `boots_01`.
fn checkout(seller: &str, quantity: i64) -> Value {
    let mut command = checkout_command(seller);
    command["payload"]["lines"][0]["quantity"] = json!(quantity);
    command
}

async fn register(app: &TestApp, seller: &TestActor, command: &Value) {
    let (status, body) = execute(app, &seller.token, command).await;
    assert_eq!(status, StatusCode::OK, "registration failed: {body}");
}

/// A seller with `boots_01` registered by `register_priced`, and a buyer.
async fn priced_listing(
    pool: sqlx::PgPool,
    price: i64,
    shipping: i64,
) -> (TestApp, TestActor, TestActor) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register(
        &app,
        &seller,
        &register_priced(&seller.pubky, price, shipping),
    )
    .await;
    (app, seller, buyer)
}

async fn rows_for(app: &TestApp, table: &str, buyer: &TestActor) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {table} WHERE buyer_pubky = $1"
    ))
    .bind(&buyer.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("count query succeeds")
}

/// The checkout is refused as an invalid command, and the buyer is left with
/// no order, payment or reservation, and no listing holds or sells a unit.
async fn assert_refused_and_nothing_written(app: &TestApp, buyer: &TestActor, checkout: &Value) {
    let (status, body) = execute(app, &buyer.token, checkout).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"), "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("The order total exceeds the largest supported amount."),
        "{body}"
    );
    for table in ["orders", "payments", "reservations"] {
        assert_eq!(rows_for(app, table, buyer).await, 0, "{table}");
    }
    assert_eq!(
        common::count(
            &app.pool,
            "SELECT COALESCE(SUM(reserved_quantity + sold_quantity), 0)::BIGINT FROM listings",
        )
        .await,
        0,
        "no stock is held or sold"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_line_subtotal_above_the_largest_safe_total_is_refused(pool: sqlx::PgPool) {
    let (app, seller, buyer) = priced_listing(pool, MAX_SAFE_INTEGER, 0).await;

    assert_refused_and_nothing_written(&app, &buyer, &checkout(&seller.pubky, STOCK)).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_variant_line_subtotal_above_the_largest_safe_total_is_refused(pool: sqlx::PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // The listing price is the fixture's; only size L sells at the largest
    // safe amount.
    let mut command = register_command(&seller.pubky, STOCK);
    command["payload"]["shipping_minor"] = json!(0);
    command["payload"]["variants"] = json!([
        { "id": "size_s", "quantity": STOCK },
        { "id": "size_l", "quantity": STOCK, "unit_price": money(MAX_SAFE_INTEGER) },
    ]);
    register(&app, &seller, &command).await;

    let mut order = checkout(&seller.pubky, STOCK);
    order["payload"]["lines"][0]["variant_id"] = json!("size_l");
    assert_refused_and_nothing_written(&app, &buyer, &order).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_subtotal_above_the_largest_safe_total_is_refused(pool: sqlx::PgPool) {
    let (app, seller, buyer) = priced_listing(pool, MAX_SAFE_INTEGER, 0).await;
    let mut second = register_listing_command(&seller.pubky, "boots_02", STOCK, 1);
    second["payload"]["unit_price"] = money(MAX_SAFE_INTEGER);
    second["payload"]["shipping_minor"] = json!(0);
    register(&app, &seller, &second).await;

    // Each line alone is at the bound; the one order holding both is not.
    let mut order = checkout(&seller.pubky, 1);
    let first_line = order["payload"]["lines"][0].clone();
    let mut second_line = first_line.clone();
    second_line["listing_aggregate_id"] = json!(format!("listing:{}_boots_02", seller.pubky));
    order["payload"]["lines"] = json!([first_line, second_line]);
    assert_refused_and_nothing_written(&app, &buyer, &order).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn shipping_that_lifts_the_total_above_the_largest_safe_total_is_refused(pool: sqlx::PgPool) {
    let (app, seller, buyer) = priced_listing(pool, MAX_SAFE_INTEGER, 1).await;

    assert_refused_and_nothing_written(&app, &buyer, &checkout(&seller.pubky, 1)).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_largest_safe_total_is_charged_exactly(pool: sqlx::PgPool) {
    let (app, seller, buyer) = priced_listing(pool, MAX_SAFE_INTEGER, 0).await;

    let (status, body) = execute(&app, &buyer.token, &checkout(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let projected = &body["result"]["orders"][0];
    assert_eq!(projected["total"], money(MAX_SAFE_INTEGER), "{body}");
    assert_eq!(
        projected["lines"][0]["subtotal"],
        money(MAX_SAFE_INTEGER),
        "{body}"
    );

    let stored: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (o.lines -> 0 -> 'subtotal' ->> 'amount_minor')::BIGINT, o.subtotal_minor, \
                o.shipping_minor, o.total_minor, p.amount_minor \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.buyer_pubky = $1",
    )
    .bind(&buyer.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("one order with its payment");
    assert_eq!(
        stored,
        (
            MAX_SAFE_INTEGER,
            MAX_SAFE_INTEGER,
            0,
            MAX_SAFE_INTEGER,
            MAX_SAFE_INTEGER
        )
    );
}
