//! The largest order total a checkout accepts is `MAX_SAFE_INTEGER` (the
//! largest integer the TypeScript contracts carry exactly). A checkout whose
//! line subtotal, order subtotal or total would go above it is refused before
//! anything is written; one that lands exactly on it is charged exactly that.
//! The bound holds for each order a checkout creates (one per seller and
//! fulfillment group), not for the checkout as a whole.
//! Every request drives the real router, executor and PostgreSQL.

mod common;

use axum::http::StatusCode;
use common::{
    checkout_command, drop_aggregate, drop_record_json, execute, listing_aggregate, new_actor,
    register_command, register_listing_command, sync_drop_command, test_app,
    test_app_with_homeserver, ts_after, TestActor, TestApp,
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
/// no order, payment or reservation.
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
}

/// Two sellers, each with `boots_01` registered by `register_priced` at the
/// given `(price, shipping)`, and a buyer.
async fn two_priced_sellers(
    pool: sqlx::PgPool,
    first: (i64, i64),
    second: (i64, i64),
) -> (TestApp, [TestActor; 2], TestActor) {
    let app = test_app(pool).await;
    let sellers = [new_actor(&app).await, new_actor(&app).await];
    let buyer = new_actor(&app).await;
    for (seller, (price, shipping)) in sellers.iter().zip([first, second]) {
        register(
            &app,
            seller,
            &register_priced(&seller.pubky, price, shipping),
        )
        .await;
    }
    (app, sellers, buyer)
}

/// A shipped checkout of each listed seller's `boots_01`, in the paired
/// quantity: one order per seller.
fn split_checkout(lines: &[(&TestActor, i64)]) -> Value {
    let mut command = checkout_command(&lines[0].0.pubky);
    command["payload"]["lines"] = lines
        .iter()
        .map(|(seller, quantity)| {
            json!({
                "listing_aggregate_id": listing_aggregate(&seller.pubky),
                "expected_revision": 1,
                "quantity": quantity,
            })
        })
        .collect();
    command
}

/// The seller's order in the checkout response and in `orders` carries
/// `(subtotal, shipping, total)`, and its payment, in the response and in
/// `payments`, is for exactly that total.
async fn assert_seller_order(
    app: &TestApp,
    body: &Value,
    buyer: &TestActor,
    seller: &TestActor,
    (subtotal, shipping, total): (i64, i64, i64),
) {
    let result = &body["result"];
    let order = result["orders"]
        .as_array()
        .expect("orders")
        .iter()
        .find(|order| order["seller_pubky"] == json!(seller.pubky))
        .expect("one order for the seller");
    assert_eq!(order["subtotal"], money(subtotal), "{order}");
    assert_eq!(order["shipping"], money(shipping), "{order}");
    assert_eq!(order["total"], money(total), "{order}");
    let payment = result["payments"]
        .as_array()
        .expect("payments")
        .iter()
        .find(|payment| payment["order_id"] == order["id"])
        .expect("one payment for the order");
    assert_eq!(payment["seller_pubky"], json!(seller.pubky), "{payment}");
    assert_eq!(payment["amount"], money(total), "{payment}");

    let stored: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT o.subtotal_minor, o.shipping_minor, o.total_minor, p.amount_minor \
         FROM orders o JOIN payments p ON p.order_id = o.id \
         WHERE o.buyer_pubky = $1 AND o.seller_pubky = $2 AND p.seller_pubky = $2",
    )
    .bind(&buyer.pubky)
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("one stored order with its payment for the seller");
    assert_eq!(stored, (subtotal, shipping, total, total));
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_bound_checkout_above_the_largest_safe_total_is_refused_and_writes_nothing(
    pool: sqlx::PgPool,
) {
    const DROP_ID: &str = "winter_drop";
    const DROP_UNITS: i64 = 5;
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let listing = register_priced(&seller.pubky, MAX_SAFE_INTEGER, 1);
    homeserver.publish_registration(&listing);
    register(&app, &seller, &listing).await;
    // An open drop over the listing that this buyer may still buy from.
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01"],
            &ts_after(0),
            None,
            DROP_UNITS,
            1,
        ),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync failed: {body}");

    // The drop gate claims its unit before the order total is known; the
    // refusal must leave that claim unwritten too.
    assert_refused_and_nothing_written(&app, &buyer, &checkout(&seller.pubky, 1)).await;

    let remaining: i64 =
        sqlx::query_scalar("SELECT remaining_quantity FROM drops WHERE aggregate_id = $1")
            .bind(drop_aggregate(&seller.pubky, DROP_ID))
            .fetch_one(&app.pool)
            .await
            .expect("drop row exists");
    assert_eq!(remaining, DROP_UNITS, "the drop keeps every unit");
    assert_eq!(
        rows_for(&app, "drop_purchases", &buyer).await,
        0,
        "the buyer has no drop purchase"
    );
    let (state, held): (String, i64) = sqlx::query_as(
        "SELECT state, reserved_quantity + sold_quantity FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .expect("listing row exists");
    assert_eq!(
        (state.as_str(), held),
        ("available", 0),
        "the listing holds no unit for the buyer"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn each_order_of_a_split_checkout_charges_its_own_amounts(pool: sqlx::PgPool) {
    let (app, [first, second], buyer) = two_priced_sellers(pool, (4_000, 300), (2_500, 700)).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &split_checkout(&[(&first, 3), (&second, 2)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(rows_for(&app, "orders", &buyer).await, 2);
    assert_eq!(rows_for(&app, "payments", &buyer).await, 2);
    assert_seller_order(&app, &body, &buyer, &first, (12_000, 300, 12_300)).await;
    assert_seller_order(&app, &body, &buyer, &second, (5_000, 700, 5_700)).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_split_checkout_with_one_order_above_the_largest_safe_total_is_refused(
    pool: sqlx::PgPool,
) {
    // The first order lands exactly on the bound; the second goes above it.
    let (app, [first, second], buyer) =
        two_priced_sellers(pool, (MAX_SAFE_INTEGER, 0), (MAX_SAFE_INTEGER, 1)).await;

    assert_refused_and_nothing_written(&app, &buyer, &split_checkout(&[(&first, 1), (&second, 1)]))
        .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_order_of_a_split_checkout_may_reach_the_largest_safe_total(pool: sqlx::PgPool) {
    let (app, [first, second], buyer) =
        two_priced_sellers(pool, (MAX_SAFE_INTEGER, 0), (MAX_SAFE_INTEGER, 0)).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &split_checkout(&[(&first, 1), (&second, 1)]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let at_bound = (MAX_SAFE_INTEGER, 0, MAX_SAFE_INTEGER);
    assert_seller_order(&app, &body, &buyer, &first, at_bound).await;
    assert_seller_order(&app, &body, &buyer, &second, at_bound).await;
}
