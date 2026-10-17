//! Made-to-order stock (migration 0073): a row its store sells without a stock
//! count is always orderable on Pubky, and every claim, hold, sale, cancel and
//! expiry still balances (available + reserved + sold = total) with nothing
//! invented. Every request drives the real router, executor and PostgreSQL;
//! the interleaving tests race real requests.

mod common;

use axum::http::StatusCode;
use common::{
    count, execute, indexed_command_id, listing_aggregate, new_actor, order_command,
    payment_command, register_command, send, test_app, test_app_with_homeserver,
    test_app_with_pickup_and_paypal, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::inventory::{set_stock_tracking, StockTracking, TrackingOutcome};
use marketplace_service::workers::expire_due_payment_windows;
use serde_json::{json, Value};
use sqlx::PgPool;

async fn revision(pool: &PgPool, seller: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

/// `(available, reserved, sold, total, state, made_to_order)` of the listing.
async fn listing(pool: &PgPool, seller: &str) -> (i64, i64, i64, i64, String, bool) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, total_quantity, state, \
                made_to_order FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller))
    .fetch_one(pool)
    .await
    .expect("listing totals")
}

/// `(available, reserved, sold, total, made_to_order)` of one variant row.
async fn row(pool: &PgPool, seller: &str, variant: &str) -> (i64, i64, i64, i64, bool) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, total_quantity, \
                made_to_order FROM listing_variant_inventory \
         WHERE listing_aggregate_id = $1 AND variant_id = $2",
    )
    .bind(listing_aggregate(seller))
    .bind(variant)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("row {variant}: {error}"))
}

async fn assert_books_balance(pool: &PgPool) {
    let off_books = count(
        pool,
        "SELECT COUNT(*) FROM listings l WHERE EXISTS (\
           SELECT 1 FROM (\
             SELECT COALESCE(SUM(total_quantity), 0) AS total, \
                    COALESCE(SUM(available_quantity), 0) AS available, \
                    COALESCE(SUM(reserved_quantity), 0) AS reserved, \
                    COALESCE(SUM(sold_quantity), 0) AS sold, \
                    COALESCE(BOOL_OR(made_to_order) FILTER (WHERE enabled), false) AS mto \
             FROM listing_variant_inventory v WHERE v.listing_aggregate_id = l.aggregate_id) s \
           WHERE (s.total, s.available, s.reserved, s.sold, s.mto) \
                 <> (l.total_quantity, l.available_quantity, l.reserved_quantity, \
                     l.sold_quantity, l.made_to_order))",
    )
    .await;
    assert_eq!(off_books, 0, "a listing's totals differ from its rows");
    let unbalanced = count(
        pool,
        "SELECT COUNT(*) FROM listing_variant_inventory \
         WHERE available_quantity + reserved_quantity + sold_quantity <> total_quantity \
            OR (made_to_order AND available_quantity <> 0)",
    )
    .await;
    assert_eq!(unbalanced, 0);
}

async fn register_variants(app: &TestApp, seller: &TestActor, variants: &[(&str, i64)]) {
    let total: i64 = variants.iter().map(|(_, quantity)| quantity).sum();
    let mut command = register_command(&seller.pubky, total.max(1));
    command["payload"]["variants"] = json!(variants
        .iter()
        .map(|(id, quantity)| json!({ "id": id, "quantity": quantity }))
        .collect::<Vec<_>>());
    let (status, body) = execute(app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn made_to_order(app: &TestApp, seller: &str, variant: Option<&str>) -> TrackingOutcome {
    set_stock_tracking(
        &app.pool,
        seller,
        "boots_01",
        variant,
        StockTracking::MadeToOrder,
        app.clock.now(),
    )
    .await
    .expect("switch")
}

fn checkout(seller: &str, n: u64, variant: Option<&str>, revision: i64, quantity: i64) -> Value {
    let mut command = common::checkout_command_with_id(seller, &indexed_command_id(0x6d00, n));
    let line = &mut command["payload"]["lines"][0];
    line["expected_revision"] = json!(revision);
    line["quantity"] = json!(quantity);
    if let Some(variant) = variant {
        line["variant_id"] = json!(variant);
    }
    command
}

/// A sandbox checkout and payment; returns the order id.
async fn buy(
    app: &TestApp,
    buyer: &TestActor,
    seller: &str,
    n: u64,
    variant: Option<&str>,
    quantity: i64,
) -> (StatusCode, Value) {
    let current = revision(&app.pool, seller).await;
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout(seller, n, variant, current, quantity),
    )
    .await;
    if status != StatusCode::OK {
        return (status, body);
    }
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 8_000 + n),
    )
    .await
}

fn adjust(seller: &str, revision: i64, delta: i64, key: u64, variant: Option<&str>) -> Value {
    let mut request = json!({
        "schema_version": 1,
        "kind": "inventory.adjust",
        "aggregate_id": listing_aggregate(seller),
        "listing_id": "boots_01",
        "expected_revision": revision,
        "delta": delta,
        "idempotency_key": format!("00000000-0000-4000-8000-{key:012}"),
    });
    if let Some(variant) = variant {
        request["variant"] = json!({ "id": variant });
    }
    request
}

async fn post_adjust(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(token),
        body,
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_made_to_order_listing_sells_past_any_count_and_its_books_balance(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let before = revision(&app.pool, &seller.pubky).await;

    let outcome = made_to_order(&app, &seller.pubky, None).await;
    assert_eq!(
        outcome,
        TrackingOutcome::Switched {
            server_revision: before + 1
        }
    );
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 0, 0, "available".to_string(), true),
        "the one counted unit is withdrawn; the listing is orderable without a count"
    );
    assert_eq!(
        count(
            &app.pool,
            &format!(
                "SELECT COUNT(*) FROM events WHERE aggregate_id = '{}' \
                 AND kind = 'inventory.adjusted'",
                listing_aggregate(&seller.pubky)
            )
        )
        .await,
        1,
        "the switch is on the seller's event feed"
    );
    assert_eq!(
        made_to_order(&app, &seller.pubky, None).await,
        TrackingOutcome::Unchanged,
        "a repeat is a no-op"
    );
    assert_eq!(revision(&app.pool, &seller.pubky).await, before + 1);

    let first = new_actor(&app).await;
    let second = new_actor(&app).await;
    let (status, body) = buy(&app, &first, &seller.pubky, 1, None, 3).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first_order = body["result"]["order"]["id"].as_str().unwrap().to_string();
    let (status, body) = buy(&app, &second, &seller.pubky, 2, None, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 5, 5, "available".to_string(), true)
    );

    // A cancelled sale leaves the total: there is no count to restock.
    let (status, body) = execute(
        &app,
        &first.token,
        &order_command(
            "order.cancel_request",
            &first_order,
            2,
            json!({ "reason": "Changed my mind" }),
            8_100,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("order.cancel_approve", &first_order, 3, json!({}), 8_101),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 2, 2, "available".to_string(), true)
    );

    // The seller's own adjustment has nothing to move.
    let current = revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, current, 4, 1, None),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "stock_not_tracked");

    // Public and seller views say made to order, never sold out.
    let (status, public) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/listings/{}/boots_01/availability", seller.pubky),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{public}");
    assert_eq!(public["state"], "available");
    assert_eq!(public["made_to_order"], true);
    let (status, projection) = send(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/inventory/listings/{}",
            listing_aggregate(&seller.pubky)
        ),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{projection}");
    assert_eq!(projection["stock"]["made_to_order"], true);
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_lapsed_hold_on_made_to_order_stock_leaves_nothing_behind(pool: PgPool) {
    let app = test_app_with_pickup_and_paypal(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    made_to_order(&app, &seller.pubky, None).await;
    let current = revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, 10, None, current, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    common::bind_paypal_method(&app, &seller, &buyer, &order_id).await;
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 4, 0, 4, "available".to_string(), true),
        "the bind holds four units it added to the total; others can still order"
    );
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert_eq!(
        expire_due_payment_windows(&app.state, after).await.unwrap(),
        1
    );
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 0, 0, "available".to_string(), true),
        "the lapsed hold leaves the total, not onto sale"
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn counting_again_puts_the_stores_units_on_sale_and_keeps_every_sale(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    made_to_order(&app, &seller.pubky, None).await;
    let (status, body) = buy(&app, &buyer, &seller.pubky, 20, None, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let outcome = set_stock_tracking(
        &app.pool,
        &seller.pubky,
        "boots_01",
        None,
        StockTracking::Counted(4),
        app.clock.now(),
    )
    .await
    .expect("switch");
    assert!(matches!(outcome, TrackingOutcome::Switched { .. }));
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (4, 0, 2, 6, "available".to_string(), false)
    );
    // Counted again: a level change is an adjustment, not a switch.
    assert_eq!(
        set_stock_tracking(
            &app.pool,
            &seller.pubky,
            "boots_01",
            None,
            StockTracking::Counted(9),
            app.clock.now(),
        )
        .await
        .expect("switch"),
        TrackingOutcome::Unchanged
    );
    let current = revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, current, -1, 2, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = buy(&app, &buyer, &seller.pubky, 21, None, 4).await;
    assert_eq!(status, StatusCode::CONFLICT, "only three are counted now");
    let (status, body) = buy(&app, &buyer, &seller.pubky, 22, None, 3).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 5, 5, "sold".to_string(), false)
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn one_made_to_order_size_keeps_a_listing_open_while_counted_sizes_sell_out(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
    assert_eq!(
        made_to_order(&app, &seller.pubky, None).await,
        TrackingOutcome::VariantRequired
    );
    assert_eq!(
        made_to_order(&app, &seller.pubky, Some("size_xl")).await,
        TrackingOutcome::VariantNotFound
    );
    assert!(matches!(
        made_to_order(&app, &seller.pubky, Some("size_m")).await,
        TrackingOutcome::Switched { .. }
    ));
    assert_eq!(
        row(&app.pool, &seller.pubky, "size_m").await,
        (0, 0, 0, 0, true)
    );

    let (status, body) = buy(&app, &buyer, &seller.pubky, 30, Some("size_s"), 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 1, 1, "available".to_string(), true),
        "no counted unit is left, but size M is still orderable"
    );
    let (status, body) = buy(&app, &buyer, &seller.pubky, 31, Some("size_s"), 1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], "variant_sold_out");
    let (status, body) = buy(&app, &buyer, &seller.pubky, 32, Some("size_m"), 7).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        row(&app.pool, &seller.pubky, "size_m").await,
        (0, 0, 7, 7, true)
    );

    let (status, public) = send(
        app.router.clone(),
        "GET",
        &format!("/v0/listings/{}/boots_01/availability", seller.pubky),
        None,
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        public["variants"],
        json!([
            { "variant_id": "size_s", "sold_out": true },
            { "variant_id": "size_m", "sold_out": false, "made_to_order": true },
        ])
    );
    let current = revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, current, 2, 3, Some("size_m")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "stock_not_tracked");
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, current, 2, 4, Some("size_s")),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a counted size still adjusts: {body}"
    );
    assert_books_balance(&app.pool).await;
}

fn record(revision: i64, variants: &[(&str, i64)]) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "title": "Winter boots",
        "revision": revision,
        "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
        "variants": variants.iter().map(|(id, quantity)| json!({
            "id": id, "enabled": true, "quantity": quantity,
        })).collect::<Vec<_>>(),
        "shippingOptions": [{ "pricing": "free" }],
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_record_edit_keeps_a_made_to_order_row_made_to_order(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        record(1, &[("size_s", 2), ("size_m", 1)]),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    made_to_order(&app, &seller.pubky, Some("size_m")).await;
    let (status, body) = buy(&app, &buyer, &seller.pubky, 40, Some("size_m"), 3).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The seller's record says size M has 0: for a made-to-order row that is
    // not a count, and nothing falls below the units already sold.
    homeserver.put_record(
        &seller.pubky,
        "boots_01",
        record(2, &[("size_s", 5), ("size_m", 0)]),
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        row(&app.pool, &seller.pubky, "size_m").await,
        (0, 0, 3, 3, true)
    );
    assert_eq!(
        row(&app.pool, &seller.pubky, "size_s").await,
        (5, 0, 0, 5, false)
    );
    assert_eq!(
        body["result"]["listing"]["variants"][1],
        json!({ "variant_id": "size_m", "available_quantity": 0, "sold_out": false,
                "made_to_order": true })
    );
    assert_eq!(body["result"]["listing"]["made_to_order"], true);
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_single_variant_record_edit_keeps_made_to_order_stock(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    homeserver.put_record(&seller.pubky, "boots_01", record(1, &[("only", 3)]));
    execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 1),
    )
    .await;
    made_to_order(&app, &seller.pubky, None).await;
    let (status, body) = buy(&app, &buyer, &seller.pubky, 50, None, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    homeserver.put_record(&seller.pubky, "boots_01", record(2, &[("only", 1)]));
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::sync_command(&seller.pubky, "boots_01", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 2, 2, "available".to_string(), true),
        "a record quantity below the units sold is no refusal and no count"
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyers_racing_for_made_to_order_stock_all_get_their_units(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    made_to_order(&app, &seller.pubky, None).await;
    let mut buyers = Vec::new();
    for _ in 0..4 {
        buyers.push(new_actor(&app).await);
    }
    // Every checkout names the same listing revision, as concurrent buyers
    // reading the same page do, and the payments race.
    let current = revision(&app.pool, &seller.pubky).await;
    let mut payments = Vec::new();
    for (n, buyer) in buyers.iter().enumerate() {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout(&seller.pubky, 60 + n as u64, None, current, 3),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        payments.push(payment_command(
            body["result"]["payments"][0]["id"]
                .as_str()
                .expect("payment id"),
            1,
            "confirmed",
            1,
            8_600 + n as u64,
        ));
    }
    let (a, b, c, d) = tokio::join!(
        execute(&app, &buyers[0].token, &payments[0]),
        execute(&app, &buyers[1].token, &payments[1]),
        execute(&app, &buyers[2].token, &payments[2]),
        execute(&app, &buyers[3].token, &payments[3]),
    );
    for (status, body) in [a, b, c, d] {
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(
        listing(&app.pool, &seller.pubky).await,
        (0, 0, 12, 12, "available".to_string(), true),
        "every buyer got their three units"
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_tracking_switch_racing_holds_never_loses_or_invents_a_unit(pool: PgPool) {
    let app = test_app_with_pickup_and_paypal(pool).await;
    for round in 0..6u64 {
        let seller = new_actor(&app).await;
        execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
        made_to_order(&app, &seller.pubky, None).await;
        let current = revision(&app.pool, &seller.pubky).await;
        let mut orders = Vec::new();
        let mut buyers = Vec::new();
        for n in 0..3u64 {
            let buyer = new_actor(&app).await;
            let (status, body) = execute(
                &app,
                &buyer.token,
                &checkout(&seller.pubky, 100 + round * 10 + n, None, current, 1),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{body}");
            orders.push(
                body["result"]["orders"][0]["id"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
            buyers.push(buyer);
        }
        // Three binds (each holds a unit) race the store switching the row
        // back to a count of 3. A hold made while the row was made to order
        // is a claim on those 3 as much as one made after.
        let switch = set_stock_tracking(
            &app.pool,
            &seller.pubky,
            "boots_01",
            None,
            StockTracking::Counted(3),
            app.clock.now(),
        );
        let (_, _, _, switched) = tokio::join!(
            common::bind_paypal_method(&app, &seller, &buyers[0], &orders[0]),
            common::bind_paypal_method(&app, &seller, &buyers[1], &orders[1]),
            common::bind_paypal_method(&app, &seller, &buyers[2], &orders[2]),
            switch,
        );
        assert!(matches!(
            switched.expect("switch"),
            TrackingOutcome::Switched { .. }
        ));
        assert_eq!(
            listing(&app.pool, &seller.pubky).await,
            (0, 3, 0, 3, "reserved".to_string(), false),
            "round {round}: the three holds take exactly the store's three"
        );
        assert_books_balance(&app.pool).await;
        let after = app.clock.now() + chrono::Duration::seconds(7_300);
        assert_eq!(
            expire_due_payment_windows(&app.state, after).await.unwrap(),
            3
        );
        assert_eq!(
            listing(&app.pool, &seller.pubky).await,
            (3, 0, 0, 3, "available".to_string(), false),
            "round {round}: lapsed holds put back the store's count, no more"
        );
        assert_books_balance(&app.pool).await;
    }
}
