//! Per-variant inventory: a listing whose record has several enabled variants
//! reserves, holds, releases and sells stock on the variant row a line names,
//! so a sold-out size can never be bought while another size has stock. Every
//! request drives the real router, bearer sessions, executor and PostgreSQL.
//!
//! The interleaving tests race real requests: two buyers for the last unit of
//! one size, buyers across sizes, and a payment-window expiry against the
//! payment confirmation that would sell the same unit.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{bound_order, poll_now, status_confirmed};
use common::{
    count, execute, indexed_command_id, listing_aggregate, new_actor, order_command,
    payment_command, register_command, send, test_app, test_app_with_homeserver,
    test_app_with_payments, test_app_with_pickup_and_paypal, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::expiry::expire_due_reservations;
use marketplace_service::workers::{expire_due_offers, expire_due_payment_windows, run_once};
use serde_json::{json, Value};
use sqlx::PgPool;

type Row = (String, i64, i64, i64, i64);

async fn register_variants(
    app: &TestApp,
    seller: &TestActor,
    variants: &[(&str, i64)],
) -> (StatusCode, Value) {
    let total: i64 = variants.iter().map(|(_, quantity)| quantity).sum();
    let mut command = register_command(&seller.pubky, total.max(1));
    command["payload"]["variants"] = json!(variants
        .iter()
        .map(|(id, quantity)| json!({ "id": id, "quantity": quantity }))
        .collect::<Vec<_>>());
    execute(app, &seller.token, &command).await
}

fn checkout(
    seller: &str,
    command_id: &str,
    variant: Option<&str>,
    expected_revision: i64,
    quantity: i64,
) -> Value {
    let mut command = common::checkout_command_with_id(seller, command_id);
    let line = &mut command["payload"]["lines"][0];
    line["expected_revision"] = json!(expected_revision);
    line["quantity"] = json!(quantity);
    if let Some(variant) = variant {
        line["variant_id"] = json!(variant);
    }
    command
}

async fn listing_revision(pool: &PgPool, seller: &str) -> i64 {
    sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

/// `(variant_id, available, reserved, sold, total)`, ordered by variant id.
async fn rows(pool: &PgPool, seller: &str) -> Vec<Row> {
    sqlx::query_as(
        "SELECT variant_id, available_quantity, reserved_quantity, sold_quantity, total_quantity \
         FROM listing_variant_inventory WHERE listing_aggregate_id = $1 ORDER BY variant_id",
    )
    .bind(listing_aggregate(seller))
    .fetch_all(pool)
    .await
    .expect("variant rows")
}

fn row(rows: &[Row], variant: &str) -> (i64, i64, i64, i64) {
    let found = rows
        .iter()
        .find(|row| row.0 == variant)
        .unwrap_or_else(|| panic!("no row for {variant}: {rows:?}"));
    (found.1, found.2, found.3, found.4)
}

async fn totals(pool: &PgPool, seller: &str) -> (i64, i64, i64, i64, String) {
    sqlx::query_as(
        "SELECT available_quantity, reserved_quantity, sold_quantity, total_quantity, state \
         FROM listings WHERE aggregate_id = $1",
    )
    .bind(listing_aggregate(seller))
    .fetch_one(pool)
    .await
    .expect("listing totals")
}

/// The books of every listing balance: the listing's quantity columns are the
/// sum of its variant rows, and each row balances on its own.
async fn assert_books_balance(pool: &PgPool) {
    let off_books = count(
        pool,
        "SELECT COUNT(*) FROM listings l WHERE EXISTS (\
           SELECT 1 FROM (\
             SELECT COALESCE(SUM(total_quantity), 0) AS total, \
                    COALESCE(SUM(available_quantity), 0) AS available, \
                    COALESCE(SUM(reserved_quantity), 0) AS reserved, \
                    COALESCE(SUM(sold_quantity), 0) AS sold \
             FROM listing_variant_inventory v WHERE v.listing_aggregate_id = l.aggregate_id) s \
           WHERE (s.total, s.available, s.reserved, s.sold) \
                 <> (l.total_quantity, l.available_quantity, l.reserved_quantity, l.sold_quantity))",
    )
    .await;
    assert_eq!(off_books, 0, "a listing's totals differ from its rows");
    let unbalanced = count(
        pool,
        "SELECT COUNT(*) FROM listing_variant_inventory \
         WHERE available_quantity + reserved_quantity + sold_quantity <> total_quantity",
    )
    .await;
    assert_eq!(unbalanced, 0);
}

async fn buy_sandbox(
    app: &TestApp,
    buyer: &TestActor,
    seller: &str,
    variant: &str,
    n: u64,
) -> (StatusCode, Value) {
    let revision = listing_revision(&app.pool, seller).await;
    let (status, body) = execute(
        app,
        &buyer.token,
        &checkout(
            seller,
            &indexed_command_id(0x7a00, n),
            Some(variant),
            revision,
            1,
        ),
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
        &payment_command(&payment_id, 1, "confirmed", 1, 7_000 + n),
    )
    .await
}

fn error_reason(body: &Value) -> Option<&str> {
    body["error"]["reason"].as_str()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn registration_with_variants_keeps_one_row_per_variant_and_sums_them(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = register_variants(
        &app,
        &seller,
        &[("size_s", 1), ("size_m", 2), ("size_l", 4)],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(stored.len(), 3, "no implicit row remains: {stored:?}");
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    assert_eq!(row(&stored, "size_m"), (2, 0, 0, 2));
    assert_eq!(row(&stored, "size_l"), (4, 0, 0, 4));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (7, 0, 0, 7, "available".to_string())
    );
    assert_books_balance(&app.pool).await;

    let projection = &body["result"]["listing"];
    assert_eq!(projection["available_quantity"], json!(7));
    assert_eq!(
        projection["variants"],
        json!([
            { "variant_id": "size_s", "available_quantity": 1, "sold_out": false },
            { "variant_id": "size_m", "available_quantity": 2, "sold_out": false },
            { "variant_id": "size_l", "available_quantity": 4, "sold_out": false },
        ])
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_that_sells_one_variant_keeps_its_implicit_row_and_projection(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 3, 0, 0, 3)]
    );
    assert!(
        body["result"]["listing"].get("variants").is_none(),
        "a single-variant projection carries no variants key: {body}"
    );

    // A variant on the line is the buyer's snapshot, as before per-variant
    // stock: any name takes units from the one row.
    let buyer = new_actor(&app).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(
            &seller.pubky,
            &indexed_command_id(0x7a01, 1),
            Some("whatever"),
            revision,
            2,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["orders"][0]["lines"][0]["variant_id"],
        json!("whatever")
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sold_out_size_is_refused_while_another_size_has_stock(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
    let first = new_actor(&app).await;
    let second = new_actor(&app).await;

    let (status, body) = buy_sandbox(&app, &first, &seller.pubky, "size_s", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("paid"));
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (0, 0, 1, 1));
    assert_eq!(row(&stored, "size_m"), (2, 0, 0, 2));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (2, 0, 1, 3, "available".to_string()),
        "the listing is still on sale: another size has stock"
    );

    let (status, body) = buy_sandbox(&app, &second, &seller.pubky, "size_s", 2).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(error_reason(&body), Some("variant_sold_out"));
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM orders").await,
        1,
        "the refused checkout created no order"
    );

    let (status, body) = buy_sandbox(&app, &second, &seller.pubky, "size_m", 3).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        row(&rows(&app.pool, &seller.pubky).await, "size_m"),
        (1, 0, 1, 2)
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_line_without_an_offered_variant_is_refused_with_a_typed_reason(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
    let buyer = new_actor(&app).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    for (variant, status, code, reason) in [
        (
            None,
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_COMMAND",
            "variant_required",
        ),
        (
            Some("size_xl"),
            StatusCode::UNPROCESSABLE_ENTITY,
            "INVALID_COMMAND",
            "unknown_variant",
        ),
    ] {
        let (got, body) = execute(
            &app,
            &buyer.token,
            &checkout(
                &seller.pubky,
                &indexed_command_id(0x7a02, 1),
                variant,
                revision,
                1,
            ),
        )
        .await;
        assert_eq!(got, status, "{body}");
        assert_eq!(body["error"]["code"], json!(code));
        assert_eq!(error_reason(&body), Some(reason));
    }
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_books_balance(&app.pool).await;
}

/// Two buyers confirm payment for the last unit of one size at once. Exactly
/// one pays; the other is refused and sees the unit as gone; the other size
/// is untouched and the books balance. Repeated so the interleavings differ.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_buyers_race_for_the_last_unit_of_one_size(pool: PgPool) {
    let app = test_app(pool).await;
    for round in 0..6u64 {
        let seller = new_actor(&app).await;
        register_variants(&app, &seller, &[("size_s", 1), ("size_m", 5)]).await;
        let first = new_actor(&app).await;
        let second = new_actor(&app).await;
        let revision = listing_revision(&app.pool, &seller.pubky).await;
        let mut payments = Vec::new();
        for (index, buyer) in [&first, &second].into_iter().enumerate() {
            let (status, body) = execute(
                &app,
                &buyer.token,
                &checkout(
                    &seller.pubky,
                    &indexed_command_id(0x7a03, round * 2 + index as u64 + 1),
                    Some("size_s"),
                    revision,
                    1,
                ),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "checkout holds nothing: {body}");
            payments.push(
                body["result"]["payments"][0]["id"]
                    .as_str()
                    .expect("payment id")
                    .to_string(),
            );
        }
        let cmd_a = payment_command(&payments[0], 1, "confirmed", 1, 7_100 + round * 2);
        let cmd_b = payment_command(&payments[1], 1, "confirmed", 1, 7_101 + round * 2);
        let (res_a, res_b) = tokio::join!(
            execute(&app, &first.token, &cmd_a),
            execute(&app, &second.token, &cmd_b),
        );
        let mut paid = 0;
        for (status, body) in [res_a, res_b] {
            if body["ok"] == json!(true) {
                assert_eq!(status, StatusCode::OK, "{body}");
                paid += 1;
            } else {
                assert_eq!(status, StatusCode::CONFLICT, "{body}");
            }
        }
        assert_eq!(
            paid, 1,
            "round {round}: exactly one buyer gets the last unit"
        );
        let stored = rows(&app.pool, &seller.pubky).await;
        assert_eq!(row(&stored, "size_s"), (0, 0, 1, 1), "round {round}");
        assert_eq!(row(&stored, "size_m"), (5, 0, 0, 5), "round {round}");
        assert_books_balance(&app.pool).await;
    }
}

/// Many buyers race across two sizes with little stock each: every unit goes
/// to exactly one buyer, each size sells exactly its own stock, and no size
/// ever sells more than it has.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn buyers_racing_across_sizes_sell_each_size_exactly_its_stock(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 3)]).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let mut checkouts = Vec::new();
    for index in 0..12u64 {
        let buyer = new_actor(&app).await;
        let variant = if index % 2 == 0 { "size_s" } else { "size_m" };
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout(
                &seller.pubky,
                &indexed_command_id(0x7a04, index + 1),
                Some(variant),
                revision,
                1,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let payment_id = body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string();
        checkouts.push((index, variant, buyer, payment_id));
    }
    let mut tasks = Vec::new();
    for (index, variant, buyer, payment_id) in checkouts {
        let router = app.router.clone();
        let token = buyer.token.clone();
        let command = payment_command(&payment_id, 1, "confirmed", 1, 7_200 + index);
        tasks.push((
            variant,
            tokio::spawn(async move {
                send(router, "POST", "/v1/commands", Some(&token), &command).await
            }),
        ));
    }
    let mut paid_s = 0;
    let mut paid_m = 0;
    for (variant, task) in tasks {
        let (status, body) = task.await.expect("confirm task");
        if body["ok"] == json!(true) {
            assert_eq!(status, StatusCode::OK, "{body}");
            if variant == "size_s" {
                paid_s += 1;
            } else {
                paid_m += 1;
            }
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
        }
    }
    assert_eq!((paid_s, paid_m), (1, 3));
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (0, 0, 1, 1));
    assert_eq!(row(&stored, "size_m"), (0, 0, 3, 3));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (0, 0, 4, 4, "sold".to_string())
    );
    assert_books_balance(&app.pool).await;
}

/// A payment-window expiry races the payment confirmation that would sell the
/// same held unit. Whichever order the two take, the held unit ends in
/// exactly one place: sold to the buyer, or back on sale as size S.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn payment_window_expiry_races_a_payment_confirmation(pool: PgPool) {
    let app = test_app_with_pickup_and_paypal(pool).await;
    for round in 0..6u64 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
        let revision = listing_revision(&app.pool, &seller.pubky).await;
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout(
                &seller.pubky,
                &indexed_command_id(0x7a05, round + 1),
                Some("size_s"),
                revision,
                1,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let order_id = body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string();
        let total_minor = common::bind_paypal_method(&app, &seller, &buyer, &order_id).await;
        assert_eq!(
            row(&rows(&app.pool, &seller.pubky).await, "size_s"),
            (0, 1, 0, 1),
            "the bind holds the unit on size S"
        );

        let after = app.clock.now() + chrono::Duration::seconds(7_300);
        let txn = format!("RACE-TXN-{round:08x}");
        let (expired, confirmed) = tokio::join!(
            expire_due_payment_windows(&app.state, after),
            common::post_completed_ipn(&app, &order_id, &txn, total_minor),
        );
        expired.expect("the sweep completes");
        assert!(
            confirmed.is_success() || confirmed == StatusCode::CONFLICT,
            "round {round}: IPN answered {confirmed}"
        );

        let (state, stock_held): (String, bool) =
            sqlx::query_as("SELECT state, stock_held FROM orders WHERE id = $1::uuid")
                .bind(&order_id)
                .fetch_one(&app.pool)
                .await
                .expect("order row");
        let stored = rows(&app.pool, &seller.pubky).await;
        assert!(!stock_held, "round {round}: no hold outlives the race");
        match state.as_str() {
            "paid" => assert_eq!(row(&stored, "size_s"), (0, 0, 1, 1), "round {round}"),
            "cancelled" => assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1), "round {round}"),
            other => panic!("round {round}: unexpected order state {other}"),
        }
        assert_eq!(
            row(&stored, "size_m"),
            (2, 0, 0, 2),
            "round {round}: the other size is never touched"
        );
        assert_books_balance(&app.pool).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_lapsed_payment_window_restocks_the_size_the_buyer_held(pool: PgPool) {
    let app = test_app_with_pickup_and_paypal(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 2), ("size_m", 2)]).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (_, body) = execute(
        &app,
        &buyer.token,
        &checkout(
            &seller.pubky,
            &indexed_command_id(0x7a06, 1),
            Some("size_m"),
            revision,
            2,
        ),
    )
    .await;
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    common::bind_paypal_method(&app, &seller, &buyer, &order_id).await;
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (0, 2, 0, 2));
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (2, 2, 0, 4, "available".to_string())
    );

    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert_eq!(
        expire_due_payment_windows(&app.state, after).await.unwrap(),
        1
    );
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (2, 0, 0, 2));
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn cancelling_a_paid_order_restocks_the_size_it_sold(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_m", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["order"]["id"].as_str().unwrap().to_string();
    assert_eq!(
        row(&rows(&app.pool, &seller.pubky).await, "size_m"),
        (1, 0, 1, 2)
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            2,
            json!({ "reason": "Wrong size" }),
            7_300,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("order.cancel_approve", &order_id, 3, json!({}), 7_301),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(
        row(&stored, "size_m"),
        (2, 0, 0, 2),
        "size M is back on sale"
    );
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1), "size S never moved");
    assert_books_balance(&app.pool).await;
}

fn reserve(seller: &str, n: u64, variant: Option<&str>, revision: i64, quantity: i64) -> Value {
    let mut command = common::reserve_command(seller, n, quantity, revision);
    if let Some(variant) = variant {
        command["payload"]["variant_id"] = json!(variant);
    }
    command
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn inventory_reserve_holds_and_expires_on_the_named_variant(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 3)]).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve(&seller.pubky, 1, None, revision, 1),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(error_reason(&body), Some("variant_required"));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve(&seller.pubky, 2, Some("size_m"), revision, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (1, 2, 0, 3));
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    let held_variant: Option<String> =
        sqlx::query_scalar("SELECT variant_id FROM reservations WHERE listing_aggregate_id = $1")
            .bind(listing_aggregate(&seller.pubky))
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(held_variant.as_deref(), Some("size_m"));

    // Asking for more of M than is available is refused even though the
    // listing as a whole has the units (S has one).
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve(&seller.pubky, 3, Some("size_m"), revision, 2),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INSUFFICIENT_INVENTORY"));

    let later = app.clock.now() + chrono::Duration::seconds(601);
    assert_eq!(expire_due_reservations(&app.pool, later).await.unwrap(), 1);
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(
        row(&stored, "size_m"),
        (3, 0, 0, 3),
        "the expiry restocks M"
    );
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    assert_books_balance(&app.pool).await;
}

/// The seller's record, as `listing.sync` reads it, with two variants.
fn record(revision: i64, variants: &[(&str, bool, i64)]) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "title": "Winter boots",
        "revision": revision,
        "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
        "variants": variants.iter().map(|(id, enabled, quantity)| json!({
            "id": id, "enabled": enabled, "quantity": quantity, "sku": format!("SKU-{id}"),
        })).collect::<Vec<_>>(),
        "shippingOptions": [{ "pricing": "free" }],
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

fn sync(seller: &str, n: u64) -> Value {
    common::sync_command(seller, "boots_01", n)
}

fn put_record(homeserver: &common::FakeHomeserver, seller: &str, record: Value) {
    homeserver.put_record(seller, "boots_01", record);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_record_edit_resyncs_variants_and_never_drops_below_committed_stock(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_eq!(row(&stored, "size_m"), (3, 0, 0, 3));
    let sku: Option<String> = sqlx::query_scalar(
        "SELECT sku FROM listing_variant_inventory WHERE listing_aggregate_id = $1 \
         AND variant_id = 'size_m'",
    )
    .bind(listing_aggregate(&seller.pubky))
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(sku.as_deref(), Some("SKU-size_m"));

    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_m", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        row(&rows(&app.pool, &seller.pubky).await, "size_m"),
        (2, 0, 1, 3)
    );

    // Lowering M below the unit already sold is a typed conflict and changes
    // nothing.
    put_record(
        &homeserver,
        &seller.pubky,
        record(2, &[("size_s", true, 2), ("size_m", true, 0)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
    assert_eq!(error_reason(&body), Some("variant_below_committed"));
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (2, 0, 1, 3));

    // Raising M, adding L and withdrawing S in one edit: S keeps the units
    // already committed (none) and leaves sale; L is new; M's total follows
    // the record and its available count the committed unit.
    put_record(
        &homeserver,
        &seller.pubky,
        record(
            3,
            &[
                ("size_m", true, 6),
                ("size_l", true, 2),
                ("size_s", false, 2),
            ],
        ),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (5, 0, 1, 6));
    assert_eq!(row(&stored, "size_l"), (2, 0, 0, 2));
    assert_eq!(
        row(&stored, "size_s"),
        (0, 0, 0, 0),
        "a withdrawn size shows no stock"
    );
    assert_eq!(
        body["result"]["listing"]["variants"],
        json!([
            { "variant_id": "size_m", "available_quantity": 5, "sold_out": false },
            { "variant_id": "size_l", "available_quantity": 2, "sold_out": false },
        ]),
        "the projection lists offered sizes only, in record order"
    );
    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_s", 2).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(error_reason(&body), Some("variant_unavailable"));
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withdrawn_size_with_a_hold_releases_into_its_total_not_onto_sale(pool: PgPool) {
    let app = test_app_with_pickup_and_paypal(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 2), ("size_m", 2)]).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (_, body) = execute(
        &app,
        &buyer.token,
        &checkout(
            &seller.pubky,
            &indexed_command_id(0x7a07, 1),
            Some("size_s"),
            revision,
            1,
        ),
    )
    .await;
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    common::bind_paypal_method(&app, &seller, &buyer, &order_id).await;

    // The seller withdraws S while the buyer's hold is open: the row keeps
    // the held unit, nothing of S is on sale.
    sqlx::query(
        "UPDATE listing_variant_inventory SET enabled = false, total_quantity = 1, \
         available_quantity = 0 WHERE listing_aggregate_id = $1 AND variant_id = 'size_s'",
    )
    .bind(listing_aggregate(&seller.pubky))
    .execute(&app.pool)
    .await
    .unwrap();
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert_eq!(
        expire_due_payment_windows(&app.state, after).await.unwrap(),
        1
    );
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(
        row(&stored, "size_s"),
        (0, 0, 0, 0),
        "the lapsed hold leaves a withdrawn size's total, not onto sale"
    );
    assert_eq!(row(&stored, "size_m"), (2, 0, 0, 2));
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_single_variant_record_edit_still_follows_the_listing_total(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_record(&homeserver, &seller.pubky, record(1, &[("only", true, 4)]));
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 0, 4)]
    );
    assert!(body["result"]["listing"].get("variants").is_none());

    put_record(&homeserver, &seller.pubky, record(2, &[("only", true, 9)]));
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 9, 0, 0, 9)]
    );
    assert_books_balance(&app.pool).await;
}

/// A listing registered before per-variant stock holds one shared total while
/// its record lists several sizes. The next sync converts it to per-variant
/// rows when its sold units can be attributed exactly, and leaves it alone
/// while they cannot.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_registered_before_variants_converts_when_its_sales_attribute(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // Registered with a total only (what a client without variants sends).
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(rows(&app.pool, &seller.pubky).await.len(), 1);

    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_m", true, 5)]),
    );
    // One unit sells, naming size M (the buyer's snapshot at the time).
    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_m", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 1, 5)]
    );

    // The record at the SAME revision lists two sizes: the sync converts the
    // listing and attributes the sold unit to M.
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(stored.len(), 2, "the implicit row is replaced: {stored:?}");
    assert_eq!(row(&stored, "size_m"), (2, 0, 1, 3));
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (4, 0, 1, 5, "available".to_string())
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_with_unattributable_sales_stays_on_its_shared_total(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    put_record(&homeserver, &seller.pubky, record(1, &[("only", true, 5)]));
    // The sold unit names a variant the record does not list.
    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "legacy_name", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    put_record(
        &homeserver,
        &seller.pubky,
        record(2, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 1, 5)],
        "the sale could not be attributed, so the listing keeps its shared total"
    );
}

fn adjust(seller: &str, revision: i64, delta: i64, key: u128, variant: Option<Value>) -> Value {
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
        request["variant"] = variant;
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
async fn inventory_adjust_moves_the_addressed_variant_row(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 3)]).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, revision, 2, 1, None),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("variant_required"));

    let first_adjust = adjust(
        &seller.pubky,
        revision,
        2,
        2,
        Some(json!({ "id": "size_m" })),
    );
    let (status, body) = post_adjust(&app, &seller.token, &first_adjust).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first_body = body.clone();
    assert_eq!(
        body["result"]["stock"],
        json!({
            "authority": "variant",
            "variant_id": "size_m",
            "total": 5,
            "available": 5,
            "reserved": 0,
            "sold": 0,
            "listing_total": { "total": 6, "available": 6, "reserved": 0, "sold": 0 },
        })
    );
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (5, 0, 0, 5));
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (6, 0, 0, 6, "available".to_string())
    );

    // Taking more from S than it has is refused; M is unaffected.
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(
            &seller.pubky,
            revision,
            -2,
            3,
            Some(json!({ "id": "size_s" })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("negative_stock"));

    // A variant the listing does not have is a typed mismatch.
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(
            &seller.pubky,
            revision,
            1,
            4,
            Some(json!({ "id": "size_xl" })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("variant_assertion_mismatch"));

    // Replaying the first successful request returns the stored result and
    // moves nothing.
    let (status, replay) = post_adjust(&app, &seller.token, &first_adjust).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, first_body);
    assert_eq!(
        row(&rows(&app.pool, &seller.pubky).await, "size_m"),
        (5, 0, 0, 5)
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn inventory_adjust_by_sku_and_projection_list_the_variant_rows(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 1), ("size_m", true, 3)]),
    );
    let (status, body) = execute(&app, &seller.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(
            &seller.pubky,
            revision,
            -1,
            1,
            Some(json!({ "sku": "SKU-size_s" })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["stock"]["variant_id"], json!("size_s"));
    assert_eq!(body["result"]["stock"]["available"], json!(0));

    // id and sku that name different rows are a mismatch.
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(
            &seller.pubky,
            revision,
            1,
            2,
            Some(json!({ "id": "size_m", "sku": "SKU-size_s" })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/inventory/listings/{}",
            listing_aggregate(&seller.pubky)
        ),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["stock"]["authority"], json!("listing_total"));
    assert_eq!(body["stock"]["available"], json!(3));
    assert_eq!(
        body["variants"],
        json!([
            {
                "variant_id": "size_s", "sku": "SKU-size_s", "enabled": true,
                "total": 0, "available": 0, "reserved": 0, "sold": 0, "sold_out": true,
            },
            {
                "variant_id": "size_m", "sku": "SKU-size_m", "enabled": true,
                "total": 3, "available": 3, "reserved": 0, "sold": 0, "sold_out": false,
            },
        ])
    );
    // The public listing shows S as sold out so the Shop can grey it out.
    let (status, listing) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/{}", listing_aggregate(&seller.pubky)),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listing}");
    assert_eq!(
        listing["variants"][0],
        json!({ "variant_id": "size_s", "available_quantity": 0, "sold_out": true })
    );
    assert_books_balance(&app.pool).await;
}

const DROP_ID: &str = "winter_drop";

/// A keyed `boots_01` (S: 1, M: 5) with a live drop over it.
async fn keyed_drop(
    app: &TestApp,
    homeserver: &common::FakeHomeserver,
    seller: &TestActor,
    syncer: &TestActor,
) {
    let (status, body) = register_variants(app, seller, &[("size_s", 1), ("size_m", 5)]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Commitments need the seller's published record (listing lifecycle).
    put_record(
        homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 1), ("size_m", true, 5)]),
    );
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        common::drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01"],
            &common::ts_after(0),
            None,
            6,
            1,
        ),
    );
    let (status, body) = execute(
        app,
        &syncer.token,
        &common::sync_drop_command(&seller.pubky, DROP_ID, 900),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_stamped_reservation_holds_and_restocks_its_variant(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    keyed_drop(&app, &homeserver, &seller, &buyer).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve(&seller.pubky, 1, Some("size_m"), revision, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (4, 1, 0, 5));
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));

    app.clock.advance_seconds(601);
    let summary = run_once(&app.state, uuid::Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass");
    assert_eq!(summary.reservations_expired, 1);
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (5, 0, 0, 5));
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_claim_holds_the_chosen_variant_and_a_cancel_releases_it(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    keyed_drop(&app, &homeserver, &seller, &buyer).await;
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(
            &seller.pubky,
            &indexed_command_id(0x7a08, 1),
            Some("size_m"),
            revision,
            1,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["orders"][0]["stock_held"], json!(true));
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(
        row(&stored, "size_m"),
        (4, 1, 0, 5),
        "the claim holds size M"
    );
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "order.cancel_request",
            &order_id,
            1,
            json!({ "reason": "Changed my mind" }),
            7_400,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (5, 0, 0, 5));
    assert_books_balance(&app.pool).await;
}

/// An accepted offer holds the offered variant; letting the award lapse
/// returns exactly that variant, and the other size is never touched.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_accepted_offer_holds_its_variant_and_a_lapsed_award_releases_it(pool: PgPool) {
    let app = test_app(pool).await;
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 3)]).await;
    // The seller's record lists size M as the one enabled variant, so the
    // offer names it.
    let mut published = record(1, &[("size_s", false, 1), ("size_m", true, 3)]);
    published["ownerPubky"] = json!(seller.pubky);
    published["listingId"] = json!("boots_01");
    common::put_command_mirror_record(&seller.pubky, "boots_01", published);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::create_offer_command(&seller.pubky, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let offered: Option<String> = sqlx::query_scalar("SELECT variant_id FROM offers")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(offered.as_deref(), Some("size_m"));
    let (status, body) = execute(
        &app,
        &seller.token,
        &common::offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (1, 2, 0, 3), "the award holds M");
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    let held: Option<String> =
        sqlx::query_scalar("SELECT variant_id FROM reservations WHERE offer_award_id IS NOT NULL")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(held.as_deref(), Some("size_m"));

    app.clock.advance_seconds(1_801);
    expire_due_offers(&app.pool, app.clock.now())
        .await
        .expect("award expiry");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_m"), (3, 0, 0, 3));
    assert_eq!(row(&stored, "size_s"), (1, 0, 0, 1));
    assert_books_balance(&app.pool).await;
}

/// The seller adjusts the last unit of a size away while a buyer confirms
/// payment for it. Whichever commits first wins; the unit is never both
/// sold and removed, and the books balance.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_adjustment_races_a_payment_confirmation_for_the_last_unit(pool: PgPool) {
    let app = test_app(pool).await;
    for round in 0..6u64 {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        register_variants(&app, &seller, &[("size_s", 1), ("size_m", 2)]).await;
        let revision = listing_revision(&app.pool, &seller.pubky).await;
        let (_, body) = execute(
            &app,
            &buyer.token,
            &checkout(
                &seller.pubky,
                &indexed_command_id(0x7a09, round + 1),
                Some("size_s"),
                revision,
                1,
            ),
        )
        .await;
        let payment_id = body["result"]["payments"][0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let confirm = payment_command(&payment_id, 1, "confirmed", 1, 7_500 + round);
        let take = adjust(
            &seller.pubky,
            revision,
            -1,
            100 + u128::from(round),
            Some(json!({ "id": "size_s" })),
        );
        let (confirmed, adjusted) = tokio::join!(
            execute(&app, &buyer.token, &confirm),
            post_adjust(&app, &seller.token, &take),
        );
        let confirmed_ok = confirmed.1["ok"] == json!(true);
        let adjusted_ok = adjusted.1["ok"] == json!(true);
        let stored = rows(&app.pool, &seller.pubky).await;
        let size_s = row(&stored, "size_s");
        match (confirmed_ok, adjusted_ok) {
            (true, false) => assert_eq!(size_s, (0, 0, 1, 1), "round {round}: sold"),
            (false, true) => assert_eq!(size_s, (0, 0, 0, 0), "round {round}: removed"),
            other => {
                panic!("round {round}: exactly one wins, got {other:?}: {confirmed:?} {adjusted:?}")
            }
        }
        assert_eq!(row(&stored, "size_m"), (2, 0, 0, 2), "round {round}");
        assert_books_balance(&app.pool).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_public_availability_names_sold_out_sizes_without_quantities(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    register_variants(&app, &seller, &[("size_s", 1), ("size_m", 4)]).await;
    let path = format!("/v0/listings/{}/boots_01/availability", seller.pubky);

    let (status, body) = send(app.router.clone(), "GET", &path, None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["variants"],
        json!([
            { "variant_id": "size_s", "sold_out": false },
            { "variant_id": "size_m", "sold_out": false },
        ])
    );

    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_s", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(app.router.clone(), "GET", &path, None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["state"], json!("available"));
    assert_eq!(
        body["variants"][0],
        json!({ "variant_id": "size_s", "sold_out": true })
    );
    let rendered = body.to_string();
    assert!(
        !rendered.contains("quantity") && !rendered.contains(&buyer.pubky),
        "stock or buyer leaked: {rendered}"
    );
}

/// Moves a bound Bitcoin order's listing to per-variant rows after its hold
/// lapsed, and names the order's line `size_s`, so the late settlement's
/// re-hold runs against a listing that tracks stock per variant.
async fn into_variants_after_the_hold_lapsed(
    pool: &PgPool,
    seller: &str,
    order_id: &str,
    size_s: (i64, i64, i64, i64),
) {
    let aggregate = listing_aggregate(seller);
    for (position, (variant, (total, available, reserved, sold))) in
        [("size_s", size_s), ("size_m", (4, 4, 0, 0))]
            .into_iter()
            .enumerate()
    {
        sqlx::query(
            "INSERT INTO listing_variant_inventory (listing_aggregate_id, variant_id, position, \
             total_quantity, available_quantity, reserved_quantity, sold_quantity) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&aggregate)
        .bind(variant)
        .bind(position as i32)
        .bind(total)
        .bind(available)
        .bind(reserved)
        .bind(sold)
        .execute(pool)
        .await
        .expect("keyed row");
    }
    sqlx::query(
        "DELETE FROM listing_variant_inventory WHERE listing_aggregate_id = $1 AND variant_id = ''",
    )
    .bind(&aggregate)
    .execute(pool)
    .await
    .expect("implicit row dropped");
    sqlx::query("UPDATE orders SET lines = jsonb_set(lines, '{0,variant_id}', '\"size_s\"') WHERE id = $1::uuid")
        .bind(order_id)
        .execute(pool)
        .await
        .expect("order line names its variant");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_bitcoin_settlement_re_holds_the_size_the_buyer_chose(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    into_variants_after_the_hold_lapsed(&pool, &seller.pubky, &order_id, (16, 16, 0, 0)).await;

    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);

    let (state,): (String,) = sqlx::query_as("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "paid");
    let stored = rows(&pool, &seller.pubky).await;
    assert_eq!(
        row(&stored, "size_s"),
        (15, 0, 1, 16),
        "the late money re-held and sold size S"
    );
    assert_eq!(row(&stored, "size_m"), (4, 0, 0, 4));
    assert_books_balance(&pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_bitcoin_settlement_for_a_sold_out_size_needs_a_refund_while_another_size_has_stock(
    pool: PgPool,
) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (order_id, _payment_id, reference) =
        bound_order(&app, &paykit, &seller, &buyer, "exclusive").await;
    let after = app.clock.now() + chrono::Duration::seconds(7_300);
    assert!(
        expire_due_payment_windows(&app.state, after)
            .await
            .expect("expire")
            >= 1
    );
    // Size S sold out to another buyer meanwhile; size M still has stock.
    into_variants_after_the_hold_lapsed(&pool, &seller.pubky, &order_id, (16, 0, 0, 16)).await;

    let mut late = status_confirmed("exclusive", true, 2);
    late["late_settlement"] = json!(true);
    paykit.set_status(&reference, late);
    assert!(poll_now(&app, after + chrono::Duration::seconds(60)).await >= 1);

    let (order_state, payment_state, reason): (String, String, Option<String>) = sqlx::query_as(
        "SELECT o.state, p.state, p.review_reason \
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1::uuid",
    )
    .bind(&order_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(order_state, "cancelled");
    assert_eq!(payment_state, "manual_review");
    assert_eq!(reason.as_deref(), Some("refund_required"));
    let stored = rows(&pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (0, 0, 16, 16));
    assert_eq!(
        row(&stored, "size_m"),
        (4, 0, 0, 4),
        "size M is never taken for a size S order"
    );
    assert_books_balance(&pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_record_that_enables_no_variant_leaves_the_rows_alone(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    put_record(
        &homeserver,
        &seller.pubky,
        record(2, &[("size_s", false, 2), ("size_m", false, 3)]),
    );
    let (status, body) = execute(&app, &buyer.token, &sync(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_eq!(row(&stored, "size_m"), (3, 0, 0, 3));
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (5, 0, 0, 5, "available".to_string())
    );
    assert_books_balance(&app.pool).await;
}

async fn sync_status(app: &TestApp, token: &str, seller: &str, n: u64) -> (StatusCode, Value) {
    execute(app, token, &sync(seller, n)).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_record_with_one_enabled_variant_counts_only_its_stock(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_m", true, 4), ("size_xl", false, 9)]),
    );
    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 0, 4)]
    );
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (4, 0, 0, 4, "available".to_string()),
        "the withdrawn size's nine units are not for sale"
    );

    // With no disabled variant the listing is what it always was.
    let other = new_actor(&app).await;
    put_record(&homeserver, &other.pubky, record(1, &[("only", true, 4)]));
    let (status, body) = sync_status(&app, &buyer.token, &other.pubky, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &other.pubky).await,
        [("".into(), 4, 0, 0, 4)]
    );
    assert_books_balance(&app.pool).await;
}

/// A listing registered before this change counted the disabled variant's
/// stock. The next sync of its record, at the same revision, recomputes the
/// total from the enabled variant: once, and only while the total is still
/// exactly that legacy sum.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_legacy_total_that_counted_disabled_stock_is_recomputed_once(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 13)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_m", true, 4), ("size_xl", false, 9)]),
    );
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 13, 0, 0, 13)]
    );

    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 0, 4)]
    );
    let revision = listing_revision(&app.pool, &seller.pubky).await;

    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing_revision(&app.pool, &seller.pubky).await,
        revision,
        "a second sync finds nothing to recompute"
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_recompute_never_goes_below_the_units_already_sold(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 13)).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_m", true, 4), ("size_xl", false, 9)]),
    );
    for n in 1..=6 {
        let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "size_m", n).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 7, 0, 6, 13)]
    );

    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 0, 0, 6, 6)],
        "four enabled units are fewer than six sold, so the total is the sold units"
    );
    assert_eq!(
        totals(&app.pool, &seller.pubky).await,
        (0, 0, 6, 6, "sold".to_string())
    );
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sellers_adjustment_is_not_undone_by_the_recompute(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 13)).await;
    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_m", true, 4), ("size_xl", false, 9)]),
    );
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = post_adjust(
        &app,
        &seller.token,
        &adjust(&seller.pubky, revision, 1, 1, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 14, 0, 0, 14)],
        "the total moved by the seller is not the legacy sum, so the same-revision sync keeps it"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_edit_below_committed_units_is_still_refused_without_a_disabled_variant(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    put_record(&homeserver, &seller.pubky, record(1, &[("only", true, 5)]));
    for n in 1..=3 {
        let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "only", n).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    put_record(&homeserver, &seller.pubky, record(2, &[("only", true, 2)]));
    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVARIANT_VIOLATION"));
    assert_eq!(
        body["error"]["message"],
        json!("Listing quantity cannot fall below committed inventory.")
    );
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 2, 0, 3, 5)]
    );
}

async fn blocked(pool: &PgPool, seller: &str) -> Option<(String, i32, i64, i64)> {
    sqlx::query_as(
        "SELECT reason, attempts, reserved_quantity, sold_quantity \
         FROM listing_variant_conversion_blocks WHERE listing_aggregate_id = $1",
    )
    .bind(listing_aggregate(seller))
    .fetch_optional(pool)
    .await
    .expect("marker query")
}

async fn sync_many(app: &TestApp, seller: &TestActor) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "boots_01"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    body["results"][0].clone()
}

fn conversion_warnings(seller: &str) -> usize {
    common::captured_logs()
        .lines()
        .filter(|line| line.contains("variant_conversion") && line.contains(seller))
        .count()
}

/// Open holds block conversion. The listing is marked, the warn is logged once,
/// `sync-many` retries it and reports the state, and once the hold lapses the
/// next `sync-many` converts it and clears the marker.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_blocked_by_open_holds_is_marked_retried_and_cleared(pool: PgPool) {
    common::install_log_capture();
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    put_record(&homeserver, &seller.pubky, record(1, &[("only", true, 5)]));
    let revision = listing_revision(&app.pool, &seller.pubky).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &reserve(&seller.pubky, 1, None, revision, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    put_record(
        &homeserver,
        &seller.pubky,
        record(1, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&app.pool, &seller.pubky).await.len(),
        1,
        "still one shared total"
    );
    assert_eq!(
        blocked(&app.pool, &seller.pubky).await,
        Some(("open_holds".to_string(), 1, 1, 0))
    );
    assert_eq!(
        conversion_warnings(&seller.pubky),
        1,
        "{}",
        common::captured_logs()
    );

    let item = sync_many(&app, &seller).await;
    assert_eq!(
        item["variant_conversion"],
        json!({ "state": "deferred", "reason": "open_holds", "attempts": 2 })
    );
    assert_eq!(
        conversion_warnings(&seller.pubky),
        1,
        "the warn is logged when the listing is first blocked, not on every retry"
    );

    // The sweep the operator runs lists it.
    let swept: Vec<(String, String, String, String)> = {
        use sqlx::Row;
        sqlx::query(include_str!(
            "../../../scripts/variant-conversion-sweep.sql"
        ))
        .fetch_all(&app.pool)
        .await
        .expect("the sweep query runs")
        .iter()
        .map(|row| {
            (
                row.get::<String, _>(0),
                row.get::<String, _>(2),
                row.get::<String, _>(3),
                row.get::<i32, _>(7).to_string(),
            )
        })
        .collect()
    };
    assert_eq!(
        swept,
        [(
            listing_aggregate(&seller.pubky),
            "boots_01".to_string(),
            "open_holds".to_string(),
            "2".to_string()
        )]
    );

    // The operator script prints the same listing, exits 2 while one is
    // blocked, and with --sync-many prints the body that retries it.
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    let base = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let url = format!(
        "{}/{database}",
        base.rsplit_once('/').expect("a database URL").0
    );
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../scripts/variant-conversion-sweep.sh"
    );
    let listed = std::process::Command::new("bash")
        .arg(script)
        .env("DATABASE_URL", &url)
        .output()
        .expect("the sweep runs");
    assert_eq!(listed.status.code(), Some(2), "{listed:?}");
    let listed = String::from_utf8_lossy(&listed.stdout).into_owned();
    assert!(
        listed.starts_with(&format!(
            "{}\t{}\tboots_01\topen_holds\t2\t1\t0\t2\t",
            listing_aggregate(&seller.pubky),
            seller.pubky
        )),
        "{listed}"
    );
    let body = std::process::Command::new("bash")
        .arg(script)
        .arg("--sync-many")
        .env("DATABASE_URL", &url)
        .output()
        .expect("the sweep runs");
    let body: Value = serde_json::from_slice(&body.stdout).expect("a sync-many body");
    assert_eq!(
        body,
        json!({ "listings": [{ "seller_pubky": seller.pubky, "listing_id": "boots_01" }] })
    );

    app.clock.advance_seconds(601);
    assert_eq!(
        expire_due_reservations(&app.pool, app.clock.now())
            .await
            .unwrap(),
        1
    );
    let item = sync_many(&app, &seller).await;
    assert!(item.get("variant_conversion").is_none(), "{item}");
    assert_eq!(blocked(&app.pool, &seller.pubky).await, None);
    let stored = rows(&app.pool, &seller.pubky).await;
    assert_eq!(row(&stored, "size_s"), (2, 0, 0, 2));
    assert_eq!(row(&stored, "size_m"), (3, 0, 0, 3));
    assert_books_balance(&app.pool).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn unattributable_sales_mark_the_listing_until_its_record_stops_listing_variants(
    pool: PgPool,
) {
    common::install_log_capture();
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    put_record(&homeserver, &seller.pubky, record(1, &[("only", true, 5)]));
    let (status, body) = buy_sandbox(&app, &buyer, &seller.pubky, "legacy_name", 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    put_record(
        &homeserver,
        &seller.pubky,
        record(2, &[("size_s", true, 2), ("size_m", true, 3)]),
    );
    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 1).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        blocked(&app.pool, &seller.pubky).await,
        Some(("unattributable_sales".to_string(), 1, 0, 1))
    );
    assert_eq!(
        rows(&app.pool, &seller.pubky).await,
        [("".into(), 4, 0, 1, 5)]
    );
    let item = sync_many(&app, &seller).await;
    assert_eq!(
        item["variant_conversion"]["reason"],
        json!("unattributable_sales")
    );
    assert!(conversion_warnings(&seller.pubky) >= 1);

    // The seller republishes one variant: nothing is left to convert.
    put_record(&homeserver, &seller.pubky, record(3, &[("only", true, 5)]));
    let (status, body) = sync_status(&app, &buyer.token, &seller.pubky, 2).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(blocked(&app.pool, &seller.pubky).await, None);
    assert_books_balance(&app.pool).await;
}
