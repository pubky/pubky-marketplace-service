//! A Bitcoin refund is recorded in the sats the order received, whatever its
//! price currency, like the refunded review resolution records it:
//! `refund.record_external` caps it at the observed amount frozen at
//! settlement, else at the payable the request asked for, payment code
//! included. The seller's projection names the cap and its unit
//! (`refund_cap`), the command names the unit it was entered in
//! (`amount_unit`), and the record keeps it. Each order here is paid through
//! the sandbox and then carries the Bitcoin columns a settled Paykit bind
//! leaves.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::StatusCode;
use common::*;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const TOTAL_MINOR: i64 = 13_700;
/// What a 137.00 USD order's Bitcoin request asked for, payment code included.
const USD_PAYABLE_SATS: i64 = 51_637;
/// A 13,700-sat order's payable: the price and a 37-sat payment code.
const SAT_PAYABLE_SATS: i64 = 13_737;

static COMMAND_NUMBER: AtomicU64 = AtomicU64::new(47_000);

#[derive(Clone, Copy)]
struct Bitcoin {
    method: &'static str,
    currency: &'static str,
    payable: i64,
    observed: Option<i64>,
}

const USD_PRICED: Bitcoin = Bitcoin {
    method: "paykit",
    currency: "USD",
    payable: USD_PAYABLE_SATS,
    observed: None,
};

const SAT_PRICED: Bitcoin = Bitcoin {
    method: "paykit",
    currency: "SAT",
    payable: SAT_PAYABLE_SATS,
    observed: None,
};

/// A cancelled order paid in Bitcoin as `bitcoin` describes.
async fn cancelled_bitcoin_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    bitcoin: Bitcoin,
) -> String {
    let order = create_paid_order(app, seller, buyer).await;
    assert_eq!(order.total_minor, TOTAL_MINOR);
    let exponent = if bitcoin.currency == "SAT" { 0 } else { 2 };
    sqlx::query(
        "UPDATE orders SET currency = $2, exponent = $3, payment_method = $4, \
         paykit_total_sats = $5, paykit_observed_sats = $6, state = 'cancelled', \
         revision = revision + 1 WHERE id = $1",
    )
    .bind(Uuid::parse_str(&order.order_id).expect("order id"))
    .bind(bitcoin.currency)
    .bind(exponent)
    .bind(bitcoin.method)
    .bind(bitcoin.payable)
    .bind(bitcoin.observed)
    .execute(&app.pool)
    .await
    .expect("a Bitcoin-paid cancelled order");
    order.order_id
}

/// Records a refund; `Err` carries the refusal's reason, if it has one.
async fn record(
    app: &TestApp,
    seller: &TestActor,
    order_id: &str,
    amount_minor: i64,
    unit: Option<&str>,
) -> Result<(), Option<String>> {
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("revision");
    let mut payload = json!({ "amount_minor": amount_minor, "transaction_id": "bitcoin-refund-1" });
    if let Some(unit) = unit {
        payload["amount_unit"] = json!(unit);
    }
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            revision,
            payload,
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
        ),
    )
    .await;
    match status {
        StatusCode::OK => Ok(()),
        StatusCode::CONFLICT => {
            assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
            Err(body["error"]["reason"].as_str().map(str::to_string))
        }
        other => panic!("unexpected {other}: {body}"),
    }
}

async fn recorded(app: &TestApp, order_id: &str) -> (String, Option<i64>) {
    sqlx::query_as(
        "SELECT state, (external_refund->>'amount_minor')::bigint FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order")
}

async fn recorded_unit(app: &TestApp, order_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT external_refund->>'amount_unit' FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order")
}

async fn read_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

/// One sat above `cap` is refused, `cap` is recorded as it was sent, in sats.
async fn assert_capped_at(app: &TestApp, bitcoin: Bitcoin, cap: i64) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let order_id = cancelled_bitcoin_order(app, &seller, &buyer, bitcoin).await;
    assert_eq!(
        read_order(app, &seller.token, &order_id).await["refund_cap"],
        json!({ "amount_minor": cap, "unit": "sat", "exponent": 0 }),
    );
    assert_eq!(
        record(app, &seller, &order_id, cap + 1, Some("sat")).await,
        Err(None),
        "{} sats is above the {cap} received",
        cap + 1
    );
    assert_eq!(recorded(app, &order_id).await, ("cancelled".into(), None));
    assert_eq!(
        record(app, &seller, &order_id, cap, Some("sat")).await,
        Ok(()),
        "{cap} sats is what was received"
    );
    assert_eq!(
        recorded(app, &order_id).await,
        ("refunded_external".into(), Some(cap))
    );
    assert_eq!(recorded_unit(app, &order_id).await.as_deref(), Some("sat"));
    assert_eq!(
        read_order(app, &buyer.token, &order_id).await["external_refund_full"],
        json!(true),
        "the buyer reads the refund of everything received as full"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usd_priced_bitcoin_order_refunds_in_the_sats_it_was_paid(pool: PgPool) {
    let app = test_app(pool).await;
    assert_capped_at(&app, USD_PRICED, USD_PAYABLE_SATS).await;
    // The Bitcoin-only bind keeps the legacy method name.
    let legacy = Bitcoin {
        method: "bitcoin",
        ..USD_PRICED
    };
    assert_capped_at(&app, legacy, USD_PAYABLE_SATS).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_refund_of_a_usd_priced_bitcoin_order_is_in_sats_too(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = cancelled_bitcoin_order(&app, &seller, &buyer, USD_PRICED).await;
    assert_eq!(
        read_order(&app, &buyer.token, &order_id).await["external_refund_full"],
        Value::Null,
        "nothing is refunded yet"
    );
    // More sats than the order's cents, and half of what was paid.
    assert_eq!(
        record(&app, &seller, &order_id, 25_819, Some("sat")).await,
        Ok(())
    );
    assert_eq!(
        recorded(&app, &order_id).await,
        ("refunded_external".into(), Some(25_819))
    );
    // The state is `refunded_external` either way: the buyer, who sees no
    // `refund_cap`, reads the partial refund from the flag.
    for token in [&buyer.token, &seller.token] {
        assert_eq!(
            read_order(&app, token, &order_id).await["external_refund_full"],
            json!(false)
        );
    }
    // A record that names no unit is not read against the sats cap.
    sqlx::query(
        "UPDATE orders SET external_refund = external_refund - 'amount_unit' WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .execute(&app.pool)
    .await
    .expect("an unnamed record");
    assert_eq!(
        read_order(&app, &buyer.token, &order_id).await["external_refund_full"],
        Value::Null
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sats_priced_bitcoin_refund_may_include_the_payment_code(pool: PgPool) {
    let app = test_app(pool).await;
    assert_capped_at(&app, SAT_PRICED, SAT_PAYABLE_SATS).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_overpaid_bitcoin_order_refunds_what_it_received(pool: PgPool) {
    let app = test_app(pool).await;
    for (priced, observed) in [(USD_PRICED, 53_000), (SAT_PRICED, 14_000)] {
        let overpaid = Bitcoin {
            observed: Some(observed),
            ..priced
        };
        assert_capped_at(&app, overpaid, observed).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_underpaid_bitcoin_order_refunds_no_more_than_it_received(pool: PgPool) {
    let app = test_app(pool).await;
    // Short of the payable, and of a sats price.
    for (priced, observed) in [(USD_PRICED, 40_000), (SAT_PRICED, 13_000)] {
        let underpaid = Bitcoin {
            observed: Some(observed),
            ..priced
        };
        assert_capped_at(&app, underpaid, observed).await;
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_paid_off_the_bitcoin_rail_still_refunds_its_total(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    sqlx::query(
        "UPDATE orders SET state = 'cancelled', revision = revision + 1 WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .execute(&app.pool)
    .await
    .expect("a sandbox-paid cancelled order");
    assert_eq!(
        read_order(&app, &seller.token, &order.order_id).await["refund_cap"],
        json!({ "amount_minor": TOTAL_MINOR, "unit": "order", "exponent": 2 }),
    );
    assert_eq!(
        record(&app, &seller, &order.order_id, TOTAL_MINOR + 1, None).await,
        Err(None)
    );
    // A client that names no unit records in the order total's units, as
    // before.
    assert_eq!(
        record(&app, &seller, &order.order_id, TOTAL_MINOR, None).await,
        Ok(())
    );
    assert_eq!(
        recorded(&app, &order.order_id).await,
        ("refunded_external".into(), Some(TOTAL_MINOR))
    );
    assert_eq!(
        recorded_unit(&app, &order.order_id).await.as_deref(),
        Some("order")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_the_seller_sees_the_refund_cap(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = cancelled_bitcoin_order(&app, &seller, &buyer, USD_PRICED).await;
    let buyer_view = read_order(&app, &buyer.token, &order_id).await;
    assert!(buyer_view.get("refund_cap").is_none(), "{buyer_view}");
    let (status, list) = send(
        app.router.clone(),
        "GET",
        "/v1/orders",
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let listed = list["orders"]
        .as_array()
        .expect("orders")
        .iter()
        .find(|order| order["id"] == json!(order_id))
        .cloned()
        .expect("listed");
    assert_eq!(listed["refund_cap"]["unit"], json!("sat"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_in_another_unit_than_the_orders_is_refused(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = cancelled_bitcoin_order(&app, &seller, &buyer, USD_PRICED).await;
    // The cents an older client enters for a USD price are not sats.
    for unit in [Some("order"), Some("usdt0")] {
        assert_eq!(
            record(&app, &seller, &order_id, TOTAL_MINOR, unit).await,
            Err(Some("refund_unit_mismatch".into())),
            "{unit:?}"
        );
    }
    assert_eq!(
        record(&app, &seller, &order_id, TOTAL_MINOR, None).await,
        Err(Some("refund_unit_required".into()))
    );
    assert_eq!(recorded(&app, &order_id).await, ("cancelled".into(), None));

    // A sats price is in sats already: no unit to misread.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let sat_order = cancelled_bitcoin_order(&app, &seller, &buyer, SAT_PRICED).await;
    assert_eq!(
        record(&app, &seller, &sat_order, TOTAL_MINOR, None).await,
        Ok(())
    );
    assert_eq!(
        recorded_unit(&app, &sat_order).await.as_deref(),
        Some("sat")
    );
}

/// A cancelled 137.00 USD Paykit order whose request offered Bitcoin and
/// USDT and whose settlement recorded no rail (`payment_asset` NULL).
async fn cancelled_unrecorded_rail_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    observed: Option<i64>,
) -> String {
    let order_id = cancelled_bitcoin_order(app, seller, buyer, USD_PRICED).await;
    sqlx::query(
        "UPDATE orders SET paykit_amount_minor = $2, paykit_accepted_assets = ARRAY['BTC', 'USDT'], \
         paykit_payment_amounts = jsonb_build_object('BTC', $3::bigint, 'USDT', 137000000), \
         paykit_observed_sats = $4 WHERE id = $1::uuid",
    )
    .bind(&order_id)
    .bind(TOTAL_MINOR)
    .bind(USD_PAYABLE_SATS)
    .bind(observed)
    .execute(&app.pool)
    .await
    .expect("multi-asset terms");
    order_id
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unrecorded_rail_on_a_request_that_offered_usdt_refunds_the_total(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // No Bitcoin observed: the money may have been USDT, so no sats cap.
    let order_id = cancelled_unrecorded_rail_order(&app, &seller, &buyer, None).await;
    assert_eq!(
        read_order(&app, &seller.token, &order_id).await["refund_cap"],
        json!({ "amount_minor": TOTAL_MINOR, "unit": "order", "exponent": 2 }),
    );
    assert_eq!(
        record(&app, &seller, &order_id, USD_PAYABLE_SATS, Some("sat")).await,
        Err(Some("refund_unit_mismatch".into()))
    );
    assert_eq!(
        record(&app, &seller, &order_id, TOTAL_MINOR, Some("order")).await,
        Ok(())
    );
    assert_eq!(
        recorded_unit(&app, &order_id).await.as_deref(),
        Some("order")
    );

    // Bitcoin observed on such a request is the rail that was paid.
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let observed = cancelled_unrecorded_rail_order(&app, &seller, &buyer, Some(51_000)).await;
    assert_eq!(
        read_order(&app, &seller.token, &observed).await["refund_cap"],
        json!({ "amount_minor": 51_000, "unit": "sat", "exponent": 0 }),
    );
    assert_eq!(
        record(&app, &seller, &observed, 51_000, Some("sat")).await,
        Ok(())
    );
}
