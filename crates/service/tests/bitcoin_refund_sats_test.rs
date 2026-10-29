//! A Bitcoin refund is recorded in the sats the order received, whatever its
//! price currency, like the refunded review resolution records it:
//! `refund.record_external` caps it at the observed amount frozen at
//! settlement, else at the payable the request asked for, payment code
//! included. Each order here is paid through the sandbox and then carries the
//! Bitcoin columns a settled Paykit bind leaves.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};

use axum::http::StatusCode;
use common::*;
use serde_json::json;
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

async fn record(app: &TestApp, seller: &TestActor, order_id: &str, amount_minor: i64) -> bool {
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("revision");
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            revision,
            json!({ "amount_minor": amount_minor, "transaction_id": "bitcoin-refund-1" }),
            COMMAND_NUMBER.fetch_add(1, Ordering::Relaxed),
        ),
    )
    .await;
    match status {
        StatusCode::OK => true,
        StatusCode::CONFLICT => {
            assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
            false
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

/// One sat above `cap` is refused, `cap` is recorded as it was sent.
async fn assert_capped_at(app: &TestApp, bitcoin: Bitcoin, cap: i64) {
    let seller = new_actor(app).await;
    let buyer = new_actor(app).await;
    let order_id = cancelled_bitcoin_order(app, &seller, &buyer, bitcoin).await;
    assert!(
        !record(app, &seller, &order_id, cap + 1).await,
        "{} sats is above the {cap} received",
        cap + 1
    );
    assert_eq!(recorded(app, &order_id).await, ("cancelled".into(), None));
    assert!(
        record(app, &seller, &order_id, cap).await,
        "{cap} sats is what was received"
    );
    assert_eq!(
        recorded(app, &order_id).await,
        ("refunded_external".into(), Some(cap))
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
    // More sats than the order's cents, and half of what was paid.
    assert!(record(&app, &seller, &order_id, 25_819).await);
    assert_eq!(
        recorded(&app, &order_id).await,
        ("refunded_external".into(), Some(25_819))
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
    assert!(!record(&app, &seller, &order.order_id, TOTAL_MINOR + 1).await);
    assert!(record(&app, &seller, &order.order_id, TOTAL_MINOR).await);
    assert_eq!(
        recorded(&app, &order.order_id).await,
        ("refunded_external".into(), Some(TOTAL_MINOR))
    );
}
