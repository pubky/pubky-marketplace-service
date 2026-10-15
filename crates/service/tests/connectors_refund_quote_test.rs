//! The payment basis an order recorded travels with its refund to the
//! provider: the Bitcoin quote pinned at bind for a refund in sats, the USDT
//! payment terms for one in USDT, nothing for a fiat rail. Driven through the
//! real event feed and worker with the framework's in-test provider.

mod common;

use axum::http::StatusCode;
use common::connector_provider::*;
use common::{create_paid_order, execute, new_actor, order_command, PaidOrder, TestActor, TestApp};
use marketplace_service::connectors::provider::RefundQuote;
use marketplace_service::connectors::worker;
use serde_json::json;
use sqlx::PgPool;

struct Paid {
    app: TestApp,
    store: std::sync::Arc<TestStore>,
    seller: TestActor,
    buyer: TestActor,
    order: PaidOrder,
}

async fn pushed(pool: PgPool) -> Paid {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    Paid {
        app,
        store,
        seller,
        buyer,
        order,
    }
}

/// Ship, deliver, return, approve, receive and record the refund. The handler
/// (and `orders_external_refund_within_total`) keep `amount_minor` within the
/// order total, so it is always an amount in the order currency.
async fn refund_the_order(paid: &Paid) {
    let order_id = paid.order.order_id.as_str();
    for (seller, kind, revision, payload, number) in [
        (
            true,
            "fulfillment.ship",
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "T-1" }),
            1_210,
        ),
        (false, "fulfillment.confirm_delivery", 3, json!({}), 1_211),
        (
            false,
            "return.request",
            4,
            json!({ "reason": "wrong size", "requested_amount_minor": paid.order.total_minor }),
            1_212,
        ),
        (true, "return.approve", 5, json!({}), 1_213),
        (true, "return.receive", 6, json!({}), 1_214),
        (
            true,
            "refund.record_external",
            7,
            json!({ "amount_minor": paid.order.total_minor, "transaction_id": "bitcoin-tx-1" }),
            1_215,
        ),
    ] {
        let token = if seller {
            &paid.seller.token
        } else {
            &paid.buyer.token
        };
        let (status, body) = execute(
            &paid.app,
            token,
            &order_command(kind, order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sat_refund_carries_the_bitcoin_quote_the_order_was_bound_at(pool: PgPool) {
    let paid = pushed(pool).await;
    refund_the_order(&paid).await;
    sqlx::query(
        "UPDATE orders SET payment_method = 'bitcoin', bitcoin_quote_rate = 65000, \
         bitcoin_quote_currency = 'USD', bitcoin_quote_exponent = 2, bitcoin_quoted_sats = 210770, \
         bitcoin_quote_source = 'blocktank', bitcoin_quote_fetched_at = '2026-08-19T21:59:00Z' \
         WHERE id = $1::uuid",
    )
    .bind(&paid.order.order_id)
    .execute(&paid.app.pool)
    .await
    .expect("order paid in bitcoin");
    worker::run_pass(&paid.app.state).await.expect("pass");
    let refund = paid
        .store
        .last_refund
        .lock()
        .expect("lock")
        .clone()
        .expect("a refund was mirrored");
    assert_eq!(refund.amount_unit, "sat");
    assert_eq!(refund.amount_minor, Some(paid.order.total_minor));
    match refund.quote {
        Some(RefundQuote::Bitcoin {
            currency,
            exponent,
            rate,
            quoted_sats,
            source,
            fetched_at,
        }) => {
            assert_eq!(
                (currency.as_str(), exponent, rate.as_str()),
                ("USD", 2, "65000")
            );
            assert_eq!(quoted_sats, Some(210_770));
            assert_eq!(source.as_deref(), Some("blocktank"));
            assert_eq!(
                fetched_at.map(|at| at.to_rfc3339()),
                Some("2026-08-19T21:59:00+00:00".to_string())
            );
        }
        other => panic!("expected the bitcoin quote, got {other:?}"),
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_carries_the_payment_terms_of_the_order(pool: PgPool) {
    let paid = pushed(pool).await;
    refund_the_order(&paid).await;
    sqlx::query(
        "UPDATE orders SET payment_method = 'usdt', payment_asset = 'USDT', \
         payment_network = 'ethereum', payment_amount_minor = 137000000, payment_exponent = 6, \
         payment_quote_basis = 'usd_parity' WHERE id = $1::uuid",
    )
    .bind(&paid.order.order_id)
    .execute(&paid.app.pool)
    .await
    .expect("order paid in usdt");
    worker::run_pass(&paid.app.state).await.expect("pass");
    let refund = paid
        .store
        .last_refund
        .lock()
        .expect("lock")
        .clone()
        .expect("a refund was mirrored");
    assert_eq!(refund.amount_unit, "usdt");
    match refund.quote {
        Some(RefundQuote::Asset {
            asset,
            network,
            paid_minor,
            paid_exponent,
            basis,
        }) => {
            assert_eq!(
                (asset.as_str(), paid_minor, paid_exponent),
                ("USDT", 137_000_000, 6)
            );
            assert_eq!(network.as_deref(), Some("ethereum"));
            assert_eq!(basis.as_deref(), Some("usd_parity"));
        }
        other => panic!("expected the USDT terms, got {other:?}"),
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_fiat_refund_carries_no_quote(pool: PgPool) {
    let paid = pushed(pool).await;
    refund_the_order(&paid).await;
    worker::run_pass(&paid.app.state).await.expect("pass");
    let refund = paid
        .store
        .last_refund
        .lock()
        .expect("lock")
        .clone()
        .expect("a refund was mirrored");
    assert_eq!(refund.quote, None);
    assert_eq!(refund.amount_minor, Some(paid.order.total_minor));
}
