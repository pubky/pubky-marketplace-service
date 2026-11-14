//! The seller's wallet Receipt as the seller's confirmation
//! (`PAYMENT_CONFIRMATION=seller`, pubky/paykit-rs#245): an upstream order
//! inside the seller window is paid when paykit-server returns the Receipt
//! the seller's wallet issued for it. No Receipt, or a paykit-server without
//! the route, keeps the order waiting for the seller's tap.

mod common;

use axum::http::StatusCode;
use common::paykit_review::{bound_order, confirm_call};
use common::{
    exclusive_facts, new_actor, spawn_fake_homeserver, with_shop_homeserver, FakePaykitStatus,
    TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::payments::{
    parse_seller_receipt, pubky_app_key, PaykitObservation, PaykitStatusFacts, PaykitStatusOutcome,
    SellerReceipt, SellerReceiptAmount, SellerReceiptOutcome,
};
use marketplace_service::workers::verify_due_paykit_payments;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_SATS: u64 = 51_637;

fn confirmed() -> PaykitStatusOutcome {
    PaykitStatusOutcome::Confirmed {
        amount_matched: true,
        facts: PaykitStatusFacts {
            observation: PaykitObservation {
                txid: Some("9f2c".repeat(16)),
                observed_sats: Some(OBSERVED_SATS),
                confirmations: Some(2),
                ..PaykitObservation::default()
            },
            ..exclusive_facts()
        },
    }
}

fn receipt(sats: u64) -> SellerReceipt {
    let mut metadata = serde_json::Map::new();
    metadata.insert("txid".into(), json!("9f2c".repeat(16)));
    SellerReceipt {
        receipt_id: "4b0f3d7e-1c55-4f1e-9a51-0c1d2e3f4a5b".to_string(),
        app_id: "bitkit".to_string(),
        payment_request_id: "7d1e2f3a-4b5c-4d6e-8f70-81a2b3c4d5e6".to_string(),
        payment_reference: "ref-1".to_string(),
        payment_endpoint_identifier: Some("onchain".to_string()),
        amount: Some(SellerReceiptAmount {
            value: sats.to_string(),
            currency: "SAT".to_string(),
        }),
        metadata,
        issued_at: "2026-11-14T10:00:00Z".to_string(),
        stored_at: "2026-11-14T10:00:05Z".to_string(),
    }
}

struct Fixture {
    app: TestApp,
    paykit: common::FakePaykit,
    status: FakePaykitStatus,
    seller: TestActor,
    buyer: TestActor,
}

async fn fixture(pool: PgPool) -> Fixture {
    let mut config = marketplace_service::config::Config::for_tests();
    config.payment_confirmation = marketplace_service::config::PaymentConfirmation::Seller;
    let (app, paykit, _ipn) = common::test_app_with_payments_config(pool, config).await;
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app, &homeserver);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    Fixture {
        app,
        paykit,
        status: FakePaykitStatus::default(),
        seller,
        buyer,
    }
}

/// An upstream seller-confirmed order whose payment Paykit reports matched
/// and on time: it waits in the seller window. Returns the order, payment and
/// invoice ids.
async fn waiting_order(fx: &Fixture) -> (String, String, Uuid) {
    let (order_id, payment_id, reference) =
        bound_order(&fx.app, &fx.paykit, &fx.seller, &fx.buyer, "exclusive").await;
    let order_uuid = Uuid::parse_str(&order_id).expect("order uuid");
    let (invoice_id,): (Uuid,) = sqlx::query_as(
        "UPDATE orders SET paykit_api = 'upstream', paykit_stack_id = NULL, \
         paykit_stack_endpoint = NULL, paykit_allocation_mode = NULL, \
         paykit_payment_reference = gen_random_uuid(), paykit_operation_id = 'upstream-op', \
         paykit_payment_window_seconds = 1800, paykit_asset = 'BTC' WHERE id = $1 \
         RETURNING paykit_invoice_id",
    )
    .bind(order_uuid)
    .fetch_one(&fx.app.pool)
    .await
    .expect("present the order as upstream");
    fx.status.set_outcome(&reference, confirmed());
    poll(fx).await;
    assert_eq!(
        request_state(fx, &order_id).await,
        "awaiting_seller_confirmation"
    );
    (order_id, payment_id, invoice_id)
}

async fn poll(fx: &Fixture) {
    fx.app.clock.advance_seconds(60);
    verify_due_paykit_payments(&fx.app.state, &fx.status, fx.app.clock.now())
        .await
        .expect("poll runs");
}

async fn request_state(fx: &Fixture, order_id: &str) -> String {
    sqlx::query_scalar("SELECT paykit_request_state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(&fx.app.pool)
        .await
        .expect("order row")
}

async fn order_state(fx: &Fixture, order_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM orders WHERE id = $1")
        .bind(Uuid::parse_str(order_id).expect("order uuid"))
        .fetch_one(&fx.app.pool)
        .await
        .expect("order row")
}

async fn confirmation(fx: &Fixture, order_id: &str) -> Option<(String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT confirmed_by_pubky, confirmation_basis, wallet_receipt->>'receipt_id' \
         FROM paykit_seller_confirmations WHERE order_id = $1",
    )
    .bind(Uuid::parse_str(order_id).expect("order uuid"))
    .fetch_optional(&fx.app.pool)
    .await
    .expect("confirmation row")
}

async fn count(fx: &Fixture, sql: &str) -> i64 {
    common::count(&fx.app.pool, sql).await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_wallet_receipt_pays_the_waiting_order_once(pool: PgPool) {
    let fx = fixture(pool).await;
    let (order_id, payment_id, invoice_id) = waiting_order(&fx).await;

    fx.status.set_receipt(
        invoice_id,
        SellerReceiptOutcome::Issued(Box::new(receipt(OBSERVED_SATS))),
    );
    poll(&fx).await;

    assert_eq!(order_state(&fx, &order_id).await, "paid");
    assert_eq!(request_state(&fx, &order_id).await, "confirmed");
    assert_eq!(
        confirmation(&fx, &order_id).await,
        Some((
            fx.seller.pubky.clone(),
            "seller_wallet_receipt".to_string(),
            Some(receipt(OBSERVED_SATS).receipt_id)
        ))
    );
    assert_eq!(
        count(
            &fx,
            &format!(
                "SELECT COUNT(*) FROM events WHERE kind = 'payment.confirmed' \
                 AND aggregate_id = 'payment:{payment_id}'"
            )
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &fx,
            &format!(
                "SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = '{order_id}' \
                 AND resolution = 'paid_manually'"
            )
        )
        .await,
        1
    );

    // Later polls and the seller's own tap change nothing.
    poll(&fx).await;
    let (code, body) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(
        body["confirmation"]["confirmation_basis"],
        json!("seller_wallet_receipt")
    );
    assert_eq!(
        count(
            &fx,
            &format!("SELECT COUNT(*) FROM paykit_resolve_outbox WHERE order_id = '{order_id}'")
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_a_receipt_or_the_route_the_order_waits_for_the_seller(pool: PgPool) {
    let fx = fixture(pool).await;
    let (order_id, _, invoice_id) = waiting_order(&fx).await;

    // An older paykit-server answers 404: the double's default.
    poll(&fx).await;
    assert_eq!(
        request_state(&fx, &order_id).await,
        "awaiting_seller_confirmation"
    );

    // The server has the route but no Receipt yet, or the read fails.
    for outcome in [
        SellerReceiptOutcome::NotIssued,
        SellerReceiptOutcome::Unavailable,
    ] {
        fx.status.set_receipt(invoice_id, outcome);
        poll(&fx).await;
        assert_eq!(
            request_state(&fx, &order_id).await,
            "awaiting_seller_confirmation"
        );
    }
    assert_eq!(confirmation(&fx, &order_id).await, None);

    // The seller's tap still pays it.
    let (code, body) = confirm_call(&fx.app, &fx.seller.token, &order_id, &json!({})).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(
        confirmation(&fx, &order_id).await,
        Some((
            fx.seller.pubky.clone(),
            "seller_attestation".to_string(),
            None
        ))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_receipt_for_a_different_amount_leaves_the_order_to_the_seller(pool: PgPool) {
    let fx = fixture(pool).await;
    let (order_id, _, invoice_id) = waiting_order(&fx).await;

    fx.status.set_receipt(
        invoice_id,
        SellerReceiptOutcome::Issued(Box::new(receipt(OBSERVED_SATS - 1_000))),
    );
    poll(&fx).await;
    assert_eq!(
        request_state(&fx, &order_id).await,
        "awaiting_seller_confirmation"
    );
    assert_eq!(confirmation(&fx, &order_id).await, None);
}

#[test]
fn the_receipt_route_answer_parses_to_an_outcome() {
    let invoice_id = Uuid::new_v4();
    let present = json!({"invoice_id": invoice_id, "receipt": receipt(OBSERVED_SATS), "later": 1});
    assert_eq!(
        parse_seller_receipt(StatusCode::OK, &present.to_string(), invoice_id),
        SellerReceiptOutcome::Issued(Box::new(receipt(OBSERVED_SATS)))
    );
    let absent = json!({"invoice_id": invoice_id, "receipt": null});
    assert_eq!(
        parse_seller_receipt(StatusCode::OK, &absent.to_string(), invoice_id),
        SellerReceiptOutcome::NotIssued
    );
    assert_eq!(
        parse_seller_receipt(StatusCode::NOT_FOUND, "", invoice_id),
        SellerReceiptOutcome::Unsupported
    );
    // Another invoice's answer, or a failure, is no Receipt for this one.
    assert_eq!(
        parse_seller_receipt(StatusCode::OK, &present.to_string(), Uuid::new_v4()),
        SellerReceiptOutcome::Unavailable
    );
    assert_eq!(
        parse_seller_receipt(StatusCode::SERVICE_UNAVAILABLE, "{}", invoice_id),
        SellerReceiptOutcome::Unavailable
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_client_reads_the_receipt_route_and_falls_back_on_404(pool: PgPool) {
    let fx = fixture(pool).await;
    let client = fx
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("paykit client");
    // The route is upstream-only: requests are signed over method, path and body.
    fx.paykit.use_upstream_api();
    let invoice_id = Uuid::new_v4();
    let read = || client.marketplace_seller_receipt(&fx.seller.pubky, invoice_id);

    // A paykit-server without the route.
    assert_eq!(read().await, SellerReceiptOutcome::Unsupported);

    // The route, with no Receipt yet, then with the wallet's Receipt.
    fx.paykit
        .set_marketplace_receipt(invoice_id, serde_json::Value::Null);
    assert_eq!(read().await, SellerReceiptOutcome::NotIssued);
    fx.paykit
        .set_marketplace_receipt(invoice_id, json!(receipt(OBSERVED_SATS)));
    assert_eq!(
        read().await,
        SellerReceiptOutcome::Issued(Box::new(receipt(OBSERVED_SATS)))
    );

    // An invoice the server does not know answers 404 too: no Receipt.
    assert_eq!(
        client
            .marketplace_seller_receipt(&fx.seller.pubky, Uuid::new_v4())
            .await,
        SellerReceiptOutcome::Unsupported
    );

    // Each read was signed and named the seller as the request's creator.
    let calls: Vec<_> = fx
        .paykit
        .calls()
        .into_iter()
        .filter(|call| call.path == "/marketplace/payment-requests/receipt")
        .collect();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0].body,
        json!({"creator": pubky_app_key(&fx.seller.pubky), "invoice_id": invoice_id})
    );
}
