//! An auction winner's order is created at close, so it has no delivery
//! address until the winner sends one with the payment method, sealed like a
//! buy-now checkout. The order's deadline is the winning reservation's end,
//! never the bind time plus a payment window.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::enable_bitcoin;
use common::*;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use serde_json::{json, Value};
use sqlx::PgPool;

const AUCTION_HOLD_SECONDS: i64 = 30 * 60;

fn require_encrypted_config() -> Config {
    let mut config = Config::for_tests();
    config.require_encrypted_delivery_address = true;
    config
}

fn sealed(seed: u8) -> Value {
    json!({ "key_id": TEST_DELIVERY_KEY_ID, "envelope": test_envelope(seed) })
}

fn plaintext_address() -> Value {
    json!({
        "name": "Ada Lovelace",
        "line1": "12 Analytical St",
        "line2": "",
        "city": "London",
        "region": "",
        "postal_code": "N1 9GU",
        "country_code": "GB",
    })
}

async fn bind(app: &TestApp, token: &str, order_id: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        body,
    )
    .await
}

async fn get_order(app: &TestApp, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

async fn enable_paypal(app: &TestApp, seller: &TestActor) {
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A USD auction closed through the seller's `auction.close` command with
/// a winning and a runner-up bid. Returns the winner's order id.
async fn usd_auction_winning_order(
    app: &TestApp,
    seller: &TestActor,
    winner: &TestActor,
) -> String {
    execute(app, &seller.token, &register_auction_command(&seller.pubky)).await;
    let (status, body) = execute(
        app,
        &winner.token,
        &place_bid_command(&seller.pubky, 20, 10_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid: {body}");
    let runner_up = new_actor(app).await;
    let (status, body) = execute(
        app,
        &runner_up.token,
        &place_bid_command(&seller.pubky, 21, 8_000, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid: {body}");
    app.clock.advance_seconds(11 * 60);
    let (status, body) = execute(
        app,
        &seller.token,
        &close_auction_command(&seller.pubky, 3, 950),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close: {body}");
    let (order_id,): (String,) = sqlx::query_as(
        "SELECT id::text FROM orders WHERE buyer_pubky = $1 AND auction_aggregate_id IS NOT NULL",
    )
    .bind(&winner.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("winning order");
    order_id
}

async fn stored_address(
    app: &TestApp,
    order_id: &str,
) -> (bool, Option<Vec<u8>>, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT delivery_address IS NULL, delivery_address_envelope, delivery_key_id, \
         checkout_command_id::text FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order row")
}

async fn reservation_end(app: &TestApp, buyer_pubky: &str) -> DateTime<Utc> {
    sqlx::query_scalar(
        "SELECT expires_at FROM reservations WHERE buyer_pubky = $1 AND status = 'active'",
    )
    .bind(buyer_pubky)
    .fetch_one(&app.pool)
    .await
    .expect("winner reservation")
}

async fn order_hold(app: &TestApp, order_id: &str) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT hold_expires_at FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_winning_order_carries_the_reservation_end_and_asks_for_an_address(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    let reservation = reservation_end(&app, &winner.pubky).await;
    assert_eq!(
        reservation,
        app.clock.now() + chrono::Duration::seconds(AUCTION_HOLD_SECONDS)
    );
    assert_eq!(order_hold(&app, &order_id).await, Some(reservation));

    let view = get_order(&app, &winner.token, &order_id).await;
    let order = &view;
    assert_eq!(
        order["hold_expires_at"],
        json!(marketplace_service::clock::format_timestamp(reservation))
    );
    assert_eq!(order["stock_held"], json!(false));
    assert_eq!(order["shipping_address_required"], json!(true));
    assert!(order["auction_aggregate_id"].is_string(), "{order}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bind_without_an_address_is_refused_and_binds_nothing(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["reason"], json!("delivery_address_required"));
    let view = get_order(&app, &winner.token, &order_id).await;
    assert_eq!(view["payment_method"], Value::Null);
    assert_eq!(view["shipping_address_required"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_on_a_sealed_address_binds_and_plaintext_is_refused(pool: PgPool) {
    let (app, _paykit, _ipn) =
        test_app_with_payments_config(pool, require_encrypted_config()).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal", "delivery_address": plaintext_address() }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("encrypted_delivery_required")
    );
    assert!(
        !body.to_string().contains("Analytical"),
        "a refusal never echoes the address: {body}"
    );
    let (plaintext_is_null, envelope, ..) = stored_address(&app, &order_id).await;
    assert!(plaintext_is_null && envelope.is_none());

    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal", "delivery_address_envelope": sealed(3) }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["payment_method"], json!("paypal"));
    assert_eq!(body["order"]["shipping_address_required"], json!(false));
    assert!(
        !body.to_string().contains(&test_envelope(3)),
        "the buyer's response never carries the envelope"
    );

    let (plaintext_is_null, envelope, key_id, command_id) = stored_address(&app, &order_id).await;
    assert!(plaintext_is_null, "a sealed order stores no plaintext");
    assert_eq!(envelope.as_deref(), Some(test_envelope_bytes(3).as_slice()));
    assert_eq!(key_id.as_deref(), Some(TEST_DELIVERY_KEY_ID));
    assert_eq!(command_id.as_deref(), Some(order_id.as_str()));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_of_the_paid_auction_order_receives_the_sealed_envelope(pool: PgPool) {
    let (app, _paykit, _ipn) =
        test_app_with_payments_config(pool, require_encrypted_config()).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;
    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal", "delivery_address_envelope": sealed(5) }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let total = body["order"]["total"]["amount_minor"]
        .as_i64()
        .expect("total");
    let mut ipn = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in [
        ("payment_status", "Completed".to_string()),
        ("receiver_email", "merchant@example.com".to_string()),
        ("business", "merchant@example.com".to_string()),
        ("mc_gross", format!("{}.{:02}", total / 100, total % 100)),
        ("mc_currency", "USD".to_string()),
        ("custom", order_id.clone()),
        ("txn_id", "7XP31449AB123456C".to_string()),
    ] {
        ipn.append_pair(name, &value);
    }
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        ipn.finish().into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let seller_view = get_order(&app, &seller.token, &order_id).await;
    assert_eq!(seller_view["state"], json!("paid"), "{seller_view}");
    let address = &seller_view["delivery_address"];
    assert_eq!(address["format"], json!("hpke_v1"), "{seller_view}");
    assert_eq!(address["key_id"], json!(TEST_DELIVERY_KEY_ID));
    assert_eq!(
        address["command_id"],
        json!(order_id),
        "the seller's client rebuilds the envelope binding from the order id"
    );
    assert_eq!(address["envelope"], json!(test_envelope(5)));
    let buyer_view = get_order(&app, &winner.token, &order_id).await;
    assert!(buyer_view.get("delivery_address").is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_off_a_plaintext_address_still_binds(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal", "delivery_address": plaintext_address() }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (plaintext_is_null, envelope, key_id, command_id) = stored_address(&app, &order_id).await;
    assert!(!plaintext_is_null);
    assert!(envelope.is_none() && key_id.is_none() && command_id.is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn malformed_and_doubled_addresses_are_refused_without_echoing_them(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    let mut short = sealed(1);
    short["envelope"] = json!("AAAA");
    let mut bad_key = sealed(1);
    bad_key["key_id"] = json!("not-a-key-id");
    let mut incomplete = plaintext_address();
    incomplete["line1"] = json!("");
    for (label, body, reason) in [
        (
            "short envelope",
            json!({ "method": "paypal", "delivery_address_envelope": short }),
            "invalid_delivery_address",
        ),
        (
            "bad key id",
            json!({ "method": "paypal", "delivery_address_envelope": bad_key }),
            "invalid_delivery_address",
        ),
        (
            "incomplete address",
            json!({ "method": "paypal", "delivery_address": incomplete }),
            "invalid_delivery_address",
        ),
        (
            "both",
            json!({
                "method": "paypal",
                "delivery_address": plaintext_address(),
                "delivery_address_envelope": sealed(1),
            }),
            "delivery_address_and_envelope",
        ),
    ] {
        let (status, response) = bind(&app, &winner.token, &order_id, &body).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{label}: {response}"
        );
        assert_eq!(response["error"]["reason"], json!(reason), "{label}");
        assert!(
            !response.to_string().contains("Analytical"),
            "{label} echoed the address"
        );
    }
    let view = get_order(&app, &winner.token, &order_id).await;
    assert_eq!(view["payment_method"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_ordinary_order_does_not_take_an_address_at_bind(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = bind(
        &app,
        &buyer.token,
        &order.order_id,
        &json!({ "method": "paypal", "delivery_address_envelope": sealed(2) }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("delivery_address_not_accepted")
    );
    let view = get_order(&app, &buyer.token, &order.order_id).await;
    assert_eq!(view["shipping_address_required"], json!(false));
    assert_eq!(view["auction_aggregate_id"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bind_after_the_reservation_ended_is_refused(pool: PgPool) {
    let (app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_paypal(&app, &seller).await;
    let order_id = usd_auction_winning_order(&app, &seller, &winner).await;

    app.clock.advance_seconds(AUCTION_HOLD_SECONDS + 1);
    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "paypal", "delivery_address_envelope": sealed(4) }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("auction_window_ended"));
    let (plaintext_is_null, envelope, ..) = stored_address(&app, &order_id).await;
    assert!(
        plaintext_is_null && envelope.is_none(),
        "the refusal stored nothing"
    );
}

/// The bind of an upstream Bitcoin payment used to set the order's hold to
/// the Paykit prepare expiry plus the payment window and a clock margin, a
/// deadline well past the 30-minute winner window the buyer was shown.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_upstream_bitcoin_bind_keeps_the_reservation_end_as_the_deadline(pool: PgPool) {
    let (app, paykit) = test_app_with_upstream_paykit(pool).await;
    let seller = new_actor(&app).await;
    let winner = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let (order_id, _aggregate) = auction_winning_order(&app, &seller, &winner, 1).await;
    let reservation = reservation_end(&app, &winner.pubky).await;
    assert_eq!(order_hold(&app, &order_id).await, Some(reservation));

    let (status, body) = bind(
        &app,
        &winner.token,
        &order_id,
        &json!({ "method": "bitcoin", "delivery_address_envelope": sealed(6) }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        order_hold(&app, &order_id).await,
        Some(reservation),
        "the deadline is the reservation end, not the bind time plus a window"
    );
    let view = get_order(&app, &winner.token, &order_id).await;
    assert_eq!(
        view["hold_expires_at"],
        json!(marketplace_service::clock::format_timestamp(reservation))
    );
}
