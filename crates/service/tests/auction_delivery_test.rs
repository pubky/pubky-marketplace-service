//! An auction winner's order is created at close, so it has no delivery
//! address until the winner sends one with the payment method, sealed like a
//! buy-now checkout. The order's deadline is the winning reservation's end,
//! never the bind time plus a payment window.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::paykit_review::{auction_winning_order, enable_bitcoin};
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
/// deadline well past the 30-minute winner window the buyer was shown. It now
/// ends where the reservation does, less the one clock margin the Paykit
/// activation guard keeps, and the order page shows that same instant.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_upstream_bitcoin_bind_ends_the_deadline_within_the_reservation(pool: PgPool) {
    use marketplace_service::payment_methods::UPSTREAM_HOLD_CLOCK_SKEW_SECONDS;
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
    let deadline = reservation - chrono::Duration::seconds(UPSTREAM_HOLD_CLOCK_SKEW_SECONDS);
    assert_eq!(
        order_hold(&app, &order_id).await,
        Some(deadline),
        "the deadline is the reservation end less one margin, not the bind time plus a window"
    );
    let view = get_order(&app, &winner.token, &order_id).await;
    assert_eq!(
        view["hold_expires_at"],
        json!(marketplace_service::clock::format_timestamp(deadline))
    );
}

async fn wait_for_db_lock_waiter(pool: &PgPool) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() \
               AND pid <> pg_backend_pid() \
               AND state = 'active' \
               AND wait_event_type = 'Lock'",
        )
        .fetch_one(pool)
        .await
        .expect("query pg_stat_activity for lock waiter");
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for transaction to block on PostgreSQL lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[derive(sqlx::FromRow)]
struct OrderBindState {
    payment_method: Option<String>,
    stock_held: bool,
    hold_expires_at: Option<DateTime<Utc>>,
    plaintext_is_null: bool,
    delivery_address_envelope: Option<Vec<u8>>,
    delivery_key_id: Option<String>,
}

async fn order_bind_state(pool: &PgPool, order_id: &str) -> OrderBindState {
    sqlx::query_as(
        "SELECT payment_method, stock_held, hold_expires_at, \
         delivery_address IS NULL AS plaintext_is_null, \
         delivery_address_envelope, delivery_key_id \
         FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order bind state")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bind_blocked_on_reservation_lock_across_expiry_fails_closed_with_no_side_effects(
    pool: PgPool,
) {
    // 1. PayPal rail (has no later upstream Paykit auction-window gate): block
    //    on the winning reservation FOR UPDATE lock while the clock crosses
    //    `expires_at`, then unlock and assert `auction_window_ended` with zero
    //    persisted bind, delivery address/envelope, hold, or payment mutation.
    let (paypal_app, _paykit, _ipn) = test_app_with_payments_and_ipn(pool.clone()).await;
    let seller = new_actor(&paypal_app).await;
    let winner = new_actor(&paypal_app).await;
    enable_paypal(&paypal_app, &seller).await;
    let paypal_order_id = usd_auction_winning_order(&paypal_app, &seller, &winner).await;
    let initial_hold = order_hold(&paypal_app, &paypal_order_id).await;

    let mut blocker_tx = pool.begin().await.expect("paypal reservation blocker tx");
    let locked_res_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT id FROM reservations \
         WHERE buyer_pubky = $1 AND status = 'active' \
         ORDER BY expires_at DESC LIMIT 1 FOR UPDATE",
    )
    .bind(&winner.pubky)
    .fetch_one(&mut *blocker_tx)
    .await
    .expect("lock winning reservation");
    assert!(!locked_res_id.is_nil());

    let router = paypal_app.router.clone();
    let token = winner.token.clone();
    let order_id_for_task = paypal_order_id.clone();
    let bind_task = tokio::spawn(async move {
        send(
            router,
            "POST",
            &format!("/v0/orders/{order_id_for_task}/payment-method"),
            Some(&token),
            &json!({ "method": "paypal", "delivery_address_envelope": sealed(7) }),
        )
        .await
    });

    wait_for_db_lock_waiter(&pool).await;
    paypal_app.clock.advance_seconds(AUCTION_HOLD_SECONDS + 1);
    blocker_tx.rollback().await.expect("release lock");

    let (status, body) = bind_task.await.expect("paypal bind task");
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "expired during reservation lock wait must refuse: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("auction_window_ended"));
    let paypal_state = order_bind_state(&pool, &paypal_order_id).await;
    assert_eq!(
        paypal_state.payment_method, None,
        "must not bind payment_method"
    );
    assert!(!paypal_state.stock_held, "must not acquire stock_held");
    assert_eq!(
        paypal_state.hold_expires_at, initial_hold,
        "must not alter hold"
    );
    assert!(
        paypal_state.plaintext_is_null
            && paypal_state.delivery_address_envelope.is_none()
            && paypal_state.delivery_key_id.is_none(),
        "must not persist winner delivery address or envelope"
    );
    let (adapter, p_state): (String, String) =
        sqlx::query_as("SELECT adapter, state FROM payments WHERE order_id = $1::uuid")
            .bind(&paypal_order_id)
            .fetch_one(&pool)
            .await
            .expect("paypal payment state");
    assert_eq!(adapter, "sandbox");
    assert_eq!(p_state, "awaiting_entitlement");
    let bound_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events \
         WHERE aggregate_id = $1 AND kind = 'order.payment_method_bound'",
    )
    .bind(format!("order:{paypal_order_id}"))
    .fetch_one(&pool)
    .await
    .expect("bound events count");
    assert_eq!(bound_events, 0, "must not emit payment_method_bound event");

    // 2. Fork Paykit rail (skips the upstream-only auction-window check):
    //    verify the same FOR UPDATE expiry race fails closed with zero
    //    side effects and zero Paykit prepare requests.
    let (fork_app, fork_paykit) = test_app_with_payments(pool.clone()).await;
    let fork_seller = new_actor(&fork_app).await;
    let fork_winner = new_actor(&fork_app).await;
    enable_bitcoin(&fork_app, &fork_paykit, &fork_seller).await;
    let (fork_order_id, _agg) =
        auction_winning_order(&fork_app, &fork_seller, &fork_winner, 2).await;
    let fork_initial_hold = order_hold(&fork_app, &fork_order_id).await;

    let mut fork_blocker = pool.begin().await.expect("fork reservation blocker tx");
    let _: uuid::Uuid = sqlx::query_scalar(
        "SELECT id FROM reservations \
         WHERE buyer_pubky = $1 AND status = 'active' \
         ORDER BY expires_at DESC LIMIT 1 FOR UPDATE",
    )
    .bind(&fork_winner.pubky)
    .fetch_one(&mut *fork_blocker)
    .await
    .expect("lock fork winning reservation");

    let fork_router = fork_app.router.clone();
    let fork_token = fork_winner.token.clone();
    let fork_order_for_task = fork_order_id.clone();
    let fork_task = tokio::spawn(async move {
        send(
            fork_router,
            "POST",
            &format!("/v0/orders/{fork_order_for_task}/payment-method"),
            Some(&fork_token),
            &json!({ "method": "bitcoin", "delivery_address_envelope": sealed(8) }),
        )
        .await
    });

    wait_for_db_lock_waiter(&pool).await;
    fork_app.clock.advance_seconds(AUCTION_HOLD_SECONDS + 1);
    fork_blocker.rollback().await.expect("release fork lock");

    let (status, body) = fork_task.await.expect("fork bind task");
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "fork paykit expired during reservation lock wait must refuse: {body}"
    );
    assert_eq!(body["error"]["reason"], json!("auction_window_ended"));
    assert!(
        fork_paykit.requests().is_empty(),
        "must not issue Paykit prepare request after auction expiry"
    );
    let fork_state = order_bind_state(&pool, &fork_order_id).await;
    assert_eq!(fork_state.payment_method, None);
    assert!(!fork_state.stock_held);
    assert_eq!(fork_state.hold_expires_at, fork_initial_hold);
    assert!(
        fork_state.plaintext_is_null
            && fork_state.delivery_address_envelope.is_none()
            && fork_state.delivery_key_id.is_none()
    );
}
