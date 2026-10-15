//! A PayPal refund that is reversed and the reversal cancelled, driven through
//! the real IPN route, against the refund mirror the connector worker queues
//! for it. The restore writes the order's refund afresh, so what keeps the
//! mirror once, and once only, is the identity the record keeps.

mod common;

use axum::http::StatusCode;
use common::connector_provider::*;
use common::*;
use marketplace_service::connectors::worker;
use serde_json::{json, Value};
use sqlx::PgPool;

const PAYMENT_TXN: &str = "7XP31449AB123456C";
const TOTAL_MINOR: i64 = 13_700;
const FULL_REFUND_TXN: &str = "2WF58163VJ0384519";
const PARTIAL_1_TXN: &str = "3KA71027LM5520841";

type Fields = Vec<(String, String)>;

fn fixture(name: &str, order_id: &str) -> Fields {
    let path = format!(
        "{}/tests/fixtures/paypal_ipn/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    url::form_urlencoded::parse(text.trim_end().as_bytes())
        .map(|(name, value)| {
            let value = if value == "{{ORDER_ID}}" {
                order_id.to_string()
            } else {
                value.into_owned()
            };
            (name.into_owned(), value)
        })
        .collect()
}

fn with(mut fields: Fields, name: &str, value: &str) -> Fields {
    let entry = fields
        .iter_mut()
        .find(|(key, _)| key == name)
        .unwrap_or_else(|| panic!("fixture carries {name}"));
    entry.1 = value.to_string();
    fields
}

/// A refund-family fixture for the order's payment, with its own gross and
/// transaction id.
fn notice(name: &str, order_id: &str, gross: &str, txn: &str) -> Fields {
    with(
        with(
            with(fixture(name, order_id), "parent_txn_id", PAYMENT_TXN),
            "mc_gross",
            gross,
        ),
        "txn_id",
        txn,
    )
}

async fn post_ipn(app: &TestApp, fields: &Fields) {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in fields {
        serializer.append_pair(name, value);
    }
    let (status, _) = send_bytes(
        app.router.clone(),
        "POST",
        "/v0/paypal/ipn",
        serializer.finish().into_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

struct World {
    app: TestApp,
    store: std::sync::Arc<TestStore>,
    seller: TestActor,
    order_id: String,
    _paykit: FakePaykit,
    _ipn: FakePaypalIpn,
}

/// A connected, mapped store and a PayPal order paid after the connection,
/// whose push has been created.
async fn paid_paypal_order_in_a_connected_store(pool: PgPool) -> World {
    let (base, paykit, ipn) = test_app_with_payments_and_ipn(pool).await;
    let (app, stores) = with_connectors(base, test_keys(), test_settings());
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {body}");
    let order = create_pending_order(&app, &seller, &buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", order.order_id),
        Some(&buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    post_ipn(&app, &fixture("completed.ipn", &order.order_id)).await;
    worker::run_pass(&app.state).await.expect("pass");
    let state: String = sqlx::query_scalar(
        "SELECT state FROM connector_order_push WHERE pubky_order_id = $1::uuid",
    )
    .bind(&order.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("a push");
    assert_eq!(state, "created");
    World {
        app,
        store: stores.shopify,
        seller,
        order_id: order.order_id,
        _paykit: paykit,
        _ipn: ipn,
    }
}

impl World {
    async fn ipn(&self, name: &str, gross: &str, txn: &str) {
        post_ipn(&self.app, &notice(name, &self.order_id, gross, txn)).await;
    }

    async fn pass(&self) {
        worker::run_pass(&self.app.state).await.expect("pass");
    }

    async fn consume(&self) {
        worker::consume_events(&self.app.state)
            .await
            .expect("consume");
    }

    async fn external_refund(&self) -> Value {
        sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1::uuid")
            .bind(&self.order_id)
            .fetch_one(&self.app.pool)
            .await
            .expect("order refund")
    }

    async fn refund_mirrors(&self) -> Vec<(String, String, Value)> {
        sqlx::query_as(
            "SELECT a.state, a.action_key, a.payload FROM connector_order_actions a \
             WHERE a.kind = 'refund' ORDER BY a.created_at, a.id",
        )
        .fetch_all(&self.app.pool)
        .await
        .expect("refund mirrors")
    }

    fn refunds_at_the_store(&self) -> Vec<Option<i64>> {
        self.store.refund_amounts.lock().expect("amounts").clone()
    }

    async fn alerts(&self) -> Vec<(String, String)> {
        let (_, alerts) = api(
            &self.app,
            "GET",
            "/v1/connectors/alerts",
            Some(&self.seller.token),
            &json!(null),
        )
        .await;
        alerts["alerts"]
            .as_array()
            .expect("alerts")
            .iter()
            .map(|alert| {
                (
                    alert["kind"].as_str().expect("kind").to_string(),
                    alert["detail"].as_str().expect("detail").to_string(),
                )
            })
            .collect()
    }
}

/// A full refund, its event consumed: the mirror is queued. The tests then
/// reverse a little of it and cancel that, which writes the same refund afresh.
async fn a_full_refund_is_recorded(world: &World) {
    world
        .ipn("refund-full.ipn", "-137.00", FULL_REFUND_TXN)
        .await;
    world.consume().await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_restored_refund_that_stands_keeps_its_unsent_mirror(pool: PgPool) {
    let world = paid_paypal_order_in_a_connected_store(pool).await;
    a_full_refund_is_recorded(&world).await;
    let recorded = world.external_refund().await;
    let before = world.refund_mirrors().await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].0, "pending");

    world.app.clock.advance_seconds(600);
    world
        .ipn("reversal-full.ipn", "-40.00", "5MC93249NP7742999")
        .await;
    world.app.clock.advance_seconds(600);
    world
        .ipn("canceled-reversal.ipn", "40.00", "6ND04350PQ8853999")
        .await;
    let restored = world.external_refund().await;
    assert_eq!(restored["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(restored["transaction_id"], json!(FULL_REFUND_TXN));
    assert_eq!(
        restored["recorded_at"], recorded["recorded_at"],
        "a refund restored unchanged keeps the identity it was recorded with"
    );
    world.consume().await;
    assert_eq!(
        world.refund_mirrors().await,
        before,
        "the queued mirror is neither dropped nor replaced"
    );
    world.pass().await;
    assert_eq!(world.refunds_at_the_store(), vec![Some(TOTAL_MINOR)]);
    assert!(world.alerts().await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_restored_refund_that_stands_does_not_alert_for_its_sent_mirror(pool: PgPool) {
    let world = paid_paypal_order_in_a_connected_store(pool).await;
    a_full_refund_is_recorded(&world).await;
    world.pass().await;
    assert_eq!(world.refunds_at_the_store(), vec![Some(TOTAL_MINOR)]);

    world.app.clock.advance_seconds(600);
    world
        .ipn("reversal-full.ipn", "-40.00", "5MC93249NP7742999")
        .await;
    world.app.clock.advance_seconds(600);
    world
        .ipn("canceled-reversal.ipn", "40.00", "6ND04350PQ8853999")
        .await;
    world.pass().await;
    assert_eq!(
        world.refunds_at_the_store(),
        vec![Some(TOTAL_MINOR)],
        "the store still has the one refund"
    );
    assert!(
        world.alerts().await.is_empty(),
        "a refund that stands is not reported as reversed"
    );
}

/// A partial refund, then a chargeback of the rest that completes the
/// refund, then the cancelled chargeback: the order is open again and holds
/// the partial refund, under a record of its own.
async fn partial_refund_with_a_cancelled_chargeback(world: &World) {
    world
        .ipn("refund-partial-1.ipn", "-40.00", PARTIAL_1_TXN)
        .await;
    world
        .ipn("reversal-full.ipn", "-97.00", "5MC93249NP7742888")
        .await;
    world.consume().await;
    let mirrors = world.refund_mirrors().await;
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0].2["amount_minor"], json!(TOTAL_MINOR));
    assert_eq!(mirrors[0].2["full"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_reversal_replaces_the_unsent_mirror_with_the_refund_that_stands(pool: PgPool) {
    let world = paid_paypal_order_in_a_connected_store(pool).await;
    partial_refund_with_a_cancelled_chargeback(&world).await;
    let first_key = world.refund_mirrors().await[0].1.clone();

    world.app.clock.advance_seconds(600);
    world
        .ipn("canceled-reversal.ipn", "97.00", "6ND04350PQ8853888")
        .await;
    let standing = world.external_refund().await;
    assert_eq!(standing["amount_minor"], json!(4_000));
    assert_eq!(standing["transaction_id"], json!(PARTIAL_1_TXN));
    world.consume().await;
    let mirrors = world.refund_mirrors().await;
    assert_eq!(
        mirrors.len(),
        1,
        "the unsent mirror of the old amount is gone"
    );
    assert_ne!(mirrors[0].1, first_key);
    assert_eq!(mirrors[0].0, "pending");
    assert_eq!(mirrors[0].2["amount_minor"], json!(4_000));
    assert_eq!(mirrors[0].2["full"], json!(false));

    world.pass().await;
    assert_eq!(
        world.refunds_at_the_store(),
        vec![Some(4_000)],
        "the refund that stands reaches the store once"
    );
    assert!(world.alerts().await.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_reversal_after_the_mirror_was_sent_alerts_and_adds_no_second_refund(
    pool: PgPool,
) {
    let world = paid_paypal_order_in_a_connected_store(pool).await;
    partial_refund_with_a_cancelled_chargeback(&world).await;
    world.pass().await;
    assert_eq!(world.refunds_at_the_store(), vec![Some(TOTAL_MINOR)]);

    world.app.clock.advance_seconds(600);
    world
        .ipn("canceled-reversal.ipn", "97.00", "6ND04350PQ8853888")
        .await;
    world.pass().await;
    assert_eq!(
        world.refunds_at_the_store(),
        vec![Some(TOTAL_MINOR)],
        "a second record would add to what the store already holds"
    );
    let alerts = world.alerts().await;
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].0, "refund_reversed_after_mirror");
    assert!(alerts[0].1.contains("reversed or reduced"));
    world.pass().await;
    assert_eq!(world.alerts().await.len(), 1, "one alert per mirror");
}
