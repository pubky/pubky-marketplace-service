//! Money and echo behaviour of the Shopify provider inside the framework:
//! which store webhooks are the echo of a refund or cancel this connector
//! applied (and which are the merchant), how a recorded Pubky refund becomes a
//! store refund on every rail, and the interleavings where two workers or a
//! webhook and a worker meet. The store is the schema-validating fake in
//! `common/shopify_fake.rs`; it refuses a refund beyond the payment, as
//! Shopify does, so an over-refund fails these tests.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::connector_provider::{
    api, as_json, map_listing, push_state, query_param, raw, rebuild, save_credentials,
    start_connect, test_keys, test_settings,
};
use common::shopify_fake::{FakeShopify, Fault, GOOD_CODE};
use common::{create_paid_order, execute, new_actor, order_command, TestActor, TestApp};
use hmac::{Hmac, Mac};
use marketplace_service::clock::Clock;
use marketplace_service::connectors::provider::ProviderRegistry;
use marketplace_service::connectors::providers::shopify::client::Origin;
use marketplace_service::connectors::providers::shopify::orders::refund_mark;
use marketplace_service::connectors::providers::shopify::ShopifyProvider;
use marketplace_service::connectors::{ledger, worker, ConnectorRuntime};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::PgPool;
use tokio::sync::Notify;
use uuid::Uuid;

const SHOP: &str = "dezentral.myshopify.com";
const CLIENT_ID: &str = "dev-dashboard-client-id";
const APP_SECRET: &str = "shpss_merchants_own_app_secret";
const PRODUCT: &str = "gid://shopify/Product/9001";
const VARIANT: &str = "gid://shopify/ProductVariant/50001";

struct Shop {
    app: TestApp,
    fake: FakeShopify,
    seller: TestActor,
    buyer: TestActor,
    connection: Uuid,
    order_id: String,
    total_minor: i64,
    next_command: u64,
}

fn signed_query(secret: &str, extra: &[(&str, &str)]) -> String {
    let mut pairs: Vec<String> = vec![
        format!("shop={SHOP}"),
        format!("timestamp={}", chrono::Utc::now().timestamp()),
    ];
    pairs.extend(extra.iter().map(|(key, value)| format!("{key}={value}")));
    pairs.sort();
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("key");
    mac.update(pairs.join("&").as_bytes());
    format!(
        "{}&hmac={}",
        pairs.join("&"),
        hex::encode(mac.finalize().into_bytes())
    )
}

/// An installed store, one mapped listing, and a paid order already pushed.
async fn pushed(pool: PgPool) -> Shop {
    shop(pool, None).await
}

type Prepare = Box<dyn FnOnce(&FakeShopify)>;

/// The same, but `prepare` sets up the fake store before the first push
/// attempt, and the push is then not expected to have created the order.
async fn shop(pool: PgPool, prepare: Option<Prepare>) -> Shop {
    let fake = FakeShopify::start().await;
    fake.expect_client_secret(APP_SECRET);
    let provider = Arc::new(ShopifyProvider::with_origin(Origin::Fixed(
        fake.base_url.clone(),
    )));
    let runtime = ConnectorRuntime::new(
        test_keys(),
        ProviderRegistry::new().with(provider),
        test_settings(),
    );
    let base = common::test_app(pool).await;
    let state = base.state.clone().with_connectors(Some(Arc::new(runtime)));
    let app = rebuild(base, state);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = start_connect(&app, &seller, SHOP).await;
    let (status, body) = save_credentials(&app, &seller, connection, CLIENT_ID, APP_SECRET).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, headers, body) = raw(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/connectors/shopify/c/{connection}/launch?{}",
            signed_query(APP_SECRET, &[("host", "YWJj")])
        ),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let location = headers
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("redirect")
        .to_string();
    let state_param = query_param(&location, "state");
    let (status, _, body) = raw(
        app.router.clone(),
        "GET",
        &format!(
            "/v1/connectors/shopify/c/{connection}/oauth/callback?{}",
            signed_query(APP_SECRET, &[("code", GOOD_CODE), ("state", &state_param)])
        ),
        &[],
        Vec::new(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", PRODUCT, VARIANT).await;
    let expect_created = prepare.is_none();
    if let Some(prepare) = prepare {
        prepare(&fake);
    }
    worker::run_pass(&app.state).await.expect("pass");
    if expect_created {
        assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    }
    Shop {
        app,
        fake,
        seller,
        buyer,
        connection,
        order_id: order.order_id,
        total_minor: order.total_minor,
        next_command: 2_000,
    }
}

fn minor_text(minor: i64) -> String {
    format!("{}.{:02}", minor / 100, minor % 100)
}

impl Shop {
    fn store_order_number(&self) -> u64 {
        self.fake.orders()[0]
            .id
            .rsplit('/')
            .next()
            .expect("id")
            .parse()
            .expect("number")
    }

    fn command_number(&mut self) -> u64 {
        self.next_command += 1;
        self.next_command
    }

    async fn run(&mut self, seller: bool, kind: &str, revision: i64, payload: Value) {
        let number = self.command_number();
        let token = if seller {
            &self.seller.token
        } else {
            &self.buyer.token
        };
        let (status, body) = execute(
            &self.app,
            token,
            &order_command(kind, &self.order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }

    /// The seller records a refund of `amount` (order currency minor units)
    /// after a return, which is how a Pubky refund reaches the books.
    async fn record_refund(&mut self, amount: i64) {
        let total = self.total_minor;
        self.run(
            true,
            "fulfillment.ship",
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "T-1" }),
        )
        .await;
        self.run(false, "fulfillment.confirm_delivery", 3, json!({}))
            .await;
        self.run(
            false,
            "return.request",
            4,
            json!({ "reason": "wrong size", "requested_amount_minor": total }),
        )
        .await;
        self.run(true, "return.approve", 5, json!({})).await;
        self.run(true, "return.receive", 6, json!({})).await;
        self.run(
            true,
            "refund.record_external",
            7,
            json!({ "amount_minor": amount, "transaction_id": "bitcoin-tx-0001" }),
        )
        .await;
    }

    async fn pass(&self) {
        worker::run_pass(&self.app.state).await.expect("pass");
    }

    async fn inbound(&self) {
        worker::drive_inbound(&self.app.state, 20)
            .await
            .expect("inbound");
    }

    async fn alerts(&self) -> Vec<Value> {
        let (_, body) = api(
            &self.app,
            "GET",
            "/v1/connectors/alerts?unresolved=true",
            Some(&self.seller.token),
            &json!(null),
        )
        .await;
        body["alerts"].as_array().cloned().unwrap_or_default()
    }

    async fn webhook(&self, event_id: &str, topic: &str, body: &Value) {
        let bytes = serde_json::to_vec(body).expect("body");
        let mut mac = Hmac::<Sha256>::new_from_slice(APP_SECRET.as_bytes()).expect("key");
        mac.update(&bytes);
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        let (status, _, response) = raw(
            self.app.router.clone(),
            "POST",
            &format!("/v1/connectors/shopify/c/{}/webhooks", self.connection),
            &[
                ("content-type", "application/json"),
                ("x-shopify-hmac-sha256", &signature),
                ("x-shopify-topic", topic),
                ("x-shopify-event-id", event_id),
                ("x-shopify-shop-domain", SHOP),
            ],
            bytes,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{}", as_json(&response));
    }

    /// Shopify's `refunds/create` for a refund in the store: `note` and
    /// per-transaction amounts as the store holds them.
    fn refund_payload(&self, note: &str, transactions: &[(&str, &str)]) -> Value {
        json!({
            "id": 5,
            "order_id": self.store_order_number(),
            "note": note,
            "transactions": transactions
                .iter()
                .map(|(amount, currency)| json!({ "kind": "refund", "status": "success", "amount": amount, "currency": currency }))
                .collect::<Vec<_>>(),
        })
    }

    /// The webhook the store sends for the refund at `index` this connector
    /// created: its note (with the Pubky mark) and amount, as stored.
    fn echo_of(&self, index: usize) -> Value {
        let refund = self.fake.orders()[0].refunds[index].clone();
        self.refund_payload(
            refund["note"].as_str().expect("note"),
            &[(
                refund["transactions"][0]["amount"]
                    .as_str()
                    .expect("amount"),
                "USD",
            )],
        )
    }

    async fn inbound_outcome(&self, event_id: &str) -> (String, Option<String>) {
        sqlx::query_as(
            "SELECT state, outcome FROM connector_inbound_events WHERE provider_event_id = $1",
        )
        .bind(event_id)
        .fetch_one(&self.app.pool)
        .await
        .expect("inbound row")
    }

    async fn refund_actions(&self) -> Vec<(String, String, Value)> {
        sqlx::query_as(
            "SELECT action_key, state, payload FROM connector_order_actions \
             WHERE kind = 'refund' ORDER BY created_at, action_key",
        )
        .fetch_all(&self.app.pool)
        .await
        .expect("actions")
    }

    /// Queues a second mirrored refund the way the event consumer does, for
    /// a record whose cumulative total is `cumulative` (a PayPal running
    /// partial, or a larger record).
    async fn queue_cumulative_refund(&self, key: &str, cumulative: i64) {
        let payload = json!({
            "amount_minor": cumulative,
            "amount_unit": "usd",
            "transaction_id": "bitcoin-tx-0002",
            "order_total_minor": self.total_minor,
            "order_currency": "USD",
            "order_exponent": 2,
            "full": cumulative >= self.total_minor,
            "basis": "Amount recorded in USD, the order currency; no currency conversion.",
        });
        let mut tx = self.app.pool.begin().await.expect("tx");
        ledger::on_refund_recorded(
            &mut tx,
            self.app.clock.now(),
            self.order_id.parse().expect("uuid"),
            key,
            &payload,
            Uuid::new_v4(),
        )
        .await
        .expect("queued");
        tx.commit().await.expect("commit");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_real_merchant_refund_after_our_mirrored_one_is_not_swallowed(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    s.pass().await;
    assert_eq!(
        s.fake.orders()[0].refunds.len(),
        1,
        "the partial refund was mirrored"
    );

    // The store reports our own refund back: its note carries the Pubky mark
    // and the amount matches.
    s.webhook("evt-echo", "refunds/create", &s.echo_of(0)).await;
    s.inbound().await;
    assert_eq!(
        s.inbound_outcome("evt-echo").await,
        ("ignored".into(), Some("pubky_refund_echo".into()))
    );
    assert!(s.alerts().await.is_empty());

    // A week of silence later the merchant refunds the rest in the admin. It
    // has no mark, so it is not our echo, whatever the action table holds.
    s.app.clock.advance_seconds(7 * 24 * 3600);
    let rest = minor_text(s.total_minor - 5_000);
    s.fake
        .merchant_refund(&s.fake.orders()[0].id, s.total_minor - 5_000);
    s.webhook(
        "evt-merchant",
        "refunds/create",
        &s.refund_payload("customer returned the boots", &[(&rest, "USD")]),
    )
    .await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-merchant").await.0, "applied");
    let alerts = s.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["kind"], "merchant_refunded");
    assert!(alerts[0]["detail"]
        .as_str()
        .expect("detail")
        .contains(&format!("{} minor units USD", s.total_minor - 5_000)));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_merchant_refund_of_the_same_amount_within_the_window_is_still_alerted(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    s.pass().await;
    // The same figure, a minute later, but made by the merchant: no mark.
    s.app.clock.advance_seconds(60);
    s.webhook(
        "evt-same",
        "refunds/create",
        &s.refund_payload("goodwill", &[("50.00", "USD")]),
    )
    .await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-same").await.0, "applied");
    assert_eq!(s.alerts().await.len(), 1);

    // A refund that carries our mark but not our amount is not our echo
    // either (identity and amount must both match).
    let mark = s.fake.orders()[0].refunds[0]["note"]
        .as_str()
        .expect("note")
        .to_string();
    s.webhook(
        "evt-wrong-amount",
        "refunds/create",
        &s.refund_payload(&mark, &[("49.00", "USD")]),
    )
    .await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-wrong-amount").await.0, "applied");
    assert_eq!(s.alerts().await.len(), 2);

    // Nor is a mark for a Pubky refund this connector never created.
    let forged = refund_mark(&Uuid::new_v4().to_string(), 5_000, "USD");
    s.webhook(
        "evt-forged",
        "refunds/create",
        &s.refund_payload(&forged, &[("50.00", "USD")]),
    )
    .await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-forged").await.0, "applied");
    assert_eq!(s.alerts().await.len(), 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_echo_that_arrives_after_the_window_raises_an_alert_rather_than_hiding_a_refund(
    pool: PgPool,
) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    s.pass().await;
    s.app.clock.advance_seconds(7 * 3600);
    s.webhook("evt-late", "refunds/create", &s.echo_of(0)).await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-late").await.0, "applied");
    assert_eq!(
        s.alerts().await.len(),
        1,
        "a late webhook is surfaced, never swallowed"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_cancel_is_not_an_echo_when_our_cancel_never_reached_the_store(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.fake.fault("OrderCancel", vec![Fault::AccessDenied]);
    s.run(
        false,
        "order.cancel_request",
        2,
        json!({ "reason": "changed my mind" }),
    )
    .await;
    s.run(true, "order.cancel_approve", 3, json!({})).await;
    s.pass().await;
    let state: String =
        sqlx::query_scalar("SELECT state FROM connector_order_actions WHERE kind = 'cancel'")
            .fetch_one(&s.app.pool)
            .await
            .expect("cancel action");
    assert_eq!(state, "quarantined", "the store refused the cancel");
    let alerts_before = s.alerts().await.len();
    // The merchant cancels in the store's admin.
    let body = json!({ "id": s.store_order_number(), "cancel_reason": "other", "tags": format!("pubky, pubky-order-{}", s.order_id) });
    s.webhook("evt-merchant-cancel", "orders/cancelled", &body)
        .await;
    s.inbound().await;
    assert_eq!(s.inbound_outcome("evt-merchant-cancel").await.0, "applied");
    let alerts = s.alerts().await;
    assert_eq!(alerts.len(), alerts_before + 1);
    assert!(alerts
        .iter()
        .any(|alert| alert["kind"] == "merchant_cancelled"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_echo_that_arrives_while_the_mirror_is_still_running_waits_for_it(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.fake.fault(
        "RefundCreate",
        vec![Fault::Hold {
            entered: entered.clone(),
            release: release.clone(),
        }],
    );
    worker::consume_events(&s.app.state).await.expect("consume");
    let worker_state = s.app.state.clone();
    let running = tokio::spawn(async move { worker::drive_actions(&worker_state, 10).await });
    entered.notified().await;

    let actions = s.refund_actions().await;
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].1, "sending", "the store call is open");
    let mark = refund_mark(&actions[0].0, 5_000, "USD");
    let echo = s.refund_payload(&format!("Pubky refund. {mark}"), &[("50.00", "USD")]);
    s.webhook("evt-early-echo", "refunds/create", &echo).await;
    s.inbound().await;
    let (state, outcome) = s.inbound_outcome("evt-early-echo").await;
    assert_eq!(
        (state.as_str(), outcome.as_deref()),
        ("pending", Some("pubky_refund_in_flight")),
        "judged later, not on a half-written state"
    );
    assert!(
        s.alerts().await.is_empty(),
        "no alert while the mirror is running"
    );

    release.notify_one();
    running.await.expect("task").expect("actions");
    assert_eq!(s.refund_actions().await[0].1, "done");
    s.app.clock.advance_seconds(700);
    s.inbound().await;
    assert_eq!(
        s.inbound_outcome("evt-early-echo").await,
        ("ignored".into(), Some("pubky_refund_echo".into()))
    );
    assert!(s.alerts().await.is_empty());
    s.fake.assert_clean();
}

async fn partial_refund_on_rail(pool: PgPool, rail: &str) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    // The rail is read when the event is consumed; set what the order
    // records for it (a quote for Bitcoin, the asset terms for USDT).
    match rail {
        "bitcoin" => {
            sqlx::query(
                "UPDATE orders SET payment_method = 'bitcoin', bitcoin_quote_rate = 98123.45, \
                 bitcoin_quote_source = 'blocktank', bitcoin_quote_fetched_at = $2, \
                 bitcoin_quoted_sats = 139_600, bitcoin_quote_expires_at = $2, \
                 paykit_expires_at = $2, bitcoin_quote_currency = 'USD', \
                 bitcoin_quote_exponent = 2, bitcoin_quote_spread_bps = 100 \
                 WHERE id = $1::uuid",
            )
            .bind(&s.order_id)
            .bind(s.app.clock.now())
            .execute(&s.app.pool)
            .await
            .expect("bitcoin terms");
        }
        _ => {
            sqlx::query(
                "UPDATE orders SET payment_method = 'usdt', payment_asset = 'USDT', \
                 payment_network = 'arbitrum-one', payment_amount_minor = 137000000, \
                 payment_exponent = 6, payment_quote_basis = 'parity' WHERE id = $1::uuid",
            )
            .bind(&s.order_id)
            .execute(&s.app.pool)
            .await
            .expect("usdt terms");
        }
    }
    s.pass().await;
    let actions = s.refund_actions().await;
    assert_eq!(
        actions[0].2["amount_unit"], "usd",
        "the unit is the order currency on every rail"
    );
    assert_eq!(
        actions[0].2["full"], false,
        "a partial amount is not a full refund"
    );
    assert_eq!(actions[0].1, "done", "{actions:?}");
    let refunds = s.fake.orders()[0].refunds.clone();
    assert_eq!(refunds.len(), 1);
    assert_eq!(
        refunds[0]["transactions"][0]["amount"], "50.00",
        "exactly the recorded amount, never the order total"
    );
    let note = refunds[0]["note"].as_str().expect("note");
    assert!(note.contains("no currency conversion"), "{note}");
    if rail == "bitcoin" {
        assert!(note.contains("Paid with Bitcoin"), "{note}");
        assert!(note.contains("BTCUSD 98123.45 from blocktank"), "{note}");
    } else {
        assert!(note.contains("Paid with USDT"), "{note}");
        assert!(note.contains("USDT amount basis: parity"), "{note}");
    }
    assert_eq!(s.fake.orders()[0].refunded_minor(), 5_000);

    // The seller later records a larger total (a partial followed by a full
    // refund): the store gets only the difference and never over-refunds.
    s.queue_cumulative_refund("second-record", s.total_minor)
        .await;
    s.pass().await;
    let order = &s.fake.orders()[0];
    assert_eq!(order.refunds.len(), 2);
    assert_eq!(
        order.refunds[1]["transactions"][0]["amount"],
        minor_text(s.total_minor - 5_000)
    );
    assert_eq!(order.refunded_minor(), s.total_minor);
    // Recording the full total again changes nothing.
    s.queue_cumulative_refund("third-record", s.total_minor)
        .await;
    s.pass().await;
    assert_eq!(s.fake.orders()[0].refunds.len(), 2);
    assert!(s
        .refund_actions()
        .await
        .iter()
        .all(|(_, state, _)| state == "done"));
    s.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_refund_on_the_bitcoin_rail_is_mirrored_in_the_order_currency_then_capped(
    pool: PgPool,
) {
    partial_refund_on_rail(pool, "bitcoin").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_refund_on_the_usdt_rail_is_mirrored_in_the_order_currency_then_capped(
    pool: PgPool,
) {
    partial_refund_on_rail(pool, "usdt").await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_reversed_before_it_is_consumed_mirrors_nothing(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    // The refund was reversed (PayPal `Canceled_Reversal`) before the worker
    // read the event: the order no longer holds a refund record.
    sqlx::query("UPDATE orders SET external_refund = NULL WHERE id = $1::uuid")
        .bind(&s.order_id)
        .execute(&s.app.pool)
        .await
        .expect("reversed");
    s.pass().await;
    assert!(s.refund_actions().await.is_empty());
    assert!(s.fake.orders()[0].refunds.is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_refund_mirrors_of_one_order_never_run_at_the_same_time(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    s.queue_cumulative_refund("second-record", s.total_minor)
        .await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.fake.fault(
        "RefundCreate",
        vec![Fault::Hold {
            entered: entered.clone(),
            release: release.clone(),
        }],
    );

    let first_state = s.app.state.clone();
    let first = tokio::spawn(async move { worker::drive_actions(&first_state, 10).await });
    entered.notified().await;
    // A second worker looks while the first has an action open on this push:
    // it must find nothing to claim, not read "refunded so far" too early.
    let second = worker::drive_actions(&s.app.state, 10)
        .await
        .expect("second worker");
    assert_eq!(second, 0, "the push has an action in flight");
    assert_eq!(s.fake.calls("RefundCreate").len(), 1);
    let actions = s.refund_actions().await;
    assert_eq!(
        actions
            .iter()
            .filter(|(_, state, _)| state == "sending")
            .count(),
        1
    );
    assert_eq!(
        actions
            .iter()
            .filter(|(_, state, _)| state == "pending")
            .count(),
        1
    );

    release.notify_one();
    first.await.expect("task").expect("first worker");
    s.pass().await;
    // Either order ends the same, because each mirror sends only the
    // difference to what the store already shows: the recorded total, once.
    let order = &s.fake.orders()[0];
    assert_eq!(
        order.refunded_minor(),
        s.total_minor,
        "exactly the recorded total"
    );
    assert!(order.refunds.len() <= 2);
    assert!(s
        .refund_actions()
        .await
        .iter()
        .all(|(_, state, _)| state == "done"));
    s.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_claims_of_one_pushs_actions_hand_out_one_at_a_time(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    let providers = vec!["shopify".to_string()];
    for round in 0..12 {
        s.queue_cumulative_refund(&format!("round-{round}"), 5_000 + round)
            .await;
    }
    let pool = s.app.pool.clone();
    let now = s.app.clock.now();
    let mut held = Vec::new();
    for _ in 0..6 {
        let (a, b) = tokio::join!(
            ledger::claim_next_action(&pool, now, std::time::Duration::from_secs(60), &providers),
            ledger::claim_next_action(&pool, now, std::time::Duration::from_secs(60), &providers),
        );
        held.extend(
            [a.expect("claim a"), b.expect("claim b")]
                .into_iter()
                .flatten(),
        );
        assert!(
            held.len() <= 1,
            "two actions of one push were claimed at once"
        );
    }
    assert_eq!(held.len(), 1);
    // Finishing the open action frees the next one.
    let (action, _) = held.remove(0);
    assert!(ledger::mark_action_done(&pool, &action, now)
        .await
        .expect("done"));
    let next =
        ledger::claim_next_action(&pool, now, std::time::Duration::from_secs(60), &providers)
            .await
            .expect("claim");
    assert!(next.is_some());
    // A lease that expired does not block: that worker is presumed dead.
    let later = now + chrono::Duration::seconds(120);
    let reclaimed =
        ledger::claim_next_action(&pool, later, std::time::Duration::from_secs(60), &providers)
            .await
            .expect("claim after expiry");
    assert!(reclaimed.is_some());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_claim_that_is_still_open_on_a_push_makes_a_second_worker_move_on(pool: PgPool) {
    let mut s = pushed(pool).await;
    s.record_refund(5_000).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    s.queue_cumulative_refund("second-record", s.total_minor)
        .await;
    let providers = vec!["shopify".to_string()];
    let now = s.app.clock.now();
    let lease = std::time::Duration::from_secs(60);
    let lease_until = now + chrono::Duration::seconds(60);

    // Worker A is half way through claiming: it holds the push and has set
    // one action `sending`, and has not committed yet.
    let (first, push): (Uuid, Uuid) = sqlx::query_as(
        "SELECT id, push_id FROM connector_order_actions WHERE state = 'pending' \
         ORDER BY next_attempt_at, id LIMIT 1",
    )
    .fetch_one(&s.app.pool)
    .await
    .expect("a pending action");
    let mut open = s.app.pool.begin().await.expect("tx");
    sqlx::query("SELECT id FROM connector_order_push WHERE id = $1 FOR NO KEY UPDATE")
        .bind(push)
        .fetch_one(&mut *open)
        .await
        .expect("push lock");
    sqlx::query(
        "UPDATE connector_order_actions SET state = 'sending', attempts = attempts + 1, \
         lease_until = $2, updated_at = $3 WHERE id = $1",
    )
    .bind(first)
    .bind(lease_until)
    .bind(now)
    .execute(&mut *open)
    .await
    .expect("claim");

    // Worker B sees the other action still `pending` in its snapshot; the
    // push lock is what makes it move on instead of claiming a second action
    // of the same push (and it must not wait for A either).
    let blocked = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ledger::claim_next_action(&s.app.pool, now, lease, &providers),
    )
    .await
    .expect("the claim does not wait on the other worker")
    .expect("claim");
    assert!(
        blocked.is_none(),
        "a second action of the push was handed out"
    );
    open.commit().await.expect("commit");
    assert!(
        ledger::claim_next_action(&s.app.pool, now, lease, &providers)
            .await
            .expect("claim")
            .is_none()
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_in_another_currency_tells_the_seller_why_and_what_to_do(pool: PgPool) {
    let s = shop(
        pool,
        Some(Box::new(|fake| fake.lock().shop_currency = "EUR".into())),
    )
    .await;
    let (state, _, reason, _) = push_state(&s.app.pool, &s.order_id).await;
    assert_eq!(state, "quarantined");
    assert!(reason.is_some());
    assert!(s.fake.orders().is_empty());
    let alerts = s.alerts().await;
    assert_eq!(alerts.len(), 1);
    let detail = alerts[0]["detail"].as_str().expect("detail");
    assert!(detail.contains("USD") && detail.contains("EUR"), "{detail}");
    assert!(detail.contains("does not convert"), "{detail}");
    assert!(detail.contains("retry the push"), "{detail}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_in_other_currencies_is_alerted_with_each_amount_not_one_sum(pool: PgPool) {
    let s = pushed(pool).await;
    s.webhook(
        "evt-mixed",
        "refunds/create",
        &s.refund_payload("goodwill", &[("10.00", "USD"), ("500", "JPY")]),
    )
    .await;
    s.inbound().await;
    let alerts = s.alerts().await;
    assert_eq!(alerts.len(), 1);
    let detail = alerts[0]["detail"].as_str().expect("detail");
    assert!(
        detail.contains("1000 minor units USD") && detail.contains("500 minor units JPY"),
        "{detail}"
    );
    assert!(detail.contains("more than one currency"), "{detail}");
    s.webhook(
        "evt-eur",
        "refunds/create",
        &s.refund_payload("goodwill", &[("10.00", "EUR")]),
    )
    .await;
    s.inbound().await;
    let alerts = s.alerts().await;
    let detail = alerts
        .iter()
        .filter_map(|alert| alert["detail"].as_str())
        .find(|detail| detail.contains("EUR"))
        .expect("eur alert");
    assert!(detail.contains("the order is in USD"), "{detail}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn asking_for_split_mode_on_a_connection_with_a_shared_listing_is_a_conflict(pool: PgPool) {
    let s = pushed(pool).await;
    // A draft for another store that already carries two mappings on one
    // listing (written directly: the mapping API refuses drafts).
    let other = "other.myshopify.com";
    let (status, body) = api(
        &s.app,
        "POST",
        "/v1/connectors",
        Some(&s.seller.token),
        &json!({ "provider": "shopify", "domain": other }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let draft = body["connection"]["id"].as_str().expect("id").to_string();
    for variant in ["a", "b"] {
        sqlx::query(
            "INSERT INTO connector_variant_map \
             (id, connection_id, listing_id, variant_id, provider_product_id, provider_variant_id, \
              sku, created_at, updated_at) \
             VALUES ($1, $2::uuid, 'boots_01', $3, 'p', $3, NULL, now(), now())",
        )
        .bind(Uuid::new_v4())
        .bind(&draft)
        .bind(variant)
        .execute(&s.app.pool)
        .await
        .expect("mapping");
    }
    let (status, body) = api(
        &s.app,
        "POST",
        "/v1/connectors",
        Some(&s.seller.token),
        &json!({ "provider": "shopify", "domain": other, "variant_mode": "split" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "mappings_conflict");
    assert_eq!(body["error"]["listing_id"], "boots_01");
    let mode: String =
        sqlx::query_scalar("SELECT variant_mode FROM connector_connections WHERE id = $1::uuid")
            .bind(&draft)
            .fetch_one(&s.app.pool)
            .await
            .expect("mode");
    assert_eq!(
        mode, "native",
        "the answer is the refusal, not a silent downgrade"
    );
    let _ = s.connection;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_busy_store_whose_search_lags_is_never_pushed_to_twice(pool: PgPool) {
    let s = shop(
        pool,
        Some(Box::new(|fake| {
            fake.lock().search_lags = true;
            fake.fault("OrderCreate", vec![Fault::AppliedThenServerError]);
        })),
    )
    .await;
    assert_eq!(
        s.fake.orders().len(),
        1,
        "the store applied the push whose answer was lost"
    );
    assert_ne!(push_state(&s.app.pool, &s.order_id).await.0, "created");
    // Meanwhile the store takes more orders than one scan can read.
    s.fake.add_other_orders(520);
    s.app.clock.advance_seconds(700);
    s.pass().await;
    let (state, _, _, _) = push_state(&s.app.pool, &s.order_id).await;
    assert_ne!(
        state, "created",
        "the lookup could not rule the order out, so it did not guess"
    );
    assert_ne!(state, "quarantined");
    assert_eq!(
        s.fake
            .orders()
            .iter()
            .filter(|order| order
                .tags()
                .iter()
                .any(|tag| tag.starts_with("pubky-order-")))
            .count(),
        1,
        "no second copy"
    );
    // The search index catches up: the next attempt finds the order.
    s.fake.lock().search_lags = false;
    s.app.clock.advance_seconds(700);
    s.pass().await;
    let (state, external, _, _) = push_state(&s.app.pool, &s.order_id).await;
    assert_eq!(state, "created");
    let first = s
        .fake
        .orders()
        .into_iter()
        .find(|order| {
            order
                .tags()
                .iter()
                .any(|tag| tag.starts_with("pubky-order-"))
        })
        .expect("our order");
    assert_eq!(external.as_deref(), Some(first.id.as_str()));
    assert_eq!(s.fake.calls("OrderCreate").len(), 1, "orderCreate ran once");
    s.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_that_adds_tax_gets_the_order_and_the_seller_gets_an_alert(pool: PgPool) {
    let s = shop(
        pool,
        Some(Box::new(|fake| fake.lock().forced_tax_bps = 800)),
    )
    .await;
    let (state, external, _, _) = push_state(&s.app.pool, &s.order_id).await;
    assert_eq!(state, "created", "the order exists in the store");
    assert_eq!(external.as_deref(), Some(s.fake.orders()[0].id.as_str()));
    let alerts = s.alerts().await;
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0]["kind"], "push_total_mismatch");
    assert_eq!(alerts[0]["pubky_order_id"], s.order_id);
    let detail = alerts[0]["detail"].as_str().expect("detail");
    assert!(
        detail.contains("tax") && detail.contains("buyer paid"),
        "{detail}"
    );
    assert_eq!(s.fake.calls("OrderCreate").len(), 1);
    s.fake.assert_clean();
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pushed_order_whose_total_matches_raises_no_alert(pool: PgPool) {
    let s = pushed(pool).await;
    assert!(s.alerts().await.is_empty());
    let sent = &s.fake.orders()[0].input;
    assert_eq!(sent["taxesIncluded"], false);
    assert_eq!(sent["taxLines"], json!([]));
    let paid = sent["transactions"][0]["amountSet"]["shopMoney"]["amount"]
        .as_str()
        .expect("paid")
        .to_string();
    assert_eq!(
        paid,
        minor_text(s.total_minor),
        "the store order total is what the buyer paid"
    );
}
