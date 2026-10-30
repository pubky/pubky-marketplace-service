//! One Paykit bind (`paykit`) for every asset a seller accepts: the request
//! the service sends, the offer it records, settlement on the rail the buyer
//! actually paid, USDT finality gating physical fulfilment, both rails paid,
//! and the compatibility rules (rc16 upstream, fork, legacy `bitcoin` bind).
//!
//! The Marketplace status bodies built here have the shape of the bodies
//! captured from paykit-server (`usdt_arbitrum` with `observed_amount`,
//! `accepted_assets`, `payment_amounts` as digit strings, `paid_asset`), with
//! this order's amounts; `single_paykit_captured_test.rs` replays the
//! captured bodies themselves, byte for byte.

mod common;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use common::fx_feed::{fx_body, FakeFxFeed};
use common::lock_order::{hold_order_row, wait_until_blocked_by};
use common::paykit_review::{
    create_sat_order, delivered_notifications, enable_bitcoin, poll_now, resolve_call,
};
use common::*;
use marketplace_service::bitcoin_review::{watch_manual_reviews, MANUAL_REVIEW_INACTIVITY_DAYS};
use marketplace_service::clock::Clock;
use marketplace_service::config::{Config, PaymentConfirmation};
use marketplace_service::payment_attempt::{MarketplaceAssets, MarketplaceContract};
use marketplace_service::payments::{attempt_reference, PaykitApi};
use marketplace_service::workers::drain_outbox;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Notify;
use uuid::Uuid;

const RATE: &str = "77197";
const TOTAL_CENTS: u64 = 13_700;
/// What an underpaid USDT leg (`usdt_leg` unmatched) actually sent: one USDT short.
const UNDERPAID_CENTS: u64 = TOTAL_CENTS - 100;
const TOTAL_USDT_UNITS: u64 = 137_000_000;
const TX_HASH: &str = "0x9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c9f2c";
const BTC_TXID: &str = "00000000000000000000000000000000000000000000000000000000000000a1";

fn quoted_sats() -> u64 {
    (u128::from(TOTAL_CENTS) * 100_000_000).div_ceil(100 * 77_197) as u64
}

fn config(usdt_flag: bool, multi_asset: bool, usdt_listed: bool) -> Config {
    let mut config = Config::for_tests();
    config.usdt_payments_enabled = usdt_flag;
    if multi_asset {
        config.paykit_marketplace_contract = MarketplaceContract::MultiAsset;
    }
    if usdt_listed {
        config.paykit_marketplace_assets =
            MarketplaceAssets::parse(Some("BTC,USDT"), PaykitApi::Upstream).expect("assets parse");
    }
    config
}

struct Fixture {
    app: TestApp,
    paykit: FakePaykit,
    fx: FakeFxFeed,
    seller: TestActor,
    buyer: TestActor,
}

struct Setup {
    api: PaykitApi,
    usdt_flag: bool,
    /// The deployed paykit-server speaks the multi-asset contract.
    multi_asset: bool,
    /// The deployment lists USDT in `PAYKIT_MARKETPLACE_ASSETS`.
    usdt_listed: bool,
    consent: bool,
    receivable: &'static [&'static str],
    usd_listing: bool,
}

impl Setup {
    /// The deployment this change targets: upstream rc17, USDT on, a seller
    /// who accepts both assets.
    fn both() -> Self {
        Self {
            api: PaykitApi::Upstream,
            usdt_flag: true,
            multi_asset: true,
            usdt_listed: true,
            consent: true,
            receivable: &["BTC", "USDT"],
            usd_listing: true,
        }
    }
}

async fn seed_fx_samples(pool: &PgPool, now: DateTime<Utc>) {
    for back in 0..3_i64 {
        let accepted = now - Duration::seconds(60 * back);
        let bucket_ts = accepted.timestamp() - accepted.timestamp().rem_euclid(60);
        let bucket = DateTime::from_timestamp(bucket_ts, 0).expect("valid bucket");
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, $2, $2, $3, 'blocktank') \
             ON CONFLICT DO NOTHING",
        )
        .bind(RATE)
        .bind(accepted)
        .bind(bucket)
        .execute(pool)
        .await
        .expect("seed fx sample");
    }
}

async fn put_config(app: &TestApp, token: &str, body: Value) {
    let (status, response) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(token),
        &body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "config put failed: {response}");
}

async fn fixture(pool: PgPool, setup: Setup) -> Fixture {
    fixture_confirmed_by(pool, setup, PaymentConfirmation::Automatic).await
}

async fn fixture_confirmed_by(
    pool: PgPool,
    setup: Setup,
    payment_confirmation: PaymentConfirmation,
) -> Fixture {
    let mut config = config(setup.usdt_flag, setup.multi_asset, setup.usdt_listed);
    config.payment_confirmation = payment_confirmation;
    let (app, paykit, fx) = test_app_with_paykit_fx_config(pool.clone(), config, setup.api).await;
    if setup.multi_asset {
        paykit.use_multi_asset_contract();
    }
    paykit.set_usd_per_btc(77_197);
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let mut rails = json!({ "bitcoin_enabled": true });
    if setup.usdt_flag {
        rails["usdt_enabled"] = json!(setup.consent);
    }
    put_config(&app, &seller.token, rails).await;
    paykit.set_claimed(&seller.pubky);
    paykit.set_receivable(&seller.pubky, setup.receivable);
    if setup.usd_listing {
        let (status, body) =
            execute(&app, &seller.token, &register_command(&seller.pubky, 40)).await;
        assert_eq!(status, StatusCode::OK, "register failed: {body}");
    }
    if setup.api == PaykitApi::Upstream {
        let now = app.clock.now();
        fx.set_body(fx_body(RATE, now.timestamp_millis()));
        seed_fx_samples(&pool, now).await;
    }
    Fixture {
        app,
        paykit,
        fx,
        seller,
        buyer,
    }
}

async fn checkout_usd(f: &Fixture) -> PendingOrder {
    let revision: i64 =
        sqlx::query_scalar("SELECT server_revision FROM listings WHERE aggregate_id = $1")
            .bind(format!("listing:{}_boots_01", f.seller.pubky))
            .fetch_one(&f.app.pool)
            .await
            .expect("listing row");
    let mut checkout = checkout_command_with_id(&f.seller.pubky, &Uuid::new_v4().to_string());
    checkout["payload"]["lines"][0]["expected_revision"] = json!(revision);
    let (status, body) = execute(&f.app, &f.buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {body}");
    PendingOrder {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    }
}

async fn bind(f: &Fixture, order_id: &str, method: &str) -> (StatusCode, Value) {
    send(
        f.app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(&f.buyer.token),
        &json!({ "method": method }),
    )
    .await
}

async fn drain(f: &Fixture) {
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref());
    drain_outbox(&f.app.pool, client, f.app.clock.now(), 30)
        .await
        .expect("outbox drains");
}

async fn read_order(f: &Fixture, token: &str, order_id: &str) -> Value {
    let (status, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v1/orders/{order_id}"),
        Some(token),
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "order read failed: {body}");
    body
}

fn uuid(id: &str) -> Uuid {
    Uuid::parse_str(id).expect("uuid")
}

fn prepare_body(paykit: &FakePaykit) -> Value {
    paykit
        .calls()
        .into_iter()
        .rev()
        .find(|call| call.path == "/marketplace/payment-requests/prepare")
        .expect("a prepare call")
        .body
}

#[derive(Debug, sqlx::FromRow)]
struct OrderFacts {
    state: String,
    payment_method: Option<String>,
    paykit_invoice_id: Option<Uuid>,
    paykit_asset: Option<String>,
    paykit_amount_minor: Option<i64>,
    paykit_total_sats: Option<i64>,
    paykit_request_state: Option<String>,
    paykit_expires_at: Option<DateTime<Utc>>,
    bitcoin_quoted_sats: Option<i64>,
    payment_asset: Option<String>,
    payment_finality: Option<String>,
    paykit_second_rail_at: Option<DateTime<Utc>>,
    revision: i64,
}

async fn facts(pool: &PgPool, order_id: &str) -> OrderFacts {
    sqlx::query_as(
        "SELECT state, payment_method, paykit_invoice_id, paykit_asset, paykit_amount_minor, \
         paykit_total_sats, paykit_request_state, paykit_expires_at, bitcoin_quoted_sats, \
         payment_asset, payment_finality, paykit_second_rail_at, revision \
         FROM orders WHERE id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("order row")
}

async fn payment_row(pool: &PgPool, order_id: &str) -> (String, i64, String, i32, Option<String>) {
    sqlx::query_as(
        "SELECT state, amount_minor, currency, exponent, review_reason FROM payments \
         WHERE order_id = $1",
    )
    .bind(uuid(order_id))
    .fetch_one(pool)
    .await
    .expect("payment row")
}

async fn count(pool: &PgPool, sql: &str, order_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(uuid(order_id))
        .fetch_one(pool)
        .await
        .expect("count")
}

/// A USDT leg; an unmatched one is really short, by one USDT.
fn usdt_leg(confirmations: u32, matched: bool, on_time: bool, finalized: bool) -> Value {
    let observed = if matched {
        TOTAL_USDT_UNITS
    } else {
        TOTAL_USDT_UNITS - 1_000_000
    };
    json!({
        "tx_hash": TX_HASH,
        "observed_amount": observed.to_string(),
        "confirmations": confirmations,
        "amount_matched": matched,
        "paid_on_time": on_time,
        "finalized": finalized,
    })
}

fn bitcoin_leg(confirmations: u32, matched: bool) -> Value {
    json!({
        "txid": BTC_TXID,
        "observed_sats": quoted_sats(),
        "confirmations": confirmations,
        "amount_matched": matched,
        "paid_on_time": true,
    })
}

struct StatusSpec {
    payment_state: &'static str,
    bitcoin: Option<Value>,
    usdt: Option<Value>,
    paid_asset: Option<&'static str>,
}

/// A Marketplace status body in the multi-asset contract's shape, for an
/// active invoice offering both assets.
fn status_body(invoice: Uuid, deadline: DateTime<Utc>, spec: StatusSpec) -> Value {
    json!({
        "invoice_id": invoice,
        "state": "active",
        "proposal_delivery_state": "delivered",
        "request_state": "accepted",
        "payment_state": spec.payment_state,
        "activated_at": (deadline - Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Micros, true),
        "payment_deadline": deadline.to_rfc3339_opts(SecondsFormat::Micros, true),
        "bitcoin": spec.bitcoin,
        "usdt_arbitrum": spec.usdt,
        "accepted_assets": ["BTC", "USDT"],
        "payment_amounts": {
            "BTC": quoted_sats().to_string(),
            "USDT": TOTAL_USDT_UNITS.to_string(),
        },
        "paid_asset": spec.paid_asset,
        "outcome": null,
        "resolved_at": null,
    })
}

fn usdt_status(invoice: Uuid, deadline: DateTime<Utc>, usdt: Value) -> Value {
    status_body(
        invoice,
        deadline,
        StatusSpec {
            payment_state: "confirmed",
            bitcoin: None,
            usdt: Some(usdt),
            paid_asset: Some("USDT"),
        },
    )
}

fn nothing_observed(invoice: Uuid, deadline: DateTime<Utc>) -> Value {
    status_body(
        invoice,
        deadline,
        StatusSpec {
            payment_state: "undetected",
            bitcoin: None,
            usdt: None,
            paid_asset: None,
        },
    )
}

struct Active {
    order: PendingOrder,
    invoice: Uuid,
    deadline: DateTime<Utc>,
}

/// A `paykit` order bound and activated at paykit-server, ready to poll.
async fn active_order(f: &Fixture) -> Active {
    let order = checkout_usd(f).await;
    let (status, body) = bind(f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "paykit bind failed: {body}");
    drain(f).await;
    let facts = facts(&f.app.pool, &order.order_id).await;
    assert_eq!(facts.paykit_request_state.as_deref(), Some("pending"));
    Active {
        invoice: facts.paykit_invoice_id.expect("invoice pinned"),
        deadline: facts.paykit_expires_at.expect("deadline pinned"),
        order,
    }
}

/// The poll instant for the n-th poll of a test: every call is past the
/// previous call's poll interval.
fn at(n: i64) -> DateTime<Utc> {
    Utc::now() + Duration::seconds(300 * n)
}

fn ship(order_id: &str, revision: i64, number: u64) -> Value {
    order_command(
        "fulfillment.ship",
        order_id,
        revision,
        json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-1" }),
        number,
    )
}

// ---------------------------------------------------------------------------
// The request the service sends and the offer it records
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_paykit_bind_sends_the_denomination_and_records_every_offered_asset(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");

    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount"], json!(TOTAL_CENTS.to_string()));
    assert_eq!(prepare["asset"], json!("USD"));
    assert_eq!(
        prepare["description"],
        json!(format!("Pubky Shop order {}", &order.order_id[..8])),
        "the multi-asset request is labelled like the legacy one"
    );
    assert!(
        prepare.get("accepted_assets").is_none(),
        "a seller that takes both assets is never narrowed: {prepare}"
    );
    assert!(prepare.get("amount_sats").is_none(), "{prepare}");

    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC", "USDT"]));
    assert_eq!(
        view["payment_amounts"],
        json!({ "BTC": quoted_sats(), "USDT": TOTAL_USDT_UNITS })
    );
    assert!(
        view.get("payment_asset").is_none() && view.get("payment_finality").is_none(),
        "nothing is paid yet: {view}"
    );
    assert_eq!(view["paykit_total_sats"], json!(quoted_sats()));

    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_asset.as_deref(), Some("USD"));
    assert_eq!(stored.paykit_amount_minor, Some(TOTAL_CENTS as i64));
    assert_eq!(stored.bitcoin_quoted_sats, Some(quoted_sats() as i64));
    let payment = payment_row(&pool, &order.order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (quoted_sats() as i64, "SAT", 0)
    );

    drain(&f).await;
    let activate = f
        .paykit
        .calls()
        .into_iter()
        .find(|call| call.path == "/marketplace/payment-requests/activate")
        .expect("activation sent");
    assert_eq!(activate.body["amount"], json!(TOTAL_CENTS.to_string()));
    assert_eq!(activate.body["asset"], json!("USD"));
    assert!(
        activate.body.get("total_sats").is_none(),
        "{}",
        activate.body
    );
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_request_state.as_deref(), Some("pending"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_seller_consent_and_the_flag_narrow_the_request_to_bitcoin(pool: PgPool) {
    for (usdt_flag, consent) in [(true, false), (false, false)] {
        let f = fixture(
            pool.clone(),
            Setup {
                usdt_flag,
                consent,
                ..Setup::both()
            },
        )
        .await;
        let order = checkout_usd(&f).await;
        let (status, body) = bind(&f, &order.order_id, "paykit").await;
        assert_eq!(status, StatusCode::OK, "bind failed: {body}");
        assert_eq!(
            prepare_body(&f.paykit)["accepted_assets"],
            json!(["BTC"]),
            "flag {usdt_flag}, consent {consent}"
        );
        assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
        assert_eq!(
            body["order"]["payment_amounts"],
            json!({ "BTC": quoted_sats() })
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_without_a_usdt_address_offers_bitcoin_only(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            receivable: &["BTC"],
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert!(prepare_body(&f.paykit).get("accepted_assets").is_none());
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreadable_fx_feed_narrows_to_usdt_and_fails_a_bitcoin_only_bind(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    f.fx.set_status(500);
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(prepare_body(&f.paykit)["accepted_assets"], json!(["USDT"]));
    let view = &body["order"];
    assert_eq!(view["accepted_assets"], json!(["USDT"]));
    assert_eq!(view["payment_amounts"], json!({ "USDT": TOTAL_USDT_UNITS }));
    assert!(view["paykit_total_sats"].is_null(), "{view}");
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(
        stored.bitcoin_quoted_sats, None,
        "no Bitcoin quote is pinned"
    );
    let payment = payment_row(&pool, &order.order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (TOTAL_USDT_UNITS as i64, "USDT", 6),
        "a USDT-only request types its payment in USDT0"
    );
    drain(&f).await;
    assert_eq!(
        facts(&pool, &order.order_id)
            .await
            .paykit_request_state
            .as_deref(),
        Some("pending"),
        "a USDT-only request activates by its denomination"
    );

    let g = fixture(
        pool.clone(),
        Setup {
            consent: false,
            ..Setup::both()
        },
    )
    .await;
    g.fx.set_status(500);
    let order = checkout_usd(&g).await;
    let (status, body) = bind(&g, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert!(
        g.paykit
            .calls()
            .iter()
            .all(|call| !call.path.starts_with("/marketplace/payment-requests")),
        "no request left the process"
    );
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_who_takes_only_usdt_is_offered_a_usdt_only_request(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            receivable: &["BTC", "USDT"],
            ..Setup::both()
        },
    )
    .await;
    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    let order = checkout_usd(&f).await;
    let unbound = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(
        unbound["next_actor"],
        json!("buyer"),
        "the seller has a rail"
    );
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(
        prepare_body(&f.paykit)["accepted_assets"],
        json!(["USDT"]),
        "the Bitcoin switch is off, so Bitcoin is never requested"
    );
    assert_eq!(f.fx.fetch_count(), 0, "no Bitcoin leg, no FX quote");
    let view = &body["order"];
    assert_eq!(view["accepted_assets"], json!(["USDT"]));
    assert_eq!(view["payment_amounts"], json!({ "USDT": TOTAL_USDT_UNITS }));

    // A legacy Bitcoin bind of the same seller is refused: the switch is off.
    let other = checkout_usd(&f).await;
    let (status, body) = bind(&f, &other.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("method_unavailable"));

    // With the consent off as well, Paykit is not available at all.
    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": false }),
    )
    .await;
    let third = checkout_usd(&f).await;
    let (status, body) = bind(&f, &third.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("method_unavailable"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_amount_far_from_the_service_quote_is_refused_and_voided(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    f.paykit.set_usd_per_btc(40_000);
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.payment_method, None);
    assert_eq!(stored.paykit_invoice_id, None);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        f.paykit
            .calls()
            .iter()
            .any(|call| call.path == "/marketplace/payment-requests/void"),
        "the prepared invoice is voided"
    );
}

fn usdt_units_for(sats: u64) -> u64 {
    (u128::from(sats) * 77_197).div_ceil(100) as u64
}

async fn sat_fixture(pool: PgPool, setup: Setup) -> Fixture {
    fixture(
        pool,
        Setup {
            usd_listing: false,
            ..setup
        },
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_priced_order_offers_every_rail_the_seller_enabled(pool: PgPool) {
    let f = sat_fixture(pool.clone(), Setup::both()).await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["asset"], json!("BTC"));
    assert!(prepare.get("accepted_assets").is_none(), "{prepare}");
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_asset.as_deref(), Some("BTC"));
    assert_eq!(
        stored.paykit_amount_minor, stored.paykit_total_sats,
        "no conversion: the request amount is the Bitcoin amount"
    );
    let sats = stored.paykit_total_sats.expect("sats") as u64;
    assert_eq!(prepare["amount"], json!(sats.to_string()));
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC", "USDT"]));
    assert_eq!(
        body["order"]["payment_amounts"],
        json!({ "BTC": sats, "USDT": usdt_units_for(sats) }),
        "the USDT amount is paykit-server's fixed rate, checked against the service's"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_priced_order_narrows_when_usdt_cannot_be_checked(pool: PgPool) {
    // No readable rate: USDT cannot be checked, so Bitcoin alone is offered.
    let f = sat_fixture(pool.clone(), Setup::both()).await;
    f.fx.set_status(500);
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(prepare_body(&f.paykit)["accepted_assets"], json!(["BTC"]));
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));

    // A seller who takes only USDT has nothing left without a rate.
    let g = sat_fixture(pool.clone(), Setup::both()).await;
    put_config(
        &g.app,
        &g.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    g.fx.set_status(500);
    let order = create_sat_order(&g.app, &g.seller, &g.buyer).await;
    let (status, body) = bind(&g, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_reference_unavailable"));
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_priced_usdt_only_seller_is_offered_usdt_at_the_checked_rate(pool: PgPool) {
    let f = sat_fixture(pool.clone(), Setup::both()).await;
    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    assert_eq!(prepare_body(&f.paykit)["accepted_assets"], json!(["USDT"]));
    let view = &body["order"];
    assert_eq!(view["accepted_assets"], json!(["USDT"]));
    assert!(view["paykit_total_sats"].is_null(), "{view}");
    let units = view["payment_amounts"]["USDT"]
        .as_u64()
        .expect("USDT amount");
    let payment = payment_row(&pool, &order.order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (units as i64, "USDT", 6)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_amount_far_from_the_service_rate_is_refused_on_a_bitcoin_priced_order(
    pool: PgPool,
) {
    let f = sat_fixture(pool.clone(), Setup::both()).await;
    f.paykit.set_usd_per_btc(40_000);
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fx_deviation_exceeded"));
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        f.paykit
            .calls()
            .iter()
            .any(|call| call.path == "/marketplace/payment-requests/void"),
        "the prepared invoice is voided"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_of_a_bitcoin_priced_order_is_recorded_in_usdt_units(pool: PgPool) {
    let f = sat_fixture(pool.clone(), Setup::both()).await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    drain(&f).await;
    let stored = facts(&pool, &order_id).await;
    let invoice = stored.paykit_invoice_id.expect("invoice");
    let deadline = stored.paykit_expires_at.expect("deadline");
    let sats = stored.paykit_total_sats.expect("sats") as u64;
    let paid_units = usdt_units_for(sats);
    // The transfer is included but one unit short: review, never paid.
    let mut status_body = json!({
        "invoice_id": invoice,
        "state": "active",
        "proposal_delivery_state": "delivered",
        "request_state": "accepted",
        "payment_state": "confirmed",
        "activated_at": (deadline - Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Micros, true),
        "payment_deadline": deadline.to_rfc3339_opts(SecondsFormat::Micros, true),
        "accepted_assets": ["BTC", "USDT"],
        "payment_amounts": { "BTC": sats.to_string(), "USDT": paid_units.to_string() },
        "bitcoin": null,
        "paid_asset": null,
        "outcome": null,
        "resolved_at": null,
    });
    status_body["usdt_arbitrum"] = json!({
        "tx_hash": TX_HASH,
        "observed_amount": (paid_units - 1).to_string(),
        "confirmations": 2,
        "amount_matched": false,
        "paid_on_time": false,
        "finalized": false,
    });
    f.paykit.set_marketplace_status(invoice, status_body);
    poll_now(&f.app, at(1)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert_ne!(stored.state, "paid");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["payment_amount_minor"], json!(paid_units - 1));
    assert_eq!(view["payment_quote_basis"], json!("fixed_rate"));

    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            stored.revision,
            json!({ "address": address }),
            31,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "address confirmed: {body}");
    let reference = format!("0x{}", "cd34".repeat(16));
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund recorded: {body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(paid_units - 1),
        "a satoshi-priced order refunds in the paid asset's units"
    );
    assert_eq!(
        view["external_refund"]["destination_address"],
        json!(address)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_multi_asset_request_is_chosen_by_its_own_setting_not_by_usdt(pool: PgPool) {
    // The contract is deployed, USDT is not listed and the flag is off: the
    // request is the new body and asks for Bitcoin only.
    let f = fixture(
        pool.clone(),
        Setup {
            usdt_flag: false,
            usdt_listed: false,
            consent: false,
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount"], json!(TOTAL_CENTS.to_string()));
    assert_eq!(prepare["asset"], json!("USD"));
    assert_eq!(prepare["accepted_assets"], json!(["BTC"]));
    assert!(prepare.get("amount_sats").is_none(), "{prepare}");
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
    let (_, public) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert!(
        public.get("usdt_available").is_none() && public.get("paykit_available").is_none(),
        "the flag is off, so the USDT keys are absent: {public}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_is_never_offered_on_the_legacy_contract_even_when_listed(pool: PgPool) {
    // A deployment that lists USDT but whose paykit-server still speaks the
    // legacy contract: the request is the legacy one and USDT is not offered.
    let f = fixture(
        pool.clone(),
        Setup {
            multi_asset: false,
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount_sats"], json!(quoted_sats()));
    assert!(prepare.get("accepted_assets").is_none(), "{prepare}");
    assert_eq!(body["order"]["accepted_assets"], json!(["BTC"]));
    let (_, public) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(public["usdt_available"], json!(false), "{public}");
    assert_eq!(public["accepted_assets"], json!(["BTC"]), "{public}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_bitcoin_bind_keeps_its_service_quote_on_the_new_contract(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["asset"], json!("BTC"));
    assert_eq!(prepare["amount"], json!(quoted_sats().to_string()));
    assert_eq!(prepare["accepted_assets"], json!(["BTC"]));
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("bitcoin"));
    assert!(
        view.get("accepted_assets").is_none() && view.get("payment_amounts").is_none(),
        "a legacy order keeps its projection: {view}"
    );
    assert_eq!(view["paykit_total_sats"], json!(quoted_sats()));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_an_rc16_upstream_the_paykit_bind_is_the_legacy_sats_request(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            multi_asset: false,
            usdt_listed: false,
            usdt_flag: false,
            consent: false,
            ..Setup::both()
        },
    )
    .await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let prepare = prepare_body(&f.paykit);
    assert_eq!(prepare["amount_sats"], json!(quoted_sats()));
    for key in ["amount", "asset", "accepted_assets"] {
        assert!(prepare.get(key).is_none(), "{key} in {prepare}");
    }
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC"]));
    assert_eq!(view["payment_amounts"], json!({ "BTC": quoted_sats() }));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_amount_minor, None, "legacy activation");
    drain(&f).await;
    let activate = f
        .paykit
        .calls()
        .into_iter()
        .find(|call| call.path == "/marketplace/payment-requests/activate")
        .expect("activation sent");
    assert_eq!(activate.body["total_sats"], json!(quoted_sats()));
    assert!(activate.body.get("amount").is_none(), "{}", activate.body);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn on_the_fork_the_paykit_bind_is_bitcoin_through_the_fork(pool: PgPool) {
    let f = fixture(
        pool.clone(),
        Setup {
            api: PaykitApi::Fork,
            multi_asset: false,
            usdt_listed: false,
            usdt_flag: false,
            consent: false,
            usd_listing: false,
            ..Setup::both()
        },
    )
    .await;
    enable_bitcoin(&f.app, &f.paykit, &f.seller).await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    let requests = f.paykit.requests();
    assert_eq!(requests.len(), 1, "one fork prepare");
    let view = &body["order"];
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["accepted_assets"], json!(["BTC"]));
    let stored = facts(&pool, &order.order_id).await;
    assert_eq!(stored.paykit_amount_minor, None);
    assert!(
        f.paykit
            .calls()
            .iter()
            .all(|call| !call.path.starts_with("/marketplace/")),
        "the fork has no Marketplace routes"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_legacy_usdt_bind_stays_refused_and_rebinding_another_method_is_refused(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "usdt").await;
    assert_ne!(status, StatusCode::OK, "{body}");
    assert_eq!(body["error"]["reason"], json!("usdt_unavailable"), "{body}");
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);

    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "same method is idempotent: {body}");
    let (status, body) = bind(&f, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("payment_method_already_bound")
    );
    assert_eq!(
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path == "/marketplace/payment-requests/prepare")
            .count(),
        1,
        "the idempotent rebind prepared nothing"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_public_config_names_the_one_paykit_method_and_its_assets(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let (status, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["paykit_available"], json!(true), "{body}");
    assert_eq!(body["accepted_assets"], json!(["BTC", "USDT"]), "{body}");
    assert_eq!(body["bitcoin_available"], json!(true));
    assert_eq!(body["usdt_available"], json!(true));

    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": true, "usdt_enabled": false }),
    )
    .await;
    let (_, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(body["paykit_available"], json!(true), "{body}");
    assert_eq!(body["accepted_assets"], json!(["BTC"]), "{body}");
    assert_eq!(body["usdt_available"], json!(false));

    put_config(
        &f.app,
        &f.seller.token,
        json!({ "bitcoin_enabled": false, "usdt_enabled": true }),
    )
    .await;
    let (_, body) = send(
        f.app.router.clone(),
        "GET",
        &format!("/v0/sellers/{}/payment-config", f.seller.pubky),
        None,
        &Value::Null,
    )
    .await;
    assert_eq!(
        body["paykit_available"],
        json!(true),
        "a seller who takes only USDT is offered Paykit: {body}"
    );
    assert_eq!(body["accepted_assets"], json!(["USDT"]), "{body}");
    assert_eq!(body["bitcoin_available"], json!(false));
}

// ---------------------------------------------------------------------------
// Settlement by the rail that paid
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_pays_at_inclusion_and_fulfilment_waits_for_finality(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);

    let (payment_state, amount, currency, exponent, _) = payment_row(&pool, &order_id).await;
    assert_eq!(payment_state, "confirmed");
    assert_eq!(
        (amount, currency.as_str(), exponent),
        (TOTAL_USDT_UNITS as i64, "USDT", 6),
        "the payment is retyped to the rail that paid"
    );
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_method"], json!("paykit"));
    assert_eq!(view["payment_asset"], json!("USDT"));
    assert_eq!(view["payment_network"], json!("arbitrum-one"));
    assert_eq!(view["payment_amount_minor"], json!(TOTAL_USDT_UNITS));
    assert_eq!(view["payment_exponent"], json!(6));
    assert_eq!(view["payment_quote_basis"], json!("parity"));
    assert_eq!(view["payment_finality"], json!("pending"));
    assert_eq!(view["accepted_assets"], json!(["BTC", "USDT"]));
    let receipt: (String, i64, i32) =
        sqlx::query_as("SELECT currency, total_minor, exponent FROM receipts WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("receipt");
    assert_eq!(
        (receipt.0.as_str(), receipt.1, receipt.2),
        ("USDT", TOTAL_USDT_UNITS as i64, 6)
    );

    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(&f.app, &f.seller.token, &ship(&order_id, revision, 1)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        body["error"]["reason"],
        json!("payment_not_final"),
        "{body}"
    );
    assert_eq!(facts(&pool, &order_id).await.state, "paid");

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(220, true, true, true),
        ),
    );
    assert_eq!(poll_now(&f.app, at(2)).await, 1, "finality moves the order");
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("final"));
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship after finality: {body}");
    assert_eq!(body["result"]["order"]["payment_finality"], json!("final"));
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:' || $1::text \
             AND kind = 'payment.finality_changed'",
            &order_id
        )
        .await,
        2,
        "pending, then final"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_reorged_usdt_transfer_reverts_until_it_returns_and_final_is_terminal(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(2, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );

    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("reverted"));
    assert_eq!(stored.state, "paid", "finality never un-pays the order");
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 3),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("payment_not_final"));

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending"),
        "a transfer that reappears is pending again"
    );

    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(300, true, true, true),
        ),
    );
    poll_now(&f.app, at(4)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("final")
    );
    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(5)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("final"),
        "a finalized transfer is never reverted by a later poll"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unavailable_status_never_reverts_a_usdt_payment(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    f.paykit.fail_marketplace_status_with(503, "unavailable");
    poll_now(&f.app, at(2)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );
    f.paykit.clear_marketplace_status_failure();
    // `paid_asset` names Bitcoin, which has no observation: the echo
    // disagrees with itself and the whole status fails closed.
    let mut malformed = usdt_status(
        active.invoice,
        active.deadline,
        usdt_leg(1, true, true, false),
    );
    malformed["paid_asset"] = json!("BTC");
    f.paykit.set_marketplace_status(active.invoice, malformed);
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending"),
        "a status that fails closed is never read as a reorg"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_through_the_paykit_method_settles_as_before_and_records_its_rail(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_asset"], json!("BTC"));
    assert_eq!(view["payment_network"], json!("bitcoin"));
    assert_eq!(view["payment_amount_minor"], json!(quoted_sats()));
    assert_eq!(view["payment_exponent"], json!(8));
    assert_eq!(view["payment_quote_basis"], json!("fixed_rate"));
    assert!(view.get("payment_finality").is_none(), "{view}");
    let payment = payment_row(&pool, &order_id).await;
    assert_eq!(
        (payment.1, payment.2.as_str(), payment.3),
        (quoted_sats() as i64, "SAT", 0)
    );
    let stored = facts(&pool, &order_id).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 4),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Bitcoin ships at once: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_transfer_for_the_wrong_amount_or_too_late_goes_to_review_never_paid(pool: PgPool) {
    for (matched, on_time) in [(false, true), (true, false)] {
        let f = fixture(pool.clone(), Setup::both()).await;
        let active = active_order(&f).await;
        let order_id = active.order.order_id.clone();
        f.paykit.set_marketplace_status(
            active.invoice,
            status_body(
                active.invoice,
                active.deadline,
                StatusSpec {
                    payment_state: "confirmed",
                    bitcoin: None,
                    usdt: Some(usdt_leg(1, matched, on_time, false)),
                    paid_asset: None,
                },
            ),
        );
        poll_now(&f.app, at(1)).await;
        let stored = facts(&pool, &order_id).await;
        assert_ne!(stored.state, "paid", "matched {matched}, on time {on_time}");
        assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
        let payment = payment_row(&pool, &order_id).await;
        assert_ne!(
            payment.0, "confirmed",
            "matched {matched}, on time {on_time}"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_matched_usdt_transfer_for_another_amount_than_offered_goes_to_review(pool: PgPool) {
    // paykit-server says matched, but the amount it reports is not the one
    // it offered for this request: the service never settles on that.
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let mut leg = usdt_leg(1, true, true, false);
    leg["observed_amount"] = json!((TOTAL_USDT_UNITS + 10_000).to_string());
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(active.invoice, active.deadline, leg),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_ne!(stored.state, "paid");
    let (state, _, _, _, reason) = payment_row(&pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("amount_mismatch"))
    );
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_both_rails_goes_to_manual_review_and_is_never_refunded(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("BTC"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let payment = payment_row(&pool, &order_id).await;
    assert_eq!(payment.0, "manual_review");
    assert_eq!(payment.4.as_deref(), Some("amount_mismatch"));
    let stored = facts(&pool, &order_id).await;
    assert_eq!(
        stored.state, "pending_payment",
        "nothing settles on its own"
    );
    assert_eq!(
        stored.payment_asset.as_deref(),
        Some("BTC"),
        "the rail Paykit names as paid first is the order's paid rail"
    );
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM receipts WHERE order_id = $1",
            &order_id
        )
        .await,
        0
    );
    let (external, observation): (Option<Value>, Option<Value>) =
        sqlx::query_as("SELECT external_refund, paykit_observation FROM orders WHERE id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("order row");
    assert!(external.is_none(), "never an automatic refund");
    let observation = observation.expect("both rails frozen");
    assert_eq!(observation["other_rail_asset"], json!("USDT"));
    assert_eq!(observation["other_rail_reference"], json!(TX_HASH));
    let seller_view = {
        let (status, body) = send(
            f.app.router.clone(),
            "GET",
            &format!("/v1/orders/{order_id}"),
            Some(&f.seller.token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        body
    };
    assert_eq!(
        seller_view["paykit_observation"]["other_rail_asset"],
        json!("USDT"),
        "the seller sees both rails"
    );
    let notices = delivered_notifications(&f.app, &f.seller.token, "bitcoin_manual_review").await;
    assert_eq!(notices.len(), 1, "the seller is told once: {notices:?}");
    assert_eq!(notices[0]["review_reason"], json!("amount_mismatch"));

    // A later poll leaves the review alone.
    assert_eq!(poll_now(&f.app, at(2)).await, 0);
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");

    // The seller resolves it through the existing path.
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "reason": "returned the extra payment from my wallet" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolve failed: {body}");
    assert_eq!(facts(&pool, &order_id).await.state, "paid");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_both_rails_order_resolved_paid_on_usdt_still_waits_for_finality(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let both = |finalized: bool, confirmations: u32| {
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: Some(usdt_leg(confirmations, true, true, finalized)),
                paid_asset: Some("USDT"),
            },
        )
    };
    f.paykit
        .set_marketplace_status(active.invoice, both(false, 1));
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");

    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid", "reason": "returned the Bitcoin payment" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolve failed: {body}");
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid");
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 21),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("payment_not_final"));

    // The paid order keeps polling for finality although a second rail was
    // already recorded.
    f.paykit
        .set_marketplace_status(active.invoice, both(true, 400));
    assert_eq!(poll_now(&f.app, at(2)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("final"));
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 22),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship once final: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn money_on_the_other_rail_after_settlement_is_held_for_an_operator_once(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(facts(&pool, &order_id).await.state, "paid");

    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(2, true)),
                usdt: Some(usdt_leg(5, true, true, false)),
                paid_asset: Some("USDT"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(2)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid", "the order is never un-paid");
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(payment_row(&pool, &order_id).await.0, "confirmed");
    let held: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_all(&pool)
    .await
    .expect("held attempts");
    assert_eq!(
        held,
        vec![(
            "needs_review".to_string(),
            Some("payment_settled".to_string())
        )]
    );
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM paykit_superseded_attempts WHERE order_id = $1",
            &order_id
        )
        .await,
        1,
        "flagged once"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_after_a_bitcoin_settlement_is_held_for_an_operator(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_asset.as_deref(),
        Some("BTC")
    );
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(4, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.payment_asset.as_deref(), Some("BTC"));
    assert!(stored.paykit_second_rail_at.is_some());
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM paykit_superseded_attempts \
             WHERE order_id = $1 AND state = 'needs_review'",
            &order_id
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_refund_follows_the_paid_rail_and_a_bitcoin_refund_does_not(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": address }),
            5,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "USDT order takes an address: {body}"
    );
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["refund_destination"]["address"], json!(address));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bitcoin_order_has_no_refund_address_to_confirm(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            6,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_on_a_released_attempt_is_held_for_review_not_routed(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first = facts(&pool, &order_id).await;
    let invoice = first.paykit_invoice_id.expect("invoice");
    // Paykit finalized the invoice before the activation arrived: the
    // marketplace releases the attempt.
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order_id).await.payment_method, None);
    let deadline = Utc::now() + Duration::minutes(30);
    f.paykit.set_marketplace_status(
        invoice,
        usdt_status(invoice, deadline, usdt_leg(1, true, true, false)),
    );
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("client");
    marketplace_service::paykit_attempts::verify_due_released_attempts(&f.app.state, client, at(1))
        .await
        .expect("released attempts poll");
    let held: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT state, review_reason FROM paykit_superseded_attempts WHERE order_id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_all(&pool)
    .await
    .expect("held attempts");
    assert_eq!(
        held,
        vec![("needs_review".to_string(), Some("other_rail".to_string()))]
    );
    assert_eq!(
        payment_row(&pool, &order_id).await.0,
        "awaiting_entitlement"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pickup_order_paid_in_usdt_waits_for_finality_before_handover(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    // The same guard covers the pickup commands; the order is flipped to a
    // pickup order directly so the fixture needs no pickup listing.
    sqlx::query("UPDATE orders SET fulfillment = 'pickup' WHERE id = $1")
        .bind(uuid(&order_id))
        .execute(&pool)
        .await
        .expect("pickup order");
    for (kind, actor, number) in [
        ("fulfillment.mark_ready", &f.seller, 11_u64),
        ("fulfillment.confirm_pickup", &f.seller, 12),
        ("fulfillment.confirm_pickup", &f.buyer, 13),
    ] {
        let revision = facts(&pool, &order_id).await.revision;
        let (status, body) = execute(
            &f.app,
            &actor.token,
            &order_command(kind, &order_id, revision, json!({}), number),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{kind}: {body}");
        assert_eq!(
            body["error"]["reason"],
            json!("payment_not_final"),
            "{kind}"
        );
    }
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(90, true, true, true),
        ),
    );
    poll_now(&f.app, at(2)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &order_command("fulfillment.mark_ready", &order_id, revision, json!({}), 14),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ready once final: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_payment_in_review_is_refunded_to_the_buyers_arbitrum_address(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: None,
                usdt: Some(usdt_leg(1, false, true, false)),
                paid_asset: None,
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    let (state, _, _, _, reason) = payment_row(&pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("amount_mismatch"))
    );

    let reference = format!("0x{}", "ab12".repeat(16));
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "no address confirmed yet: {body}"
    );
    assert_eq!(
        body["error"]["reason"],
        json!("refund_destination_required")
    );

    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": address }),
            15,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "address confirmed: {body}");
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "refund recorded: {body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["destination_address"],
        json!(address)
    );
    assert_eq!(view["external_refund"]["transaction_id"], json!(reference));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(UNDERPAID_CENTS),
        "the refund is what the buyer sent"
    );
}

/// An upstream `paykit` order whose USDT transfer did not match the amount:
/// the payment waits in review on the USDT rail (`payment_asset = 'USDT'`).
async fn upstream_usdt_review(f: &Fixture) -> (String, DateTime<Utc>) {
    let active = active_order(f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: None,
                usdt: Some(usdt_leg(1, false, true, false)),
                paid_asset: None,
            },
        ),
    );
    poll_now(&f.app, at(1)).await;
    let (method, asset, entered_at): (Option<String>, Option<String>, Option<DateTime<Utc>>) =
        sqlx::query_as(
            "SELECT o.payment_method, o.payment_asset, p.manual_review_entered_at \
             FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id = $1",
        )
        .bind(uuid(&order_id))
        .fetch_one(&f.app.pool)
        .await
        .expect("review facts");
    assert_eq!(
        (method.as_deref(), asset.as_deref()),
        (Some("paykit"), Some("USDT"))
    );
    (order_id, entered_at.expect("in review"))
}

async fn confirm_refund_address(f: &Fixture, order_id: &str, number: u64) -> (StatusCode, Value) {
    let revision = facts(&f.app.pool, order_id).await.revision;
    execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            order_id,
            revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            number,
        ),
    )
    .await
}

async fn record_usdt_refund(
    f: &Fixture,
    order_id: &str,
    transaction_id: &str,
    number: u64,
) -> (StatusCode, Value) {
    let revision = facts(&f.app.pool, order_id).await.revision;
    execute(
        &f.app,
        &f.seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            revision,
            json!({ "amount_minor": UNDERPAID_CENTS, "transaction_id": transaction_id }),
            number,
        ),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_review_the_seller_ignores_is_reaped_and_its_refund_stays_open(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let (order_id, entered_at) = upstream_usdt_review(&f).await;

    let (alerts, abandoned) = watch_manual_reviews(
        &f.app.state,
        entered_at + Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS),
    )
    .await
    .expect("watch runs");
    assert_eq!((alerts, abandoned), (1, 1));
    let (state, _, _, _, _) = payment_row(&pool, &order_id).await;
    assert_eq!(state, "expired");
    assert_eq!(facts(&pool, &order_id).await.state, "cancelled");

    // The money was received: the buyer can still give a refund address and
    // the seller can still record the refund to it.
    let (status, body) = confirm_refund_address(&f, &order_id, 31).await;
    assert_eq!(status, StatusCode::OK, "address after abandonment: {body}");
    let (status, body) = record_usdt_refund(&f, &order_id, TX_HASH, 32).await;
    assert_eq!(status, StatusCode::OK, "refund after abandonment: {body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["destination_address"],
        json!("0xd8da6bf26964af9d7eed9e03e53415d37aa96045")
    );
}

/// The seven-day reaper and the buyer's refund address, in both orders,
/// forced: a test transaction holds the order row, the first contender is
/// seen blocked on it, then the second, and only then is the row released,
/// so the first one queued goes first.
///
/// Reaper first: the buyer's command carries the revision it read before the
/// abandonment, so it is refused as stale, and the retry a client makes then
/// meets an abandoned payment and must still be accepted. Buyer first: the
/// address lands while the review is open and the reaper abandons after it.
/// Either way the seller can then record the refund.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_reaper_and_the_buyers_refund_address_race_and_the_refund_stays_open(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    for (round, reaper_first) in [(0u64, true), (1, false)] {
        let (order_id, entered_at) = upstream_usdt_review(&f).await;
        let at = entered_at + Duration::days(MANUAL_REVIEW_INACTIVITY_DAYS);
        let stale = order_command(
            "refund.confirm_destination",
            &order_id,
            facts(&pool, &order_id).await.revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            40 + round * 4,
        );
        let (holder, holder_pid) = hold_order_row(&pool, uuid(&order_id)).await;
        let second_go = Notify::new();
        let reaper = async {
            if !reaper_first {
                second_go.notified().await;
            }
            watch_manual_reviews(&f.app.state, at)
                .await
                .expect("watch runs")
        };
        let buyer = async {
            if reaper_first {
                second_go.notified().await;
            }
            execute(&f.app, &f.buyer.token, &stale).await
        };
        let release = async {
            let first = wait_until_blocked_by(&pool, &[holder_pid], &[]).await;
            second_go.notify_one();
            wait_until_blocked_by(&pool, &[holder_pid, first], &[first]).await;
            holder.commit().await.expect("release the order row");
        };
        let ((alerts, abandoned), (status, body), ()) = tokio::join!(reaper, buyer, release);
        assert_eq!((alerts, abandoned), (1, 1), "round {round}");
        assert_eq!(
            payment_row(&pool, &order_id).await.0,
            "expired",
            "round {round}"
        );
        if reaper_first {
            assert_eq!(
                status,
                StatusCode::CONFLICT,
                "the stale address command: {body}"
            );
            assert_eq!(body["error"]["code"], json!("REVISION_CONFLICT"), "{body}");
            let (status, body) = confirm_refund_address(&f, &order_id, 41 + round * 4).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "the retry after abandonment: {body}"
            );
        } else {
            assert_eq!(
                status,
                StatusCode::OK,
                "the address before abandonment: {body}"
            );
        }
        let hash = format!("0x{:064x}", round + 1);
        let (status, body) = record_usdt_refund(&f, &order_id, &hash, 42 + round * 4).await;
        assert_eq!(status, StatusCode::OK, "round {round}: {body}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bitcoin_on_a_released_paykit_attempt_takes_the_late_money_path(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first = facts(&pool, &order_id)
        .await
        .paykit_invoice_id
        .expect("invoice");
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order_id).await.payment_method, None);
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "the buyer pays again: {body}");
    drain(&f).await;
    let second = facts(&pool, &order_id)
        .await
        .paykit_invoice_id
        .expect("invoice");
    assert_ne!(first, second);

    // The first attempt's request was in fact live: Bitcoin lands on it.
    let deadline = Utc::now() + Duration::minutes(30);
    f.paykit.set_marketplace_status(
        first,
        status_body(
            first,
            deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("client");
    let routed = marketplace_service::paykit_attempts::verify_due_released_attempts(
        &f.app.state,
        client,
        at(1),
    )
    .await
    .expect("released attempts poll");
    assert_eq!(routed, 1, "the paid released attempt is routed");
    // Attempt 2 still holds the stock, so the late money takes the existing
    // late-money path: the paid attempt becomes the attempt of record and a
    // human decides.
    let (state, _, _, _, reason) = payment_row(&pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("late_settlement"))
    );
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.paykit_invoice_id, Some(first));
    assert_eq!(stored.payment_method.as_deref(), Some("paykit"));
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["accepted_assets"], json!(["BTC"]));
    assert_eq!(view["payment_amounts"], json!({ "BTC": quoted_sats() }));
}

/// A status body captured from paykit-server. Only what the capture itself
/// replaces is changed (the invoice and its two instants), plus the amounts,
/// which the capture made for a 5-cent request: they are set to this order's
/// own offer so the observed amounts equal what was offered.
fn captured_status(name: &str, invoice: Uuid, deadline: DateTime<Utc>, offer: (u64, u64)) -> Value {
    let (sats, usdt_units) = offer;
    let path = format!(
        "{}/tests/fixtures/paykit-server-pk1/status/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut body: Value = serde_json::from_str(
        &std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}")),
    )
    .expect("captured JSON");
    body["invoice_id"] = json!(invoice);
    body["activated_at"] =
        json!((deadline - Duration::minutes(30)).to_rfc3339_opts(SecondsFormat::Micros, true));
    body["payment_deadline"] = json!(deadline.to_rfc3339_opts(SecondsFormat::Micros, true));
    if body["payment_amounts"].get("BTC").is_some() {
        body["payment_amounts"]["BTC"] = json!(sats.to_string());
    }
    if body["payment_amounts"].get("USDT").is_some() {
        body["payment_amounts"]["USDT"] = json!(usdt_units.to_string());
    }
    if body["bitcoin"].is_object() {
        body["bitcoin"]["observed_sats"] = json!(sats);
    }
    if let Some(observed) = body["usdt_arbitrum"].get("observed_amount").cloned() {
        body["usdt_arbitrum"]["observed_amount"] = match observed.as_str() {
            Some("49999") => json!((usdt_units - 1).to_string()),
            _ => json!(usdt_units.to_string()),
        };
    }
    body
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn captured_status_bodies_drive_usdt_settlement_and_finality_end_to_end(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let offer = (quoted_sats(), TOTAL_USDT_UNITS);
    let serve = |name: &str| {
        f.paykit.set_marketplace_status(
            active.invoice,
            captured_status(name, active.invoice, active.deadline, offer),
        )
    };

    serve("usdt-included");
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_asset"], json!("USDT"));
    assert_eq!(view["payment_amount_minor"], json!(TOTAL_USDT_UNITS));
    assert_eq!(view["payment_finality"], json!("pending"));

    serve("usdt-reorged");
    poll_now(&f.app, at(2)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("reverted")
    );
    serve("usdt-included");
    poll_now(&f.app, at(3)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );
    serve("usdt-finalized");
    poll_now(&f.app, at(4)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("final")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn captured_bitcoin_and_cross_rail_bodies_settle_and_review_end_to_end(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;

    let bitcoin = active_order(&f).await;
    f.paykit.set_marketplace_status(
        bitcoin.invoice,
        captured_status(
            "btc-on-dual-request",
            bitcoin.invoice,
            bitcoin.deadline,
            (quoted_sats(), TOTAL_USDT_UNITS),
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let stored = facts(&pool, &bitcoin.order.order_id).await;
    assert_eq!(stored.state, "paid");
    assert_eq!(stored.payment_asset.as_deref(), Some("BTC"));
    assert_eq!(stored.payment_finality, None);

    for (round, name, first) in [
        (2, "cross-rail-usdt-first", "USDT"),
        (3, "cross-rail-bitcoin-first", "BTC"),
    ] {
        let dual = active_order(&f).await;
        f.paykit.set_marketplace_status(
            dual.invoice,
            captured_status(
                name,
                dual.invoice,
                dual.deadline,
                (quoted_sats(), TOTAL_USDT_UNITS),
            ),
        );
        assert_eq!(poll_now(&f.app, at(round)).await, 1, "{name}");
        let stored = facts(&pool, &dual.order.order_id).await;
        assert_eq!(stored.state, "pending_payment", "{name}");
        assert_eq!(stored.payment_asset.as_deref(), Some(first), "{name}");
        let (state, _, _, _, reason) = payment_row(&pool, &dual.order.order_id).await;
        assert_eq!(
            (state.as_str(), reason.as_deref()),
            ("manual_review", Some("amount_mismatch")),
            "{name}"
        );
    }

    for (round, name) in [(4, "usdt-underpaid"), (5, "usdt-late")] {
        let short = active_order(&f).await;
        f.paykit.set_marketplace_status(
            short.invoice,
            captured_status(
                name,
                short.invoice,
                short.deadline,
                (quoted_sats(), TOTAL_USDT_UNITS),
            ),
        );
        poll_now(&f.app, at(round)).await;
        let stored = facts(&pool, &short.order.order_id).await;
        assert_ne!(stored.state, "paid", "{name}");
        assert_ne!(
            payment_row(&pool, &short.order.order_id).await.0,
            "confirmed"
        );
    }
}

// ---------------------------------------------------------------------------
// Review findings (Kimi, PR #129 at 7f067ac)
// ---------------------------------------------------------------------------

/// The database URL of the test's own database, for the admin binary.
fn test_database_url(pool: &PgPool) -> String {
    let mut url = url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL"))
        .expect("database url");
    let database = pool
        .connect_options()
        .get_database()
        .expect("test database name")
        .to_string();
    url.set_path(&format!("/{database}"));
    url.to_string()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_review_row_without_a_satoshi_total_lists_through_the_query_and_the_admin_binary(
    pool: PgPool,
) {
    // The FX quote is unreadable, so the request offers USDT only and its
    // released attempt has no satoshi total.
    let f = fixture(pool.clone(), Setup::both()).await;
    f.fx.set_status(500);
    let order = checkout_usd(&f).await;
    let order_id = order.order_id.clone();
    let (status, body) = bind(&f, &order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["order"]["paykit_total_sats"].is_null());
    let invoice = facts(&pool, &order_id)
        .await
        .paykit_invoice_id
        .expect("invoice");
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    let deadline = Utc::now() + Duration::minutes(30);
    f.paykit.set_marketplace_status(
        invoice,
        usdt_status(invoice, deadline, usdt_leg(1, true, true, false)),
    );
    let client = f
        .app
        .state
        .payments
        .as_ref()
        .and_then(|payments| payments.paykit.as_ref())
        .expect("client");
    marketplace_service::paykit_attempts::verify_due_released_attempts(&f.app.state, client, at(1))
        .await
        .expect("released attempts poll");
    let total: Option<i64> =
        sqlx::query_scalar("SELECT total_sats FROM paykit_superseded_attempts WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("held row");
    assert_eq!(total, None, "a USDT-only attempt has no satoshi total");

    let listed = marketplace_service::paykit_attempts::list_needs_review(&pool)
        .await
        .expect("the operator queue lists a row without a total");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].total_sats, None);
    assert_eq!(listed[0].review_reason, "other_rail");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_paykit-attempts-admin"))
        .arg("list")
        .env("DATABASE_URL", test_database_url(&pool))
        .output()
        .expect("admin binary runs");
    assert!(
        output.status.success(),
        "admin list failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = String::from_utf8(output.stdout).expect("utf-8");
    let row: Value = serde_json::from_str(line.trim()).expect("one JSON row");
    assert_eq!(row["order_id"], json!(order_id));
    assert!(row["total_sats"].is_null(), "{row}");
    assert_eq!(row["review_reason"], json!("other_rail"));
}

/// A writer that keeps every log line for the test to read.
#[derive(Clone, Default)]
struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;
    fn make_writer(&self) -> LogBuffer {
        self.clone()
    }
}

impl LogBuffer {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (buffer, guard)
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log lock").clone()).expect("utf-8 log")
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_fork_paykit_settlement_logs_no_alert_but_an_unknown_denomination_does(pool: PgPool) {
    let (logs, _guard) = LogBuffer::capture();
    let f = fixture(
        pool.clone(),
        Setup {
            api: PaykitApi::Fork,
            multi_asset: false,
            usdt_listed: false,
            usdt_flag: false,
            consent: false,
            usd_listing: false,
            ..Setup::both()
        },
    )
    .await;
    enable_bitcoin(&f.app, &f.paykit, &f.seller).await;
    let order = create_sat_order(&f.app, &f.seller, &f.buyer).await;
    let order_id = uuid(&order.order_id);
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    drain(&f).await;
    f.paykit.set_status(
        &attempt_reference(order_id, 1),
        common::paykit_review::status_confirmed("exclusive", true, 2),
    );
    assert_eq!(poll_now(&f.app, f.app.clock.now()).await, 1);
    assert_eq!(facts(&pool, &order.order_id).await.state, "paid");
    assert!(
        !logs.text().contains("ALERT"),
        "a routine fork settlement is quiet:\n{}",
        logs.text()
    );

    // An upstream order whose denomination is unknown and not NULL is a
    // real fault and is alerted.
    let g = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&g).await;
    sqlx::query("UPDATE orders SET paykit_asset = 'EUR' WHERE id = $1")
        .bind(uuid(&active.order.order_id))
        .execute(&pool)
        .await
        .expect("denomination");
    g.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&g.app, at(1)).await;
    assert!(
        logs.text()
            .contains("ALERT paykit order carries a denomination this build does not know"),
        "{}",
        logs.text()
    );
    assert_eq!(
        facts(&pool, &active.order.order_id).await.payment_asset,
        None
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn both_rails_without_a_named_first_rail_leaves_the_paid_rail_to_the_operator(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    // Both rails hold money, neither qualifies as paid on time: Paykit names
    // no first rail.
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, false)),
                usdt: Some(usdt_leg(1, false, false, false)),
                paid_asset: None,
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let (state, _, _, _, reason) = payment_row(&pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("amount_mismatch"))
    );
    let stored = facts(&pool, &order_id).await;
    assert_eq!(
        stored.payment_asset, None,
        "the service does not guess which rail was paid first"
    );
    assert_eq!(stored.payment_finality, None);
    assert!(stored.paykit_second_rail_at.is_some());
    let seller_view = {
        let (_, body) = send(
            f.app.router.clone(),
            "GET",
            &format!("/v1/orders/{order_id}"),
            Some(&f.seller.token),
            &Value::Null,
        )
        .await;
        body
    };
    assert_eq!(
        seller_view["paykit_observation"]["paid_rail_unknown"],
        json!(true)
    );
    assert!(seller_view.get("payment_asset").is_none());

    // A refund needs a rail to follow: it is refused with a named reason, the
    // seller resolves as paid or abandoned and returns each payment.
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": "tx-1" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("refund_rail_unknown"),
        "{body}"
    );
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned", "reason": "returned both payments" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_review_refund_between_two_cents_records_the_lower_cent(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    // A matched transfer for a different amount than offered goes to review
    // with a USDT amount that is not a whole number of cents: the refunded
    // resolution records it in cents rounded down, like `refund.record_external`.
    let mut leg = usdt_leg(1, true, true, false);
    leg["observed_amount"] = json!((TOTAL_USDT_UNITS + 1).to_string());
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(active.invoice, active.deadline, leg),
    );
    poll_now(&f.app, at(1)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");
    let address = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            stored.revision,
            json!({ "address": address }),
            41,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let reference = format!("0x{}", "ef56".repeat(16));
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "refunded", "external_refund_reference": reference }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(view["external_refund"]["amount_minor"], json!(TOTAL_CENTS));
}

/// paykit-server's body for a confirmed USDT transfer of another amount than
/// it offered (`usdt-underpaid.json`): not matched, and no rail paid.
fn mismatched_usdt_status(active: &Active, leg: Value) -> Value {
    status_body(
        active.invoice,
        active.deadline,
        StatusSpec {
            payment_state: "confirmed",
            bitcoin: None,
            usdt: Some(leg),
            paid_asset: None,
        },
    )
}

/// A USD order whose USDT transfer carried `received` units instead of the
/// quote: manual review, the buyer's refund address, then the seller closes
/// the review as abandoned, which cancels the order with the money still to
/// return.
async fn abandoned_mismatched_usdt_order(
    f: &Fixture,
    received: u64,
    command_number: u64,
) -> String {
    let active = active_order(f).await;
    let order_id = active.order.order_id.clone();
    let mut leg = usdt_leg(1, false, true, false);
    leg["observed_amount"] = json!(received.to_string());
    f.paykit
        .set_marketplace_status(active.invoice, mismatched_usdt_status(&active, leg));
    poll_now(&f.app, at(1)).await;
    let (state, _, _, _, reason) = payment_row(&f.app.pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("amount_mismatch"))
    );
    let revision = facts(&f.app.pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            command_number,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "address confirmed: {body}");
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "abandoned", "reason": "returning it from my wallet" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "abandoned: {body}");
    assert_eq!(facts(&f.app.pool, &order_id).await.state, "cancelled");
    order_id
}

async fn record_refund(
    f: &Fixture,
    order_id: &str,
    amount_minor: u64,
    transaction_id: &str,
    command_number: u64,
) -> (StatusCode, Value) {
    let revision = facts(&f.app.pool, order_id).await.revision;
    execute(
        &f.app,
        &f.seller.token,
        &order_command(
            "refund.record_external",
            order_id,
            revision,
            json!({ "amount_minor": amount_minor, "transaction_id": transaction_id }),
            command_number,
        ),
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_underpaid_usdt_refund_is_capped_at_what_arrived_not_the_total(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    // One USDT short of the 137.00 quote.
    let order_id = abandoned_mismatched_usdt_order(&f, TOTAL_USDT_UNITS - 1_000_000, 51).await;
    let received_cents = TOTAL_CENTS - 100;
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(
        view["payment_amount_minor"],
        json!(TOTAL_USDT_UNITS - 1_000_000)
    );

    let hash = format!("0x{}", "a1b2".repeat(16));
    let (status, body) = record_refund(&f, &order_id, TOTAL_CENTS, &hash, 52).await;
    assert_eq!(status, StatusCode::CONFLICT, "more than arrived: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
    assert_eq!(facts(&pool, &order_id).await.state, "cancelled");

    let (status, body) = record_refund(&f, &order_id, received_cents, &hash, 53).await;
    assert_eq!(status, StatusCode::OK, "what arrived: {body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(received_cents)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_overpaid_usdt_refund_may_return_the_excess(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    // One USDT over the 137.00 quote.
    let order_id = abandoned_mismatched_usdt_order(&f, TOTAL_USDT_UNITS + 1_000_000, 61).await;
    let received_cents = TOTAL_CENTS + 100;

    let hash = format!("0x{}", "c3d4".repeat(16));
    let (status, body) = record_refund(&f, &order_id, received_cents + 1, &hash, 62).await;
    assert_eq!(status, StatusCode::CONFLICT, "more than arrived: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");

    let (status, body) = record_refund(&f, &order_id, received_cents, &hash, 63).await;
    assert_eq!(status, StatusCode::OK, "the whole amount received: {body}");
    let view = read_order(&f, &f.seller.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(received_cents)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_overpaid_usdt_review_resolved_as_refunded_records_what_arrived(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let mut leg = usdt_leg(1, false, true, false);
    leg["observed_amount"] = json!((TOTAL_USDT_UNITS + 1_000_000).to_string());
    f.paykit
        .set_marketplace_status(active.invoice, mismatched_usdt_status(&active, leg));
    poll_now(&f.app, at(1)).await;
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            81,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({
            "outcome": "refunded",
            "external_refund_reference": format!("0x{}", "0a1b".repeat(16)),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("refunded_external"));
    assert_eq!(
        view["external_refund"]["amount_minor"],
        json!(TOTAL_CENTS + 100)
    );
}

/// A USD order whose USDT transfer carried `dust` millionths, less than one
/// cent: the refunded resolution has no cent to record and refuses with
/// `refund_amount_unrecordable`, leaving the review open.
async fn a_usdt_review_refund_of_dust_is_unrecordable(pool: PgPool, dust: u64) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let mut leg = usdt_leg(1, false, true, false);
    leg["observed_amount"] = json!(dust.to_string());
    f.paykit
        .set_marketplace_status(active.invoice, mismatched_usdt_status(&active, leg));
    poll_now(&f.app, at(1)).await;
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");
    let before = facts(&pool, &order_id).await;
    assert_eq!(before.payment_asset.as_deref(), Some("USDT"));
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "refund.confirm_destination",
            &order_id,
            before.revision,
            json!({ "address": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045" }),
            91,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let revision = facts(&pool, &order_id).await.revision;
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({
            "outcome": "refunded",
            "external_refund_reference": format!("0x{}", "d0d0".repeat(16)),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!("refund_amount_unrecordable"),
        "{body}"
    );
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");
    let after = facts(&pool, &order_id).await;
    assert_eq!(after.revision, revision, "the order is untouched");
    let view = read_order(&f, &f.seller.token, &order_id).await;
    assert_eq!(view["external_refund"], Value::Null);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_review_refund_of_one_millionth_is_unrecordable(pool: PgPool) {
    a_usdt_review_refund_of_dust_is_unrecordable(pool, 1).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_review_refund_just_short_of_a_cent_is_unrecordable(pool: PgPool) {
    a_usdt_review_refund_of_dust_is_unrecordable(pool, 9_999).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_recorded_refund_of_usdt_dust_is_refused_with_a_typed_error(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    // 0.009999 USDT arrived: the cap is 0 cents, so no record fits.
    let order_id = abandoned_mismatched_usdt_order(&f, 9_999, 95).await;
    let hash = format!("0x{}", "d1d1".repeat(16));
    let (status, body) = record_refund(&f, &order_id, 1, &hash, 96).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
    assert_eq!(facts(&pool, &order_id).await.state, "cancelled");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt_amount_between_two_cents_caps_the_refund_at_the_lower_cent(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    // 138.009999 USDT: 138.00 is what can be recorded in cents.
    let order_id = abandoned_mismatched_usdt_order(&f, TOTAL_USDT_UNITS + 1_009_999, 71).await;
    let hash = format!("0x{}", "e5f6".repeat(16));
    let (status, body) = record_refund(&f, &order_id, TOTAL_CENTS + 101, &hash, 72).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = record_refund(&f, &order_id, TOTAL_CENTS + 100, &hash, 73).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_finality_that_stays_pending_for_seven_days_goes_to_an_operator(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );
    let status_reads = || {
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path == "/marketplace/payment-requests/status")
            .count()
    };
    let day = |days: i64| Utc::now() + Duration::days(days);

    // Inside the bound it keeps polling and nothing is escalated.
    let before = status_reads();
    poll_now(&f.app, day(6)).await;
    assert!(status_reads() > before, "still polled on day 6");
    assert!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .is_empty()
    );

    // Past it, one durable operator row is written and polling stops.
    poll_now(&f.app, day(8)).await;
    let queue = marketplace_service::paykit_attempts::list_needs_review(&pool)
        .await
        .expect("queue");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].order_id, uuid(&order_id));
    assert_eq!(queue[0].review_reason, "detected_unconfirmed");
    let after = status_reads();
    poll_now(&f.app, day(9)).await;
    poll_now(&f.app, day(10)).await;
    assert_eq!(status_reads(), after, "no longer polled after the bound");
    assert_eq!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .len(),
        1,
        "escalated once"
    );
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid", "the order stays paid");
    assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn usdt_finality_that_stays_reverted_for_seven_days_goes_to_an_operator(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    // The transfer is reorganised away on day 1 and never returns.
    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(2)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("reverted")
    );
    let status_reads = || {
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path == "/marketplace/payment-requests/status")
            .count()
    };
    let day = |days: i64| Utc::now() + Duration::days(days);

    let before = status_reads();
    poll_now(&f.app, day(6)).await;
    assert!(status_reads() > before, "still polled on day 6");
    assert!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .is_empty()
    );

    poll_now(&f.app, day(8)).await;
    let queue = marketplace_service::paykit_attempts::list_needs_review(&pool)
        .await
        .expect("queue");
    assert_eq!(queue.len(), 1, "a reverted payment is not left silent");
    assert_eq!(queue[0].order_id, uuid(&order_id));
    assert_eq!(queue[0].review_reason, "detected_unconfirmed");
    assert_eq!(
        queue[0].observation.as_ref().expect("observation")["status"],
        json!("usdt_finality_reverted")
    );
    let after = status_reads();
    poll_now(&f.app, day(9)).await;
    poll_now(&f.app, day(10)).await;
    assert_eq!(status_reads(), after, "no longer polled after the bound");
    assert_eq!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .len(),
        1,
        "escalated once"
    );
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "paid", "the order stays paid");
    assert_eq!(stored.payment_finality.as_deref(), Some("reverted"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_reappearing_usdt_transfer_restarts_the_finality_clock(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let included = usdt_status(
        active.invoice,
        active.deadline,
        usdt_leg(1, true, true, false),
    );
    let gone = nothing_observed(active.invoice, active.deadline);
    let day = |days: i64| Utc::now() + Duration::days(days);
    f.paykit
        .set_marketplace_status(active.invoice, included.clone());
    poll_now(&f.app, day(0)).await;
    f.paykit.set_marketplace_status(active.invoice, gone);
    poll_now(&f.app, day(1)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("reverted")
    );
    // It comes back on day 5: pending again, with a new clock.
    f.paykit.set_marketplace_status(active.invoice, included);
    poll_now(&f.app, day(5)).await;
    assert_eq!(
        facts(&pool, &order_id).await.payment_finality.as_deref(),
        Some("pending")
    );
    poll_now(&f.app, day(9)).await;
    assert!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .is_empty(),
        "eight days after the first inclusion but four after the return"
    );
    poll_now(&f.app, day(13)).await;
    let queue = marketplace_service::paykit_attempts::list_needs_review(&pool)
        .await
        .expect("queue");
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue[0].observation.as_ref().expect("observation")["status"],
        json!("usdt_finality_pending")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_escalation_that_cannot_be_recorded_is_alerted_every_pass(pool: PgPool) {
    let (logs, _guard) = LogBuffer::capture();
    let f = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    // An order without complete request terms: the review row cannot be
    // written (`record_released_attempt` needs a satoshi total or an amount).
    sqlx::query(
        "UPDATE orders SET paykit_total_sats = NULL, paykit_amount_minor = NULL, \
         paykit_accepted_assets = NULL, paykit_payment_amounts = NULL WHERE id = $1",
    )
    .bind(uuid(&order_id))
    .execute(&pool)
    .await
    .expect("strip the terms");
    let day = |days: i64| Utc::now() + Duration::days(days);
    poll_now(&f.app, day(8)).await;
    poll_now(&f.app, day(9)).await;
    let text = logs.text();
    assert_eq!(
        text.matches("usdt_finality_escalation_not_recorded")
            .count(),
        2,
        "one ALERT per pass, never a silent no-op:\n{text}"
    );
    assert!(
        marketplace_service::paykit_attempts::list_needs_review(&pool)
            .await
            .expect("queue")
            .is_empty()
    );
    assert_eq!(facts(&pool, &order_id).await.state, "paid");
}

// ---------------------------------------------------------------------------
// Money path: concurrent polls of one order
// ---------------------------------------------------------------------------

/// Two polls of one order overlap (the first is held at the status call
/// until the second one's request arrives, with no timing involved) and
/// apply different observations. Whatever the interleaving, the order settles
/// exactly once or goes to review exactly once, never both, never twice.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_polls_settle_or_review_an_order_exactly_once(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    for round in 0..4_i64 {
        let active = active_order(&f).await;
        let order_id = active.order.order_id.clone();
        let usdt_only = usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        );
        let both = status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: Some(usdt_leg(1, true, true, false)),
                paid_asset: Some("USDT"),
            },
        );
        let (first, second) = if round % 2 == 0 {
            (usdt_only.clone(), both.clone())
        } else {
            (both.clone(), usdt_only.clone())
        };
        f.paykit.set_marketplace_status(active.invoice, first);
        f.paykit.rendezvous_marketplace_status(active.invoice, 2);
        let state = f.app.state.clone();
        let started = tokio::spawn(async move {
            let client = state
                .payments
                .as_ref()
                .and_then(|payments| payments.paykit.as_ref())
                .expect("client");
            marketplace_service::workers::verify_due_paykit_payments(
                &state,
                client,
                at(10 * (round + 1)),
            )
            .await
            .expect("first poll")
        });
        // The first poll has claimed the order and its request waits at the
        // barrier holding the first body; only now does the second body go
        // up, and the second poll's request releases both answers together.
        f.paykit
            .wait_for_marketplace_status_requests(active.invoice, 1)
            .await;
        f.paykit.set_marketplace_status(active.invoice, second);
        let second_poll = poll_now(&f.app, at(10 * (round + 1) + 5)).await;
        let first_poll = started.await.expect("first poll joins");
        // Orders settled in earlier rounds are polled too (finality and
        // second-rail watch), so the count only proves something moved.
        assert!(
            first_poll + second_poll >= 1,
            "round {round}: the order moved ({first_poll}, {second_poll})"
        );
        let status_reads = f
            .paykit
            .calls()
            .iter()
            .filter(|call| {
                call.path == "/marketplace/payment-requests/status"
                    && call.body["invoice_id"] == json!(active.invoice)
            })
            .count();
        assert_eq!(status_reads, 2, "round {round}: both polls read the status");

        let stored = facts(&pool, &order_id).await;
        let payment = payment_row(&pool, &order_id).await;
        let receipts = count(
            &pool,
            "SELECT COUNT(*) FROM receipts WHERE order_id = $1",
            &order_id,
        )
        .await;
        let reviews = count(
            &pool,
            "SELECT COUNT(*) FROM events WHERE aggregate_id LIKE 'payment:%' \
             AND command_id = $1 AND kind = 'payment.manual_review'",
            &order_id,
        )
        .await;
        let confirmed = count(
            &pool,
            "SELECT COUNT(*) FROM events e JOIN payments p \
             ON e.aggregate_id = 'payment:' || p.id::text \
             WHERE p.order_id = $1 AND e.kind = 'payment.confirmed'",
            &order_id,
        )
        .await;
        match payment.0.as_str() {
            "confirmed" => {
                assert_eq!(stored.state, "paid", "round {round}");
                assert_eq!(receipts, 1, "round {round}: one receipt");
                assert_eq!((confirmed, reviews), (1, 0), "round {round}");
                assert_eq!(
                    stored.payment_asset.as_deref(),
                    Some("USDT"),
                    "round {round}"
                );
                assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
            }
            "manual_review" => {
                assert_eq!(stored.state, "pending_payment", "round {round}");
                assert_eq!(receipts, 0, "round {round}: nothing settled");
                assert_eq!((confirmed, reviews), (0, 1), "round {round}: one review");
            }
            other => panic!("round {round}: payment ended {other}"),
        }
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:' || $1::text \
                 AND kind = 'payment.finality_changed'",
                &order_id
            )
            .await,
            i64::from(stored.payment_asset.as_deref() == Some("USDT")),
            "round {round}: finality recorded once, only for a USDT rail"
        );
    }
}

// ---------------------------------------------------------------------------
// PayPal is untouched
// ---------------------------------------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn paypal_checkout_and_ipn_settlement_are_untouched_on_the_multi_asset_deployment(
    pool: PgPool,
) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let total = bind_paypal_method(&f.app, &f.seller, &f.buyer, &order.order_id).await;
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["payment_method"], json!("paypal"));
    assert!(view["fiat_checkout_url"].as_str().is_some(), "{view}");
    for key in [
        "accepted_assets",
        "payment_amounts",
        "payment_asset",
        "payment_finality",
    ] {
        assert!(view.get(key).is_none(), "{key} on a PayPal order: {view}");
    }
    assert_eq!(
        f.paykit
            .calls()
            .iter()
            .filter(|call| call.path.starts_with("/marketplace/payment-requests"))
            .count(),
        0,
        "PayPal never reaches Paykit"
    );

    assert_eq!(
        post_completed_ipn(&f.app, &order.order_id, "TXN-PK1-1", total).await,
        StatusCode::OK
    );
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["fiat_verification"], json!("gateway-notified"));
    for key in ["accepted_assets", "payment_asset", "payment_finality"] {
        assert!(
            view.get(key).is_none(),
            "{key} on a paid PayPal order: {view}"
        );
    }
    // The worker never polls a PayPal order, and a PayPal order ships at once.
    assert_eq!(poll_now(&f.app, at(1)).await, 0);
    let stored = facts(&pool, &order.order_id).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order.order_id, stored.revision, 7),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PayPal ships without finality: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_voided_paykit_bind_leaves_nothing_on_a_later_paypal_bind(pool: PgPool) {
    let f = fixture(pool.clone(), Setup::both()).await;
    let order = checkout_usd(&f).await;
    let (status, body) = bind(&f, &order.order_id, "paykit").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    f.paykit.fail_commands_with(409, "invoice_finalized");
    drain(&f).await;
    f.paykit.clear_command_failure();
    assert_eq!(facts(&pool, &order.order_id).await.payment_method, None);

    bind_paypal_method(&f.app, &f.seller, &f.buyer, &order.order_id).await;
    let view = read_order(&f, &f.buyer.token, &order.order_id).await;
    assert_eq!(view["payment_method"], json!("paypal"));
    for key in ["accepted_assets", "payment_amounts", "payment_finality"] {
        assert!(view.get(key).is_none(), "{key} survived the rebind: {view}");
    }
    let leftovers: (
        Option<i64>,
        Option<Vec<String>>,
        Option<Value>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT paykit_amount_minor, paykit_accepted_assets, paykit_payment_amounts, \
             paykit_asset FROM orders WHERE id = $1",
    )
    .bind(uuid(&order.order_id))
    .fetch_one(&pool)
    .await
    .expect("order row");
    assert_eq!(leftovers, (None, None, None, None));
}

// ---------------------------------------------------------------------------
// Seller-confirmed payment (PAYMENT_CONFIRMATION=seller)
// ---------------------------------------------------------------------------

async fn seller_fixture(pool: PgPool) -> Fixture {
    fixture_confirmed_by(pool, Setup::both(), PaymentConfirmation::Seller).await
}

async fn stored_confirmation(pool: &PgPool, order_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT payment_confirmation FROM orders WHERE id = $1")
        .bind(uuid(order_id))
        .fetch_one(pool)
        .await
        .expect("order row")
}

async fn confirm(f: &Fixture, token: &str, order_id: &str) -> (StatusCode, Value) {
    common::paykit_review::confirm_call(&f.app, token, order_id, &json!({})).await
}

async fn payment_confirmed_events(pool: &PgPool, order_id: &str) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM events e JOIN payments p ON e.aggregate_id = 'payment:' || p.id::text \
         WHERE p.order_id = $1 AND e.kind = 'payment.confirmed'",
        order_id,
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn who_marks_the_order_paid_freezes_at_the_bind(pool: PgPool) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    assert_eq!(
        stored_confirmation(&pool, &active.order.order_id)
            .await
            .as_deref(),
        Some("seller")
    );
    let view = read_order(&f, &f.buyer.token, &active.order.order_id).await;
    assert_eq!(view["payment_confirmation"], json!("seller"));
    let (status, health) = send(f.app.router.clone(), "GET", "/health", None, &Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["payment_confirmation"], json!("seller"));

    let g = fixture(pool.clone(), Setup::both()).await;
    let active = active_order(&g).await;
    assert_eq!(
        stored_confirmation(&pool, &active.order.order_id)
            .await
            .as_deref(),
        Some("automatic")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_confirmed_usdt_waits_for_the_seller_while_finality_is_tracked(pool: PgPool) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(
        stored.state, "pending_payment",
        "the transfer alone pays nothing"
    );
    assert_eq!(
        stored.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(stored.payment_asset.as_deref(), Some("USDT"));
    assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
    assert_eq!(
        payment_row(&pool, &order_id).await.0,
        "awaiting_entitlement"
    );
    assert_eq!(payment_confirmed_events(&pool, &order_id).await, 0);
    let seen = delivered_notifications(&f.app, &f.seller.token, "usdt_payment_seen").await;
    assert_eq!(seen.len(), 1, "the seller is told the transfer arrived");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["next_actor"], json!("seller"));

    // Finality moves while the seller decides; the order still waits.
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(220, true, true, true),
        ),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("final"));
    assert_eq!(stored.state, "pending_payment");

    // Only the listing's seller may attest it.
    let (status, _) = confirm(&f, &f.buyer.token, &order_id).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = confirm(&f, &f.seller.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "seller confirms: {body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(payment_confirmed_events(&pool, &order_id).await, 1);
    let receipt: (String, i64, i32) =
        sqlx::query_as("SELECT currency, total_minor, exponent FROM receipts WHERE order_id = $1")
            .bind(uuid(&order_id))
            .fetch_one(&pool)
            .await
            .expect("receipt");
    assert_eq!(
        (receipt.0.as_str(), receipt.1, receipt.2),
        ("USDT", TOTAL_USDT_UNITS as i64, 6),
        "the receipt follows the rail recorded at entry"
    );
    let stored = facts(&pool, &order_id).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship(&order_id, stored.revision, 31),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a final transfer ships: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_confirmed_usdt_transfer_that_disappears_is_marked_and_reverted(pool: PgPool) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(2, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    f.paykit.set_marketplace_status(
        active.invoice,
        nothing_observed(active.invoice, active.deadline),
    );
    poll_now(&f.app, at(2)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("reverted"));
    assert_eq!(
        stored.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation"),
        "the order neither advances nor silently reverts"
    );
    let disappeared: Option<bool> = sqlx::query_scalar(
        "SELECT (paykit_observation->>'disappeared')::boolean FROM orders WHERE id = $1",
    )
    .bind(uuid(&order_id))
    .fetch_one(&pool)
    .await
    .expect("observation");
    assert_eq!(disappeared, Some(true));

    // The transfer comes back: pending again, still waiting for the seller.
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(3, true, true, false),
        ),
    );
    poll_now(&f.app, at(3)).await;
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.payment_finality.as_deref(), Some("pending"));
    assert_eq!(stored.state, "pending_payment");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_confirmed_late_or_short_usdt_keeps_its_review_route(pool: PgPool) {
    for (matched, on_time) in [(false, true), (true, false)] {
        let f = seller_fixture(pool.clone()).await;
        let active = active_order(&f).await;
        let order_id = active.order.order_id.clone();
        f.paykit.set_marketplace_status(
            active.invoice,
            status_body(
                active.invoice,
                active.deadline,
                StatusSpec {
                    payment_state: "confirmed",
                    bitcoin: None,
                    usdt: Some(usdt_leg(1, matched, on_time, false)),
                    paid_asset: None,
                },
            ),
        );
        poll_now(&f.app, at(1)).await;
        let stored = facts(&pool, &order_id).await;
        assert_ne!(
            stored.paykit_request_state.as_deref(),
            Some("awaiting_seller_confirmation"),
            "matched {matched}, on time {on_time}"
        );
        assert_ne!(stored.state, "paid");
        assert_ne!(payment_row(&pool, &order_id).await.0, "confirmed");
    }
    // A matched transfer of another amount than offered is never shown as waiting.
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    let mut leg = usdt_leg(1, true, true, false);
    leg["observed_amount"] = json!((TOTAL_USDT_UNITS + 10_000).to_string());
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(active.invoice, active.deadline, leg),
    );
    poll_now(&f.app, at(1)).await;
    let (state, _, _, _, reason) = payment_row(&pool, &order_id).await;
    assert_eq!(
        (state.as_str(), reason.as_deref()),
        ("manual_review", Some("amount_mismatch"))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn seller_confirmed_bitcoin_through_the_paykit_method_waits_and_records_its_rail(
    pool: PgPool,
) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        status_body(
            active.invoice,
            active.deadline,
            StatusSpec {
                payment_state: "confirmed",
                bitcoin: Some(bitcoin_leg(3, true)),
                usdt: None,
                paid_asset: Some("BTC"),
            },
        ),
    );
    assert_eq!(poll_now(&f.app, at(1)).await, 1);
    let stored = facts(&pool, &order_id).await;
    assert_eq!(stored.state, "pending_payment");
    assert_eq!(
        stored.paykit_request_state.as_deref(),
        Some("awaiting_seller_confirmation")
    );
    assert_eq!(
        stored.payment_asset.as_deref(),
        Some("BTC"),
        "the rail is recorded at entry, so a refund can follow it"
    );
    let seen = delivered_notifications(&f.app, &f.seller.token, "bitcoin_payment_seen").await;
    assert_eq!(seen.len(), 1);
    let (status, body) = confirm(&f, &f.seller.token, &order_id).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["order"]["state"], json!("paid"));
    assert_eq!(payment_confirmed_events(&pool, &order_id).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unconfirmed_usdt_payment_goes_to_review_and_the_seller_resolves_it_paid(pool: PgPool) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    let routed = marketplace_service::bitcoin_review::route_due_seller_confirmation_windows(
        &f.app.state,
        at(1)
            + Duration::seconds(
                marketplace_service::bitcoin_review::SELLER_CONFIRMATION_WINDOW_SECONDS + 1,
            ),
    )
    .await
    .expect("window reaper runs");
    assert_eq!(routed, 1);
    assert_eq!(payment_row(&pool, &order_id).await.0, "manual_review");
    let (status, body) = resolve_call(
        &f.app,
        &f.seller.token,
        &order_id,
        Some(Uuid::new_v4()),
        &json!({ "outcome": "paid" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let view = read_order(&f, &f.buyer.token, &order_id).await;
    assert_eq!(view["state"], json!("paid"));
    assert_eq!(view["payment_asset"], json!("USDT"));
}

// --- interleaving (money path) ----------------------------------------------

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_usdt_confirmations_polls_and_the_window_reaper_pay_exactly_once(pool: PgPool) {
    let f = seller_fixture(pool.clone()).await;
    let active = active_order(&f).await;
    let order_id = active.order.order_id.clone();
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(1, true, true, false),
        ),
    );
    poll_now(&f.app, at(1)).await;
    assert_eq!(
        facts(&pool, &order_id)
            .await
            .paykit_request_state
            .as_deref(),
        Some("awaiting_seller_confirmation")
    );
    f.paykit.set_marketplace_status(
        active.invoice,
        usdt_status(
            active.invoice,
            active.deadline,
            usdt_leg(220, true, true, true),
        ),
    );
    let reaper_at = at(2)
        + Duration::seconds(
            marketplace_service::bitcoin_review::SELLER_CONFIRMATION_WINDOW_SECONDS,
        );
    let (first, second, _polled, reaped) = tokio::join!(
        confirm(&f, &f.seller.token, &order_id),
        confirm(&f, &f.seller.token, &order_id),
        poll_now(&f.app, at(2)),
        marketplace_service::bitcoin_review::route_due_seller_confirmation_windows(
            &f.app.state,
            reaper_at,
        ),
    );
    let reaped = reaped.expect("window reaper runs");
    let answers = [first, second];
    let paid_by_seller = answers
        .iter()
        .filter(|(code, _)| *code == StatusCode::OK)
        .count();
    for (code, body) in &answers {
        if *code != StatusCode::OK {
            assert_eq!(*code, StatusCode::CONFLICT, "{body}");
        }
    }
    let payment = payment_row(&pool, &order_id).await.0;
    if reaped == 1 {
        // The reaper won: the payment waits in review, never paid twice.
        assert_eq!(paid_by_seller, 0, "{answers:?}");
        assert_eq!(payment, "manual_review");
        assert_eq!(payment_confirmed_events(&pool, &order_id).await, 0);
    } else {
        assert!(paid_by_seller >= 1, "{answers:?}");
        assert_eq!(payment, "confirmed");
        assert_eq!(facts(&pool, &order_id).await.state, "paid");
        assert_eq!(payment_confirmed_events(&pool, &order_id).await, 1);
    }
    let receipts = count(
        &pool,
        "SELECT COUNT(*) FROM receipts WHERE order_id = $1",
        &order_id,
    )
    .await;
    assert!(receipts <= 1, "at most one receipt: {receipts}");
}
