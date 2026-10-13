//! Checkout, bids and offers on a moderated listing are refused from the
//! index's moderation state (legal task 12).

mod common;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use marketplace_service::http::build_router;
use marketplace_service::listing_moderation::{
    HttpListingModerationClient, ListingModerationClient, ListingModerationGate, ModerationLookup,
};
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    captured_logs, checkout_command, checkout_command_with_id, close_auction_command, count,
    counter_offer_command, create_offer_command, execute, install_log_capture, new_actor,
    offer_action, place_bid_command, register_auction_command, register_command, test_app,
    TestActor, TestApp, OFFER_COMMAND_ID,
};

const MODERATED_MESSAGE: &str =
    "This listing was removed by moderation and cannot be bought, bid on or offered on.";

/// The index double: a set of moderated `(seller, listing_id)` pairs, an
/// outage switch, and a count of lookups.
#[derive(Default)]
struct FakeIndex {
    moderated: Mutex<HashSet<(String, String)>>,
    unavailable: AtomicBool,
    lookups: AtomicUsize,
}

impl FakeIndex {
    fn moderate(&self, seller_pubky: &str, listing_id: &str) {
        self.moderated
            .lock()
            .unwrap()
            .insert((seller_pubky.to_string(), listing_id.to_string()));
    }

    fn lift(&self, seller_pubky: &str, listing_id: &str) {
        self.moderated
            .lock()
            .unwrap()
            .remove(&(seller_pubky.to_string(), listing_id.to_string()));
    }

    fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }
}

impl ListingModerationClient for FakeIndex {
    fn lookup<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ModerationLookup> + Send + 'a>> {
        Box::pin(async move {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                return ModerationLookup::Unavailable;
            }
            let moderated = self
                .moderated
                .lock()
                .unwrap()
                .contains(&(seller_pubky.to_string(), listing_id.to_string()));
            if moderated {
                ModerationLookup::Moderated
            } else {
                ModerationLookup::Clear
            }
        })
    }
}

/// A test app whose commands pass the moderation gate over `index`. The
/// moderated answer is not cached, so the double is asked every time.
async fn moderated_app(pool: PgPool) -> (TestApp, Arc<FakeIndex>) {
    let mut app = test_app(pool).await;
    let index = Arc::new(FakeIndex::default());
    let gate = ListingModerationGate::with_moderated_ttl(index.clone(), Duration::ZERO);
    app.state = app
        .state
        .clone()
        .with_listing_moderation(Some(Arc::new(gate)));
    app.router = build_router(app.state.clone());
    (app, index)
}

fn assert_moderated_refusal(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::CONFLICT, "expected a refusal: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("LISTING_MODERATED"));
    assert_eq!(body["error"]["message"], json!(MODERATED_MESSAGE));
}

async fn listing_row(app: &TestApp, seller: &TestActor) -> (String, i64, i64, i64) {
    sqlx::query_as(
        "SELECT state, available_quantity, reserved_quantity, sold_quantity \
         FROM listings WHERE seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("listing row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_on_a_moderated_listing_is_refused_and_writes_nothing(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let before = listing_row(&app, &seller).await;

    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 0);
    assert_eq!(listing_row(&app, &seller).await, before);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM command_results WHERE actor_pubky <> ''"
        )
        .await,
        1,
        "only the seller's register command was stored; the refusal is not"
    );

    // Lifted, the same buyer's next attempt goes through
    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed after the lift: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_checkout_with_one_moderated_line_is_refused_whole(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let other_seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    execute(
        &app,
        &other_seller.token,
        &register_command(&other_seller.pubky, 1),
    )
    .await;

    let mut command = checkout_command(&seller.pubky);
    command["payload"]["lines"]
        .as_array_mut()
        .expect("lines")
        .push(json!({
            "listing_aggregate_id": common::listing_aggregate(&other_seller.pubky),
            "expected_revision": 1,
            "quantity": 1,
        }));
    index.moderate(&other_seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bids_on_a_moderated_auction_are_refused(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "auction register failed: {body}");

    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 5_000, 1),
    )
    .await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 0);

    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 5_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid failed after the lift: {body}");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 1);

    // Closing an auction sells nothing and stays open
    index.moderate(&seller.pubky, "boots_01");
    app.clock.set(
        "2026-08-19T22:11:00Z"
            .parse()
            .expect("an instant after the auction ends"),
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &close_auction_command(&seller.pubky, 2, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "close failed: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_counters_and_acceptance_on_a_moderated_listing_are_refused(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");

    // Moderated mid-negotiation: the seller can neither counter nor accept
    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &seller.token, &counter_offer_command(1)).await;
    assert_moderated_refusal(status, &body);
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_moderated_refusal(status, &body);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM offers WHERE state = 'pending'"
        )
        .await,
        1
    );

    // Winding the negotiation down sells nothing and stays open
    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.withdraw", 1, "00000000-0000-4000-8000-000000001102"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "withdraw failed: {body}");
}

fn offer_checkout_command(
    offer_id: &str,
    award_id: &str,
    listing: &str,
    revision: i64,
    hash: &str,
) -> Value {
    json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000001103",
        "aggregate_id": format!("offer:{offer_id}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": offer_id,
            "award_id": award_id,
            "listing_aggregate_id": listing,
            "listing_revision": revision,
            "listing_record_sha256": hash,
            "variant_id": "boots_01",
            "quantity": 1,
            "delivery_address": {
                "name": "Alice Buyer",
                "line1": "1 Market Street",
                "line2": "",
                "city": "New York",
                "region": "NY",
                "postal_code": "10001",
                "country_code": "US"
            },
            "guarantee_policy_version": 1
        }
    })
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_of_an_accepted_offer_on_a_moderated_listing_is_refused(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    // timestamptz stores microseconds; Linux Utc::now() often has leftover nanos.
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    sqlx::query(
        "UPDATE offers SET accepted_listing_record_sha256 = $2, accepted_variant_id = $3 \
         WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .bind("a".repeat(64))
    .bind("boots_01")
    .execute(&app.pool)
    .await
    .expect("normalize accepted snapshot fixture");
    let row: (uuid::Uuid, uuid::Uuid, String, i64, String) = sqlx::query_as(
        "SELECT award_id, id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256 FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    let command = offer_checkout_command(
        &row.1.to_string(),
        &row.0.to_string(),
        &row.2,
        row.3,
        &row.4,
    );

    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);

    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "offer checkout failed after the lift: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_of_an_accepted_checkout_is_answered_from_its_stored_result(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let command = checkout_command(&seller.pubky);
    let (status, first) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {first}");

    index.moderate(&seller.pubky, "boots_01");
    let lookups = index.lookups();
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "replay was refused: {replay}");
    assert_eq!(replay, first);
    assert_eq!(index.lookups(), lookups, "a replay never asks the index");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);

    // A new checkout command on the same listing is refused
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001"),
    )
    .await;
    assert_moderated_refusal(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unmoderated_listing_is_checked_on_every_command_and_never_cached_as_clear(
    pool: PgPool,
) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;

    for number in 0..2 {
        let (status, body) = execute(
            &app,
            &buyer.token,
            &checkout_command_with_id(
                &seller.pubky,
                &format!("00000000-0000-4000-8000-00000000200{number}"),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "checkout {number} failed: {body}");
    }
    assert_eq!(index.lookups(), 2);

    // A moderation placed between two commands is seen by the second
    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000002002"),
    )
    .await;
    assert_moderated_refusal(status, &body);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_index_outage_fails_open_with_an_alert_but_keeps_a_known_moderated_listing_refused(
    pool: PgPool,
) {
    install_log_capture();
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;

    // The gate has never heard of the listing and the index is down: the
    // checkout goes through and the outage is alerted
    index.set_unavailable(true);
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed during the outage: {body}"
    );
    assert!(
        captured_logs().contains("ALERT the index moderation lookup is unavailable"),
        "the outage must be alerted"
    );

    // The index says moderated, then goes down: the listing stays refused
    index.set_unavailable(false);
    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000003001"),
    )
    .await;
    assert_moderated_refusal(status, &body);
    index.set_unavailable(true);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000003002"),
    )
    .await;
    assert_moderated_refusal(status, &body);
    assert!(
        captured_logs().contains("ALERT the index could not confirm a moderated listing"),
        "the stale refusal must be alerted"
    );

    // The index recovers and says clear: sellable again
    index.set_unavailable(false);
    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000003003"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed after recovery: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn commands_that_sell_nothing_never_ask_the_index(pool: PgPool) {
    let (app, index) = moderated_app(pool).await;
    let seller = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(index.lookups(), 0, "registering a listing is not a sale");
}

/// A payments-enabled app whose bind passes the moderation gate over `index`.
async fn moderated_payments_app(pool: PgPool) -> (TestApp, Arc<FakeIndex>, common::FakePaykit) {
    let (mut app, paykit) = common::test_app_with_payments(pool).await;
    let index = Arc::new(FakeIndex::default());
    let gate = ListingModerationGate::with_moderated_ttl(index.clone(), Duration::ZERO);
    app.state = app
        .state
        .clone()
        .with_listing_moderation(Some(Arc::new(gate)));
    app.router = build_router(app.state.clone());
    (app, index, paykit)
}

async fn bind(app: &TestApp, token: &str, order_id: &str, method: &str) -> (StatusCode, Value) {
    common::send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{order_id}/payment-method"),
        Some(token),
        &json!({ "method": method }),
    )
    .await
}

fn assert_bind_moderated_refusal(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::CONFLICT, "expected a refusal: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("LISTING_MODERATED"));
    assert_eq!(body["error"]["reason"], json!("listing_moderated"));
    assert_eq!(body["error"]["message"], json!(MODERATED_MESSAGE));
}

/// Registers a SAT-priced listing, enables Bitcoin and PayPal for the seller
/// and checks out while the listing is clear, leaving a pending order.
async fn pending_sat_order(
    app: &TestApp,
    paykit: &common::FakePaykit,
    seller: &TestActor,
    buyer: &TestActor,
) -> common::PendingOrder {
    let (status, body) = common::send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&seller.token),
        &json!({ "bitcoin_enabled": true, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment config failed: {body}");
    paykit.set_claimed(&seller.pubky);
    let (status, body) = execute(
        app,
        &seller.token,
        &common::register_sat_command(&seller.pubky, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    let (status, body) = execute(app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "checkout fixture failed: {body}");
    common::PendingOrder {
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

async fn order_is_unbound_and_unheld(app: &TestApp, order_id: &str) {
    let (method, attempt, hold): (Option<String>, i32, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as(
            "SELECT payment_method, paykit_bind_attempt, hold_expires_at FROM orders \
             WHERE id = $1::uuid",
        )
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order row");
    assert_eq!(method, None, "the refused bind left a method on the order");
    assert_eq!(attempt, 0, "the refused bind spent a Paykit attempt");
    assert_eq!(hold, None, "the refused bind armed a hold");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_moderation_landing_between_checkout_and_bind_refuses_the_bind(pool: PgPool) {
    let (app, index, paykit) = moderated_payments_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = pending_sat_order(&app, &paykit, &seller, &buyer).await;
    assert_eq!(index.lookups(), 1, "only the checkout asked so far");

    // The race: the listing was clear when the order was created and is
    // moderated before the buyer chooses how to pay.
    index.moderate(&seller.pubky, "boots_01");
    for method in ["bitcoin", "paypal"] {
        let (status, body) = bind(&app, &buyer.token, &order.order_id, method).await;
        assert_bind_moderated_refusal(status, &body);
    }
    order_is_unbound_and_unheld(&app, &order.order_id).await;
    assert!(paykit.requests().is_empty(), "no Paykit request was made");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'paykit.activate'"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM orders WHERE state = 'pending_payment'"
        )
        .await,
        1,
        "the order itself is not cancelled by the refusal"
    );

    // Lifted, the same order binds
    index.lift(&seller.pubky, "boots_01");
    let (status, body) = bind(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bind failed after the lift: {body}");
    assert_eq!(body["order"]["payment_method"], json!("bitcoin"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_rebind_of_the_bound_method_and_other_actors_never_ask_the_index(pool: PgPool) {
    let (app, index, paykit) = moderated_payments_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order = pending_sat_order(&app, &paykit, &seller, &buyer).await;

    // A stranger is told what they are told without a moderation lookup
    let lookups = index.lookups();
    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = bind(&app, &stranger.token, &order.order_id, "paypal").await;
    assert_ne!(
        body["error"]["code"],
        json!("LISTING_MODERATED"),
        "a non-buyer must not learn the moderation state: {body}"
    );
    assert_ne!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(index.lookups(), lookups);

    // Once bound, moderation does not strand the buyer's own re-read
    index.lift(&seller.pubky, "boots_01");
    let (status, body) = bind(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "bind failed: {body}");
    index.moderate(&seller.pubky, "boots_01");
    let lookups = index.lookups();
    let (status, body) = bind(&app, &buyer.token, &order.order_id, "bitcoin").await;
    assert_eq!(status, StatusCode::OK, "re-bind was refused: {body}");
    assert_eq!(index.lookups(), lookups, "a re-bind never asks the index");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_bind_during_an_index_outage_fails_open_with_an_alert_unless_known_moderated(
    pool: PgPool,
) {
    install_log_capture();
    let (app, index, paykit) = moderated_payments_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = pending_sat_order(&app, &paykit, &seller, &buyer).await;

    // Known moderated, then the index goes down: still refused, alerted
    index.moderate(&seller.pubky, "boots_01");
    let (status, body) = bind(&app, &buyer.token, &order.order_id, "paypal").await;
    assert_bind_moderated_refusal(status, &body);
    index.set_unavailable(true);
    let (status, body) = bind(&app, &buyer.token, &order.order_id, "paypal").await;
    assert_bind_moderated_refusal(status, &body);
    assert!(
        captured_logs().contains("ALERT the index could not confirm a moderated listing"),
        "the stale refusal must be alerted"
    );
    order_is_unbound_and_unheld(&app, &order.order_id).await;

    // Never seen moderated and the index is down: the bind goes through
    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let other = pending_sat_order(&app, &paykit, &other_seller, &other_buyer).await;
    let (status, body) = bind(&app, &other_buyer.token, &other.order_id, "bitcoin").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "bind failed during the outage: {body}"
    );
    assert!(
        captured_logs().contains("ALERT the index moderation lookup is unavailable"),
        "the outage must be alerted"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn locks_payment_preparation_on_a_moderated_listing_is_refused(pool: PgPool) {
    let (mut app, _fake) = common::test_app_with_locks(pool).await;
    let index = Arc::new(FakeIndex::default());
    let gate = ListingModerationGate::with_moderated_ttl(index.clone(), Duration::ZERO);
    app.state = app
        .state
        .clone()
        .with_listing_moderation(Some(Arc::new(gate)));
    app.router = build_router(app.state.clone());
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = common::create_pending_order(&app, &seller, &buyer).await;

    index.moderate(&seller.pubky, "boots_01");
    let command = common::prepare_locks_command(&order.payment_id, 120);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_moderated_refusal(status, &body);
    let adapter: String = sqlx::query_scalar("SELECT adapter FROM payments WHERE id = $1::uuid")
        .bind(&order.payment_id)
        .fetch_one(&app.pool)
        .await
        .expect("payment row");
    assert_ne!(adapter, "locks", "the refused preparation pinned Locks");

    // Lifted, the preparation goes through; moderated again afterwards, the
    // payment already in flight is not stranded by a later registration.
    index.lift(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "preparation failed: {body}");
    index.moderate(&seller.pubky, "boots_01");
    let lookups = index.lookups();
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::prepare_locks_command(&order.payment_id, 121),
    )
    .await;
    assert_ne!(
        body["error"]["code"],
        json!("LISTING_MODERATED"),
        "a payment already pinned to Locks is not gated again: {status} {body}"
    );
    assert_eq!(index.lookups(), lookups);
}

/// The production client against a local index: the path, the `moderated`
/// field, and every answer that is not a usable verdict.
#[tokio::test]
async fn the_http_client_reads_the_index_moderation_route() {
    let router = Router::new().route(
        "/v0/listing/{seller_id}/{listing_id}/moderation",
        get(
            |Path((seller_id, listing_id)): Path<(String, String)>| async move {
                let (status, body) = match (seller_id.as_str(), listing_id.as_str()) {
                    ("seller_clear", "boots") => (
                        StatusCode::OK,
                        json!({
                            "moderated": false,
                            "listing_moderated": false,
                            "seller_moderated": false,
                        }),
                    ),
                    ("seller_listing", "boots") => (
                        StatusCode::OK,
                        json!({
                            "moderated": true,
                            "listing_moderated": true,
                            "seller_moderated": false,
                        }),
                    ),
                    ("seller_user", _) => (
                        StatusCode::OK,
                        json!({
                            "moderated": true,
                            "listing_moderated": false,
                            "seller_moderated": true,
                        }),
                    ),
                    ("seller_broken", _) => (StatusCode::OK, json!({ "unexpected": true })),
                    _ => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        json!({ "error": "internal" }),
                    ),
                };
                (status, Json(body))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the index double");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("index double serves");
    });
    let client = HttpListingModerationClient::new(&format!("http://{address}")).expect("client");

    assert_eq!(
        client.lookup("seller_clear", "boots").await,
        ModerationLookup::Clear
    );
    assert_eq!(
        client.lookup("seller_listing", "boots").await,
        ModerationLookup::Moderated
    );
    assert_eq!(
        client.lookup("seller_user", "hat").await,
        ModerationLookup::Moderated
    );
    assert_eq!(
        client.lookup("seller_broken", "boots").await,
        ModerationLookup::Unavailable,
        "a body without the moderated field is no verdict"
    );
    assert_eq!(
        client.lookup("seller_other", "boots").await,
        ModerationLookup::Unavailable,
        "a 500 is no verdict"
    );

    let unreachable = HttpListingModerationClient::new("http://127.0.0.1:1").expect("client");
    assert_eq!(
        unreachable.lookup("seller_clear", "boots").await,
        ModerationLookup::Unavailable
    );
}

/// An index that predates the moderation route answers 404: that is no
/// verdict, never "clear" by default, so it is alerted like any outage.
#[tokio::test]
async fn an_index_without_the_moderation_route_is_unavailable_not_clear() {
    let router = Router::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the index double");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("index double serves");
    });
    let client = HttpListingModerationClient::new(&format!("http://{address}")).expect("client");
    assert_eq!(
        client.lookup("seller", "boots").await,
        ModerationLookup::Unavailable
    );
}
