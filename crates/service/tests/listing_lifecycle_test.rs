//! Checkout, bids and offers on a listing the seller paused, ended or removed
//! are refused from the seller's current homeserver record.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HomeserverRawFetchOutcome,
};
use marketplace_service::http::build_router;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    bind_paypal_method, checkout_command, checkout_command_with_id, close_auction_command, count,
    counter_offer_command, create_offer_command, create_pending_locks_order, create_pending_order,
    drop_command_mirror_record, execute, new_actor, offer_action, place_bid_command,
    prepare_locks_command, register_auction_command, register_command, send,
    set_command_mirror_record_state, test_app, test_app_with_homeserver, test_app_with_locks,
    test_app_with_payments, CommandMirrorHomeserver, TestActor, TestApp, OFFER_COMMAND_ID,
};
use marketplace_service::clock::Clock;
use marketplace_service::workers::close_due_auctions;

const NOT_ACTIVE_MESSAGE: &str = "The seller is not selling this listing right now.";

/// The command mirror homeserver with an outage switch and a count of the
/// listing-record reads the lifecycle gate makes.
#[derive(Default)]
struct GatedHomeserver {
    unavailable: AtomicBool,
    listing_reads: AtomicUsize,
}

impl GatedHomeserver {
    fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::SeqCst);
    }

    fn listing_reads(&self) -> usize {
        self.listing_reads.load(Ordering::SeqCst)
    }
}

impl HomeserverListingClient for GatedHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.listing_reads.fetch_add(1, Ordering::SeqCst);
            if self.unavailable.load(Ordering::SeqCst) {
                return HomeserverFetchOutcome::Unavailable;
            }
            CommandMirrorHomeserver
                .fetch_listing(seller_pubky, listing_id)
                .await
        })
    }

    fn fetch_listing_raw<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverRawFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            if self.unavailable.load(Ordering::SeqCst) {
                return HomeserverRawFetchOutcome::Unavailable;
            }
            CommandMirrorHomeserver
                .fetch_listing_raw(seller_pubky, listing_id)
                .await
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        seller_pubky: &'a str,
        drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        CommandMirrorHomeserver.fetch_drop(seller_pubky, drop_id)
    }
}

async fn gated_app(pool: PgPool) -> (TestApp, Arc<GatedHomeserver>) {
    let mut app = test_app(pool).await;
    let homeserver = Arc::new(GatedHomeserver::default());
    app.state = app.state.clone().with_homeserver(Some(homeserver.clone()));
    app.router = build_router(app.state.clone());
    (app, homeserver)
}

fn assert_not_active(status: StatusCode, body: &Value, reason: &str) {
    assert_eq!(status, StatusCode::CONFLICT, "expected a refusal: {body}");
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["error"]["code"], json!("LISTING_NOT_ACTIVE"));
    assert_eq!(body["error"]["message"], json!(NOT_ACTIVE_MESSAGE));
    assert_eq!(body["error"]["reason"], json!(reason));
}

fn assert_upstream_unavailable(status: StatusCode, body: &Value) {
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "expected a retriable refusal: {body}"
    );
    assert_eq!(body["error"]["code"], json!("UPSTREAM_UNAVAILABLE"));
}

async fn listing_row(app: &TestApp, seller: &TestActor) -> (String, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT state, available_quantity, reserved_quantity, sold_quantity, server_revision \
         FROM listings WHERE seller_pubky = $1",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("listing row")
}

async fn stored_results(app: &TestApp) -> i64 {
    count(
        &app.pool,
        "SELECT COUNT(*) FROM command_results WHERE actor_pubky <> ''",
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_on_a_listing_that_is_not_active_is_refused_and_writes_nothing(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let before = listing_row(&app, &seller).await;
    let results = stored_results(&app).await;

    for (state, reason) in [
        ("paused", "listing_paused"),
        ("ended", "listing_ended"),
        ("removed", "listing_removed"),
        ("draft", "listing_not_active"),
    ] {
        set_command_mirror_record_state(&seller.pubky, "boots_01", state);
        let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
        assert_not_active(status, &body, reason);
    }
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM payments").await, 0);
    assert_eq!(listing_row(&app, &seller).await, before);
    assert_eq!(
        stored_results(&app).await,
        results,
        "a refusal is never stored"
    );

    // Reactivated, the same command goes through
    set_command_mirror_record_state(&seller.pubky, "boots_01", "active");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed once active: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn checkout_on_a_listing_whose_record_is_gone_is_refused_as_removed(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    drop_command_mirror_record(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_not_active(status, &body, "listing_removed");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_checkout_with_one_paused_line_is_refused_whole(pool: PgPool) {
    let (app, homeserver) = gated_app(pool).await;
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
    set_command_mirror_record_state(&other_seller.pubky, "boots_01", "paused");
    let reads = homeserver.listing_reads();
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_not_active(status, &body, "listing_paused");
    assert_eq!(
        homeserver.listing_reads() - reads,
        2,
        "every line's record is read"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);

    set_command_mirror_record_state(&other_seller.pubky, "boots_01", "active");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed once active: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unreachable_homeserver_fails_checkout_closed(pool: PgPool) {
    let (app, homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let before = listing_row(&app, &seller).await;

    homeserver.set_unavailable(true);
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_upstream_unavailable(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
    assert_eq!(listing_row(&app, &seller).await, before);

    // The same command succeeds once the homeserver answers again
    homeserver.set_unavailable(false);
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed after recovery: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_of_an_accepted_checkout_never_reads_the_record(pool: PgPool) {
    let (app, homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let command = checkout_command(&seller.pubky);
    let (status, first) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "checkout failed: {first}");

    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let reads = homeserver.listing_reads();
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "replay was refused: {replay}");
    assert_eq!(replay, first);
    assert_eq!(homeserver.listing_reads(), reads, "a replay reads nothing");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout_command_with_id(&seller.pubky, "00000000-0000-4000-8000-000000001001"),
    )
    .await;
    assert_not_active(status, &body, "listing_paused");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn bids_on_a_paused_auction_are_refused_and_closing_stays_open(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "auction register failed: {body}");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 5_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid failed while active: {body}");

    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 2, 6_000, 2),
    )
    .await;
    assert_not_active(status, &body, "listing_paused");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM bids").await, 1);

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
async fn offers_counters_and_acceptance_on_a_paused_listing_are_refused(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;

    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_not_active(status, &body, "listing_paused");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM offers").await, 0);

    set_command_mirror_record_state(&seller.pubky, "boots_01", "active");
    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");

    // Paused mid-negotiation: nobody can counter or accept
    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let (status, body) = execute(&app, &seller.token, &counter_offer_command(1)).await;
    assert_not_active(status, &body, "listing_paused");
    let (status, body) = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_not_active(status, &body, "listing_paused");
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM offers WHERE state = 'pending'"
        )
        .await,
        1
    );

    // Winding the negotiation down commits to nothing and stays open
    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.withdraw", 1, "00000000-0000-4000-8000-000000001102"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "withdraw failed: {body}");
}

/// Registers a listing, makes and accepts an offer on it, and returns the
/// buyer's `offer.checkout` command for the accepted award.
async fn accepted_offer_checkout(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> Value {
    // timestamptz stores microseconds; Linux Utc::now() often has leftover nanos.
    app.clock.set(
        chrono::DateTime::from_timestamp_micros(chrono::Utc::now().timestamp_micros())
            .expect("microsecond timestamp"),
    );
    execute(app, &seller.token, &register_command(&seller.pubky, 2)).await;
    let (status, body) = execute(app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "offer create failed: {body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "offer accept failed: {body}");
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    json!({
        "version": 1,
        "command_id": "00000000-0000-4000-8000-000000001103",
        "aggregate_id": format!("offer:{OFFER_COMMAND_ID}"),
        "expected_revision": 2,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "offer.checkout",
        "payload": {
            "offer_id": OFFER_COMMAND_ID,
            "award_id": row.0.to_string(),
            "listing_aggregate_id": row.1,
            "listing_revision": row.2,
            "listing_record_sha256": row.3,
            "variant_id": row.4,
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
async fn an_offer_accepted_before_the_pause_still_checks_out(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let command = accepted_offer_checkout(&app, &seller, &buyer).await;

    // The seller pauses to stop new buyers while the accepted buyer pays
    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "offer checkout was refused: {body}");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_accepted_offer_on_a_removed_listing_cannot_check_out(pool: PgPool) {
    let (app, homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let command = accepted_offer_checkout(&app, &seller, &buyer).await;

    set_command_mirror_record_state(&seller.pubky, "boots_01", "removed");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_not_active(status, &body, "listing_removed");
    drop_command_mirror_record(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_not_active(status, &body, "listing_removed");
    homeserver.set_unavailable(true);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_upstream_unavailable(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn locks_preparation_passes_a_pause_and_refuses_a_removal(pool: PgPool) {
    let (app, _locks) = test_app_with_locks(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paused = create_pending_locks_order(&app, &seller, &buyer).await;
    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    let (status, body) = execute(
        &app,
        &buyer.token,
        &prepare_locks_command(&paused.payment_id, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation after a pause: {body}");

    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let removed = create_pending_locks_order(&app, &other_seller, &other_buyer).await;
    set_command_mirror_record_state(&other_seller.pubky, "boots_01", "removed");
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &prepare_locks_command(&removed.payment_id, 2),
    )
    .await;
    assert_not_active(status, &body, "listing_removed");
    let adapter: String = sqlx::query_scalar("SELECT adapter FROM payments WHERE id = $1")
        .bind(uuid::Uuid::parse_str(&removed.payment_id).expect("payment id"))
        .fetch_one(&app.pool)
        .await
        .expect("payment row");
    assert_ne!(adapter, "locks", "a refused preparation pins nothing");

    set_command_mirror_record_state(&other_seller.pubky, "boots_01", "active");
    let (status, body) = execute(
        &app,
        &other_buyer.token,
        &prepare_locks_command(&removed.payment_id, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preparation once active: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_payment_method_bind_passes_a_pause_and_refuses_a_removal(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paused = create_pending_order(&app, &seller, &buyer).await;
    set_command_mirror_record_state(&seller.pubky, "boots_01", "paused");
    bind_paypal_method(&app, &seller, &buyer, &paused.order_id).await;

    let other_seller = new_actor(&app).await;
    let other_buyer = new_actor(&app).await;
    let removed = create_pending_order(&app, &other_seller, &other_buyer).await;
    let (status, body) = send(
        app.router.clone(),
        "PUT",
        "/v0/sellers/me/payment-config",
        Some(&other_seller.token),
        &json!({ "bitcoin_enabled": false, "paypal_merchant_email": "merchant@example.com" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment config failed: {body}");
    set_command_mirror_record_state(&other_seller.pubky, "boots_01", "removed");
    let (status, body) = send(
        app.router.clone(),
        "POST",
        &format!("/v0/orders/{}/payment-method", removed.order_id),
        Some(&other_buyer.token),
        &json!({ "method": "paypal" }),
    )
    .await;
    assert_not_active(status, &body, "listing_removed");
    let method: Option<String> =
        sqlx::query_scalar("SELECT payment_method FROM orders WHERE id = $1")
            .bind(uuid::Uuid::parse_str(&removed.order_id).expect("order id"))
            .fetch_one(&app.pool)
            .await
            .expect("order row");
    assert_eq!(method, None, "a refused bind binds nothing");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_removed_auction_refuses_the_close_command_and_the_worker_closes_it_unsold(pool: PgPool) {
    let (app, _homeserver) = gated_app(pool).await;
    let seller = new_actor(&app).await;
    let bidder = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_auction_command(&seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "auction register failed: {body}");
    let (status, body) = execute(
        &app,
        &bidder.token,
        &place_bid_command(&seller.pubky, 1, 7_000, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "bid failed while active: {body}");

    set_command_mirror_record_state(&seller.pubky, "boots_01", "removed");
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
    assert_not_active(status, &body, "listing_removed");

    let summary = close_due_auctions(&app.pool, app.state.homeserver.as_deref(), app.clock.now())
        .await
        .expect("close sweep");
    assert_eq!(summary.closed, 1);
    let status: String =
        sqlx::query_scalar("SELECT auction->>'status' FROM listings WHERE seller_pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("auction row");
    assert_eq!(status, "unsold", "the leader never becomes a buyer");
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);
}

/// The production HTTP client against a local homeserver: the record's
/// `state` is read from the wire, and a 500 fails closed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_record_state_is_read_over_http(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "register failed: {body}");
    let record = |state: &str| {
        json!({
            "title": "Boots",
            "revision": 1,
            "state": state,
            "variants": [{ "id": "variant_1", "enabled": true, "quantity": 2 }],
            "sale": {
                "format": "fixed_price",
                "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 }
            }
        })
    };

    homeserver.put_record(&seller.pubky, "boots_01", record("paused"));
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_not_active(status, &body, "listing_paused");

    homeserver.put_record(&seller.pubky, "boots_01", record("active"));
    homeserver.fail_record(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_upstream_unavailable(status, &body);
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 0);

    homeserver.heal_record(&seller.pubky, "boots_01");
    let (status, body) = execute(&app, &buyer.token, &checkout_command(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "checkout failed once active: {body}"
    );
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM orders").await, 1);
}
