//! The unlimited digital stock cap (digital delivery design §2 "Stock"):
//! Shop stores Unlimited as a quantity of 1,000,000, so a listing that ships
//! or offers pickup cannot register it, on any path.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use common::*;
use marketplace_domain::commands::{UNLIMITED_STOCK_ON_PHYSICAL_LISTING, UNLIMITED_STOCK_QUANTITY};
use marketplace_service::config::Config;
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};
use serde_json::{json, Value};
use sqlx::PgPool;

fn register(seller: &str, listing_id: &str, methods: Value, quantity: i64, index: u64) -> Value {
    let mut command = register_command(seller, quantity);
    command["command_id"] = json!(indexed_command_id(0xca90, index));
    command["aggregate_id"] = json!(format!("listing:{seller}_{listing_id}"));
    command["payload"]["listing_id"] = json!(listing_id);
    command["payload"]["fulfillment_methods"] = methods;
    command
}

fn assert_cap_refused(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
    assert_eq!(
        body["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );
}

fn assert_not_digital_only_on_register(status: StatusCode, body: &Value) {
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"), "{body}");
    assert_eq!(
        body["error"]["issues"][0]["path"],
        json!("payload.fulfillment_methods"),
        "{body}"
    );
    assert!(
        body["error"]["issues"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("digital-only")),
        "{body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_refuses_the_unlimited_cap_on_a_listing_that_ships_or_offers_pickup(pool: PgPool) {
    let (app, _server) =
        test_app_with_digital(pool, Some(test_digital_keys()), Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let physical = [
        json!(["shipping"]),
        json!(["pickup"]),
        json!(["shipping", "pickup"]),
        json!(["shipping", "digital"]),
        json!(["pickup", "digital"]),
    ];
    for (index, methods) in physical.into_iter().enumerate() {
        let listing_id = format!("cap_{index}");
        let command = register(
            &seller.pubky,
            &listing_id,
            methods.clone(),
            UNLIMITED_STOCK_QUANTITY,
            index as u64,
        );
        let (status, body) = execute(&app, &seller.token, &command).await;
        assert_cap_refused(status, &body);
        let finite = register(
            &seller.pubky,
            &format!("finite_{index}"),
            methods,
            UNLIMITED_STOCK_QUANTITY - 1,
            100 + index as u64,
        );
        let (status, body) = execute(&app, &seller.token, &finite).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    // Digital-only is how Unlimited is stored.
    let digital = register(
        &seller.pubky,
        "guide_01",
        json!(["digital"]),
        UNLIMITED_STOCK_QUANTITY,
        200,
    );
    let (status, body) = execute(&app, &seller.token, &digital).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_and_sync_many_refuse_a_physical_record_at_the_unlimited_cap(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    // The harness mirrors each register command into the seller's record, so
    // the refused register still leaves a physical record at the cap on the
    // homeserver, as Inventory Studio or another client could publish it.
    let command = register(
        &seller.pubky,
        "boots_09",
        json!(["shipping"]),
        UNLIMITED_STOCK_QUANTITY,
        300,
    );
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_cap_refused(status, &body);

    let sync = json!({
        "version": 1,
        "command_id": indexed_command_id(0xca91, 1),
        "aggregate_id": format!("listing:{}_boots_09", seller.pubky),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "listing.sync",
        "payload": { "seller_pubky": seller.pubky, "listing_id": "boots_09" }
    });
    let (status, body) = execute(&app, &seller.token, &sync).await;
    assert_cap_refused(status, &body);

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "boots_09"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    let item = &body["results"][0];
    assert_eq!(item["status"], json!(409), "{body}");
    assert_eq!(
        item["result"]["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );

    let (status, body) = send(
        app.router.clone(),
        "GET",
        &format!("/v1/listings/listing:{}_boots_09", seller.pubky),
        Some(&seller.token),
        &Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "nothing was registered: {body}"
    );
}

/// A Locks listing record at the cap, publishing `published`.
fn locks_record(seller: &str, listing_id: &str, published: Value, lock: &Value) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "ownerPubky": seller,
        "listingId": listing_id,
        "title": "Premium archive",
        "revision": 1,
        "media": [{"contentHash": "a".repeat(64)}],
        "variants": [{
            "id": "variant_1",
            "enabled": true,
            "quantity": UNLIMITED_STOCK_QUANTITY,
            "sku": null,
            "options": []
        }],
        "shippingOptions": [{"pricing": "free"}],
        "fulfillmentMethods": published,
        "sale": {
            "format": "fixed_price",
            "unitPrice": {"amountMinor": 12_500, "currency": "USD", "exponent": 2}
        },
        "digitalLock": lock
    })
}

fn locks_lock(seller: &str) -> Value {
    json!({"policyUri": lock_resource_for(seller), "criterionId": "paykit"})
}

async fn listing_methods(app: &TestApp, seller: &str, listing_id: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
        .bind(format!("listing:{seller}_{listing_id}"))
        .fetch_one(&app.pool)
        .await
        .expect("listing row")
}

fn locks_register(
    seller: &str,
    listing_id: &str,
    methods: Value,
    lock: &Value,
    index: u64,
) -> Value {
    let mut command = register(seller, listing_id, methods, UNLIMITED_STOCK_QUANTITY, index);
    command["payload"]["digital_lock"] = lock.clone();
    command
}

async fn send_command(app: &TestApp, token: &str, command: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/commands",
        Some(token),
        command,
    )
    .await
}

// A Locks listing is a digital listing: at the cap it registers like any
// other digital listing, and a Locks listing that also ships or offers pickup
// is refused as not digital-only before the cap is ever consulted.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_accepts_a_digital_locks_listing_at_the_cap_and_refuses_a_physical_one(
    pool: PgPool,
) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);

    let digital = locks_register(
        &seller.pubky,
        "locks_digital",
        json!(["digital"]),
        &lock,
        400,
    );
    let (status, body) = execute(&app, &seller.token, &digital).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing_methods(&app, &seller.pubky, "locks_digital").await,
        vec!["digital"]
    );

    for (index, methods) in [
        json!(["shipping"]),
        json!(["pickup"]),
        json!(["shipping", "digital"]),
    ]
    .into_iter()
    .enumerate()
    {
        let command = locks_register(
            &seller.pubky,
            &format!("locks_physical_{index}"),
            methods,
            &lock,
            410 + index as u64,
        );
        let (status, body) = execute(&app, &seller.token, &command).await;
        assert_not_digital_only_on_register(status, &body);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_registers_a_digital_locks_record_as_digital_and_refuses_a_physical_one(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let lock = locks_lock(&seller.pubky);
    let sync = |listing_id: &str, index: u64| {
        json!({
            "version": 1,
            "command_id": indexed_command_id(0xca92, index),
            "aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "listing.sync",
            "payload": { "seller_pubky": seller.pubky, "listing_id": listing_id }
        })
    };

    put_command_mirror_record(
        &seller.pubky,
        "reveal_digital",
        locks_record(&seller.pubky, "reveal_digital", json!(["digital"]), &lock),
    );
    let (status, body) = execute(&app, &seller.token, &sync("reveal_digital", 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        listing_methods(&app, &seller.pubky, "reveal_digital").await,
        vec!["digital"]
    );

    for (index, (listing_id, published)) in [
        ("reveal_ship", json!(["physical", "shipping", "digital"])),
        ("reveal_pickup", json!(["pickup", "digital"])),
    ]
    .into_iter()
    .enumerate()
    {
        put_command_mirror_record(
            &seller.pubky,
            listing_id,
            locks_record(&seller.pubky, listing_id, published, &lock),
        );
        let (status, body) =
            execute(&app, &seller.token, &sync(listing_id, 2 + index as u64)).await;
        assert_cap_refused(status, &body);
    }

    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&seller.token),
        &json!({"listings": [{"seller_pubky": seller.pubky, "listing_id": "reveal_ship"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(body["results"][0]["status"], json!(409), "{body}");
    assert_eq!(
        body["results"][0]["result"]["error"]["reason"],
        json!(UNLIMITED_STOCK_ON_PHYSICAL_LISTING),
        "{body}"
    );
}

/// The public auction record a `register_auction_command` for `listing_id`
/// derives, so the command passes the auction's public-authority check.
fn auction_record(seller: &str, listing_id: &str) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "ownerPubky": seller,
        "listingId": listing_id,
        "title": "Marketplace item",
        "revision": 1,
        "media": [{"contentHash": "a".repeat(64)}],
        "variants": [{
            "id": "variant_1",
            "enabled": true,
            "quantity": 1,
            "sku": null,
            "options": []
        }],
        "shippingOptions": [{
            "pricing": "flat",
            "price": {"amountMinor": 1_200, "currency": "USD", "exponent": 2}
        }],
        "fulfillmentMethods": ["shipping"],
        "sale": {
            "format": "auction",
            "startingPrice": {"amountMinor": 4_500, "currency": "USD", "exponent": 2},
            "startsAt": "2026-08-19T22:00:00.000Z",
            "endsAt": "2026-08-19T22:10:00.000Z",
            "minimumIncrement": {"amountMinor": 500, "currency": "USD", "exponent": 2},
            "antiSnipingWindowSeconds": 60,
            "antiSnipingExtensionSeconds": 120
        }
    })
}

fn auction_register(seller: &str, listing_id: &str, index: u64) -> Value {
    let mut command = register_auction_command(seller);
    command["command_id"] = json!(indexed_command_id(0xca93, index));
    command["aggregate_id"] = json!(format!("listing:{seller}_{listing_id}"));
    command["payload"]["listing_id"] = json!(listing_id);
    command
}

/// A homeserver double that counts listing fetches and, during each one,
/// tries the executor's advisory lock for the command under test from a
/// separate connection. With `wait_for_peer` set, a fetch stays open for up
/// to a second or until another fetch starts, so a concurrent duplicate that
/// reads the record too is seen while the first read is still in flight.
struct ProbeHomeserver {
    pool: PgPool,
    record: Mutex<Option<Value>>,
    lock_key: Mutex<Option<String>>,
    fetches: AtomicUsize,
    lock_free_during_fetch: Mutex<Vec<bool>>,
    wait_for_peer: AtomicBool,
}

impl ProbeHomeserver {
    fn new(pool: PgPool) -> Self {
        Self {
            pool,
            record: Mutex::new(None),
            lock_key: Mutex::new(None),
            fetches: AtomicUsize::new(0),
            lock_free_during_fetch: Mutex::new(Vec::new()),
            wait_for_peer: AtomicBool::new(false),
        }
    }
}

impl HomeserverListingClient for ProbeHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            let started = self.fetches.fetch_add(1, Ordering::SeqCst) + 1;
            if self.wait_for_peer.load(Ordering::SeqCst) {
                for _ in 0..100 {
                    if self.fetches.load(Ordering::SeqCst) > started {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            let key = self.lock_key.lock().expect("probe key").clone();
            if let Some(key) = key {
                let mut connection = self.pool.acquire().await.expect("probe connection");
                let free: bool =
                    sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 42))")
                        .bind(&key)
                        .fetch_one(&mut *connection)
                        .await
                        .expect("probe try lock");
                if free {
                    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 42))")
                        .bind(&key)
                        .execute(&mut *connection)
                        .await
                        .expect("probe unlock");
                }
                self.lock_free_during_fetch
                    .lock()
                    .expect("probe results")
                    .push(free);
            }
            match self.record.lock().expect("probe record").clone() {
                Some(record) => HomeserverFetchOutcome::Found(record),
                None => HomeserverFetchOutcome::NotFound,
            }
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        _seller_pubky: &'a str,
        _drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async { HomeserverFetchOutcome::NotFound })
    }
}

// Sol round 4 P2: the record is read once per command, after the cheap
// checks, and outside the command's transaction and advisory lock.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_reads_the_record_once_after_cheap_checks_and_outside_the_command_lock(
    pool: PgPool,
) {
    let probe = Arc::new(ProbeHomeserver::new(pool.clone()));
    let app = test_app_with_homeserver_client(pool, probe.clone()).await;
    let seller = new_actor(&app).await;
    let outsider = new_actor(&app).await;
    *probe.record.lock().expect("probe record") = Some(auction_record(&seller.pubky, "probe_01"));

    let command = auction_register(&seller.pubky, "probe_01", 600);
    *probe.lock_key.lock().expect("probe key") = Some(format!(
        "{}:{}",
        seller.pubky,
        command["command_id"].as_str().expect("command id")
    ));
    let (status, body) = send_command(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        probe.fetches.load(Ordering::SeqCst),
        1,
        "one fetch per command"
    );
    assert_eq!(
        *probe.lock_free_during_fetch.lock().expect("probe results"),
        vec![true],
        "the command's advisory lock is not held during the fetch"
    );

    // Refused on cheap checks without reading the record.
    let mut wrong_aggregate = auction_register(&seller.pubky, "probe_02", 601);
    wrong_aggregate["aggregate_id"] = json!(format!("listing:{}_probe_other", seller.pubky));
    let (status, body) = send_command(&app, &seller.token, &wrong_aggregate).await;
    assert_eq!(
        body["error"]["code"],
        json!("INVALID_COMMAND"),
        "{status}: {body}"
    );
    let not_seller = auction_register(&seller.pubky, "probe_03", 602);
    let (status, body) = send_command(&app, &outsider.token, &not_seller).await;
    assert_eq!(
        body["error"]["code"],
        json!("UNAUTHORIZED"),
        "{status}: {body}"
    );
    assert_eq!(
        probe.fetches.load(Ordering::SeqCst),
        1,
        "no fetch before cheap checks"
    );
}

// Sol round 5 P2: a replay, a conflicting reuse of the command id, and a
// concurrent duplicate are answered without another record read, and
// commands that need no record never read one.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn register_answers_replays_conflicts_and_duplicates_without_another_record_read(
    pool: PgPool,
) {
    let probe = Arc::new(ProbeHomeserver::new(pool.clone()));
    let app = test_app_with_homeserver_client(pool, probe.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let fetches = || probe.fetches.load(Ordering::SeqCst);

    // A fixed-price registration and a non-register command read nothing.
    let (status, body) =
        send_command(&app, &seller.token, &register_command(&seller.pubky, 5)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) =
        send_command(&app, &buyer.token, &reserve_command(&seller.pubky, 1, 1, 1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(fetches(), 0, "no record read without a record decision");

    *probe.record.lock().expect("probe record") = Some(auction_record(&seller.pubky, "replay_01"));
    let command = auction_register(&seller.pubky, "replay_01", 700);
    let (status, first) = send_command(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(fetches(), 1, "the first submission reads the record");

    let (status, replay) = send_command(&app, &seller.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{replay}");
    assert_eq!(replay, first, "the replay returns the stored result");
    assert_eq!(fetches(), 1, "an exact replay reads nothing");

    let mut conflicting = command.clone();
    conflicting["payload"]["listing_revision"] = json!(2);
    let (status, body) = send_command(&app, &seller.token, &conflicting).await;
    assert_eq!(
        body["error"]["code"],
        json!("IDEMPOTENCY_CONFLICT"),
        "{status}: {body}"
    );
    assert_eq!(fetches(), 1, "a conflicting reuse reads nothing");

    // Two identical first submissions in flight together read once.
    *probe.record.lock().expect("probe record") = Some(auction_record(&seller.pubky, "twice_01"));
    probe.wait_for_peer.store(true, Ordering::SeqCst);
    let duplicate = auction_register(&seller.pubky, "twice_01", 701);
    let ((status_a, body_a), (status_b, body_b)) = tokio::join!(
        send_command(&app, &seller.token, &duplicate),
        send_command(&app, &seller.token, &duplicate),
    );
    assert_eq!(status_a, StatusCode::OK, "{body_a}");
    assert_eq!(status_b, StatusCode::OK, "{body_b}");
    assert_eq!(body_a, body_b, "the duplicate returns the first result");
    assert_eq!(fetches(), 2, "concurrent duplicates read the record once");
}
