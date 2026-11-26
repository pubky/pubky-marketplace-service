//! Bounds on the listing sync path: `sync-many` spends one automation rate
//! token per item it runs, stops starting items at its batch deadline and
//! answers the rest as retryable, and admits a bounded number of items to the
//! homeserver at once, per caller and across callers. A record whose variant
//! quantities overflow an `i64`, or whose JSON would build an outsized value
//! tree within the byte cap, is refused rather than registered.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    execute, new_actor, send, sync_command, test_app_with_config, test_app_with_homeserver,
    test_app_with_homeserver_client, TestActor, TestApp,
};
use marketplace_service::automation::{
    sync_many_through, SyncManyGate, SyncManyLimits, SYNC_MANY_LIMITS,
};
use marketplace_service::homeserver::{HomeserverFetchOutcome, HomeserverListingClient};

/// A homeserver that takes `delay` to answer every listing read with a 404,
/// and records the most reads it ever had in flight, per seller and in all.
struct SlowHomeserver {
    delay: Duration,
    reads: Mutex<Reads>,
}

#[derive(Default)]
struct Reads {
    in_flight: HashMap<String, usize>,
    total: usize,
    most_per_seller: HashMap<String, usize>,
    most_total: usize,
}

impl SlowHomeserver {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            delay,
            reads: Mutex::new(Reads::default()),
        })
    }

    fn most_for(&self, seller: &str) -> usize {
        let reads = self.reads.lock().expect("reads lock");
        reads.most_per_seller.get(seller).copied().unwrap_or(0)
    }

    fn most_total(&self) -> usize {
        self.reads.lock().expect("reads lock").most_total
    }
}

impl HomeserverListingClient for SlowHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        _listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            {
                let mut reads = self.reads.lock().expect("reads lock");
                let seller = reads.in_flight.entry(seller_pubky.to_string()).or_default();
                *seller += 1;
                let now = *seller;
                reads.total += 1;
                let total = reads.total;
                let most = reads
                    .most_per_seller
                    .entry(seller_pubky.to_string())
                    .or_default();
                *most = (*most).max(now);
                reads.most_total = reads.most_total.max(total);
            }
            tokio::time::sleep(self.delay).await;
            let mut reads = self.reads.lock().expect("reads lock");
            *reads
                .in_flight
                .get_mut(seller_pubky)
                .expect("a read in flight") -= 1;
            reads.total -= 1;
            HomeserverFetchOutcome::NotFound
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

fn batch(seller: &str, prefix: &str, count: usize) -> Value {
    json!({
        "listings": (0..count)
            .map(|index| json!({
                "seller_pubky": seller,
                "listing_id": format!("{prefix}_{index:02}"),
            }))
            .collect::<Vec<_>>()
    })
}

async fn sync_many(app: &TestApp, actor: &TestActor, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/listings/sync-many",
        Some(&actor.token),
        body,
    )
    .await
}

fn statuses(body: &Value) -> Vec<u64> {
    body["results"]
        .as_array()
        .expect("batch results")
        .iter()
        .map(|result| result["status"].as_u64().expect("item status"))
        .collect()
}

fn codes(body: &Value) -> Vec<String> {
    body["results"]
        .as_array()
        .expect("batch results")
        .iter()
        .map(|result| {
            result["result"]["error"]["code"]
                .as_str()
                .unwrap_or("")
                .to_string()
        })
        .collect()
}

async fn tokens_spent(app: &TestApp, actor: &str) -> Option<f64> {
    sqlx::query_scalar(
        "SELECT tokens FROM automation_rate_limits \
         WHERE seller_pubky = $1 AND endpoint_class = 'listing.sync_many'",
    )
    .bind(actor)
    .fetch_optional(&app.pool)
    .await
    .expect("rate bucket")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sync_many_spends_one_rate_token_per_item_it_runs(pool: PgPool) {
    let mut config = marketplace_service::config::Config::for_tests();
    config.automation_rate_limit_per_minute = 1;
    config.automation_rate_limit_burst_multiplier = 3;
    let app = test_app_with_config(pool, config).await;

    // A batch the bucket covers runs whole.
    let covered = new_actor(&app).await;
    let (status, body) = sync_many(&app, &covered, &batch(&covered.pubky, "covered", 3)).await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(statuses(&body), vec![404, 404, 404], "{body}");
    assert_eq!(tokens_spent(&app, &covered.pubky).await, Some(0.0));

    // A batch past the bucket runs what it holds; the rest is retryable.
    let seller = new_actor(&app).await;
    let (status, body) = sync_many(&app, &seller, &batch(&seller.pubky, "listing", 5)).await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(statuses(&body), vec![404, 404, 404, 429, 429], "{body}");
    for refused in &body["results"].as_array().expect("results")[3..] {
        assert_eq!(refused["result"]["ok"], json!(false), "{refused}");
        assert_eq!(
            refused["result"]["error"]["code"],
            json!("RATE_LIMITED"),
            "{refused}"
        );
        // One token a minute, empty bucket, frozen clock.
        assert_eq!(
            refused["result"]["error"]["retry_after_seconds"],
            json!(60),
            "{refused}"
        );
        assert!(refused["listing_id"].as_str().is_some(), "{refused}");
    }

    let (status, body) = sync_many(&app, &seller, &batch(&seller.pubky, "again", 1)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], json!("rate_limited"));

    app.clock.advance_seconds(60);
    let (status, body) = sync_many(&app, &seller, &batch(&seller.pubky, "refilled", 2)).await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(
        statuses(&body),
        vec![404, 429],
        "one refilled token admits one item: {body}"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn one_sellers_concurrent_batches_read_the_homeserver_a_bounded_number_at_a_time(
    pool: PgPool,
) {
    let homeserver = SlowHomeserver::new(Duration::from_millis(100));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;

    let requests = (0..4)
        .map(|request| {
            let router = app.router.clone();
            let token = seller.token.clone();
            let body = batch(&seller.pubky, &format!("burst{request}"), 2);
            tokio::spawn(async move {
                send(
                    router,
                    "POST",
                    "/v1/listings/sync-many",
                    Some(&token),
                    &body,
                )
                .await
            })
        })
        .collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    let (status, body) = sync_many(&app, &other, &batch(&other.pubky, "other", 1)).await;
    let waited = started.elapsed();
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(statuses(&body), vec![404], "{body}");
    assert!(
        waited < Duration::from_millis(800),
        "another seller is not queued behind the burst: {waited:?}"
    );

    for request in requests {
        let (status, body) = request.await.expect("request task");
        assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
        assert_eq!(statuses(&body), vec![404, 404], "{body}");
    }
    assert_eq!(
        homeserver.most_for(&seller.pubky),
        SYNC_MANY_LIMITS.in_flight_per_seller,
        "four concurrent batches of one seller read at most its own slots at once"
    );
}

fn limits(deadline_ms: u64, in_flight: usize, in_flight_per_seller: usize) -> SyncManyLimits {
    SyncManyLimits {
        deadline: Duration::from_millis(deadline_ms),
        in_flight,
        in_flight_per_seller,
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_left_at_the_batch_deadline_are_answered_retryable_without_running(pool: PgPool) {
    // The first item outlasts the whole batch budget on its own.
    let homeserver = SlowHomeserver::new(Duration::from_millis(300));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let gate = SyncManyGate::new(limits(150, 16, 2));

    let started = Instant::now();
    let (status, body) = sync_many_through(
        &app.state,
        &seller.pubky,
        batch(&seller.pubky, "slow", 6),
        &gate,
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(
        statuses(&body),
        vec![404, 503, 503, 503, 503, 503],
        "{body}"
    );
    assert_eq!(
        codes(&body)[1..],
        vec!["BATCH_DEADLINE_EXCEEDED".to_string(); 5],
        "{body}"
    );
    for deferred in &body["results"].as_array().expect("results")[1..] {
        assert_eq!(deferred["seller_pubky"], json!(seller.pubky), "{deferred}");
        assert!(deferred["listing_id"].as_str().is_some(), "{deferred}");
    }
    assert!(
        elapsed < Duration::from_millis(1_000),
        "the batch stops at its deadline, not after six slow items: {elapsed:?}"
    );
    // The first item rides on the request's admission token, and the items
    // that never ran spend none.
    assert_eq!(tokens_spent(&app, &seller.pubky).await, None);
    assert_eq!(gate.tracked_sellers(), 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn waiting_for_a_slot_counts_against_the_batch_deadline(pool: PgPool) {
    let homeserver = SlowHomeserver::new(Duration::from_millis(600));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let gate = Arc::new(SyncManyGate::new(limits(200, 16, 1)));

    let holding = {
        let state = app.state.clone();
        let gate = gate.clone();
        let pubky = seller.pubky.clone();
        tokio::spawn(async move {
            sync_many_through(&state, &pubky, batch(&pubky, "holding", 1), &gate).await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (status, body) = sync_many_through(
        &app.state,
        &seller.pubky,
        batch(&seller.pubky, "queued", 3),
        &gate,
    )
    .await;
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(statuses(&body), vec![503, 503, 503], "{body}");
    assert_eq!(codes(&body), vec!["BATCH_DEADLINE_EXCEEDED".to_string(); 3]);

    let (status, body) = holding.await.expect("holding batch");
    assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
    assert_eq!(
        statuses(&body),
        vec![404],
        "the running item finishes past the deadline"
    );
    assert_eq!(gate.tracked_sellers(), 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_batches_of_many_sellers_share_a_bounded_number_of_reads(pool: PgPool) {
    let homeserver = SlowHomeserver::new(Duration::from_millis(80));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let gate = Arc::new(SyncManyGate::new(limits(30_000, 2, 2)));
    let mut sellers = Vec::new();
    for _ in 0..4 {
        sellers.push(new_actor(&app).await);
    }

    let batches = sellers
        .iter()
        .map(|seller| {
            let state = app.state.clone();
            let gate = gate.clone();
            let pubky = seller.pubky.clone();
            tokio::spawn(async move {
                sync_many_through(&state, &pubky, batch(&pubky, "shared", 2), &gate).await
            })
        })
        .collect::<Vec<_>>();
    for batch in batches {
        let (status, body) = batch.await.expect("batch task");
        assert_eq!(status, StatusCode::MULTI_STATUS, "{body}");
        assert_eq!(statuses(&body), vec![404, 404], "{body}");
    }
    assert_eq!(homeserver.most_total(), 2, "the shared bound holds");
    assert_eq!(gate.tracked_sellers(), 0);
}

/// A canonical fixed-price record with the given variants.
fn record_with(variants: Value) -> Value {
    json!({
        "title": "Winter boots",
        "revision": 1,
        "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
        "variants": variants,
        "shippingOptions": [{ "pricing": "free" }],
        "fulfillmentMethods": ["shipping"],
        "sale": {
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn variant_quantities_that_overflow_are_an_invalid_record(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    homeserver.put_record(
        &seller.pubky,
        "overflow",
        record_with(json!([
            { "id": "big_a", "enabled": true, "quantity": i64::MAX },
            { "id": "big_b", "enabled": true, "quantity": i64::MAX },
            { "id": "small", "enabled": true, "quantity": 1 },
        ])),
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &sync_command(&seller.pubky, "overflow", 1),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"), "{body}");
    let listings: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listings WHERE seller_pubky = $1")
        .bind(&seller.pubky)
        .fetch_one(&app.pool)
        .await
        .expect("listing count");
    assert_eq!(listings, 0);

    homeserver.put_record(
        &seller.pubky,
        "ordinary",
        record_with(json!([
            { "id": "size_s", "enabled": true, "quantity": 2 },
            { "id": "size_m", "enabled": true, "quantity": 3 },
        ])),
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &sync_command(&seller.pubky, "ordinary", 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_record_that_would_amplify_into_a_huge_value_tree_is_not_synced(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let seller = new_actor(&app).await;
    let one_variant = json!([{ "id": "only", "enabled": true, "quantity": 2 }]);

    // About 200 KiB on the wire, far under the 4 MiB body cap.
    let mut amplifying = record_with(one_variant.clone());
    amplifying["padding"] = json!(vec![0; 100_001]);
    homeserver.put_record(&seller.pubky, "amplifying", amplifying);
    let (status, body) = execute(
        &app,
        &seller.token,
        &sync_command(&seller.pubky, "amplifying", 1),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("UPSTREAM_UNAVAILABLE"),
        "{body}"
    );

    let mut deep = record_with(one_variant.clone());
    let mut nested = json!(0);
    for _ in 0..40 {
        nested = json!([nested]);
    }
    deep["padding"] = nested;
    homeserver.put_record(&seller.pubky, "deep", deep);
    let (status, body) =
        execute(&app, &seller.token, &sync_command(&seller.pubky, "deep", 2)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");

    let mut padded = record_with(one_variant);
    padded["padding"] = json!(vec![0; 1_000]);
    homeserver.put_record(&seller.pubky, "padded", padded);
    let (status, body) = execute(
        &app,
        &seller.token,
        &sync_command(&seller.pubky, "padded", 3),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "unknown fields within the limits still sync: {body}"
    );
}
