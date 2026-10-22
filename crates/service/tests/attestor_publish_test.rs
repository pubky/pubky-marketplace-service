//! The attestor publisher: what it writes to the attestor's homeserver, when,
//! and that it writes it once. The homeserver is a fake behind the
//! `AttestorHomeserver` trait; barriers, not sleeps, order the concurrent
//! cases, and the test clock moves the backoff.

mod common;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

use common::attestor_records::{verify_record, ANNOTATIONS_PREFIX, PROFILE_PATH};
use common::review_orders::{delivered_order, LISTINGS};
use common::{
    execute, new_actor, order_command, test_app_full, test_attestor, TestApp, TEST_ATTESTOR_SALT,
};
use marketplace_service::attestor::{Attestor, ISSUES_PRICE_DECADE};
use marketplace_service::attestor_publish::{
    run_pass, AttestorHomeserver, AttestorPublisher, ProfileSettings, PublishConfig, PublishError,
    RETRY_BASE_SECONDS, TASK_ATTESTOR_PUBLISH,
};
use marketplace_service::attestor_records::{Operator, ProfileKey};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::workers::run_once;

const OPERATOR_URL: &str = "https://pubky-market.example";
const POLICY_URL: &str = "https://pubky-market.example/attestor-policy";
const BLIND_WINDOW_DAYS: i64 = 14;
const OLD_KEY: &str = "9wm1eqn4wddis385xdgsym7b5ge9549zpcjhmdszymicopxj6acy";

#[derive(Clone, Debug)]
struct Put {
    path: String,
    body: Vec<u8>,
}

impl Put {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("a record is JSON")
    }
}

/// A rendezvous inside one PUT: the PUT announces it arrived, then waits to be
/// let go. Armed for one path, used once.
struct Gate {
    path: String,
    arrived: Arc<Barrier>,
    release: Arc<Barrier>,
}

#[derive(Default)]
struct FakeHomeserver {
    puts: Mutex<Vec<Put>>,
    failing: AtomicBool,
    gates: Mutex<VecDeque<Gate>>,
}

impl FakeHomeserver {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn fail(&self, failing: bool) {
        self.failing.store(failing, Ordering::SeqCst);
    }

    fn puts(&self) -> Vec<Put> {
        self.puts.lock().unwrap().clone()
    }

    fn puts_to(&self, path: &str) -> Vec<Put> {
        self.puts()
            .into_iter()
            .filter(|put| put.path == path)
            .collect()
    }

    fn count(&self) -> usize {
        self.puts.lock().unwrap().len()
    }

    /// Holds the next PUT to `path` between its start and its completion.
    fn gate(&self, path: &str) -> (Arc<Barrier>, Arc<Barrier>) {
        let arrived = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        self.gates.lock().unwrap().push_back(Gate {
            path: path.to_string(),
            arrived: arrived.clone(),
            release: release.clone(),
        });
        (arrived, release)
    }
}

#[async_trait]
impl AttestorHomeserver for FakeHomeserver {
    async fn put(&self, path: &str, body: &[u8]) -> Result<(), PublishError> {
        let gate = {
            let mut gates = self.gates.lock().unwrap();
            gates
                .iter()
                .position(|gate| gate.path == path)
                .and_then(|index| gates.remove(index))
        };
        if let Some(gate) = gate {
            gate.arrived.wait().await;
            gate.release.wait().await;
        }
        if self.failing.load(Ordering::SeqCst) {
            return Err(PublishError("homeserver unavailable".into()));
        }
        self.puts.lock().unwrap().push(Put {
            path: path.to_string(),
            body: body.to_vec(),
        });
        Ok(())
    }
}

fn settings(attestor: &Attestor) -> ProfileSettings {
    ProfileSettings {
        operator: Operator {
            name: "Pubky Market".into(),
            url: OPERATOR_URL.into(),
            policy_url: POLICY_URL.into(),
        },
        keys: vec![ProfileKey {
            pubky: attestor.pubky().to_string(),
            valid_from: "2026-08-01T00:00:00Z".into(),
            valid_until: None,
            compromised_from: None,
        }],
        successor: None,
    }
}

fn publisher(
    settings: ProfileSettings,
    homeserver: &Arc<FakeHomeserver>,
) -> Arc<AttestorPublisher> {
    Arc::new(AttestorPublisher::new(settings, homeserver.clone()))
}

fn publishing_config() -> Config {
    let mut config = Config::for_tests();
    config.review_blind_window_days = BLIND_WINDOW_DAYS;
    config
}

async fn publishing_app(pool: PgPool, homeserver: &Arc<FakeHomeserver>) -> TestApp {
    publishing_app_with(pool, publishing_config(), homeserver).await
}

async fn publishing_app_with(
    pool: PgPool,
    config: Config,
    homeserver: &Arc<FakeHomeserver>,
) -> TestApp {
    let attestor = test_attestor();
    let mut app = test_app_full(pool, config, Some(attestor.clone())).await;
    app.state = app
        .state
        .clone()
        .with_attestor_publisher(Some(publisher(settings(&attestor), homeserver)));
    app
}

/// The same service after a restart: same database, a new publisher over
/// `homeserver`.
fn restarted(
    app: &TestApp,
    settings: ProfileSettings,
    homeserver: &Arc<FakeHomeserver>,
) -> AppState {
    app.state
        .clone()
        .with_attestor_publisher(Some(publisher(settings, homeserver)))
}

use marketplace_service::AppState;

async fn pass(state: &AppState) -> u64 {
    run_pass(state, Uuid::new_v4(), state.clock.now()).await
}

async fn seed_order(pool: &PgPool) -> Uuid {
    let order_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 6, 'completed', '[]'::jsonb, NULL, 12500, 1200, 13700, \
         'USD', 2, 1, $2, now(), now())",
    )
    .bind(order_id)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .expect("seed order");
    order_id
}

/// An order with a purchase attestation issued by `attestor`; returns its
/// `order_ref`.
async fn attested_order(pool: &PgPool, attestor: &Attestor) -> (Uuid, String) {
    let order_id = seed_order(pool).await;
    let listing = format!(
        "pubky://{}/pub/pubky.app/marketplace/v1/listings/boots",
        "s".repeat(52)
    );
    let issued = attestor.issue_purchase_attestation(
        order_id,
        &"b".repeat(52),
        &"s".repeat(52),
        "buyer_reviewing_seller",
        &listing,
        "2026-08-19",
        None,
        "2026-08-19T22:00:00Z".parse().unwrap(),
    );
    sqlx::query(
        "INSERT INTO review_attestations \
         (order_id, reviewer_pubky, listing_uri, order_ref, jws, claims, issued_at) \
         VALUES ($1, $2, $3, $4, $5, $6, now())",
    )
    .bind(order_id)
    .bind("b".repeat(52))
    .bind(&listing)
    .bind(&issued.order_ref)
    .bind(&issued.jws)
    .bind(&issued.claims)
    .execute(pool)
    .await
    .expect("attestation");
    (order_id, issued.order_ref)
}

async fn annotate(pool: &PgPool, order_ref: &str, outcome: &str, at: &str) {
    sqlx::query(
        "INSERT INTO attestation_annotations (id, order_ref, outcome, reason, annotated_at) \
         VALUES ($1, $2, $3, NULL, $4)",
    )
    .bind(Uuid::new_v4())
    .bind(order_ref)
    .bind(outcome)
    .bind(at.parse::<DateTime<Utc>>().unwrap())
    .execute(pool)
    .await
    .expect("annotation");
}

fn annotation_file(order_ref: &str) -> String {
    format!("{ANNOTATIONS_PREFIX}{order_ref}")
}

fn verified(put: &Put) -> Value {
    verify_record(test_attestor().pubky(), &put.path, &put.body)
        .unwrap_or_else(|error| panic!("{} does not verify: {error}", put.path))
}

async fn publication(pool: &PgPool, path: &str) -> Option<(i64, String)> {
    sqlx::query_as("SELECT revision, body_hash FROM attestor_publications WHERE path = $1")
        .bind(path)
        .fetch_optional(pool)
        .await
        .expect("publication")
}

async fn attempt(pool: &PgPool, path: &str) -> Option<(i64, i32, DateTime<Utc>, Option<String>)> {
    sqlx::query_as(
        "SELECT revision, attempts, next_attempt_at, last_error \
         FROM attestor_publish_attempts WHERE path = $1",
    )
    .bind(path)
    .fetch_optional(pool)
    .await
    .expect("attempt")
}

// === what is written ========================================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_profile_and_an_annotation_per_attested_order_are_written_and_verify(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let attestor = test_attestor();
    let (_, first_ref) = attested_order(&app.pool, &attestor).await;
    let (_, second_ref) = attested_order(&app.pool, &attestor).await;
    annotate(&app.pool, &first_ref, "returned", "2026-10-12T09:00:00Z").await;
    annotate(&app.pool, &first_ref, "refunded", "2026-10-14T23:59:59Z").await;
    annotate(&app.pool, &second_ref, "refunded", "2026-10-15T00:00:00Z").await;

    assert_eq!(pass(&app.state).await, 3);

    let puts = homeserver.puts();
    assert_eq!(puts.len(), 3);
    assert_eq!(puts[0].path, PROFILE_PATH, "the profile goes first");
    let profile = verified(&puts[0]);
    assert_eq!(
        profile["operator"],
        json!({ "name": "Pubky Market", "url": OPERATOR_URL, "policyUrl": POLICY_URL })
    );
    assert_eq!(
        profile["keys"],
        json!([{ "pubky": attestor.pubky(), "validFrom": "2026-08-01T00:00:00Z" }])
    );
    assert_eq!(profile["issuancePolicy"]["editWindowHours"], json!(24));
    assert_eq!(
        profile["issuancePolicy"]["blindWindowDays"],
        json!(BLIND_WINDOW_DAYS)
    );
    assert_eq!(
        profile["issuancePolicy"]["amountBand"],
        json!("both-sides consent")
    );
    assert_eq!(profile["issuancePolicy"]["minOrderValue"], Value::Null);
    assert!(profile.get("successor").is_none());

    let first = homeserver.puts_to(&annotation_file(&first_ref));
    assert_eq!(first.len(), 1);
    assert_eq!(
        verified(&first[0]),
        json!({
            "orderRef": first_ref,
            "outcomes": [
                { "kind": "returned", "on": "2026-10-12" },
                { "kind": "refunded", "on": "2026-10-14" },
            ],
            "revision": 1,
            "proof": first[0].json()["proof"],
        })
    );
    let second = homeserver.puts_to(&annotation_file(&second_ref));
    assert_eq!(second.len(), 1);
    assert_eq!(
        verified(&second[0])["outcomes"],
        json!([{ "kind": "refunded", "on": "2026-10-15" }])
    );

    for put in &puts {
        let (revision, _) = publication(&app.pool, &put.path).await.expect("recorded");
        assert_eq!(revision, 1);
    }
    assert!(attempt(&app.pool, PROFILE_PATH).await.is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_profile_states_what_the_service_issues(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    pass(&app.state).await;
    let profile = verified(&homeserver.puts_to(PROFILE_PATH)[0]);
    assert_eq!(
        profile["issuancePolicy"]["priceDecade"],
        json!(if ISSUES_PRICE_DECADE {
            "always"
        } else {
            "never"
        }),
        "priceDecade follows ISSUES_PRICE_DECADE"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_orders_this_attestor_attested_are_published(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let attestor = test_attestor();
    let other = Attestor::from_hex(&"09".repeat(32), TEST_ATTESTOR_SALT).expect("other attestor");
    let (_, attested) = attested_order(&app.pool, &attestor).await;
    let (_, by_another) = attested_order(&app.pool, &other).await;
    let unattested = "ef".repeat(32);
    for order_ref in [&attested, &by_another, &unattested] {
        annotate(&app.pool, order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    }

    assert_eq!(pass(&app.state).await, 2);

    let paths: Vec<String> = homeserver.puts().into_iter().map(|put| put.path).collect();
    assert_eq!(
        paths,
        vec![PROFILE_PATH.to_string(), annotation_file(&attested)]
    );
    assert!(publication(&app.pool, &annotation_file(&by_another))
        .await
        .is_none());
    assert!(publication(&app.pool, &annotation_file(&unattested))
        .await
        .is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_attested_order_without_an_outcome_is_not_written(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    attested_order(&app.pool, &test_attestor()).await;

    assert_eq!(pass(&app.state).await, 1, "only the profile");
    assert_eq!(homeserver.count(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_second_pass_with_nothing_new_writes_nothing(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    assert_eq!(pass(&app.state).await, 2);

    assert_eq!(pass(&app.state).await, 0);
    assert_eq!(pass(&app.state).await, 0);
    assert_eq!(homeserver.count(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pass_writes_at_most_a_batch_and_the_next_pass_the_rest(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let mut config = publishing_config();
    config.delivery_sweep_batch_size = 2;
    let app = publishing_app_with(pool, config, &homeserver).await;
    for _ in 0..3 {
        let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
        annotate(&app.pool, &order_ref, "returned", "2026-10-12T00:00:00Z").await;
    }

    assert_eq!(pass(&app.state).await, 2);
    assert_eq!(pass(&app.state).await, 2);
    assert_eq!(pass(&app.state).await, 0);
    assert_eq!(homeserver.count(), 4, "the profile and three annotations");
    let mut paths: Vec<String> = homeserver.puts().into_iter().map(|put| put.path).collect();
    paths.sort();
    paths.dedup();
    assert_eq!(paths.len(), 4, "each written once");
}

// === outcomes ===============================================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancelled_reversal_takes_back_the_refund_and_never_appears_as_an_outcome(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let attestor = test_attestor();
    let (_, refunded_then_cancelled) = attested_order(&app.pool, &attestor).await;
    let (_, never_refunded) = attested_order(&app.pool, &attestor).await;
    annotate(
        &app.pool,
        &refunded_then_cancelled,
        "refunded",
        "2026-10-12T00:00:00Z",
    )
    .await;
    annotate(
        &app.pool,
        &never_refunded,
        "refunded",
        "2026-10-12T00:00:00Z",
    )
    .await;
    assert_eq!(pass(&app.state).await, 3);

    annotate(
        &app.pool,
        &refunded_then_cancelled,
        "refund_reversal_cancelled",
        "2026-10-13T00:00:00Z",
    )
    .await;
    assert_eq!(pass(&app.state).await, 1);
    let rewritten = homeserver.puts_to(&annotation_file(&refunded_then_cancelled));
    assert_eq!(rewritten.len(), 2);
    let body = verified(&rewritten[1]);
    assert_eq!(
        body["outcomes"],
        json!([]),
        "the public record stops claiming the refund"
    );
    assert_eq!(body["revision"], json!(2));
    assert!(!rewritten[1].json().to_string().contains("refund_reversal"));
    assert_eq!(
        homeserver.puts_to(&annotation_file(&never_refunded)).len(),
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_cancelled_before_anything_was_written_is_not_written(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-12T00:00:00Z").await;
    annotate(
        &app.pool,
        &order_ref,
        "refund_reversal_cancelled",
        "2026-10-13T00:00:00Z",
    )
    .await;

    assert_eq!(pass(&app.state).await, 1, "only the profile");
    assert!(homeserver.puts_to(&annotation_file(&order_ref)).is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_returned_and_refunded_reviewed_order_publishes_both_outcomes(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let attestor = test_attestor();
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_ref = attestor.order_ref(order_id.parse().expect("uuid"));
    let mut number = 9_100;
    let mut command = |kind: &str, revision: i64, payload: Value| {
        number += 1;
        order_command(kind, &order_id, revision, payload, number)
    };

    for (actor, text) in [(&seller, "Paid at once."), (&buyer, "Boots arrived fast.")] {
        let revision: i64 = sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
            .bind(&order_id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
        let (status, body) = execute(
            &app,
            &actor.token,
            &command(
                "review.create",
                revision,
                json!({ "rating": 5, "text": text }),
            ),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    }
    let attested: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM review_attestations WHERE order_ref = $1")
            .bind(&order_ref)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert!(attested > 0, "the reviews were attested");
    assert_eq!(pass(&app.state).await, 1, "the profile; no outcome yet");

    let revision = |app: &TestApp| {
        let pool = app.pool.clone();
        let order_id = order_id.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT revision FROM orders WHERE id = $1::uuid")
                .bind(order_id)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    let steps = [
        (
            &buyer,
            "return.request",
            json!({ "reason": "Item not as described", "requested_amount_minor": 13_700 }),
        ),
        (&seller, "return.approve", json!({})),
        (&seller, "return.receive", json!({})),
    ];
    for (actor, kind, payload) in steps {
        let current = revision(&app).await;
        let (status, body) = execute(&app, &actor.token, &command(kind, current, payload)).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{kind}: {body}");
    }
    assert_eq!(pass(&app.state).await, 1);
    let returned = homeserver.puts_to(&annotation_file(&order_ref));
    assert_eq!(returned.len(), 1);
    assert_eq!(
        verified(&returned[0])["outcomes"],
        json!([{ "kind": "returned", "on": "2026-08-19" }])
    );

    app.clock.advance_seconds(86_400);
    let current = revision(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &command(
            "refund.record_external",
            current,
            json!({ "amount_minor": 13_700, "transaction_id": "tx-refund-1" }),
        ),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "{body}");

    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker tick");
    assert_eq!(summary.attestor_records_published, 1);
    let refunded = homeserver.puts_to(&annotation_file(&order_ref));
    assert_eq!(refunded.len(), 2);
    let body = verified(&refunded[1]);
    assert_eq!(
        body["outcomes"],
        json!([
            { "kind": "returned", "on": "2026-08-19" },
            { "kind": "refunded", "on": "2026-08-20" },
        ])
    );
    assert_eq!(body["revision"], json!(2));
}

// === the profile ============================================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_profile_is_written_again_only_when_its_content_changes(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let attestor = test_attestor();
    assert_eq!(pass(&app.state).await, 1);
    assert_eq!(pass(&app.state).await, 0);

    let mut rotated = settings(&attestor);
    rotated.keys.push(ProfileKey {
        pubky: OLD_KEY.into(),
        valid_from: "2026-01-01T00:00:00Z".into(),
        valid_until: Some("2026-08-01T00:00:00Z".into()),
        compromised_from: Some("2026-07-15T00:00:00Z".into()),
    });
    rotated.successor = None;
    let state = restarted(&app, rotated.clone(), &homeserver);
    assert_eq!(pass(&state).await, 1, "a key list change is announced");
    assert_eq!(pass(&state).await, 0);
    let profile = verified(&homeserver.puts_to(PROFILE_PATH)[1]);
    assert_eq!(profile["keys"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        profile["keys"][1]["compromisedFrom"],
        json!("2026-07-15T00:00:00Z")
    );

    let mut config = publishing_config();
    config.review_blind_window_days = BLIND_WINDOW_DAYS + 7;
    let changed = AppState::new(app.pool.clone(), app.clock.clone(), config)
        .with_attestor(Some(attestor.clone()))
        .with_attestor_publisher(Some(publisher(rotated, &homeserver)));
    assert_eq!(
        pass(&changed).await,
        1,
        "a blind window change is announced"
    );
    assert_eq!(pass(&changed).await, 0);
    assert_eq!(
        verified(&homeserver.puts_to(PROFILE_PATH)[2])["issuancePolicy"]["blindWindowDays"],
        json!(BLIND_WINDOW_DAYS + 7)
    );
    assert_eq!(homeserver.puts_to(PROFILE_PATH).len(), 3);
    assert_eq!(publication(&app.pool, PROFILE_PATH).await.unwrap().0, 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_successor_is_announced_in_the_profile(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let mut announcing = settings(&test_attestor());
    announcing.successor = Some(OLD_KEY.into());
    let state = restarted(&app, announcing, &homeserver);
    assert_eq!(pass(&state).await, 1);
    assert_eq!(
        verified(&homeserver.puts_to(PROFILE_PATH)[0])["successor"],
        json!(OLD_KEY)
    );
}

// === one publisher ==========================================================

/// Two publishers race for the same pass. The first holds its PUT at the
/// barrier, after the lease check and before the write; the second finds the
/// lease held and writes nothing. Each record is written once.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_concurrent_publishers_write_each_record_once(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    let (arrived, release) = homeserver.gate(PROFILE_PATH);

    let state = app.state.clone();
    let first = tokio::spawn(async move { pass(&state).await });
    arrived.wait().await;

    assert_eq!(
        pass(&app.state).await,
        0,
        "the second publisher finds the lease held"
    );
    assert_eq!(
        homeserver.count(),
        0,
        "and writes nothing, in flight or not"
    );

    release.wait().await;
    assert_eq!(first.await.expect("first publisher"), 2);
    assert_eq!(homeserver.puts_to(PROFILE_PATH).len(), 1);
    assert_eq!(homeserver.puts_to(&annotation_file(&order_ref)).len(), 1);
    assert_eq!(homeserver.count(), 2);

    assert_eq!(
        pass(&app.state).await,
        0,
        "the lease is free again and nothing is due"
    );
    assert_eq!(homeserver.count(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_runs_the_publisher_under_its_lease(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;

    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker tick");
    assert_eq!(summary.attestor_records_published, 1);
    let held: Option<(Uuid, DateTime<Utc>)> =
        sqlx::query_as("SELECT holder, lease_until FROM worker_leases WHERE task = $1")
            .bind(TASK_ATTESTOR_PUBLISH)
            .fetch_optional(&app.pool)
            .await
            .unwrap();
    let (_, lease_until) = held.expect("the pass took the task's lease");
    assert!(lease_until <= app.clock.now(), "and released it");
}

// === changes while publishing ===============================================

/// An outcome lands while the annotation's PUT is in flight. What was
/// recorded is the content the pass read, so the new outcome is a change: the
/// next pass writes it at the next revision.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_outcome_added_mid_publish_is_written_at_the_next_revision(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    let path = annotation_file(&order_ref);
    annotate(&app.pool, &order_ref, "returned", "2026-10-12T00:00:00Z").await;
    let (arrived, release) = homeserver.gate(&path);

    let state = app.state.clone();
    let running = tokio::spawn(async move { pass(&state).await });
    arrived.wait().await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    release.wait().await;
    assert_eq!(running.await.expect("pass"), 2);

    let first = homeserver.puts_to(&path);
    assert_eq!(first.len(), 1);
    assert_eq!(verified(&first[0])["revision"], json!(1));
    assert_eq!(
        verified(&first[0])["outcomes"],
        json!([{ "kind": "returned", "on": "2026-10-12" }]),
        "the write in flight did not include the new outcome"
    );

    assert_eq!(pass(&app.state).await, 1, "the next tick publishes it");
    let all = homeserver.puts_to(&path);
    assert_eq!(all.len(), 2);
    let second = verified(&all[1]);
    assert_eq!(second["revision"], json!(2));
    assert_eq!(second["outcomes"].as_array().map(Vec::len), Some(2));
    assert_eq!(publication(&app.pool, &path).await.unwrap().0, 2);
    assert_eq!(pass(&app.state).await, 0);
}

// === failures, backoff, restarts ============================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_write_backs_off_and_is_retried_when_due(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    homeserver.fail(true);

    assert_eq!(pass(&app.state).await, 0);
    let first = attempt(&app.pool, PROFILE_PATH)
        .await
        .expect("the attempt is recorded");
    assert_eq!(first.1, 1);
    assert_eq!(
        first.2,
        app.clock.now() + chrono::Duration::seconds(RETRY_BASE_SECONDS)
    );
    assert_eq!(first.3.as_deref(), Some("homeserver unavailable"));
    assert!(publication(&app.pool, PROFILE_PATH).await.is_none());
    assert!(
        attempt(&app.pool, &annotation_file(&order_ref))
            .await
            .is_none(),
        "a failed write ends the pass"
    );

    assert_eq!(
        pass(&app.state).await,
        0,
        "still backing off: no write is tried"
    );
    app.clock.advance_seconds(RETRY_BASE_SECONDS - 1);
    assert_eq!(pass(&app.state).await, 0);
    assert_eq!(attempt(&app.pool, PROFILE_PATH).await.unwrap().1, 1);

    app.clock.advance_seconds(1);
    assert_eq!(pass(&app.state).await, 0, "due, tried, failed again");
    let second = attempt(&app.pool, PROFILE_PATH).await.unwrap();
    assert_eq!(second.1, 2);
    assert_eq!(
        second.2,
        app.clock.now() + chrono::Duration::seconds(2 * RETRY_BASE_SECONDS),
        "the wait doubles"
    );

    homeserver.fail(false);
    app.clock.advance_seconds(2 * RETRY_BASE_SECONDS);
    assert_eq!(
        pass(&app.state).await,
        2,
        "the profile, then the annotation"
    );
    assert!(
        attempt(&app.pool, PROFILE_PATH).await.is_none(),
        "the attempt is cleared"
    );
    let profile = homeserver.puts_to(PROFILE_PATH);
    assert_eq!(profile.len(), 1);
    assert_eq!(publication(&app.pool, PROFILE_PATH).await.unwrap().0, 1);
    assert_eq!(pass(&app.state).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_restart_after_a_failure_keeps_the_backoff_and_resumes(pool: PgPool) {
    let broken = FakeHomeserver::new();
    let app = publishing_app(pool, &broken).await;
    broken.fail(true);
    assert_eq!(pass(&app.state).await, 0);

    let healthy = FakeHomeserver::new();
    let state = restarted(&app, settings(&test_attestor()), &healthy);
    assert_eq!(
        pass(&state).await,
        0,
        "the new instance waits out the same backoff"
    );
    assert_eq!(healthy.count(), 0);

    app.clock.advance_seconds(RETRY_BASE_SECONDS);
    assert_eq!(pass(&state).await, 1);
    assert_eq!(healthy.puts_to(PROFILE_PATH).len(), 1);
    assert_eq!(pass(&state).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_restart_after_a_success_does_not_write_again(pool: PgPool) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, order_ref) = attested_order(&app.pool, &test_attestor()).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;
    assert_eq!(pass(&app.state).await, 2);

    let after = FakeHomeserver::new();
    let state = restarted(&app, settings(&test_attestor()), &after);
    assert_eq!(pass(&state).await, 0);
    app.clock.advance_seconds(7 * 86_400);
    assert_eq!(pass(&state).await, 0);
    assert_eq!(after.count(), 0, "no duplicate write after a restart");
}

/// A write that failed may still have landed, so the retry of unchanged
/// content is the same bytes at the same revision; changed content moves to the
/// next revision, never reusing one for different content.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_retry_repeats_unchanged_content_and_never_reuses_a_revision_for_new_content(
    pool: PgPool,
) {
    let homeserver = FakeHomeserver::new();
    let app = publishing_app(pool, &homeserver).await;
    let (_, unchanged) = attested_order(&app.pool, &test_attestor()).await;
    let (_, changing) = attested_order(&app.pool, &test_attestor()).await;
    let (first_path, second_path) = {
        let mut paths = [annotation_file(&unchanged), annotation_file(&changing)];
        paths.sort();
        (paths[0].clone(), paths[1].clone())
    };
    assert_eq!(pass(&app.state).await, 1, "the profile");

    annotate(&app.pool, &unchanged, "returned", "2026-10-12T00:00:00Z").await;
    annotate(&app.pool, &changing, "returned", "2026-10-12T00:00:00Z").await;
    homeserver.fail(true);
    assert_eq!(pass(&app.state).await, 0);
    let failed_path = if attempt(&app.pool, &first_path).await.is_some() {
        first_path
    } else {
        second_path
    };
    let failed_ref = failed_path
        .trim_start_matches(ANNOTATIONS_PREFIX)
        .to_string();
    assert_eq!(attempt(&app.pool, &failed_path).await.unwrap().0, 1);

    homeserver.fail(false);
    app.clock.advance_seconds(RETRY_BASE_SECONDS);
    assert_eq!(pass(&app.state).await, 2);
    let landed = homeserver.puts_to(&failed_path);
    assert_eq!(landed.len(), 1);
    assert_eq!(verified(&landed[0])["revision"], json!(1));

    homeserver.fail(true);
    annotate(&app.pool, &failed_ref, "refunded", "2026-10-14T00:00:00Z").await;
    assert_eq!(pass(&app.state).await, 0);
    assert_eq!(attempt(&app.pool, &failed_path).await.unwrap().0, 2);
    annotate(&app.pool, &failed_ref, "returned", "2026-10-15T00:00:00Z").await;
    homeserver.fail(false);
    app.clock.advance_seconds(RETRY_BASE_SECONDS);
    assert_eq!(pass(&app.state).await, 1);
    let latest = homeserver.puts_to(&failed_path).pop().unwrap();
    let body = verified(&latest);
    assert_eq!(
        body["revision"],
        json!(3),
        "the content changed again under the failed attempt"
    );
    assert_eq!(body["outcomes"].as_array().map(Vec::len), Some(3));
}

// === off, and configuration =================================================

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn off_when_unconfigured_nothing_runs_and_nothing_is_written(pool: PgPool) {
    let attestor = test_attestor();
    let app = test_app_full(pool, publishing_config(), Some(attestor.clone())).await;
    assert!(app.state.attestor_publisher.is_none());
    let (_, order_ref) = attested_order(&app.pool, &attestor).await;
    annotate(&app.pool, &order_ref, "refunded", "2026-10-14T00:00:00Z").await;

    assert_eq!(pass(&app.state).await, 0);
    let summary = run_once(&app.state, Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker tick");
    assert_eq!(summary.attestor_records_published, 0);

    let leases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_leases WHERE task = $1")
        .bind(TASK_ATTESTOR_PUBLISH)
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(leases, 0, "the task never took its lease");
    let rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM attestor_publications) \
         + (SELECT COUNT(*) FROM attestor_publish_attempts)",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(rows, 0);
}

fn lookup<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |name| {
        pairs
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.to_string())
    }
}

fn homeserver_pubky() -> String {
    "ybndrfg8ejkmcpqxot1uwisza345h769ybndrfg8ejkmcpqxot1u".to_string()
}

fn keys_json(attestor: &Attestor) -> String {
    json!([{ "pubky": attestor.pubky(), "validFrom": "2026-08-01T00:00:00Z" }]).to_string()
}

fn full_settings(attestor: &Attestor) -> Vec<(&'static str, String)> {
    vec![
        ("ATTESTOR_PUBLISH_ENABLED", "true".to_string()),
        ("ATTESTOR_HOMESERVER", homeserver_pubky()),
        ("ATTESTOR_OPERATOR_URL", OPERATOR_URL.to_string()),
        ("ATTESTOR_POLICY_URL", POLICY_URL.to_string()),
        ("ATTESTOR_KEYS", keys_json(attestor)),
    ]
}

fn read(
    settings: &[(&'static str, String)],
    attestor: Option<&Attestor>,
) -> anyhow::Result<Option<PublishConfig>> {
    let owned: Vec<(&str, String)> = settings.to_vec();
    PublishConfig::from_lookup(
        &|name| {
            owned
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.clone())
        },
        attestor,
    )
}

#[test]
fn publishing_is_off_unless_enabled_and_off_reads_nothing_else() {
    let attestor = test_attestor();
    for pairs in [
        vec![],
        vec![("ATTESTOR_PUBLISH_ENABLED", "false")],
        vec![
            ("ATTESTOR_PUBLISH_ENABLED", "0"),
            ("ATTESTOR_HOMESERVER", "not a pubky"),
        ],
        vec![
            ("ATTESTOR_HOMESERVER", "not a pubky"),
            ("ATTESTOR_KEYS", "{"),
        ],
    ] {
        let config = PublishConfig::from_lookup(&lookup(&pairs), Some(&attestor));
        assert!(config.expect("off is not an error").is_none(), "{pairs:?}");
    }
    let config = PublishConfig::from_lookup(&lookup(&[("ATTESTOR_PUBLISH_ENABLED", "true")]), None);
    assert!(config.is_err(), "enabled without the attestor key refuses");
}

#[test]
fn a_full_configuration_reads_and_defaults_the_operator_name() {
    let attestor = test_attestor();
    let config = read(&full_settings(&attestor), Some(&attestor))
        .expect("valid")
        .expect("enabled");
    assert_eq!(config.homeserver, homeserver_pubky());
    assert_eq!(config.profile.operator.name, "Pubky Market");
    assert_eq!(config.profile.operator.url, OPERATOR_URL);
    assert_eq!(config.profile.keys.len(), 1);
    assert!(config.profile.successor.is_none());

    let mut settings = full_settings(&attestor);
    settings.push(("ATTESTOR_OPERATOR_NAME", "  Other Market ".to_string()));
    settings.push(("ATTESTOR_SUCCESSOR", format!("  {OLD_KEY} ")));
    settings.push(("ATTESTOR_HOMESERVER", format!(" {} ", homeserver_pubky())));
    let config = read(&settings, Some(&attestor)).unwrap().unwrap();
    assert_eq!(config.profile.operator.name, "Other Market");
    assert_eq!(config.profile.successor.as_deref(), Some(OLD_KEY));
}

#[test]
fn a_partial_or_invalid_configuration_is_refused() {
    let attestor = test_attestor();
    for missing in [
        "ATTESTOR_HOMESERVER",
        "ATTESTOR_OPERATOR_URL",
        "ATTESTOR_POLICY_URL",
        "ATTESTOR_KEYS",
    ] {
        let settings: Vec<_> = full_settings(&attestor)
            .into_iter()
            .filter(|(name, _)| *name != missing)
            .collect();
        let error = read(&settings, Some(&attestor)).expect_err(missing);
        assert!(error.to_string().contains(missing), "{missing}: {error}");

        let mut blank = settings;
        blank.push((missing, "  ".to_string()));
        assert!(read(&blank, Some(&attestor)).is_err(), "{missing} blank");
    }

    let invalid = |name: &'static str, value: String| {
        let mut settings: Vec<_> = full_settings(&attestor)
            .into_iter()
            .filter(|(key, _)| *key != name)
            .collect();
        settings.push((name, value));
        read(&settings, Some(&attestor))
    };
    assert!(invalid("ATTESTOR_PUBLISH_ENABLED", "maybe".into()).is_err());
    assert!(invalid("ATTESTOR_HOMESERVER", "short".into()).is_err());
    assert!(invalid(
        "ATTESTOR_OPERATOR_URL",
        "http://pubky-market.example".into()
    )
    .is_err());
    assert!(invalid("ATTESTOR_POLICY_URL", "policy".into()).is_err());
    assert!(invalid("ATTESTOR_KEYS", "not json".into()).is_err());
    assert!(invalid("ATTESTOR_KEYS", "[]".into()).is_err());
    assert!(invalid(
        "ATTESTOR_KEYS",
        r#"[{"pubky":"x","validFrom":"2026-08-01T00:00:00Z"}]"#.into()
    )
    .is_err());
    assert!(
        invalid(
            "ATTESTOR_KEYS",
            json!([{ "pubky": OLD_KEY, "validFrom": "2026-08-01T00:00:00Z" }]).to_string()
        )
        .is_err(),
        "the key list must hold the attestor's own key"
    );
    assert!(invalid(
        "ATTESTOR_KEYS",
        json!([{ "pubky": attestor.pubky(), "validFrom": "yesterday" }]).to_string()
    )
    .is_err());
    assert!(invalid(
        "ATTESTOR_KEYS",
        json!([{ "pubky": attestor.pubky(), "validFrom": "2026-08-01T00:00:00Z", "extra": 1 }])
            .to_string()
    )
    .is_err());
    assert!(invalid("ATTESTOR_SUCCESSOR", "short".into()).is_err());
}
