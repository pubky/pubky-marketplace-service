//! The authenticated inventory view (`GET /v1/inventory/listings/{id}`) is
//! served only to the listing's seller, and its rate budget belongs to the
//! seller rather than to one session. Every request drives the real router,
//! genuine AuthToken verification, the persisted bearer session and
//! PostgreSQL.

mod common;

use std::time::Duration;

use axum::http::StatusCode;
use chrono::Utc;
use common::{
    authenticate, execute, indexed_command_id, listing_aggregate, new_actor, random_keypair,
    register_command, send, send_bytes, send_with_headers, test_app, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use pubky_common::auth::AuthToken;
use pubky_common::capabilities::Capability;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const INVENTORY_SCOPE: &str = "/pub/pubky.app/marketplace-service/v1/";
const DEFAULT_BURST_TOKENS: usize = 240;

async fn projection(app: &TestApp, token: &str, aggregate_id: &str) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        &format!("/v1/inventory/listings/{aggregate_id}"),
        Some(token),
        &Value::Null,
    )
    .await
}

async fn register(app: &TestApp, seller: &TestActor, quantity: i64) {
    let (status, body) = execute(
        app,
        &seller.token,
        &register_command(&seller.pubky, quantity),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "listing registration: {body}");
}

async fn actor_with_capabilities(app: &TestApp, capabilities: Vec<Capability>) -> TestActor {
    let (keypair, pubky) = random_keypair();
    let fixture_now = app.clock.now();
    app.clock.set(Utc::now());
    let (status, body) = send_bytes(
        app.router.clone(),
        "POST",
        "/v1/auth/sessions",
        AuthToken::sign(&keypair, capabilities).serialize(),
    )
    .await;
    app.clock.set(fixture_now);
    assert_eq!(status, StatusCode::CREATED, "session creation: {body}");
    TestActor {
        keypair,
        pubky,
        token: body["token"].as_str().expect("bearer token").to_string(),
    }
}

fn adjust_request(seller_pubky: &str, expected_revision: i64, idempotency_key: Uuid) -> Value {
    json!({
        "schema_version": 1,
        "kind": "inventory.adjust",
        "aggregate_id": listing_aggregate(seller_pubky),
        "listing_id": "boots_01",
        "expected_revision": expected_revision,
        "delta": 1,
        "idempotency_key": idempotency_key,
    })
}

async fn adjust(app: &TestApp, token: &str, body: &Value) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "POST",
        "/v1/inventory/adjust",
        Some(token),
        body,
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn inventory_view_is_served_to_its_seller(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = actor_with_capabilities(
        &app,
        vec![Capability::read_write(INVENTORY_SCOPE).expect("exact scope")],
    )
    .await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);

    let (status, body) = projection(&app, &seller.token, &aggregate).await;

    assert_eq!(status, StatusCode::OK, "seller read: {body}");
    assert_eq!(body["seller_pubky"], json!(seller.pubky));
    assert_eq!(body["aggregate_id"], json!(aggregate));
    assert_eq!(body["stock"]["total"], json!(3));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn broader_grant_seller_reads_own_inventory(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = actor_with_capabilities(&app, vec![Capability::root()]).await;
    let stored: String =
        sqlx::query_scalar("SELECT capabilities FROM auth_sessions WHERE pubky = $1")
            .bind(&seller.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("stored grant");
    assert_ne!(
        stored, "/pub/pubky.app/marketplace-service/v1/:rw",
        "the grant must be broader than the exact inventory scope"
    );
    register(&app, &seller, 4).await;
    let aggregate = listing_aggregate(&seller.pubky);

    let (status, body) = projection(&app, &seller.token, &aggregate).await;

    assert_eq!(status, StatusCode::OK, "root-grant seller read: {body}");
    assert_eq!(body["seller_pubky"], json!(seller.pubky));
    assert_eq!(body["stock"]["total"], json!(4));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn inventory_view_is_not_served_to_another_seller(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let exact_other = actor_with_capabilities(
        &app,
        vec![Capability::read_write(INVENTORY_SCOPE).expect("exact scope")],
    )
    .await;
    let root_other = actor_with_capabilities(&app, vec![Capability::root()]).await;

    let (status, body) = projection(&app, &seller.token, &aggregate).await;
    assert_eq!(status, StatusCode::OK, "seller read: {body}");

    for other in [&exact_other, &root_other] {
        let (status, foreign) = projection(&app, &other.token, &aggregate).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "another seller read the inventory view: {foreign}"
        );
        assert_eq!(foreign["error"]["code"], json!("listing_not_found"));
        let missing_aggregate = listing_aggregate(&other.pubky);
        let (status, missing) = projection(&app, &other.token, &missing_aggregate).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
        assert_eq!(
            foreign, missing,
            "a foreign listing answers exactly like a missing one"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn sessions_of_one_seller_share_one_inventory_budget(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let second_token = authenticate(&app, &seller.keypair).await;

    // The clock is frozen, so nothing refills: the two sessions together get
    // exactly one burst.
    for index in 0..DEFAULT_BURST_TOKENS {
        let token = if index % 2 == 0 {
            &seller.token
        } else {
            &second_token
        };
        let (status, body) = projection(&app, token, &aggregate).await;
        assert_eq!(status, StatusCode::OK, "read {index}: {body}");
    }
    for token in [&seller.token, &second_token] {
        let (status, headers, body) = send_with_headers(
            app.router.clone(),
            "GET",
            &format!("/v1/inventory/listings/{aggregate}"),
            Some(token),
            &Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
        assert_eq!(body["error"]["code"], json!("rate_limited"));
        assert_eq!(body["error"]["limit_class"], json!("inventory.read"));
        assert_eq!(
            headers
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    let third_token = authenticate(&app, &seller.keypair).await;
    let (status, body) = projection(&app, &third_token, &aggregate).await;
    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "a new session of the same seller got a fresh inventory budget: {body}"
    );

    // Budgets are per endpoint class: adjusts keep their own bucket.
    let (status, body) = adjust(
        &app,
        &third_token,
        &adjust_request(&seller.pubky, 1, Uuid::new_v4()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "adjust after reads ran out: {body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn another_seller_keeps_its_own_inventory_budget(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let other = new_actor(&app).await;
    register(&app, &seller, 3).await;
    register(&app, &other, 2).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let other_aggregate = listing_aggregate(&other.pubky);

    for index in 0..DEFAULT_BURST_TOKENS {
        let (status, body) = projection(&app, &seller.token, &aggregate).await;
        assert_eq!(status, StatusCode::OK, "read {index}: {body}");
    }
    let (status, _) = projection(&app, &seller.token, &aggregate).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

    let (status, body) = projection(&app, &other.token, &other_aggregate).await;
    assert_eq!(status, StatusCode::OK, "other seller's own budget: {body}");
    let other_second = authenticate(&app, &other.keypair).await;
    let (status, body) = projection(&app, &other_second, &other_aggregate).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "other seller's second session: {body}"
    );
}

/// Spends the seller's `inventory.adjust` bucket down to exactly one token
/// with stale-revision requests (each refused request still costs a token).
async fn leave_one_adjust_token(app: &TestApp, seller: &TestActor, second_token: &str) {
    for index in 0..(DEFAULT_BURST_TOKENS - 1) {
        let token = if index % 2 == 0 {
            seller.token.as_str()
        } else {
            second_token
        };
        let stale = adjust_request(
            &seller.pubky,
            99,
            Uuid::parse_str(&indexed_command_id(0x9940, index as u64)).expect("fixture uuid"),
        );
        let (status, body) = adjust(app, token, &stale).await;
        assert_eq!(status, StatusCode::CONFLICT, "index {index}: {body}");
        assert_eq!(body["error"]["code"], json!("revision_conflict"));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_adjusts_from_two_sessions_race_for_the_last_token(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let second_token = authenticate(&app, &seller.keypair).await;
    leave_one_adjust_token(&app, &seller, &second_token).await;

    // Hold the seller's rate row so both requests queue behind it, then
    // release them together.
    let mut gate = app.pool.begin().await.expect("gate transaction");
    sqlx::query(
        "SELECT tokens FROM inventory_actor_rate_limits \
         WHERE actor_pubky = $1 AND endpoint_class = 'inventory.adjust' FOR UPDATE",
    )
    .bind(&seller.pubky)
    .fetch_one(&mut *gate)
    .await
    .expect("seller rate row locks");
    let mut tasks = Vec::new();
    for token in [seller.token.clone(), second_token.clone()] {
        let router = app.router.clone();
        let body = adjust_request(&seller.pubky, 1, Uuid::new_v4());
        tasks.push(tokio::spawn(async move {
            send(router, "POST", "/v1/inventory/adjust", Some(&token), &body).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.rollback().await.expect("release rate row");

    let mut statuses = Vec::new();
    for task in tasks {
        let (status, body) = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("adjust finishes")
            .expect("adjust task");
        statuses.push((status, body));
    }
    let mut codes: Vec<u16> = statuses.iter().map(|(status, _)| status.as_u16()).collect();
    codes.sort_unstable();
    assert_eq!(
        codes,
        vec![200, 429],
        "exactly one adjust gets the last shared token: {statuses:?}"
    );
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = 'inventory.adjusted'",
    )
    .bind(&aggregate)
    .fetch_one(&app.pool)
    .await
    .expect("event count");
    assert_eq!(events, 1);
    let tokens: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_actor_rate_limits \
         WHERE actor_pubky = $1 AND endpoint_class = 'inventory.adjust'",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("shared rate row");
    assert!(tokens < 1.0, "the shared bucket is spent, got {tokens}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_adjusts_from_two_sessions_do_not_deadlock(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    register(&app, &seller, 3).await;
    let aggregate = listing_aggregate(&seller.pubky);
    let second_token = authenticate(&app, &seller.keypair).await;

    const PER_SESSION: usize = 12;
    let mut tasks = Vec::new();
    for round in 0..PER_SESSION {
        for (session, token) in [&seller.token, &second_token].into_iter().enumerate() {
            let router = app.router.clone();
            let token = token.clone();
            let pubky = seller.pubky.clone();
            let aggregate = aggregate.clone();
            tasks.push(tokio::spawn(async move {
                if round % 3 == 2 {
                    send(
                        router,
                        "GET",
                        &format!("/v1/inventory/listings/{aggregate}"),
                        Some(&token),
                        &Value::Null,
                    )
                    .await
                } else {
                    let key =
                        Uuid::parse_str(&indexed_command_id(0x9950 + session as u16, round as u64))
                            .expect("fixture uuid");
                    let body = adjust_request(&pubky, 1, key);
                    send(router, "POST", "/v1/inventory/adjust", Some(&token), &body).await
                }
            }));
        }
    }
    let mut accepted = 0;
    for task in tasks {
        let (status, body) = tokio::time::timeout(Duration::from_secs(20), task)
            .await
            .expect("no request hangs on a lock cycle")
            .expect("request task");
        assert!(
            matches!(
                status,
                StatusCode::OK | StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS
            ),
            "unexpected outcome (a lock cycle surfaces as 500): {status} {body}"
        );
        if status == StatusCode::OK && body["kind"] != json!("inventory_projection") {
            accepted += 1;
        }
    }
    assert_eq!(
        accepted, 1,
        "only one adjust can win expected_revision 1 across both sessions"
    );
    let spent: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_actor_rate_limits \
         WHERE actor_pubky = $1 AND endpoint_class = 'inventory.adjust'",
    )
    .bind(&seller.pubky)
    .fetch_one(&app.pool)
    .await
    .expect("shared rate row");
    let adjusts = (0..PER_SESSION).filter(|round| round % 3 != 2).count() * 2;
    assert_eq!(
        spent,
        (DEFAULT_BURST_TOKENS - adjusts) as f64,
        "every adjust from both sessions spent one token from one bucket"
    );
}
