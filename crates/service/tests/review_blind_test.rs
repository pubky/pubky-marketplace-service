//! Double-blind seller-to-buyer reviews (Pubky Market reviews, SV2): the
//! window, withholding of the counterparty's review and of the reviewer's
//! attestations, release by the second review, a late buyer review or the
//! `review_release` worker, the refusal of a late seller review, and the
//! interleavings between those paths, forced with pauses at the points where
//! the order's revision compare-and-swap and row lock decide who goes first.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use chrono::{DateTime, Duration, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

use common::review_orders::{delivered_order, LISTINGS};
use common::{
    count, execute, new_actor, order_command, send, test_app_full, test_app_with_attestor,
    test_attestor, TestActor, TestApp, NOW,
};
use marketplace_service::clock::{format_timestamp, Clock};
use marketplace_service::config::Config;
use marketplace_service::handlers::reviews::test_faults::{
    arm_after_insert, arm_after_swap, InsertGate,
};
use marketplace_service::review_release::release_due_orders;
use marketplace_service::workers::{drain_outbox, run_once};

const WINDOW_DAYS: i64 = 14;

async fn blind_app(pool: PgPool) -> TestApp {
    let mut config = Config::for_tests();
    config.review_blind_window_days = WINDOW_DAYS;
    test_app_full(pool, config, Some(test_attestor())).await
}

/// The window end of an order delivered at the fixed test instant.
fn window_end() -> DateTime<Utc> {
    NOW.parse::<DateTime<Utc>>().expect("test instant") + Duration::days(WINDOW_DAYS)
}

fn past_the_window(app: &TestApp) {
    app.clock.advance_seconds(WINDOW_DAYS * 24 * 60 * 60 + 1);
}

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

fn review(order_id: &str, revision: i64, number: u64, rating: i64, text: &str) -> Value {
    order_command(
        "review.create",
        order_id,
        revision,
        json!({ "rating": rating, "text": text }),
        number,
    )
}

fn items_review(order_id: &str, revision: i64, items: &[(&str, i64)], number: u64) -> Value {
    let items: Vec<Value> = items
        .iter()
        .map(|(listing_id, rating)| {
            json!({ "listing_id": listing_id, "rating": rating, "text": format!("{listing_id} review") })
        })
        .collect();
    order_command(
        "review.create",
        order_id,
        revision,
        json!({ "items": items, "seller": { "shipping": 5, "communication": 4 } }),
        number,
    )
}

fn spawn_command(
    app: &TestApp,
    actor: &TestActor,
    command: Value,
) -> tokio::task::JoinHandle<(StatusCode, Value)> {
    let router = app.router.clone();
    let token = actor.token.clone();
    tokio::spawn(async move { send(router, "POST", "/v1/commands", Some(&token), &command).await })
}

async fn order_revision(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order revision")
}

async fn window_columns(
    app: &TestApp,
    order_id: &str,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    sqlx::query_as(
        "SELECT review_window_ends_at, reviews_released_at FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("window columns")
}

async fn event_count(app: &TestApp, order_id: &str, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_id = $1 AND kind = $2")
        .bind(format!("order:{order_id}"))
        .bind(kind)
        .fetch_one(&app.pool)
        .await
        .expect("event count")
}

async fn review_row_count(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM reviews WHERE order_id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("review rows")
}

async fn attestation_row_count(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM review_attestations WHERE order_id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("attestation rows")
}

/// Recipients of the order's undelivered-or-delivered `kind` intents, sorted.
async fn intents(app: &TestApp, order_id: &str, kind: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT payload->>'recipient_pubky' FROM outbox \
         WHERE kind = $1 AND payload->>'aggregate_id' = $2 ORDER BY 1",
    )
    .bind(format!("notification.{kind}"))
    .bind(format!("order:{order_id}"))
    .fetch_all(&app.pool)
    .await
    .expect("intents")
}

fn sorted(mut pubkys: Vec<&str>) -> Vec<String> {
    pubkys.sort_unstable();
    pubkys.into_iter().map(str::to_string).collect()
}

async fn one_notification_per_intent(app: &TestApp, order_id: &str) {
    drain_outbox(&app.pool, None, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let outbox: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox WHERE payload->>'aggregate_id' = $1 \
         AND kind IN ('notification.review_waiting', 'notification.review_released', \
                      'notification.review_received')",
    )
    .bind(format!("order:{order_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("outbox rows");
    let delivered: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM notifications WHERE aggregate_id = $1 \
         AND type IN ('review_waiting', 'review_released', 'review_received')",
    )
    .bind(format!("order:{order_id}"))
    .fetch_one(&app.pool)
    .await
    .expect("notifications");
    assert_eq!(
        delivered, outbox,
        "every review intent became a notification: the (event, recipient) key dropped none"
    );
}

async fn notification_rows(app: &TestApp, order_id: &str) -> i64 {
    count(
        &app.pool,
        &format!("SELECT COUNT(*) FROM review_notifications WHERE order_id = '{order_id}'::uuid"),
    )
    .await
}

fn jws_count(attestations: &Value) -> usize {
    attestations["attestations"]
        .as_array()
        .expect("attestations list")
        .iter()
        .filter(|attestation| attestation["jws"].is_string())
        .count()
}

fn redacted(role: &str) -> Value {
    json!([{ "role": role, "submitted": true }])
}

async fn gate_wait_for_blocked_release(app: &TestApp) {
    for _ in 0..400 {
        let waiting = count(
            &app.pool,
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND query LIKE 'UPDATE orders SET reviews_released_at%'",
        )
        .await;
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("the worker never blocked on the create's row lock");
}

/// With the window at 0 nothing about reviews changes: no new field in a
/// result, nothing withheld, `review_received` at the review itself, no
/// window stamp and no release.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn window_off_leaves_reviews_exactly_as_they_were(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert!(result.get("attestation_status").is_none(), "{result}");
    assert!(result.get("release_at").is_none(), "{result}");
    assert!(result["attestation"]["jws"].is_string());
    assert_eq!(result["attestations"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["result"]["order"]["revision"], json!(5));

    let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(
        seller_view["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    assert_eq!(seller_view["reviews"][0]["reviewer_role"], json!("buyer"));

    for uri in ["review-attestation", "review-attestations"] {
        let (status, attestation) =
            get(&app, &format!("/v1/orders/{order_id}/{uri}"), &buyer.token).await;
        assert_eq!(status, StatusCode::OK);
        assert!(attestation.get("status").is_none(), "{attestation}");
    }

    assert_eq!(window_columns(&app, &order_id).await, (None, None));
    assert_eq!(event_count(&app, &order_id, "review.released").await, 0);
    assert_eq!(
        intents(&app, &order_id, "review_received").await,
        vec![seller.pubky.clone()]
    );
    assert!(intents(&app, &order_id, "review_waiting").await.is_empty());
    assert!(intents(&app, &order_id, "review_released").await.is_empty());
    assert_eq!(notification_rows(&app, &order_id).await, 0);
    assert_eq!(
        release_due_orders(&app.pool, app.clock.now(), 100, 10)
            .await
            .expect("worker pass"),
        0
    );
}

/// Inside the window the first review is stored and attested but nothing is
/// served: the result is pending, both attestation routes answer pending,
/// and the counterparty's views of the order show only that the role
/// submitted, in every projection that carries reviews.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_review_inside_the_window_is_withheld_everywhere(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert_eq!(result["attestation_status"], json!("pending"));
    assert_eq!(result["release_at"], json!(format_timestamp(window_end())));
    assert!(result.get("attestation").is_none(), "{result}");
    assert!(result.get("attestations").is_none(), "{result}");
    assert!(
        !result.to_string().contains(".eyJ"),
        "no JWS anywhere in the result"
    );
    assert_eq!(
        result["review"]["text"],
        json!("Boots arrived fast."),
        "the reviewer reads their own review"
    );
    assert_eq!(
        result["order"]["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );

    // Stored as ever, append-only, and withheld only on the way out.
    assert_eq!(review_row_count(&app, &order_id).await, 1);
    assert_eq!(attestation_row_count(&app, &order_id).await, 1);
    assert_eq!(
        window_columns(&app, &order_id).await,
        (Some(window_end()), None)
    );
    assert_eq!(event_count(&app, &order_id, "review.released").await, 0);

    // The first review tells the counterparty, once, that one is waiting;
    // `review_received` waits for release.
    assert_eq!(
        intents(&app, &order_id, "review_waiting").await,
        vec![seller.pubky.clone()]
    );
    assert!(intents(&app, &order_id, "review_received").await.is_empty());

    // Both attestation routes: pending for the reviewer, as ever for the
    // participant with nothing to withhold.
    for uri in ["review-attestation", "review-attestations"] {
        let (status, pending) =
            get(&app, &format!("/v1/orders/{order_id}/{uri}"), &buyer.token).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(
            pending,
            json!({ "status": "pending", "releaseAt": format_timestamp(window_end()) }),
            "{uri}"
        );
    }
    let (status, _) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, none) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(none, json!({ "attestations": [] }));

    // The seller's list and single views.
    let (_, listed) = get(&app, "/v1/orders", &seller.token).await;
    let listed_order = listed["orders"]
        .as_array()
        .expect("orders")
        .iter()
        .find(|order| order["id"] == json!(order_id))
        .expect("the order is listed");
    assert_eq!(listed_order["reviews"], redacted("buyer"));
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(single["reviews"], redacted("buyer"));
    assert!(!single.to_string().contains("Boots arrived fast."));
    assert!(!listed.to_string().contains("Boots arrived fast."));

    // The buyer's own views carry the review in full.
    let (_, listed) = get(&app, "/v1/orders", &buyer.token).await;
    assert_eq!(
        listed["orders"][0]["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(single["reviews"][0]["text"], json!("Boots arrived fast."));

    // A command result the seller receives: the return flow returns the
    // order with its reviews.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "return.request",
            &order_id,
            5,
            json!({ "reason": "Item not as described", "requested_amount_minor": 13_700 }),
            101,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    let (status, body) = execute(
        &app,
        &seller.token,
        &order_command("return.approve", &order_id, 6, json!({}), 102),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["reviews"], redacted("buyer"));
    assert!(!body.to_string().contains("Boots arrived fast."));
}

/// A buyer who reviews three items shows the seller one withheld entry, not
/// three: the number of items reviewed is not disclosed either.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withheld_buyer_shows_one_entry_however_many_items(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(
            &order_id,
            4,
            &[("boots_01", 5), ("laces_02", 4), ("socks_03", 3)],
            100,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["reviews"].as_array().map(Vec::len), Some(3));

    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(single["reviews"], redacted("buyer"));
    let (_, buyer_view) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(buyer_view["reviews"].as_array().map(Vec::len), Some(3));
}

/// The second role's review releases both in its own transaction: one
/// `review.released` event after the `review.created`, every attestation
/// served, every review readable, and each intent once. A participant who
/// reviewed hears both `review_released` and `review_received`, and the
/// notifications table keeps one row per event and recipient, so none is
/// dropped.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_second_review_releases_the_pair_once(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 5, 101, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert_eq!(result["attestation_status"], json!("released"));
    assert!(result["attestation"]["jws"].is_string());
    assert_eq!(result["order"]["reviews"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        result["order"]["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    assert_eq!(
        body["result"]["order"]["revision"],
        json!(7),
        "created at 6, released at 7"
    );

    assert_eq!(order_revision(&app, &order_id).await, 7);
    assert_eq!(event_count(&app, &order_id, "review.created").await, 2);
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    let (ends, released) = window_columns(&app, &order_id).await;
    assert_eq!(ends, Some(window_end()));
    assert_eq!(released, Some(app.clock.now()));

    for actor in [&buyer, &seller] {
        for uri in ["review-attestation", "review-attestations"] {
            let (status, attestation) =
                get(&app, &format!("/v1/orders/{order_id}/{uri}"), &actor.token).await;
            assert_eq!(status, StatusCode::OK);
            assert!(attestation.get("status").is_none(), "{attestation}");
        }
    }
    let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(
        seller_view["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    let (_, buyer_view) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(buyer_view["reviews"][1]["text"], json!("Paid at once."));

    assert_eq!(
        intents(&app, &order_id, "review_waiting").await,
        vec![seller.pubky.clone()]
    );
    let both = sorted(vec![&buyer.pubky, &seller.pubky]);
    assert_eq!(intents(&app, &order_id, "review_released").await, both);
    assert_eq!(intents(&app, &order_id, "review_received").await, both);
    assert_eq!(notification_rows(&app, &order_id).await, 5);
    one_notification_per_intent(&app, &order_id).await;
}

/// A buyer and a seller review at the same moment. Both insert and pause;
/// the revision compare-and-swap admits one, whose transaction commits with
/// the other role's rows still uncommitted, so the review stays pending. The
/// other is refused and, on retry, sees both reviews and releases. However
/// the race falls: one release, one `review.released` event, each intent
/// once, and both attestations fetchable afterwards.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn interleaved_buyer_and_seller_creates_release_exactly_once(pool: PgPool) {
    let app = blind_app(pool).await;
    for round in 0..4u64 {
        let base = 1_000 * (round + 1);
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
        let order_uuid: Uuid = order_id.parse().expect("uuid");

        let gate = InsertGate {
            arrived: Arc::new(Barrier::new(3)),
            release: Arc::new(Barrier::new(3)),
        };
        arm_after_insert(order_uuid, gate.clone());
        arm_after_insert(order_uuid, gate.clone());
        let buyer_review = review(&order_id, 4, base, 5, "Boots arrived fast.");
        let seller_review = review(&order_id, 4, base + 1, 4, "Paid at once.");
        let from_buyer = spawn_command(&app, &buyer, buyer_review);
        let from_seller = spawn_command(&app, &seller, seller_review);
        gate.arrived.wait().await;
        gate.release.wait().await;
        let buyer_outcome = from_buyer.await.expect("buyer completes");
        let seller_outcome = from_seller.await.expect("seller completes");

        let statuses = [buyer_outcome.0, seller_outcome.0];
        assert!(
            statuses.contains(&StatusCode::OK) && statuses.contains(&StatusCode::CONFLICT),
            "round {round}: exactly one wins: {buyer_outcome:?} {seller_outcome:?}"
        );
        let (winner, winner_body, loser, loser_body) = if buyer_outcome.0 == StatusCode::OK {
            (&buyer, &buyer_outcome.1, &seller, &seller_outcome.1)
        } else {
            (&seller, &seller_outcome.1, &buyer, &buyer_outcome.1)
        };
        assert_eq!(loser_body["error"]["code"], json!("REVISION_CONFLICT"));
        assert_eq!(loser_body["error"]["current_revision"], json!(5));
        assert_eq!(
            winner_body["result"]["attestation_status"],
            json!("pending"),
            "the winner committed without the other role's rows"
        );
        assert!(winner_body["result"].get("attestation").is_none());
        assert_eq!(review_row_count(&app, &order_id).await, 1);
        assert_eq!(event_count(&app, &order_id, "review.released").await, 0);
        assert_eq!(
            intents(&app, &order_id, "review_waiting").await,
            vec![loser.pubky.clone()]
        );

        let (retry_number, text) = (base + 2, "Retried.");
        let (status, retry) = execute(
            &app,
            &loser.token,
            &review(&order_id, 5, retry_number, 4, text),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "round {round} retry: {retry}");
        assert_eq!(retry["result"]["attestation_status"], json!("released"));
        assert!(retry["result"]["attestation"]["jws"].is_string());
        assert_eq!(
            retry["result"]["order"]["reviews"].as_array().map(Vec::len),
            Some(2)
        );

        assert_eq!(review_row_count(&app, &order_id).await, 2);
        assert_eq!(attestation_row_count(&app, &order_id).await, 2);
        assert_eq!(order_revision(&app, &order_id).await, 7, "round {round}");
        assert_eq!(event_count(&app, &order_id, "review.created").await, 2);
        assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
        assert!(window_columns(&app, &order_id).await.1.is_some());
        assert_eq!(
            intents(&app, &order_id, "review_waiting").await,
            vec![loser.pubky.clone()]
        );
        let both = sorted(vec![&buyer.pubky, &seller.pubky]);
        assert_eq!(intents(&app, &order_id, "review_released").await, both);
        assert_eq!(intents(&app, &order_id, "review_received").await, both);
        assert_eq!(notification_rows(&app, &order_id).await, 5);
        one_notification_per_intent(&app, &order_id).await;

        for actor in [winner, loser] {
            let (status, attestations) = get(
                &app,
                &format!("/v1/orders/{order_id}/review-attestations"),
                &actor.token,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(jws_count(&attestations), 1, "round {round}");
        }
        let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
        assert_eq!(seller_view["reviews"].as_array().map(Vec::len), Some(2));
        assert!(seller_view["reviews"]
            .as_array()
            .expect("reviews")
            .iter()
            .all(|entry| entry["text"].is_string()));
    }
}

/// The window ends with the seller's review still alone. A late buyer create
/// pauses between its inserts and the compare-and-swap while the worker
/// releases the order: the buyer's swap fails on the bumped revision, and its
/// retry sees a released order and publishes at once. One release.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_releases_beneath_a_late_buyer_create(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");
    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 4, 100, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("pending"));
    past_the_window(&app);

    let gate = InsertGate {
        arrived: Arc::new(Barrier::new(2)),
        release: Arc::new(Barrier::new(2)),
    };
    arm_after_insert(order_uuid, gate.clone());
    let from_buyer = spawn_command(
        &app,
        &buyer,
        review(&order_id, 5, 101, 5, "Boots arrived fast."),
    );
    gate.arrived.wait().await;
    let released = release_due_orders(&app.pool, app.clock.now(), 100, 10)
        .await
        .expect("worker pass");
    assert_eq!(released, 1);
    gate.release.wait().await;
    let (status, refused) = from_buyer.await.expect("buyer completes");
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(refused["error"]["current_revision"], json!(6));
    assert_eq!(
        review_row_count(&app, &order_id).await,
        1,
        "nothing it wrote remains"
    );

    let (status, retry) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 6, 102, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["result"]["attestation_status"], json!("released"));
    assert!(retry["result"]["attestation"]["jws"].is_string());

    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    assert_eq!(event_count(&app, &order_id, "review.created").await, 2);
    assert_eq!(order_revision(&app, &order_id).await, 7);
    assert_eq!(
        intents(&app, &order_id, "review_waiting").await,
        vec![buyer.pubky.clone()]
    );
    assert_eq!(
        intents(&app, &order_id, "review_released").await,
        vec![seller.pubky.clone()],
        "the buyer's review published at once, with its JWS in the result"
    );
    assert_eq!(
        intents(&app, &order_id, "review_received").await,
        sorted(vec![&buyer.pubky, &seller.pubky])
    );
    one_notification_per_intent(&app, &order_id).await;
    for actor in [&buyer, &seller] {
        let (_, attestations) = get(
            &app,
            &format!("/v1/orders/{order_id}/review-attestations"),
            &actor.token,
        )
        .await;
        assert_eq!(jws_count(&attestations), 1);
    }
}

/// The other order of the same race: the late buyer create wins the
/// compare-and-swap and holds the order's row lock while the worker starts.
/// The worker blocks on its conditional update and, once the create commits
/// its release, finds nothing to do. One release.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_buyer_create_releases_beneath_the_worker(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");
    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 4, 100, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    past_the_window(&app);

    let gate = InsertGate {
        arrived: Arc::new(Barrier::new(2)),
        release: Arc::new(Barrier::new(2)),
    };
    arm_after_swap(order_uuid, gate.clone());
    let from_buyer = spawn_command(
        &app,
        &buyer,
        review(&order_id, 5, 101, 5, "Boots arrived fast."),
    );
    gate.arrived.wait().await;
    let pool = app.pool.clone();
    let now = app.clock.now();
    let worker = tokio::spawn(async move { release_due_orders(&pool, now, 100, 10).await });
    gate_wait_for_blocked_release(&app).await;
    gate.release.wait().await;

    let (status, body) = from_buyer.await.expect("buyer completes");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert!(body["result"]["attestation"]["jws"].is_string());
    assert_eq!(
        worker.await.expect("worker joins").expect("worker pass"),
        0,
        "the create released the order first"
    );

    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    assert_eq!(event_count(&app, &order_id, "review.created").await, 2);
    assert_eq!(order_revision(&app, &order_id).await, 7);
    let both = sorted(vec![&buyer.pubky, &seller.pubky]);
    assert_eq!(
        intents(&app, &order_id, "review_waiting").await,
        vec![buyer.pubky.clone()]
    );
    assert_eq!(intents(&app, &order_id, "review_released").await, both);
    assert_eq!(intents(&app, &order_id, "review_received").await, both);
    one_notification_per_intent(&app, &order_id).await;
}

/// A lone review is released by the worker when its window ends, not
/// before, and a replayed tick is a no-op: no second event, intent or
/// revision.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_releases_a_lone_review_and_a_replayed_tick_does_nothing(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 4, 100, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let holder = Uuid::new_v4();
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("a tick inside the window");
    assert_eq!(summary.reviews_released, 0, "the window is still open");
    assert_eq!(event_count(&app, &order_id, "review.released").await, 0);

    past_the_window(&app);
    let summary = run_once(&app.state, holder, app.clock.now())
        .await
        .expect("a tick after the window");
    assert_eq!(summary.reviews_released, 1);
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    assert_eq!(order_revision(&app, &order_id).await, 6);
    let (ends, released) = window_columns(&app, &order_id).await;
    assert_eq!(ends, Some(window_end()));
    assert_eq!(released, Some(app.clock.now()));
    assert_eq!(
        intents(&app, &order_id, "review_released").await,
        vec![seller.pubky.clone()]
    );
    assert_eq!(
        intents(&app, &order_id, "review_received").await,
        vec![buyer.pubky.clone()]
    );

    let (_, attestations) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &seller.token,
    )
    .await;
    assert_eq!(jws_count(&attestations), 1);
    let (_, buyer_view) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(buyer_view["reviews"][0]["text"], json!("Paid at once."));

    let outbox_before: i64 = count(&app.pool, "SELECT COUNT(*) FROM outbox").await;
    for _ in 0..2 {
        let replay = run_once(&app.state, holder, app.clock.now())
            .await
            .expect("a replayed tick");
        assert_eq!(replay.reviews_released, 0);
        assert_eq!(
            release_due_orders(&app.pool, app.clock.now(), 100, 10)
                .await
                .expect("a direct pass"),
            0
        );
    }
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    assert_eq!(order_revision(&app, &order_id).await, 6);
    assert_eq!(
        count(&app.pool, "SELECT COUNT(*) FROM outbox").await,
        outbox_before
    );
    assert_eq!(notification_rows(&app, &order_id).await, 3);
    one_notification_per_intent(&app, &order_id).await;
}

/// Past the window a seller's review is refused, with or without a buyer
/// review, and the refusal writes nothing.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_seller_review_is_refused(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    past_the_window(&app);

    let (status, refused) = execute(
        &app,
        &seller.token,
        &review(&order_id, 4, 100, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(refused["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        refused["error"]["message"],
        json!("The review window has closed.")
    );
    assert_eq!(review_row_count(&app, &order_id).await, 0);
    assert_eq!(attestation_row_count(&app, &order_id).await, 0);
    assert_eq!(window_columns(&app, &order_id).await, (None, None));
    assert_eq!(order_revision(&app, &order_id).await, 4);

    // A buyer's late review publishes at once and does not reopen the
    // seller's side.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 4, 101, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert!(body["result"]["attestation"]["jws"].is_string());
    let (status, refused) = execute(
        &app,
        &seller.token,
        &review(&order_id, 6, 102, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refused}");
    assert_eq!(
        refused["error"]["message"],
        json!("The review window has closed.")
    );
    assert_eq!(review_row_count(&app, &order_id).await, 1);
}

/// A buyer who reviews after the window publishes at once: the review is
/// released in its own transaction, the seller reads it in full, and the
/// seller is told it was received. Nothing waits, so nobody is told to wait,
/// and items added afterwards tell nobody a second time.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_late_buyer_review_publishes_at_once(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;
    past_the_window(&app);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 4, &[("boots_01", 5)], 100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert_eq!(
        body["result"]["attestations"].as_array().map(Vec::len),
        Some(1)
    );
    assert!(body["result"]["attestation"]["jws"].is_string());
    assert_eq!(
        body["result"]["order"]["revision"],
        json!(6),
        "created at 5, released at 6"
    );
    assert_eq!(event_count(&app, &order_id, "review.created").await, 1);
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    let (ends, released) = window_columns(&app, &order_id).await;
    assert_eq!(ends, Some(window_end()));
    assert_eq!(released, Some(app.clock.now()));

    let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(seller_view["reviews"][0]["text"], json!("boots_01 review"));
    assert!(intents(&app, &order_id, "review_waiting").await.is_empty());
    assert_eq!(
        intents(&app, &order_id, "review_released").await,
        vec![buyer.pubky.clone()]
    );
    assert_eq!(
        intents(&app, &order_id, "review_received").await,
        vec![seller.pubky.clone()]
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 6, &[("laces_02", 4)], 101),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert!(body["result"]["attestation"]["jws"].is_string());
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    assert_eq!(
        intents(&app, &order_id, "review_released").await,
        vec![buyer.pubky.clone()]
    );
    assert_eq!(
        intents(&app, &order_id, "review_received").await,
        vec![seller.pubky.clone()]
    );
    assert_eq!(notification_rows(&app, &order_id).await, 2);
    one_notification_per_intent(&app, &order_id).await;
}

/// A buyer's per-item review inside the window: every item's attestation is
/// stored and withheld, items added in a later create stay withheld, the
/// counterparty is told once, and the seller's review releases every item's
/// attestation together.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn per_item_buyer_attestations_are_withheld_and_released_together(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 4, &[("boots_01", 5), ("laces_02", 4)], 100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("pending"));
    assert!(body["result"].get("attestations").is_none());
    assert!(body["result"].get("attestation").is_none());
    assert_eq!(attestation_row_count(&app, &order_id).await, 2);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 5, &[("socks_03", 3)], 101),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("pending"));
    assert!(body["result"].get("attestations").is_none());
    assert_eq!(attestation_row_count(&app, &order_id).await, 3);
    assert_eq!(
        intents(&app, &order_id, "review_waiting").await,
        vec![seller.pubky.clone()],
        "one intent per order, kind and recipient, however many creates"
    );
    for uri in ["review-attestation", "review-attestations"] {
        let (status, pending) =
            get(&app, &format!("/v1/orders/{order_id}/{uri}"), &buyer.token).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(pending["status"], json!("pending"), "{uri}");
        assert!(pending.get("attestations").is_none() && pending.get("attestation").is_none());
    }

    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 6, 102, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));

    let (_, attestations) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &buyer.token,
    )
    .await;
    assert_eq!(
        jws_count(&attestations),
        3,
        "every item's attestation, at once"
    );
    let listing_ids: Vec<&str> = attestations["attestations"]
        .as_array()
        .expect("attestations")
        .iter()
        .map(|attestation| attestation["listing_id"].as_str().expect("listing id"))
        .collect();
    assert_eq!(listing_ids, vec!["boots_01", "laces_02", "socks_03"]);
    let (status, first) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(first["attestation"]["jws"].is_string());
    assert_eq!(event_count(&app, &order_id, "review.released").await, 1);
    let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(seller_view["reviews"].as_array().map(Vec::len), Some(4));
    one_notification_per_intent(&app, &order_id).await;
}

/// Reviews written while the window was off are already public: turning it
/// on later marks the order released without an event and withholds nothing.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn reviews_written_before_the_window_stay_public(pool: PgPool) {
    let off = test_app_with_attestor(pool.clone()).await;
    let on = blind_app(pool).await;
    let seller = new_actor(&off).await;
    let buyer = new_actor(&off).await;
    let order_id = delivered_order(&off, &seller, &buyer, &LISTINGS[..1]).await;
    let (status, body) = execute(
        &off,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &on,
        &seller.token,
        &review(&order_id, 5, 101, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert!(result.get("attestation_status").is_none(), "{result}");
    assert!(result["attestation"]["jws"].is_string());
    assert_eq!(
        result["order"]["reviews"][0]["text"],
        json!("Boots arrived fast.")
    );
    let (ends, released) = window_columns(&on, &order_id).await;
    assert_eq!(ends, None);
    assert_eq!(released, Some(on.clock.now()));
    assert_eq!(event_count(&on, &order_id, "review.released").await, 0);
    assert_eq!(
        intents(&on, &order_id, "review_received").await,
        sorted(vec![&buyer.pubky, &seller.pubky])
    );
    assert!(intents(&on, &order_id, "review_waiting").await.is_empty());
}

/// An order that already carries a window stays double-blind if the setting
/// is later set back to 0: the second review still releases the pair.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_with_a_window_stays_blind_when_the_setting_is_cleared(pool: PgPool) {
    let on = blind_app(pool.clone()).await;
    let off = test_app_with_attestor(pool).await;
    let seller = new_actor(&on).await;
    let buyer = new_actor(&on).await;
    let order_id = delivered_order(&on, &seller, &buyer, &LISTINGS[..1]).await;
    let (status, body) = execute(
        &on,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("pending"));

    let (_, seller_view) = get(&off, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(seller_view["reviews"], redacted("buyer"));
    let (status, body) = execute(
        &off,
        &seller.token,
        &review(&order_id, 5, 101, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert_eq!(event_count(&off, &order_id, "review.released").await, 1);
    assert!(window_columns(&off, &order_id).await.1.is_some());
}

/// Editing a withheld review inside the edit window changes what the
/// counterparty will read at release, not what they can read now, and its
/// result serves no attestation either.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_edit_of_a_withheld_review_stays_withheld(pool: PgPool) {
    let app = blind_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let (status, body) = execute(
        &app,
        &buyer.token,
        &review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            5,
            json!({ "rating": 4, "text": "Boots arrived late." }),
            101,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert_eq!(result["attestation_status"], json!("pending"));
    assert_eq!(result["release_at"], json!(format_timestamp(window_end())));
    assert!(result.get("attestation").is_none() && result.get("attestations").is_none());
    assert_eq!(result["review"]["text"], json!("Boots arrived late."));
    let (_, seller_view) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(seller_view["reviews"], redacted("buyer"));

    let (status, body) = execute(
        &app,
        &seller.token,
        &review(&order_id, 6, 102, 4, "Paid at once."),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["order"]["reviews"][0]["text"],
        json!("Boots arrived late.")
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            8,
            json!({ "rating": 5, "text": "Boots arrived at last." }),
            103,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["attestation_status"], json!("released"));
    assert!(body["result"]["attestation"]["jws"].is_string());
}
