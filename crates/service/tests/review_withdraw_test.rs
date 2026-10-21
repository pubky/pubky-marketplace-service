//! Review withdrawal (`review.withdraw`): the reviewer's own record that
//! they removed their public review, so another device does not republish
//! it. The flag is set once, cleared by an edit, shown to the author only
//! (never to the counterparty, never through the double-blind redaction), and
//! raced against itself and `review.update` at the points where the order's
//! row lock and revision compare-and-swap decide who goes first.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

use common::review_orders::{delivered_order, LISTINGS};
use common::{
    count, execute, new_actor, order_command, send, test_app_full, test_app_with_attestor,
    test_attestor, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::handlers::reviews::test_faults::{arm_after_edit, InsertGate};

fn review(order_id: &str, revision: i64, number: u64, rating: i64, text: &str) -> Value {
    order_command(
        "review.create",
        order_id,
        revision,
        json!({ "rating": rating, "text": text }),
        number,
    )
}

fn update(order_id: &str, revision: i64, number: u64, rating: i64, text: &str) -> Value {
    order_command(
        "review.update",
        order_id,
        revision,
        json!({ "rating": rating, "text": text }),
        number,
    )
}

fn items(order_id: &str, kind: &str, revision: i64, listings: &[&str], number: u64) -> Value {
    let items: Vec<Value> = listings
        .iter()
        .map(|listing_id| {
            json!({ "listing_id": listing_id, "rating": 4, "text": format!("{listing_id} review") })
        })
        .collect();
    order_command(
        kind,
        order_id,
        revision,
        json!({ "items": items, "seller": { "shipping": 5, "communication": 4 } }),
        number,
    )
}

fn withdraw(order_id: &str, revision: i64, number: u64, listing_id: Option<&str>) -> Value {
    let payload = match listing_id {
        Some(listing_id) => json!({ "listing_id": listing_id }),
        None => json!({}),
    };
    order_command("review.withdraw", order_id, revision, payload, number)
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

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

async fn ok(app: &TestApp, actor: &TestActor, command: Value) -> Value {
    let (status, body) = execute(app, &actor.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

fn revision(body: &Value) -> i64 {
    body["revision"].as_i64().expect("revision")
}

fn flags(reviews: &Value) -> Vec<Option<bool>> {
    reviews
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| review["public_removed"].as_bool())
        .collect()
}

/// The flags of an order's reviews by listing id, in listing order (the
/// view's order within one timestamp is not specified).
fn flags_by_listing(reviews: &Value) -> Vec<(String, Option<bool>)> {
    let mut flags: Vec<(String, Option<bool>)> = reviews
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| {
            let listing = review["listing_aggregate_id"].as_str().expect("listing");
            let id = LISTINGS
                .iter()
                .find(|id| listing.ends_with(**id))
                .expect("a known listing");
            (id.to_string(), review["public_removed"].as_bool())
        })
        .collect();
    flags.sort();
    flags
}

/// `(listing, removed_at)` of the reviewer's rows, in listing order.
async fn rows(
    app: &TestApp,
    order_id: &str,
    reviewer: &str,
) -> Vec<(String, Option<DateTime<Utc>>)> {
    sqlx::query_as(
        "SELECT listing_aggregate_id, public_removed_at FROM reviews \
         WHERE order_id = $1::uuid AND reviewer_pubky = $2 ORDER BY listing_aggregate_id",
    )
    .bind(order_id)
    .bind(reviewer)
    .fetch_all(&app.pool)
    .await
    .expect("review rows")
}

async fn removed_at(app: &TestApp, order_id: &str, reviewer: &str) -> Option<DateTime<Utc>> {
    let rows = rows(app, order_id, reviewer).await;
    assert_eq!(rows.len(), 1, "one review row");
    rows[0].1
}

async fn event_kinds(app: &TestApp, order_id: &str) -> Vec<(i64, String)> {
    sqlx::query_as(
        "SELECT revision, kind FROM events WHERE aggregate_id = $1 AND kind LIKE 'review.%' \
         ORDER BY revision",
    )
    .bind(format!("order:{order_id}"))
    .fetch_all(&app.pool)
    .await
    .expect("events")
}

async fn order_revision(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("revision")
}

async fn review_text(app: &TestApp, order_id: &str, reviewer: &str) -> String {
    sqlx::query_scalar("SELECT text FROM reviews WHERE order_id = $1::uuid AND reviewer_pubky = $2")
        .bind(order_id)
        .bind(reviewer)
        .fetch_one(&app.pool)
        .await
        .expect("review text")
}

/// Waits until a command is queued on the order's row lock (its order read is
/// `SELECT {ORDER_COLUMNS} ... FOR UPDATE`; Postgres truncates the text the
/// activity view shows, so only the start of it is matched).
async fn wait_for_blocked_command(app: &TestApp) {
    for _ in 0..400 {
        let waiting = count(
            &app.pool,
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND query LIKE 'SELECT id, auction_aggregate_id%'",
        )
        .await;
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let activity: Vec<String> = sqlx::query_scalar(
        "SELECT concat_ws(' / ', state, wait_event_type, left(query, 80)) FROM pg_stat_activity \
         WHERE datname = current_database()",
    )
    .fetch_all(&app.pool)
    .await
    .expect("activity");
    panic!("the second command never queued on the order's row lock: {activity:#?}");
}

fn paused() -> InsertGate {
    InsertGate {
        arrived: Arc::new(Barrier::new(2)),
        release: Arc::new(Barrier::new(2)),
    }
}

fn redacted(role: &str) -> Value {
    json!([{ "role": role, "submitted": true }])
}

/// The first withdraw sets the flag, bumps the revision once and writes one
/// `review.withdrawn` event; a second call with a new command id changes
/// nothing (same timestamp, revision, events) and answers the same; a replay
/// of the first returns its stored answer. The text, rating and attestation
/// are untouched. Before any withdrawal no field is added anywhere.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withdrawal_is_recorded_once_and_is_idempotent(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;

    let created = ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert!(
        !created.to_string().contains("public_removed"),
        "a review nobody withdrew carries no flag: {created}"
    );
    let (_, attested) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert!(attested["attestation"]["jws"].is_string(), "{attested}");

    let first = ok(&app, &buyer, withdraw(&order_id, 5, 101, None)).await;
    assert_eq!(revision(&first), 6);
    let result = &first["result"];
    assert_eq!(result["kind"], json!("review"));
    assert_eq!(result["review"]["public_removed"], json!(true));
    assert_eq!(result["reviews"][0]["public_removed"], json!(true));
    assert_eq!(result["order"]["reviews"][0]["public_removed"], json!(true));
    assert_eq!(result["review"]["text"], json!("Boots arrived fast."));
    assert_eq!(result["review"]["rating"], json!(5));
    let withdrawn_at = removed_at(&app, &order_id, &buyer.pubky)
        .await
        .expect("the flag is set");
    assert_eq!(
        event_kinds(&app, &order_id).await,
        vec![
            (5, "review.created".to_string()),
            (6, "review.withdrawn".to_string())
        ]
    );

    app.clock.advance_seconds(3_600);
    let again = ok(&app, &buyer, withdraw(&order_id, 6, 102, None)).await;
    assert_eq!(revision(&again), 6, "nothing changed, so no new revision");
    assert_eq!(again["result"], first["result"], "the same answer");
    assert_eq!(
        removed_at(&app, &order_id, &buyer.pubky).await,
        Some(withdrawn_at),
        "the first withdrawal's timestamp stands"
    );
    assert_eq!(event_kinds(&app, &order_id).await.len(), 2);
    assert_eq!(order_revision(&app, &order_id).await, 6);

    let replay = ok(&app, &buyer, withdraw(&order_id, 5, 101, None)).await;
    assert_eq!(replay, first, "a replay returns the stored answer");

    let (_, still) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(still, attested, "the attestation stays valid and fetchable");
    let attestations: i64 = count(&app.pool, "SELECT COUNT(*) FROM review_attestations").await;
    assert_eq!(attestations, 1);

    // The author's list and single views carry it.
    let (_, listed) = get(&app, "/v1/orders", &buyer.token).await;
    assert_eq!(flags(&listed["orders"][0]["reviews"]), vec![Some(true)]);
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(flags(&single["reviews"]), vec![Some(true)]);
}

/// Only the review's author can withdraw it, and only their own rows: a
/// participant with no review is refused, a non-participant is refused, and
/// the counterparty's views never carry the flag, before or after a
/// withdrawal by either side.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_the_author_withdraws_and_the_counterparty_never_sees_the_flag(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;

    // The seller has no review of the order: nothing to withdraw, and the
    // buyer's review is untouched.
    let (status, body) = execute(&app, &seller.token, &withdraw(&order_id, 5, 101, None)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(removed_at(&app, &order_id, &buyer.pubky).await, None);

    let (status, body) = execute(&app, &stranger.token, &withdraw(&order_id, 5, 102, None)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    assert_eq!(removed_at(&app, &order_id, &buyer.pubky).await, None);

    let missing = withdraw(&Uuid::new_v4().to_string(), 5, 103, None);
    let (status, body) = execute(&app, &buyer.token, &missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(event_kinds(&app, &order_id).await.len(), 1);

    // The seller reviews too; the buyer withdraws theirs.
    ok(&app, &seller, review(&order_id, 5, 104, 4, "Paid at once.")).await;
    let after_buyer = ok(&app, &buyer, withdraw(&order_id, 6, 105, None)).await;
    assert_eq!(revision(&after_buyer), 7);
    assert!(removed_at(&app, &order_id, &buyer.pubky).await.is_some());
    assert_eq!(removed_at(&app, &order_id, &seller.pubky).await, None);

    // Every projection the seller reads of the buyer's withdrawn review is
    // the review as published: no flag, own row untouched.
    let (_, listed) = get(&app, "/v1/orders", &seller.token).await;
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    for reviews in [&listed["orders"][0]["reviews"], &single["reviews"]] {
        assert_eq!(reviews.as_array().expect("reviews").len(), 2);
        assert_eq!(flags(reviews), vec![None, None], "{reviews}");
        assert!(reviews
            .as_array()
            .expect("reviews")
            .iter()
            .any(|review| review["text"] == json!("Boots arrived fast.")));
    }
    assert!(!listed.to_string().contains("public_removed"));
    assert!(!single.to_string().contains("public_removed"));

    // The seller's own command result shows the buyer's review without it.
    let from_seller = ok(&app, &seller, update(&order_id, 7, 106, 5, "Great buyer.")).await;
    assert!(
        !from_seller.to_string().contains("public_removed"),
        "{from_seller}"
    );

    // The seller withdraws their own review: the buyer's views of it carry
    // no flag, the buyer's own still does.
    let after_seller = ok(&app, &seller, withdraw(&order_id, 8, 107, None)).await;
    assert_eq!(
        after_seller["result"]["review"]["public_removed"],
        json!(true)
    );
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    let by_reviewer: Vec<(String, Option<bool>)> = single["reviews"]
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| {
            (
                review["reviewer_pubky"]
                    .as_str()
                    .expect("reviewer")
                    .to_string(),
                review["public_removed"].as_bool(),
            )
        })
        .collect();
    assert!(by_reviewer.contains(&(buyer.pubky.clone(), Some(true))));
    assert!(by_reviewer.contains(&(seller.pubky.clone(), None)));
}

/// An edit republishes: `review.update` clears the flag in the same
/// statement as the edit, the result and views show it absent, and a later
/// withdrawal sets it again with a new timestamp.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_edit_clears_the_withdrawal(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    ok(&app, &buyer, withdraw(&order_id, 5, 101, None)).await;
    let first = removed_at(&app, &order_id, &buyer.pubky)
        .await
        .expect("set");

    let edited = ok(
        &app,
        &buyer,
        update(&order_id, 6, 102, 4, "Boots, revised."),
    )
    .await;
    assert_eq!(revision(&edited), 7);
    let result = &edited["result"];
    assert!(result["review"].get("public_removed").is_none(), "{result}");
    assert!(!result.to_string().contains("public_removed"), "{result}");
    assert_eq!(removed_at(&app, &order_id, &buyer.pubky).await, None);
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(flags(&single["reviews"]), vec![None]);
    assert_eq!(single["reviews"][0]["text"], json!("Boots, revised."));

    app.clock.advance_seconds(60);
    ok(&app, &buyer, withdraw(&order_id, 7, 103, None)).await;
    let second = removed_at(&app, &order_id, &buyer.pubky)
        .await
        .expect("set again");
    assert!(second > first, "a new withdrawal, a new timestamp");
    assert_eq!(
        event_kinds(&app, &order_id)
            .await
            .into_iter()
            .map(|(_, kind)| kind)
            .collect::<Vec<_>>(),
        vec![
            "review.created",
            "review.withdrawn",
            "review.updated",
            "review.withdrawn"
        ]
    );
}

/// Per item: `listing_id` limits a withdrawal to one item, an edit of one
/// item clears only that item, and a withdrawal without `listing_id` takes
/// every review of the order not yet withdrawn, keeping earlier timestamps.
/// A listing the order does not carry is refused, and so is an item the
/// caller never reviewed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn withdrawals_and_edits_work_per_item(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;
    ok(
        &app,
        &buyer,
        items(
            &order_id,
            "review.create",
            4,
            &["boots_01", "laces_02", "socks_03"],
            100,
        ),
    )
    .await;

    let one = ok(&app, &buyer, withdraw(&order_id, 5, 101, Some("laces_02"))).await;
    assert_eq!(
        flags_by_listing(&one["result"]["order"]["reviews"]),
        vec![
            ("boots_01".to_string(), None),
            ("laces_02".to_string(), Some(true)),
            ("socks_03".to_string(), None)
        ]
    );
    assert_eq!(
        one["result"]["reviews"].as_array().expect("reviews").len(),
        1
    );
    assert_eq!(one["result"]["review"]["public_removed"], json!(true));
    let laces_at = rows(&app, &order_id, &buyer.pubky).await[1]
        .1
        .expect("laces");

    // Editing another item leaves the withdrawn one alone.
    let edit_boots = order_command(
        "review.update",
        &order_id,
        6,
        json!({ "items": [{ "listing_id": "boots_01", "rating": 5, "text": "Better." }] }),
        102,
    );
    let edited = ok(&app, &buyer, edit_boots).await;
    assert_eq!(
        flags_by_listing(&edited["result"]["order"]["reviews"]),
        vec![
            ("boots_01".to_string(), None),
            ("laces_02".to_string(), Some(true)),
            ("socks_03".to_string(), None)
        ]
    );

    app.clock.advance_seconds(60);
    let all = ok(&app, &buyer, withdraw(&order_id, 7, 103, None)).await;
    assert_eq!(
        flags_by_listing(&all["result"]["order"]["reviews"]),
        vec![
            ("boots_01".to_string(), Some(true)),
            ("laces_02".to_string(), Some(true)),
            ("socks_03".to_string(), Some(true))
        ]
    );
    assert_eq!(
        all["result"]["reviews"].as_array().expect("reviews").len(),
        3
    );
    let after = rows(&app, &order_id, &buyer.pubky).await;
    assert!(after.iter().all(|(_, at)| at.is_some()));
    assert_eq!(
        after[1].1,
        Some(laces_at),
        "an earlier withdrawal keeps its time"
    );
    assert!(after[0].1 > Some(laces_at));

    // Editing one item republishes only that one.
    let edit_socks = order_command(
        "review.update",
        &order_id,
        8,
        json!({ "items": [{ "listing_id": "socks_03", "rating": 3, "text": "Thin." }] }),
        104,
    );
    let edited = ok(&app, &buyer, edit_socks).await;
    assert_eq!(
        flags_by_listing(&edited["result"]["order"]["reviews"]),
        vec![
            ("boots_01".to_string(), Some(true)),
            ("laces_02".to_string(), Some(true)),
            ("socks_03".to_string(), None)
        ]
    );

    let (status, body) = execute(
        &app,
        &buyer.token,
        &withdraw(&order_id, 9, 105, Some("nope_99")),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    // A seller's review is keyed to the first line: it is withdrawn by that
    // listing's id or by none, and refused for another item.
    ok(&app, &seller, review(&order_id, 9, 106, 4, "Paid at once.")).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &withdraw(&order_id, 10, 107, Some("laces_02")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(removed_at(&app, &order_id, &seller.pubky).await, None);
    let by_listing = ok(
        &app,
        &seller,
        withdraw(&order_id, 10, 108, Some("boots_01")),
    )
    .await;
    assert_eq!(
        by_listing["result"]["review"]["public_removed"],
        json!(true)
    );
}

/// Inside the double-blind window a withdrawal is allowed and invisible to
/// the counterparty: they read only `{role, submitted: true}` (in the list,
/// the single view and their own command result), the flag never reaches
/// them, it stays set through the release, and the released view the
/// counterparty reads carries no flag either.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withdrawal_inside_the_blind_window_never_reaches_the_counterparty(pool: PgPool) {
    let mut config = Config::for_tests();
    config.review_blind_window_days = 14;
    let app = test_app_full(pool, config, Some(test_attestor())).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let created = ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    assert_eq!(created["result"]["attestation_status"], json!("pending"));

    let withdrawn = ok(&app, &buyer, withdraw(&order_id, 5, 101, None)).await;
    assert_eq!(revision(&withdrawn), 6);
    let result = &withdrawn["result"];
    assert_eq!(result["attestation_status"], json!("pending"));
    assert_eq!(result["review"]["public_removed"], json!(true));
    assert_eq!(result["order"]["reviews"][0]["public_removed"], json!(true));

    let (_, listed) = get(&app, "/v1/orders", &seller.token).await;
    let (_, single) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    assert_eq!(listed["orders"][0]["reviews"], redacted("buyer"));
    assert_eq!(single["reviews"], redacted("buyer"));
    for body in [&listed, &single] {
        assert!(!body.to_string().contains("public_removed"), "{body}");
        assert!(!body.to_string().contains("Boots arrived fast."), "{body}");
    }
    let (_, own) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    assert_eq!(flags(&own["reviews"]), vec![Some(true)]);

    // The seller's review completes the pair and releases both.
    let released = ok(&app, &seller, review(&order_id, 6, 102, 4, "Paid at once.")).await;
    let seen = &released["result"]["order"]["reviews"];
    assert_eq!(seen.as_array().expect("reviews").len(), 2);
    let buyers = seen
        .as_array()
        .expect("reviews")
        .iter()
        .find(|review| review["reviewer_pubky"] == json!(buyer.pubky))
        .expect("the buyer's released review");
    assert_eq!(buyers["text"], json!("Boots arrived fast."));
    assert!(
        !released.to_string().contains("public_removed"),
        "{released}"
    );
    let (_, listed) = get(&app, "/v1/orders", &seller.token).await;
    assert!(!listed.to_string().contains("public_removed"), "{listed}");
    assert!(removed_at(&app, &order_id, &buyer.pubky).await.is_some());
    let (_, own) = get(&app, &format!("/v1/orders/{order_id}"), &buyer.token).await;
    let by_reviewer: Vec<(bool, Option<bool>)> = own["reviews"]
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| {
            (
                review["reviewer_pubky"] == json!(buyer.pubky),
                review["public_removed"].as_bool(),
            )
        })
        .collect();
    assert_eq!(by_reviewer.len(), 2);
    assert!(by_reviewer.contains(&(true, Some(true))));
    assert!(by_reviewer.contains(&(false, None)));
}

/// A withdrawal is not bound by the edit window and does not care what the
/// order has become since: 25 hours on, with a return open, the edit is
/// refused and the withdrawal goes through.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withdrawal_outlasts_the_edit_window_and_any_order_state(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    ok(
        &app,
        &buyer,
        order_command(
            "return.request",
            &order_id,
            5,
            json!({ "reason": "Item not as described", "requested_amount_minor": 13_700 }),
            101,
        ),
    )
    .await;
    app.clock.advance_seconds(25 * 60 * 60);
    let (status, body) = execute(&app, &buyer.token, &update(&order_id, 6, 102, 3, "Later.")).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    let state: String = sqlx::query_scalar("SELECT state FROM orders WHERE id = $1::uuid")
        .bind(&order_id)
        .fetch_one(&app.pool)
        .await
        .expect("state");
    assert_ne!(
        state, "completed",
        "the order is no longer where the review left it"
    );
    let body = ok(&app, &buyer, withdraw(&order_id, 6, 103, None)).await;
    assert_eq!(body["result"]["review"]["public_removed"], json!(true));
}

/// Two withdrawals racing on the same revision: the first holds the order's
/// row lock after writing, the second queues behind it, loses the revision
/// compare, and its retry finds nothing left to do. One flag, one event, the
/// first timestamp.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_concurrent_withdrawals_settle_on_one(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;

    let gate = paused();
    arm_after_edit(order_uuid, gate.clone());
    let first = spawn_command(&app, &buyer, withdraw(&order_id, 5, 101, None));
    gate.arrived.wait().await;
    let second = spawn_command(&app, &buyer, withdraw(&order_id, 5, 102, None));
    wait_for_blocked_command(&app).await;
    app.clock.advance_seconds(60);
    gate.release.wait().await;

    let (status, won) = first.await.expect("first completes");
    assert_eq!(status, StatusCode::OK, "{won}");
    assert_eq!(revision(&won), 6);
    let (status, lost) = second.await.expect("second completes");
    assert_eq!(status, StatusCode::CONFLICT, "{lost}");
    assert_eq!(lost["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(lost["error"]["current_revision"], json!(6));

    let retried = ok(&app, &buyer, withdraw(&order_id, 6, 103, None)).await;
    assert_eq!(retried["result"], won["result"]);
    assert_eq!(revision(&retried), 6);
    assert_eq!(
        event_kinds(&app, &order_id).await,
        vec![
            (5, "review.created".to_string()),
            (6, "review.withdrawn".to_string())
        ]
    );
    let stamp = removed_at(&app, &order_id, &buyer.pubky)
        .await
        .expect("set");
    assert_eq!(stamp, app.clock.now() - chrono::Duration::seconds(60));
}

/// A withdrawal racing an edit, the withdrawal first: the edit queues behind
/// the withdrawal's row lock, loses the revision compare, and its retry
/// republishes. Final state is the edit, not removed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_edit_that_loses_to_a_withdrawal_republishes_on_retry(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;

    let gate = paused();
    arm_after_edit(order_uuid, gate.clone());
    let withdrawal = spawn_command(&app, &buyer, withdraw(&order_id, 5, 101, None));
    gate.arrived.wait().await;
    let edit = spawn_command(
        &app,
        &buyer,
        update(&order_id, 5, 102, 4, "Boots, revised."),
    );
    wait_for_blocked_command(&app).await;
    gate.release.wait().await;

    let (status, body) = withdrawal.await.expect("withdrawal completes");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(revision(&body), 6);
    let (status, lost) = edit.await.expect("edit completes");
    assert_eq!(status, StatusCode::CONFLICT, "{lost}");
    assert_eq!(lost["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(lost["error"]["current_revision"], json!(6));
    assert!(removed_at(&app, &order_id, &buyer.pubky).await.is_some());

    let retried = ok(
        &app,
        &buyer,
        update(&order_id, 6, 103, 4, "Boots, revised."),
    )
    .await;
    assert_eq!(revision(&retried), 7);
    assert!(retried["result"]["review"].get("public_removed").is_none());
    assert_eq!(removed_at(&app, &order_id, &buyer.pubky).await, None);
    assert_eq!(
        review_text(&app, &order_id, &buyer.pubky).await,
        "Boots, revised."
    );
    assert_eq!(
        event_kinds(&app, &order_id).await,
        vec![
            (5, "review.created".to_string()),
            (6, "review.withdrawn".to_string()),
            (7, "review.updated".to_string())
        ]
    );
}

/// The other order: the edit holds the lock, the withdrawal queues, loses the
/// compare and retries. Final state is the edited text, removed.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_withdrawal_that_loses_to_an_edit_removes_the_edited_review_on_retry(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");
    ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;

    let gate = paused();
    arm_after_edit(order_uuid, gate.clone());
    let edit = spawn_command(
        &app,
        &buyer,
        update(&order_id, 5, 101, 4, "Boots, revised."),
    );
    gate.arrived.wait().await;
    let withdrawal = spawn_command(&app, &buyer, withdraw(&order_id, 5, 102, None));
    wait_for_blocked_command(&app).await;
    gate.release.wait().await;

    let (status, body) = edit.await.expect("edit completes");
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(revision(&body), 6);
    let (status, lost) = withdrawal.await.expect("withdrawal completes");
    assert_eq!(status, StatusCode::CONFLICT, "{lost}");
    assert_eq!(lost["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(removed_at(&app, &order_id, &buyer.pubky).await, None);

    let retried = ok(&app, &buyer, withdraw(&order_id, 6, 103, None)).await;
    assert_eq!(revision(&retried), 7);
    assert_eq!(retried["result"]["review"]["public_removed"], json!(true));
    assert_eq!(
        retried["result"]["review"]["text"],
        json!("Boots, revised.")
    );
    assert!(removed_at(&app, &order_id, &buyer.pubky).await.is_some());
    assert_eq!(
        event_kinds(&app, &order_id).await,
        vec![
            (5, "review.created".to_string()),
            (6, "review.updated".to_string()),
            (7, "review.withdrawn".to_string())
        ]
    );
}

/// With the window at 0 and nothing withdrawn, no result or view anywhere
/// gains a field: `public_removed` exists only for a withdrawn review.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_a_withdrawal_no_view_changes(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..1]).await;
    let created = ok(
        &app,
        &buyer,
        review(&order_id, 4, 100, 5, "Boots arrived fast."),
    )
    .await;
    let edited = ok(
        &app,
        &buyer,
        update(&order_id, 5, 101, 4, "Boots, revised."),
    )
    .await;
    let (_, buyer_list) = get(&app, "/v1/orders", &buyer.token).await;
    let (_, seller_single) = get(&app, &format!("/v1/orders/{order_id}"), &seller.token).await;
    for body in [&created, &edited, &buyer_list, &seller_single] {
        assert!(!body.to_string().contains("public_removed"), "{body}");
    }
    assert_eq!(
        created["result"]["review"]
            .as_object()
            .expect("review")
            .len(),
        edited["result"]["review"]
            .as_object()
            .expect("review")
            .len()
    );
}
