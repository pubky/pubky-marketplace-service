//! Per-item reviews and seller ratings (Pubky Market reviews, SV1): one
//! review row and one purchase attestation per reviewed listing, sharing the
//! order's `order_ref`; the per-order seller ratings on every item review;
//! items added within the edit window; v1 terms rating the first line; and
//! the races between two submissions, forced with a pause between the
//! inserts and the order's compare-and-swap.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

use common::{
    count, execute, indexed_command_id, new_actor, order_command, payment_command, send,
    test_app_with_attestor, test_attestor, TestActor, TestApp,
};
use marketplace_service::clock::Clock;
use marketplace_service::handlers::reviews::test_faults::{arm_after_insert, InsertGate};
use marketplace_service::workers::drain_outbox;

const LISTINGS: [&str; 3] = ["boots_01", "laces_02", "socks_03"];

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

fn listing_uri(seller: &TestActor, listing_id: &str) -> String {
    format!(
        "pubky://{}/pub/pubky.app/marketplace/v1/listings/{listing_id}",
        seller.pubky
    )
}

/// Registers one unit of each listing at 125.00 USD, checks all of them out
/// in one order, pays, ships and confirms delivery. Returns the order id;
/// the order is `delivered` at revision 4.
async fn delivered_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listing_ids: &[&str],
) -> String {
    let priced: Vec<(&str, i64)> = listing_ids
        .iter()
        .map(|listing_id| (*listing_id, 12_500))
        .collect();
    delivered_priced_order(app, seller, buyer, &priced, "USD", 2).await
}

/// [`delivered_order`] with each listing's unit price, in `currency` at
/// `exponent`.
async fn delivered_priced_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listings: &[(&str, i64)],
    currency: &str,
    exponent: i64,
) -> String {
    for (index, (listing_id, amount_minor)) in listings.iter().enumerate() {
        let (status, body) = execute(
            app,
            &seller.token,
            &json!({
                "version": 1,
                "command_id": indexed_command_id(0x7100, index as u64),
                "aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
                "expected_revision": 0,
                "issued_at": "2026-08-19T22:00:00.000Z",
                "kind": "listing.register",
                "payload": {
                    "seller_pubky": seller.pubky,
                    "listing_id": listing_id,
                    "listing_revision": 1,
                    "content_hash": "a".repeat(64),
                    "quantity": 1,
                    "unit_price": {
                        "amount_minor": amount_minor,
                        "currency": currency,
                        "exponent": exponent,
                    },
                    "shipping_minor": 1_200,
                },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "register {listing_id}: {body}");
    }
    let checkout_id = indexed_command_id(0x7200, 1);
    let lines: Vec<Value> = listings
        .iter()
        .map(|(listing_id, _)| {
            json!({
                "listing_aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
                "expected_revision": 1,
                "quantity": 1,
            })
        })
        .collect();
    let (status, body) = execute(
        app,
        &buyer.token,
        &json!({
            "version": 1,
            "command_id": checkout_id,
            "aggregate_id": format!("checkout:{checkout_id}"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "checkout.create",
            "payload": {
                "lines": lines,
                "delivery_address": {
                    "name": "Alice Buyer",
                    "line1": "1 Market Street",
                    "line2": "",
                    "city": "New York",
                    "region": "NY",
                    "postal_code": "10001",
                    "country_code": "US",
                },
                "guarantee_policy_version": 1,
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    assert_eq!(body["result"]["orders"].as_array().map(Vec::len), Some(1));
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 7_301),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment: {body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-ITEMS" }),
            7_302,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order_id,
            3,
            json!({}),
            7_303,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery: {body}");
    order_id
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
        json!({ "items": items }),
        number,
    )
}

fn claims_of(attestation: &Value) -> Value {
    let jws = attestation["jws"].as_str().expect("attestation jws");
    let payload = jws.split('.').nth(1).expect("jws payload");
    serde_json::from_slice(
        &base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
            .expect("payload b64"),
    )
    .expect("claims JSON")
}

async fn review_rows(
    app: &TestApp,
    order_id: &str,
) -> Vec<(String, String, i32, Option<i32>, Option<i32>)> {
    sqlx::query_as(
        "SELECT reviewer_role, listing_aggregate_id, rating, shipping, communication \
         FROM reviews WHERE order_id = $1::uuid ORDER BY reviewer_role, listing_aggregate_id",
    )
    .bind(order_id)
    .fetch_all(&app.pool)
    .await
    .expect("review rows")
}

async fn attestation_listings(app: &TestApp, order_id: &str) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT reviewer_pubky, listing_uri FROM review_attestations \
         WHERE order_id = $1::uuid ORDER BY reviewer_pubky, listing_uri",
    )
    .bind(order_id)
    .fetch_all(&app.pool)
    .await
    .expect("attestation rows")
}

async fn order_revision(app: &TestApp, order_id: &str) -> i64 {
    sqlx::query_scalar("SELECT revision FROM orders WHERE id = $1::uuid")
        .bind(order_id)
        .fetch_one(&app.pool)
        .await
        .expect("order revision")
}

async fn review_notifications(app: &TestApp) -> i64 {
    drain_outbox(&app.pool, None, app.clock.now(), 30)
        .await
        .expect("outbox drains");
    count(
        &app.pool,
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.review_received'",
    )
    .await
}

/// Waits until another session is blocked on a lock inside an `INSERT INTO
/// reviews`: the second submission reached the unique index.
async fn wait_for_blocked_review_insert(app: &TestApp) {
    for _ in 0..400 {
        let waiting = count(
            &app.pool,
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND query LIKE 'INSERT INTO reviews%'",
        )
        .await;
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("the second submission never blocked on the first one's review insert");
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn three_items_get_three_attestations_sharing_one_order_ref(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;

    // Items arrive out of line order; the service answers in line order.
    let mut command = items_review(
        &order_id,
        4,
        &[("socks_03", 3), ("boots_01", 5), ("laces_02", 4)],
        7_401,
    );
    command["payload"]["seller"] = json!({ "shipping": 4, "communication": 5 });
    let (status, reviewed) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let result = &reviewed["result"];
    assert_eq!(result["order"]["state"], json!("completed"));
    assert_eq!(result["order"]["revision"], json!(5));

    let reviews = result["reviews"].as_array().expect("reviews written");
    let listed: Vec<&str> = reviews
        .iter()
        .map(|review| review["listing_aggregate_id"].as_str().expect("listing"))
        .collect();
    let expected: Vec<String> = LISTINGS
        .iter()
        .map(|listing_id| format!("listing:{}_{listing_id}", seller.pubky))
        .collect();
    assert_eq!(listed, expected);
    assert_eq!(result["review"], reviews[0]);
    for review in reviews {
        assert_eq!(review["shipping"], json!(4));
        assert_eq!(review["communication"], json!(5));
        assert_eq!(review["subject_pubky"], json!(seller.pubky));
    }
    assert_eq!(result["order"]["reviews"].as_array().map(Vec::len), Some(3));

    let attestations = result["attestations"].as_array().expect("attestations");
    assert_eq!(attestations.len(), 3);
    let order_ref = test_attestor().order_ref(order_id.parse().expect("uuid"));
    for (attestation, listing_id) in attestations.iter().zip(LISTINGS) {
        assert_eq!(attestation["listing_id"], json!(listing_id));
        let claims = claims_of(attestation);
        assert_eq!(attestation["claims"], claims);
        assert_eq!(claims["order_ref"], json!(order_ref));
        assert_eq!(claims["listing"], json!(listing_uri(&seller, listing_id)));
        assert_eq!(claims["role"], json!("buyer_reviewing_seller"));
        assert_eq!(claims["sub"], json!(buyer.pubky));
    }
    // The singular field is the first line's attestation, in the v1 shape.
    assert_eq!(
        result["attestation"],
        json!({ "jws": attestations[0]["jws"], "claims": attestations[0]["claims"] })
    );

    let (status, fetched) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{fetched}");
    assert_eq!(fetched["attestations"], json!(attestations));
    let (status, single) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{single}");
    assert_eq!(single, json!({ "attestation": result["attestation"] }));

    // The seller has none yet; a stranger cannot see the order at all.
    let (status, fetched) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched, json!({ "attestations": [] }));
    let stranger = new_actor(&app).await;
    let (status, _) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestations"),
        &stranger.token,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // One notification for the review action, not one per item.
    assert_eq!(review_notifications(&app).await, 1);
    assert_eq!(
        count(
            &app.pool,
            "SELECT COUNT(*) FROM events WHERE kind = 'review.created'"
        )
        .await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn v1_terms_rate_the_first_line_exactly_as_before(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;

    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            4,
            json!({ "rating": 5, "text": "Accurate and fast." }),
            7_411,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let result = &reviewed["result"];
    assert_eq!(result["order"]["state"], json!("completed"));
    assert_eq!(result["review"]["rating"], json!(5));
    assert_eq!(result["review"]["text"], json!("Accurate and fast."));
    assert_eq!(
        result["review"]["id"],
        json!(indexed_command_id(0x8000, 7_411))
    );
    assert_eq!(result["review"]["shipping"], Value::Null);
    // The singular attestation keeps its closed `{ jws, claims }` shape and
    // binds the first line.
    let attestation = result["attestation"].as_object().expect("attestation");
    assert_eq!(
        attestation.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["jws", "claims"]
    );
    assert_eq!(
        result["attestation"]["claims"]["listing"],
        json!(listing_uri(&seller, "boots_01"))
    );
    assert_eq!(result["attestations"].as_array().map(Vec::len), Some(1));

    let (status, single) = get(
        &app,
        &format!("/v1/orders/{order_id}/review-attestation"),
        &buyer.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(single["attestation"], result["attestation"]);

    // A repeat v1 review is refused as before.
    let (status, duplicate) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.create",
            &order_id,
            5,
            json!({ "rating": 4, "text": "Duplicate." }),
            7_412,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{duplicate}");
    assert_eq!(duplicate["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(
        duplicate["error"]["message"],
        json!("This participant already reviewed the order.")
    );

    // A v1 edit revises the first line.
    let (status, edited) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            5,
            json!({ "rating": 4, "text": "Revised." }),
            7_413,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(edited["result"]["review"]["rating"], json!(4));
    assert_eq!(edited["result"]["attestation"], result["attestation"]);

    // The seller's review of the buyer is one row, attested to the first
    // line, sharing the order_ref.
    let (status, seller_reviewed) = execute(
        &app,
        &seller.token,
        &order_command(
            "review.create",
            &order_id,
            6,
            json!({ "rating": 5, "text": "Great buyer." }),
            7_414,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{seller_reviewed}");
    let seller_claims = &seller_reviewed["result"]["attestation"]["claims"];
    assert_eq!(seller_claims["role"], json!("seller_reviewing_buyer"));
    assert_eq!(
        seller_claims["listing"],
        json!(listing_uri(&seller, "boots_01"))
    );
    assert_eq!(
        seller_claims["order_ref"],
        result["attestation"]["claims"]["order_ref"]
    );
    assert_eq!(
        review_rows(&app, &order_id).await,
        vec![
            (
                "buyer".to_string(),
                format!("listing:{}_boots_01", seller.pubky),
                4,
                None,
                None
            ),
            (
                "seller".to_string(),
                format!("listing:{}_boots_01", seller.pubky),
                5,
                None,
                None
            ),
        ]
    );
    assert_eq!(review_notifications(&app).await, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_are_addable_and_editable_within_the_window_only(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 4, &[("laces_02", 4)], 7_421),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Twelve hours later the buyer adds the other two items and rates the
    // seller: the ratings land on every item review, the earlier one too.
    app.clock.advance_seconds(12 * 60 * 60);
    let mut add = items_review(&order_id, 5, &[("boots_01", 5), ("socks_03", 2)], 7_422);
    add["payload"]["seller"] = json!({ "shipping": 3, "communication": 4 });
    let (status, added) = execute(&app, &buyer.token, &add).await;
    assert_eq!(status, StatusCode::OK, "{added}");
    assert_eq!(added["result"]["order"]["state"], json!("completed"));
    let written: Vec<&str> = added["result"]["reviews"]
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| review["listing_aggregate_id"].as_str().expect("listing"))
        .collect();
    assert_eq!(
        written.len(),
        3,
        "two new rows and the re-rated earlier one"
    );
    let attested: Vec<&str> = added["result"]["attestations"]
        .as_array()
        .expect("attestations")
        .iter()
        .map(|attestation| attestation["listing_id"].as_str().expect("listing"))
        .collect();
    assert_eq!(attested, LISTINGS.to_vec());
    let rows = review_rows(&app, &order_id).await;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| (row.3, row.4) == (Some(3), Some(4))));
    assert_eq!(attestation_listings(&app, &order_id).await.len(), 3);

    // A listing is reviewed once; one outside the order is refused.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 6, &[("boots_01", 1)], 7_423),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("This participant already reviewed listing boots_01.")
    );
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            6,
            json!({ "items": [{ "listing_id": "hat_09", "rating": 1 }] }),
            7_424,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    // Editing one item revises only it.
    let (status, edited) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            6,
            json!({ "items": [{ "listing_id": "socks_03", "rating": 4, "text": "Grew on me." }] }),
            7_425,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    let edited_reviews = edited["result"]["reviews"].as_array().expect("reviews");
    assert_eq!(edited_reviews.len(), 1);
    assert_eq!(edited_reviews[0]["rating"], json!(4));
    assert_eq!(
        edited["result"]["attestations"][0]["listing_id"],
        json!("socks_03")
    );
    let ratings: Vec<i32> = review_rows(&app, &order_id)
        .await
        .iter()
        .map(|row| row.2)
        .collect();
    assert_eq!(ratings, vec![5, 4, 4]);

    // The window opened with the first review: past 24 hours nothing is
    // added or edited.
    app.clock.advance_seconds(12 * 60 * 60 + 1);
    let (status, body) = execute(
        &app,
        &buyer.token,
        &order_command(
            "review.update",
            &order_id,
            7,
            json!({ "seller": { "shipping": 5, "communication": 5 },
                    "items": [{ "listing_id": "boots_01", "rating": 5 }] }),
            7_426,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["message"],
        json!("The review edit window has closed.")
    );
    assert_eq!(review_notifications(&app).await, 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_seller_rates_the_buyer_once_and_cannot_send_items(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS[..2]).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &items_review(&order_id, 4, &[("boots_01", 5)], 7_431),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(count(&app.pool, "SELECT COUNT(*) FROM reviews").await, 0);
}

/// Two submissions of the same items by one buyer (a double click, two
/// tabs). The first pauses after its inserts; the second runs its
/// duplicate pre-check against the uncommitted rows, which it cannot see,
/// and blocks on the unique index. Releasing the first commits it, and the
/// constraint refuses the second: one row set, one attestation set.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_double_submit_of_the_same_items_keeps_one_row_set(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");

    let gate = InsertGate {
        arrived: Arc::new(Barrier::new(2)),
        release: Arc::new(Barrier::new(2)),
    };
    arm_after_insert(order_uuid, gate.clone());
    let items = [("boots_01", 5), ("laces_02", 4), ("socks_03", 3)];
    let first = spawn_command(&app, &buyer, items_review(&order_id, 4, &items, 7_441));
    gate.arrived.wait().await;
    let second = spawn_command(&app, &buyer, items_review(&order_id, 4, &items, 7_442));
    wait_for_blocked_review_insert(&app).await;
    gate.release.wait().await;

    let (first_status, first_body) = first.await.expect("first completes");
    let (second_status, second_body) = second.await.expect("second completes");
    assert_eq!(first_status, StatusCode::OK, "{first_body}");
    assert_eq!(second_status, StatusCode::CONFLICT, "{second_body}");
    assert_eq!(second_body["error"]["code"], json!("INVARIANT_VIOLATION"));

    assert_eq!(review_rows(&app, &order_id).await.len(), 3);
    let attestations = attestation_listings(&app, &order_id).await;
    assert_eq!(attestations.len(), 3);
    assert!(attestations
        .iter()
        .all(|(reviewer, _)| *reviewer == buyer.pubky));
    assert_eq!(order_revision(&app, &order_id).await, 5);
    assert_eq!(review_notifications(&app).await, 1);
}

/// Two submissions by one buyer rating different items: both insert and
/// pause, so neither sees the other's rows; the order's compare-and-swap
/// lets one through and refuses the other, whose rows and attestations
/// roll back with it.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_double_submit_of_different_items_is_decided_by_the_compare_and_swap(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");

    let gate = InsertGate {
        arrived: Arc::new(Barrier::new(3)),
        release: Arc::new(Barrier::new(3)),
    };
    arm_after_insert(order_uuid, gate.clone());
    arm_after_insert(order_uuid, gate.clone());
    let one = spawn_command(
        &app,
        &buyer,
        items_review(&order_id, 4, &[("boots_01", 5)], 7_451),
    );
    let other = spawn_command(
        &app,
        &buyer,
        items_review(&order_id, 4, &[("laces_02", 4), ("socks_03", 3)], 7_452),
    );
    // Both submissions have inserted their rows and attestations.
    gate.arrived.wait().await;
    gate.release.wait().await;

    let mut outcomes = [
        one.await.expect("one completes"),
        other.await.expect("other completes"),
    ];
    outcomes.sort_by_key(|(status, _)| status.as_u16());
    let (won, lost) = (&outcomes[0], &outcomes[1]);
    assert_eq!(won.0, StatusCode::OK, "{}", won.1);
    assert_eq!(lost.0, StatusCode::CONFLICT, "{}", lost.1);
    assert_eq!(lost.1["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(lost.1["error"]["current_revision"], json!(5));

    let won_listings: Vec<String> = won.1["result"]["reviews"]
        .as_array()
        .expect("reviews")
        .iter()
        .map(|review| {
            review["listing_aggregate_id"]
                .as_str()
                .expect("listing")
                .to_string()
        })
        .collect();
    let rows: Vec<String> = review_rows(&app, &order_id)
        .await
        .into_iter()
        .map(|row| row.1)
        .collect();
    assert_eq!(rows, won_listings, "only the winner's rows remain");
    let attested: Vec<String> = attestation_listings(&app, &order_id)
        .await
        .into_iter()
        .map(|(_, uri)| uri)
        .collect();
    assert_eq!(attested.len(), won_listings.len());
    assert_eq!(order_revision(&app, &order_id).await, 5);
    assert_eq!(review_notifications(&app).await, 1);
}

/// The buyer and the seller review at the same moment. Both insert and
/// pause; the compare-and-swap admits one, the other is refused with the
/// current revision and succeeds on retry. Their keys never collide.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_buyer_and_seller_reviews_both_land_after_one_retry(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order_id = delivered_order(&app, &seller, &buyer, &LISTINGS).await;
    let order_uuid: Uuid = order_id.parse().expect("uuid");

    let gate = InsertGate {
        arrived: Arc::new(Barrier::new(3)),
        release: Arc::new(Barrier::new(3)),
    };
    arm_after_insert(order_uuid, gate.clone());
    arm_after_insert(order_uuid, gate.clone());
    let mut buyer_review = items_review(
        &order_id,
        4,
        &[("boots_01", 5), ("laces_02", 4), ("socks_03", 3)],
        7_461,
    );
    buyer_review["payload"]["seller"] = json!({ "shipping": 5, "communication": 4 });
    let seller_review = |revision: i64, number: u64| {
        order_command(
            "review.create",
            &order_id,
            revision,
            json!({ "rating": 5, "text": "Great buyer." }),
            number,
        )
    };
    let from_buyer = spawn_command(&app, &buyer, buyer_review.clone());
    let from_seller = spawn_command(&app, &seller, seller_review(4, 7_462));
    gate.arrived.wait().await;
    gate.release.wait().await;
    let buyer_outcome = from_buyer.await.expect("buyer completes");
    let seller_outcome = from_seller.await.expect("seller completes");

    let statuses = [buyer_outcome.0, seller_outcome.0];
    assert!(
        statuses.contains(&StatusCode::OK) && statuses.contains(&StatusCode::CONFLICT),
        "exactly one wins: {buyer_outcome:?} {seller_outcome:?}"
    );
    let (loser, loser_body) = if buyer_outcome.0 == StatusCode::CONFLICT {
        (&buyer, &buyer_outcome.1)
    } else {
        (&seller, &seller_outcome.1)
    };
    assert_eq!(loser_body["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(loser_body["error"]["current_revision"], json!(5));
    assert!(attestation_listings(&app, &order_id)
        .await
        .iter()
        .all(|(reviewer, _)| reviewer != &loser.pubky));

    let retry = if loser.pubky == buyer.pubky {
        let mut retry = buyer_review.clone();
        retry["expected_revision"] = json!(5);
        retry["command_id"] = json!(indexed_command_id(0x8000, 7_463));
        retry
    } else {
        seller_review(5, 7_463)
    };
    let (status, body) = execute(&app, &loser.token, &retry).await;
    assert_eq!(status, StatusCode::OK, "retry: {body}");

    let rows = review_rows(&app, &order_id).await;
    let roles: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(roles, vec!["buyer", "buyer", "buyer", "seller"]);
    let attestations = attestation_listings(&app, &order_id).await;
    assert_eq!(attestations.len(), 4);
    let order_refs: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT order_ref FROM review_attestations WHERE order_id = $1::uuid",
    )
    .bind(&order_id)
    .fetch_all(&app.pool)
    .await
    .expect("order refs");
    assert_eq!(order_refs.len(), 1);
    assert_eq!(order_revision(&app, &order_id).await, 6);
    assert_eq!(review_notifications(&app).await, 2);
}

fn band_consent_command(seller: &TestActor, allows: bool) -> Value {
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x9000, 1),
        "aggregate_id": format!("seller_settings:{}", seller.pubky),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "attestation.set_band_consent",
        "payload": { "allows_amount_band": allows },
    })
}

/// Reviews all three listings of an order priced `listings` (listing id,
/// unit price, expected claim), then the seller reviews the buyer. Every
/// attestation signs the decade of its own listing's unit price, in the
/// result, the JWS and the stored row; the seller's binds the first line.
/// Nobody consented to the order-total band, so none carries one.
async fn assert_each_item_signs_its_price_decade(
    pool: PgPool,
    currency: &str,
    exponent: i64,
    listings: [(&str, i64, &str); 3],
) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let priced: Vec<(&str, i64)> = listings
        .iter()
        .map(|(listing_id, price, _)| (*listing_id, *price))
        .collect();
    let order_id = delivered_priced_order(&app, &seller, &buyer, &priced, currency, exponent).await;
    let items: Vec<(&str, i64)> = listings
        .iter()
        .map(|(listing_id, _, _)| (*listing_id, 5))
        .collect();
    let (status, reviewed) = execute(
        &app,
        &buyer.token,
        &items_review(&order_id, 4, &items, 7_501),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    let attestations = reviewed["result"]["attestations"]
        .as_array()
        .expect("attestations");
    assert_eq!(attestations.len(), 3);
    for (attestation, (listing_id, _, decade)) in attestations.iter().zip(listings) {
        assert_eq!(attestation["listing_id"], json!(listing_id));
        let claims = claims_of(attestation);
        assert_eq!(attestation["claims"], claims);
        assert_eq!(claims["price_decade"], json!(decade), "{listing_id}");
        assert!(claims.get("amount_band").is_none(), "{claims}");
    }

    let (status, seller_reviewed) = execute(
        &app,
        &seller.token,
        &order_command(
            "review.create",
            &order_id,
            5,
            json!({ "rating": 5, "text": "Great buyer." }),
            7_502,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "seller review: {seller_reviewed}");
    assert_eq!(
        claims_of(&seller_reviewed["result"]["attestation"])["price_decade"],
        json!(listings[0].2)
    );

    let stored: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT reviewer_pubky, listing_uri, claims ->> 'price_decade' FROM review_attestations \
         WHERE order_id = $1::uuid ORDER BY reviewer_pubky = $2, listing_uri",
    )
    .bind(&order_id)
    .bind(&seller.pubky)
    .fetch_all(&app.pool)
    .await
    .expect("stored attestations");
    let expected: Vec<(String, String, Option<String>)> = listings
        .iter()
        .map(|(listing_id, _, decade)| {
            (
                buyer.pubky.clone(),
                listing_uri(&seller, listing_id),
                Some(decade.to_string()),
            )
        })
        .chain([(
            seller.pubky.clone(),
            listing_uri(&seller, listings[0].0),
            Some(listings[0].2.to_string()),
        )])
        .collect();
    assert_eq!(stored, expected);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_of_a_bitcoin_order_sign_their_sat_decades_under_btc(pool: PgPool) {
    assert_each_item_signs_its_price_decade(
        pool,
        "BTC",
        8,
        [
            ("boots_01", 25_000, "BTC:4"),
            ("laces_02", 1_000, "BTC:3"),
            ("socks_03", 150_000_000, "BTC:8"),
        ],
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_of_a_sat_order_sign_their_own_price_decades(pool: PgPool) {
    assert_each_item_signs_its_price_decade(
        pool,
        "SAT",
        0,
        [
            ("boots_01", 999, "SAT:2"),
            ("laces_02", 25_000, "SAT:4"),
            ("socks_03", 2_100_000, "SAT:6"),
        ],
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_of_a_fiat_order_sign_their_own_price_decades(pool: PgPool) {
    assert_each_item_signs_its_price_decade(
        pool,
        "USD",
        2,
        [
            ("boots_01", 999, "USD:2"),
            ("laces_02", 12_500, "USD:4"),
            ("socks_03", 1_000_000, "USD:6"),
        ],
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn items_of_a_yen_order_sign_their_own_price_decades(pool: PgPool) {
    assert_each_item_signs_its_price_decade(
        pool,
        "JPY",
        0,
        [
            ("boots_01", 450, "JPY:2"),
            ("laces_02", 4_500, "JPY:3"),
            ("socks_03", 1_000_000, "JPY:6"),
        ],
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_order_total_band_stays_consent_only_beside_the_price_decades(pool: PgPool) {
    let app = test_app_with_attestor(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &band_consent_command(&seller, true)).await;
    assert_eq!(status, StatusCode::OK, "consent: {body}");
    let order_id = delivered_priced_order(
        &app,
        &seller,
        &buyer,
        &[
            ("boots_01", 999),
            ("laces_02", 12_500),
            ("socks_03", 1_000_000),
        ],
        "USD",
        2,
    )
    .await;

    let mut command = items_review(
        &order_id,
        4,
        &[("boots_01", 5), ("laces_02", 4), ("socks_03", 3)],
        7_511,
    );
    command["payload"]["allow_amount_band"] = json!(true);
    let (status, reviewed) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "review failed: {reviewed}");
    // The band is the order total's decade, the same on every item: the
    // total is 1,013,499 cents of items plus shipping, under 10^7.
    let total = reviewed["result"]["order"]["total"]["amount_minor"]
        .as_i64()
        .expect("order total");
    assert!((1_000_000..10_000_000).contains(&total), "{total}");
    let decades: Vec<(Value, Value)> = reviewed["result"]["attestations"]
        .as_array()
        .expect("attestations")
        .iter()
        .map(|attestation| {
            let claims = claims_of(attestation);
            (
                claims["amount_band"].clone(),
                claims["price_decade"].clone(),
            )
        })
        .collect();
    assert_eq!(
        decades,
        vec![
            (json!("USD:6"), json!("USD:2")),
            (json!("USD:6"), json!("USD:4")),
            (json!("USD:6"), json!("USD:6")),
        ]
    );
}
