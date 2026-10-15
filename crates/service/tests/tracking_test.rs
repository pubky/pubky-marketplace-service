//! Optional tracking and `fulfillment.update_tracking`: an order ships
//! without a tracking number, the seller adds or edits the carrier and
//! number while it is shipped, and the buyer sees each update through the
//! same notification and seller event paths the ship event uses.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    count, create_paid_order, execute, new_actor, order_command, send, test_app, TestActor, TestApp,
};
use marketplace_service::clock::{format_timestamp, Clock};
use marketplace_service::workers::drain_outbox;

async fn get(app: &TestApp, uri: &str, token: &str) -> (StatusCode, Value) {
    send(app.router.clone(), "GET", uri, Some(token), &json!(null)).await
}

fn ship_command(order_id: &str, revision: i64, payload: Value, number: u64) -> Value {
    order_command("fulfillment.ship", order_id, revision, payload, number)
}

fn update_command(
    order_id: &str,
    revision: i64,
    carrier: &str,
    tracking_number: &str,
    number: u64,
) -> Value {
    order_command(
        "fulfillment.update_tracking",
        order_id,
        revision,
        json!({ "carrier": carrier, "tracking_number": tracking_number }),
        number,
    )
}

struct Fixture {
    app: TestApp,
    seller: TestActor,
    buyer: TestActor,
    order_id: String,
}

async fn paid_order(pool: PgPool) -> Fixture {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    Fixture {
        app,
        seller,
        buyer,
        order_id: order.order_id,
    }
}

/// A paid order shipped without a tracking number (revision 3).
async fn shipped_without_tracking(pool: PgPool) -> Fixture {
    let f = paid_order(pool).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship_command(&f.order_id, 2, json!({ "carrier": "Other" }), 2_001),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship failed: {body}");
    f
}

async fn read_order(f: &Fixture, token: &str) -> Value {
    let (status, order) = get(&f.app, &format!("/v1/orders/{}", f.order_id), token).await;
    assert_eq!(status, StatusCode::OK, "order read failed: {order}");
    order
}

async fn notification_types(f: &Fixture, token: &str) -> Vec<String> {
    drain_outbox(&f.app.pool, None, f.app.clock.now(), 30)
        .await
        .expect("outbox drains");
    let (status, body) = get(&f.app, "/v1/notifications", token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["notifications"]
        .as_array()
        .expect("notifications array")
        .iter()
        .map(|n| n["type"].as_str().expect("type").to_string())
        .collect()
}

async fn seller_event_types(f: &Fixture) -> Vec<String> {
    let (status, body) = get(
        &f.app,
        &format!("/v1/sellers/{}/events?limit=100", f.seller.pubky),
        &f.seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|e| e["type"].as_str().expect("type").to_string())
        .collect()
}

async fn order_event_count(f: &Fixture, kind: &str) -> i64 {
    count(
        &f.app.pool,
        &format!(
            "SELECT COUNT(*) FROM events WHERE aggregate_id = 'order:{}' AND kind = '{kind}'",
            f.order_id
        ),
    )
    .await
}

async fn assert_ships_without_tracking(pool: PgPool, payload: Value) {
    let f = paid_order(pool).await;
    let (status, shipped) = execute(
        &f.app,
        &f.seller.token,
        &ship_command(&f.order_id, 2, payload, 2_001),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship failed: {shipped}");
    let order = &shipped["result"]["order"];
    assert_eq!(order["state"], json!("shipped"));
    assert_eq!(order["revision"], json!(3));
    assert_eq!(order["shipment"]["carrier"], json!("Other"));
    assert_eq!(order["shipment"]["tracking_number"], Value::Null);
    assert_eq!(order["shipment"]["state"], json!("shipped"));
    assert_eq!(order["shipment"]["tracking_updated_at"], Value::Null);
    assert_eq!(order["shipment"]["tracking_history"], json!([]));

    for token in [&f.buyer.token, &f.seller.token] {
        let projected = read_order(&f, token).await;
        assert_eq!(projected["state"], json!("shipped"));
        assert_eq!(projected["shipment"]["carrier"], json!("Other"));
        assert_eq!(projected["shipment"]["tracking_number"], Value::Null);
    }
    let (status, listed) = get(&f.app, "/v1/orders", &f.buyer.token).await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert_eq!(
        listed["orders"][0]["shipment"]["tracking_number"],
        Value::Null
    );
    let (status, export) = get(
        &f.app,
        &format!("/v1/sellers/{}/orders", f.seller.pubky),
        &f.seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{export}");
    assert_eq!(export["orders"][0]["shipment"]["carrier"], json!("Other"));
    assert_eq!(
        export["orders"][0]["shipment"]["tracking_number"],
        Value::Null
    );

    assert!(seller_event_types(&f)
        .await
        .contains(&"fulfillment.shipped".to_string()));
    assert!(notification_types(&f, &f.buyer.token)
        .await
        .contains(&"order_shipped".to_string()));

    // The shipment without a number still completes the normal lifecycle.
    let (status, delivered) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &f.order_id,
            3,
            json!({}),
            2_002,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery failed: {delivered}");
    assert_eq!(
        delivered["result"]["order"]["shipment"]["tracking_number"],
        Value::Null
    );
    assert_eq!(
        delivered["result"]["order"]["shipment"]["state"],
        json!("delivered")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ships_without_a_tracking_number_when_the_field_is_absent(pool: PgPool) {
    assert_ships_without_tracking(pool, json!({ "carrier": "Other" })).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ships_without_a_tracking_number_when_it_is_null(pool: PgPool) {
    assert_ships_without_tracking(pool, json!({ "carrier": "Other", "tracking_number": null }))
        .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ships_without_a_tracking_number_when_it_is_empty_or_blank(pool: PgPool) {
    assert_ships_without_tracking(
        pool,
        json!({ "carrier": " Other ", "tracking_number": "  " }),
    )
    .await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ship_still_requires_a_carrier_and_a_printable_number(pool: PgPool) {
    let f = paid_order(pool).await;
    for (payload, path) in [
        (json!({}), None),
        (json!({ "carrier": "" }), Some("payload.carrier")),
        (
            json!({ "carrier": "Other", "tracking_number": "A\tB" }),
            Some("payload.tracking_number"),
        ),
        (
            json!({ "carrier": "Other", "tracking_number": "9".repeat(201) }),
            Some("payload.tracking_number"),
        ),
    ] {
        let (status, body) = execute(
            &f.app,
            &f.seller.token,
            &ship_command(&f.order_id, 2, payload.clone(), 2_010),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{payload}: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        if let Some(path) = path {
            assert!(
                body["error"]["issues"]
                    .as_array()
                    .expect("issues")
                    .iter()
                    .any(|issue| issue["path"] == json!(path)),
                "{payload}: {body}"
            );
        }
    }
    assert_eq!(
        read_order(&f, &f.seller.token).await["state"],
        json!("paid")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn ship_with_a_number_keeps_the_existing_shape(pool: PgPool) {
    let f = paid_order(pool).await;
    let (status, shipped) = execute(
        &f.app,
        &f.seller.token,
        &ship_command(
            &f.order_id,
            2,
            json!({ "carrier": "USPS", "tracking_number": " 9400 " }),
            2_001,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{shipped}");
    let shipment = &shipped["result"]["order"]["shipment"];
    assert_eq!(shipment["carrier"], json!("USPS"));
    assert_eq!(shipment["tracking_number"], json!("9400"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_adds_then_edits_and_the_buyer_sees_each_update(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    f.app.clock.advance_seconds(60);
    let added_at = format_timestamp(f.app.clock.now());

    let (status, added) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, " USPS ", " 9400-1 ", 2_100),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "add failed: {added}");
    assert_eq!(added["result"]["kind"], json!("order"));
    assert_eq!(added["result"]["changed"], json!(true));
    assert_eq!(added["result"]["order"]["state"], json!("shipped"));
    assert_eq!(added["result"]["order"]["revision"], json!(4));
    let shipment = &added["result"]["order"]["shipment"];
    assert_eq!(shipment["carrier"], json!("USPS"));
    assert_eq!(shipment["tracking_number"], json!("9400-1"));
    assert_eq!(shipment["tracking_updated_at"], json!(added_at));
    assert_eq!(
        shipment["tracking_history"],
        json!([{
            "at": added_at,
            "from": { "carrier": "Other", "tracking_number": null },
            "to": { "carrier": "USPS", "tracking_number": "9400-1" },
        }])
    );
    assert!(shipment["shipped_at"].is_string(), "shipped_at survives");
    assert_eq!(shipment["delivered_at"], Value::Null);
    assert!(!added.to_string().contains("1 Market Street"));

    f.app.clock.advance_seconds(60);
    let edited_at = format_timestamp(f.app.clock.now());
    let (status, edited) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 4, "UPS", "1Z-2", 2_101),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "edit failed: {edited}");
    assert_eq!(edited["result"]["order"]["revision"], json!(5));
    let history = edited["result"]["order"]["shipment"]["tracking_history"]
        .as_array()
        .expect("history")
        .clone();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[1],
        json!({
            "at": edited_at,
            "from": { "carrier": "USPS", "tracking_number": "9400-1" },
            "to": { "carrier": "UPS", "tracking_number": "1Z-2" },
        })
    );
    assert_eq!(history[0]["at"], json!(added_at));

    for token in [&f.buyer.token, &f.seller.token] {
        let projected = read_order(&f, token).await;
        assert_eq!(projected["revision"], json!(5));
        assert_eq!(projected["shipment"]["carrier"], json!("UPS"));
        assert_eq!(projected["shipment"]["tracking_number"], json!("1Z-2"));
        assert_eq!(
            projected["shipment"]["tracking_updated_at"],
            json!(edited_at)
        );
        assert_eq!(
            projected["shipment"]["tracking_history"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
    }
    let (status, export) = get(
        &f.app,
        &format!("/v1/sellers/{}/orders", f.seller.pubky),
        &f.seller.token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{export}");
    assert_eq!(
        export["orders"][0]["shipment"]["tracking_number"],
        json!("1Z-2")
    );

    // One event per change, at the revision it produced, in the seller's
    // feed (the webhook source) and one buyer notification per change.
    assert_eq!(
        order_event_count(&f, "fulfillment.tracking_updated").await,
        2
    );
    let revisions: Vec<i64> = sqlx::query_scalar(
        "SELECT revision FROM events WHERE aggregate_id = $1 \
         AND kind = 'fulfillment.tracking_updated' ORDER BY revision",
    )
    .bind(format!("order:{}", f.order_id))
    .fetch_all(&f.app.pool)
    .await
    .expect("event revisions");
    assert_eq!(revisions, vec![4, 5]);
    let feed = seller_event_types(&f).await;
    assert_eq!(
        feed.iter()
            .filter(|kind| *kind == "fulfillment.tracking_updated")
            .count(),
        2
    );
    let recipient: Vec<(String, String)> = sqlx::query_as(
        "SELECT payload->>'recipient_pubky', payload->>'actor_pubky' FROM outbox \
         WHERE kind = 'notification.tracking_updated' ORDER BY id",
    )
    .fetch_all(&f.app.pool)
    .await
    .expect("outbox rows");
    assert_eq!(
        recipient,
        vec![
            (f.buyer.pubky.clone(), f.seller.pubky.clone()),
            (f.buyer.pubky.clone(), f.seller.pubky.clone())
        ]
    );
    let buyer_types = notification_types(&f, &f.buyer.token).await;
    assert_eq!(
        buyer_types
            .iter()
            .filter(|kind| *kind == "tracking_updated")
            .count(),
        2,
        "{buyer_types:?}"
    );
    assert!(
        !notification_types(&f, &f.seller.token)
            .await
            .contains(&"tracking_updated".to_string()),
        "the seller is not notified of their own update"
    );

    // Delivery after an update keeps the corrected tracking and the history.
    let (status, delivered) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &f.order_id,
            5,
            json!({}),
            2_102,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{delivered}");
    let shipment = &delivered["result"]["order"]["shipment"];
    assert_eq!(shipment["tracking_number"], json!("1Z-2"));
    assert_eq!(
        shipment["tracking_history"].as_array().map(Vec::len),
        Some(2)
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_with_identical_values_changes_nothing(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    let (status, first) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_200),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let events = order_event_count(&f, "fulfillment.tracking_updated").await;
    let outbox = count(
        &f.app.pool,
        "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.tracking_updated'",
    )
    .await;

    // A new command (new id) with the same values at the current revision.
    let (status, repeat) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 4, "USPS", "9400-1", 2_201),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "repeat failed: {repeat}");
    assert_eq!(repeat["result"]["changed"], json!(false));
    assert_eq!(repeat["result"]["order"]["revision"], json!(4));
    assert_eq!(repeat["revision"], json!(4));
    assert_eq!(repeat["event_ids"], json!([]));
    assert_eq!(
        repeat["result"]["order"]["shipment"],
        first["result"]["order"]["shipment"]
    );
    assert_eq!(
        order_event_count(&f, "fulfillment.tracking_updated").await,
        events
    );
    assert_eq!(
        count(
            &f.app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.tracking_updated'"
        )
        .await,
        outbox
    );
    assert_eq!(read_order(&f, &f.buyer.token).await["revision"], json!(4));

    // Replaying the original command id returns the stored result.
    let (status, replay) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_200),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, first);

    // The optimistic revision still applies to an identical-value repeat.
    let (status, stale) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_202),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{stale}");
    assert_eq!(stale["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(stale["error"]["current_revision"], json!(4));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_is_refused_unless_the_order_is_shipped(pool: PgPool) {
    let f = paid_order(pool).await;

    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 2, "USPS", "9400-1", 2_300),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "paid: {body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["reason"], json!("order_not_shipped"));

    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship_command(
            &f.order_id,
            2,
            json!({ "carrier": "Other", "tracking_number": "T-0" }),
            2_301,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &f.order_id,
            3,
            json!({}),
            2_302,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 4, "USPS", "9400-1", 2_303),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "delivered: {body}");
    assert_eq!(body["error"]["reason"], json!("order_not_shipped"));

    let (status, body) = execute(
        &f.app,
        &f.buyer.token,
        &order_command(
            "review.create",
            &f.order_id,
            4,
            json!({ "rating": 5, "text": "Fine." }),
            2_304,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["order"]["state"], json!("completed"));
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 5, "USPS", "9400-1", 2_305),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "completed: {body}");
    assert_eq!(body["error"]["reason"], json!("order_not_shipped"));

    let order = read_order(&f, &f.seller.token).await;
    assert_eq!(order["shipment"]["tracking_number"], json!("T-0"));
    assert_eq!(
        order["shipment"]["tracking_history"],
        json!([]),
        "refusals record nothing"
    );
    assert_eq!(
        order_event_count(&f, "fulfillment.tracking_updated").await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_is_seller_only_and_validates_its_payload(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    let stranger = new_actor(&f.app).await;

    for (actor, label) in [(&f.buyer, "buyer"), (&stranger, "stranger")] {
        let (status, body) = execute(
            &f.app,
            &actor.token,
            &update_command(&f.order_id, 3, "USPS", "9400-1", 2_400),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
        assert_eq!(body["error"]["code"], json!("UNAUTHORIZED"));
    }

    let mut wrong_aggregate = update_command(&f.order_id, 3, "USPS", "9400-1", 2_401);
    wrong_aggregate["aggregate_id"] = json!(format!("order:{}", uuid::Uuid::new_v4()));
    let (status, body) = execute(&f.app, &f.seller.token, &wrong_aggregate).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));

    for (payload, path) in [
        (json!({ "carrier": "USPS" }), None::<&str>),
        (json!({ "carrier": "USPS", "tracking_number": null }), None),
        (
            json!({ "carrier": "USPS", "tracking_number": "   " }),
            Some("payload.tracking_number"),
        ),
        (
            json!({ "carrier": "", "tracking_number": "9400-1" }),
            Some("payload.carrier"),
        ),
        (
            json!({ "carrier": "USPS", "tracking_number": "9400\n1" }),
            Some("payload.tracking_number"),
        ),
        (
            json!({ "carrier": "USPS", "tracking_number": "9400-1", "note": "x" }),
            None,
        ),
    ] {
        let (status, body) = execute(
            &f.app,
            &f.seller.token,
            &order_command(
                "fulfillment.update_tracking",
                &f.order_id,
                3,
                payload.clone(),
                2_402,
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{payload}: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"));
        if let Some(path) = path {
            assert!(
                body["error"]["issues"]
                    .as_array()
                    .expect("issues")
                    .iter()
                    .any(|issue| issue["path"] == json!(path)),
                "{payload}: {body}"
            );
        }
    }
    assert_eq!(read_order(&f, &f.seller.token).await["revision"], json!(3));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_respects_the_payment_not_final_gate(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    sqlx::query(
        "UPDATE orders SET payment_finality = 'reverted', payment_finality_at = now() \
         WHERE id = $1::uuid",
    )
    .bind(&f.order_id)
    .execute(&f.app.pool)
    .await
    .expect("mark the transfer reverted after shipment");

    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_500),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["reason"], json!("payment_not_final"));
    assert_eq!(
        order_event_count(&f, "fulfillment.tracking_updated").await,
        0
    );

    sqlx::query("UPDATE orders SET payment_finality = 'final' WHERE id = $1::uuid")
        .bind(&f.order_id)
        .execute(&f.app.pool)
        .await
        .expect("mark the transfer final");
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_501),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["changed"], json!(true));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_refuses_past_the_recorded_history_limit(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    let history: Vec<Value> = (0..100)
        .map(|i| {
            json!({
                "at": "2026-08-19T22:00:00.000Z",
                "from": { "carrier": "Other", "tracking_number": format!("T-{i}") },
                "to": { "carrier": "Other", "tracking_number": format!("T-{}", i + 1) },
            })
        })
        .collect();
    sqlx::query(
        "UPDATE orders SET shipment = jsonb_set(shipment, '{tracking_history}', $2::jsonb) \
         WHERE id = $1::uuid",
    )
    .bind(&f.order_id)
    .bind(Value::Array(history))
    .execute(&f.app.pool)
    .await
    .expect("seed a full history");

    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "9400-1", 2_600),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], json!("INVALID_STATE"));
    assert_eq!(body["error"]["reason"], json!("tracking_update_limit"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_edits_a_shipment_that_predates_the_history_field(pool: PgPool) {
    let f = paid_order(pool).await;
    let (status, body) = execute(
        &f.app,
        &f.seller.token,
        &ship_command(
            &f.order_id,
            2,
            json!({ "carrier": "USPS", "tracking_number": "OLD-1" }),
            2_700,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    sqlx::query(
        "UPDATE orders SET shipment = shipment - 'tracking_history' - 'tracking_updated_at' \
         WHERE id = $1::uuid",
    )
    .bind(&f.order_id)
    .execute(&f.app.pool)
    .await
    .expect("strip the new fields like a shipment made before this release");

    let (status, edited) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 3, "USPS", "NEW-2", 2_701),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{edited}");
    assert_eq!(
        edited["result"]["order"]["shipment"]["tracking_history"][0]["from"],
        json!({ "carrier": "USPS", "tracking_number": "OLD-1" })
    );
}

/// Waits until `expected` order fetches are blocked on the order row lock.
/// The fetch is recognised by the start of its column list because
/// `pg_stat_activity.query` truncates the long statement.
async fn wait_for_blocked_order_fetches(f: &Fixture, expected: i64) {
    for _ in 0..400 {
        let waiting = count(
            &f.app.pool,
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND query LIKE 'SELECT id, auction_aggregate_id%'",
        )
        .await;
        if waiting >= expected {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("{expected} command(s) never blocked on the order row lock");
}

/// Runs two commands that both read the same revision while the order row is
/// held locked. `first` queues on the row lock before `second` is sent, so
/// the row lock (not request timing) decides the interleaving: `first` gets
/// the row and commits, and `second` then sees the new revision.
async fn race(
    f: &Fixture,
    first: (&str, Value),
    second: (&str, Value),
) -> ((StatusCode, Value), (StatusCode, Value)) {
    let mut holder = f.app.pool.begin().await.expect("holder begins");
    sqlx::query("SELECT id FROM orders WHERE id = $1::uuid FOR UPDATE")
        .bind(&f.order_id)
        .fetch_one(&mut *holder)
        .await
        .expect("holder locks the order");

    let spawn = |token: &str, command: Value| {
        let router = f.app.router.clone();
        let token = token.to_string();
        tokio::spawn(
            async move { send(router, "POST", "/v1/commands", Some(&token), &command).await },
        )
    };
    let a = spawn(first.0, first.1);
    wait_for_blocked_order_fetches(f, 1).await;
    let b = spawn(second.0, second.1);
    wait_for_blocked_order_fetches(f, 2).await;
    holder.commit().await.expect("holder releases the order");
    (
        a.await.expect("first command completes"),
        b.await.expect("second command completes"),
    )
}

async fn update_racing_confirm_delivery(pool: PgPool, update_first: bool) {
    let f = shipped_without_tracking(pool).await;
    let update = (
        f.seller.token.as_str(),
        update_command(&f.order_id, 3, "USPS", "9400-1", 2_800),
    );
    let confirm = (
        f.buyer.token.as_str(),
        order_command(
            "fulfillment.confirm_delivery",
            &f.order_id,
            3,
            json!({}),
            2_801,
        ),
    );
    let (update_result, confirm_result) = if update_first {
        race(&f, update, confirm).await
    } else {
        let (confirm_result, update_result) = race(&f, confirm, update).await;
        (update_result, confirm_result)
    };
    let (update_status, update_body) = update_result;
    let (confirm_status, confirm_body) = confirm_result;

    let order = read_order(&f, &f.buyer.token).await;
    assert_eq!(order["revision"], json!(4), "exactly one command committed");
    let events_updated = order_event_count(&f, "fulfillment.tracking_updated").await;
    let events_delivered = order_event_count(&f, "fulfillment.delivered").await;
    assert_eq!(events_updated + events_delivered, 1);

    if update_first {
        assert_eq!(update_status, StatusCode::OK, "{update_body}");
        assert_eq!(confirm_status, StatusCode::CONFLICT, "{confirm_body}");
        assert_eq!(confirm_body["error"]["code"], json!("REVISION_CONFLICT"));
        assert_eq!(confirm_body["error"]["current_revision"], json!(4));
        assert_eq!((events_updated, events_delivered), (1, 0));
        assert_eq!(order["state"], json!("shipped"));
        assert_eq!(order["shipment"]["tracking_number"], json!("9400-1"));
        // The buyer retries at the new revision and delivers the updated shipment.
        let (status, retry) = execute(
            &f.app,
            &f.buyer.token,
            &order_command(
                "fulfillment.confirm_delivery",
                &f.order_id,
                4,
                json!({}),
                2_802,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retry}");
        assert_eq!(
            retry["result"]["order"]["shipment"]["tracking_number"],
            json!("9400-1")
        );
    } else {
        assert_eq!(confirm_status, StatusCode::OK, "{confirm_body}");
        assert_eq!(update_status, StatusCode::CONFLICT, "{update_body}");
        assert_eq!(update_body["error"]["code"], json!("REVISION_CONFLICT"));
        assert_eq!(update_body["error"]["current_revision"], json!(4));
        assert_eq!((events_updated, events_delivered), (0, 1));
        assert_eq!(order["state"], json!("delivered"));
        assert_eq!(order["shipment"]["tracking_number"], Value::Null);
        assert_eq!(order["shipment"]["tracking_history"], json!([]));
        // The seller retries at the new revision and is refused: delivered.
        let (status, retry) = execute(
            &f.app,
            &f.seller.token,
            &update_command(&f.order_id, 4, "USPS", "9400-1", 2_802),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{retry}");
        assert_eq!(retry["error"]["reason"], json!("order_not_shipped"));
        assert_eq!(
            order_event_count(&f, "fulfillment.tracking_updated").await,
            0
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn update_tracking_wins_the_race_against_confirm_delivery(pool: PgPool) {
    update_racing_confirm_delivery(pool, true).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn confirm_delivery_wins_the_race_against_update_tracking(pool: PgPool) {
    update_racing_confirm_delivery(pool, false).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_racing_tracking_updates_have_one_winner_and_a_consistent_history(pool: PgPool) {
    let f = shipped_without_tracking(pool).await;
    let first = update_command(&f.order_id, 3, "USPS", "FIRST-1", 2_900);
    let second = update_command(&f.order_id, 3, "UPS", "SECOND-2", 2_901);
    let ((status_a, body_a), (status_b, body_b)) =
        race(&f, (&f.seller.token, first), (&f.seller.token, second)).await;

    assert_eq!(status_a, StatusCode::OK, "{body_a}");
    assert_eq!(status_b, StatusCode::CONFLICT, "{body_b}");
    assert_eq!(body_b["error"]["code"], json!("REVISION_CONFLICT"));
    assert_eq!(body_b["error"]["current_revision"], json!(4));
    assert_eq!(body_a["result"]["order"]["revision"], json!(4));

    let order = read_order(&f, &f.buyer.token).await;
    assert_eq!(order["revision"], json!(4));
    assert_eq!(order["shipment"]["carrier"], json!("USPS"));
    assert_eq!(order["shipment"]["tracking_number"], json!("FIRST-1"));
    let history = order["shipment"]["tracking_history"]
        .as_array()
        .expect("history");
    assert_eq!(history.len(), 1, "the loser recorded nothing");
    assert_eq!(history[0]["to"]["tracking_number"], json!("FIRST-1"));
    assert_eq!(
        order_event_count(&f, "fulfillment.tracking_updated").await,
        1
    );
    assert_eq!(
        count(
            &f.app.pool,
            "SELECT COUNT(*) FROM outbox WHERE kind = 'notification.tracking_updated'"
        )
        .await,
        1
    );

    // The loser retries at the new revision and lands as a second change.
    let (status, retry) = execute(
        &f.app,
        &f.seller.token,
        &update_command(&f.order_id, 4, "UPS", "SECOND-2", 2_902),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    let history = retry["result"]["order"]["shipment"]["tracking_history"]
        .as_array()
        .expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[1]["from"]["tracking_number"], json!("FIRST-1"));
    assert_eq!(history[1]["to"]["tracking_number"], json!("SECOND-2"));
}
