//! A Locks listing is a digital listing: it registers as digital, its checkout
//! takes the digital method and collects no delivery address.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    execute, indexed_command_id, listing_aggregate, lock_resource_for, new_actor, register_command,
    test_app, TestApp,
};

fn register_locks_listing(seller: &str, methods: Value) -> Value {
    let mut command = register_command(seller, 1);
    command["payload"]["fulfillment_methods"] = methods;
    command["payload"]["digital_lock"] =
        json!({ "policyUri": lock_resource_for(seller), "criterionId": "paykit" });
    command
}

fn checkout(seller: &str, fulfillment: Option<&str>, with_address: bool) -> Value {
    let mut line = json!({
        "listing_aggregate_id": listing_aggregate(seller),
        "expected_revision": 1,
        "quantity": 1,
    });
    if let Some(fulfillment) = fulfillment {
        line["fulfillment"] = json!(fulfillment);
    }
    let mut payload = json!({ "lines": [line], "guarantee_policy_version": 1 });
    if with_address {
        payload["delivery_address"] = json!({
            "name": "Alice Buyer",
            "line1": "1 Market Street",
            "line2": "",
            "city": "New York",
            "region": "NY",
            "postal_code": "10001",
            "country_code": "US",
        });
    }
    json!({
        "version": 1,
        "command_id": indexed_command_id(0x8003, 1),
        "aggregate_id": format!("checkout:{}", indexed_command_id(0x8003, 1)),
        "expected_revision": 0,
        "issued_at": "2026-08-19T22:00:00.000Z",
        "kind": "checkout.create",
        "payload": payload,
    })
}

async fn methods_of(app: &TestApp, seller: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT fulfillment_methods FROM listings WHERE aggregate_id = $1")
        .bind(listing_aggregate(seller))
        .fetch_one(&app.pool)
        .await
        .expect("listing row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_digital_locks_listing_checks_out_without_a_delivery_address(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let (status, body) = execute(
        &app,
        &seller.token,
        &register_locks_listing(&seller.pubky, json!(["digital"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(methods_of(&app, &seller.pubky).await, vec!["digital"]);

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, Some("digital"), false),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = &body["result"]["orders"][0];
    assert_eq!(order["state"], json!("pending_payment"));
    assert_eq!(order["fulfillment"], json!("digital"));
    assert_eq!(order["shipping"]["amount_minor"], json!(0), "{order}");
    let (stored_address,): (bool,) = sqlx::query_as(
        "SELECT delivery_address IS NULL AND delivery_address_envelope IS NULL FROM orders",
    )
    .fetch_one(&app.pool)
    .await
    .expect("order row");
    assert!(stored_address, "no address was collected or stored");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_locks_checkout_that_presents_an_address_is_still_refused_for_digital(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_locks_listing(&seller.pubky, json!(["digital"])),
    )
    .await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, Some("digital"), true),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_listing_without_a_lock_cannot_be_bought_as_digital(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let (status, body) = execute(
        &app,
        &buyer.token,
        &checkout(&seller.pubky, Some("digital"), false),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_digital_locks_listing_cannot_be_bought_as_shipping(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(
        &app,
        &seller.token,
        &register_locks_listing(&seller.pubky, json!(["digital"])),
    )
    .await;

    let (status, body) = execute(&app, &buyer.token, &checkout(&seller.pubky, None, true)).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["reason"], json!("fulfillment_not_published"));
}

// The registration invariant behind the Shop's digital-only Locks UX: a
// listing carrying a Locks policy publishes exactly [digital]; any physical or
// pickup method beside it (or instead of it) is refused with a clear issue and
// registers nothing.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_locks_listing_must_publish_exactly_digital(pool: PgPool) {
    let app = test_app(pool).await;
    let seller = new_actor(&app).await;

    for methods in [
        json!(["shipping"]),
        json!(["pickup"]),
        json!(["shipping", "digital"]),
        json!(["pickup", "digital"]),
        json!(["shipping", "pickup", "digital"]),
    ] {
        let (status, body) = execute(
            &app,
            &seller.token,
            &register_locks_listing(&seller.pubky, methods.clone()),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{methods}: {body}"
        );
        assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"), "{methods}");
        assert_eq!(
            body["error"]["issues"][0]["path"],
            json!("payload.fulfillment_methods"),
            "{methods}: {body}"
        );
        assert!(
            body["error"]["issues"][0]["message"]
                .as_str()
                .is_some_and(|message| message.contains("digital-only")),
            "{methods}: {body}"
        );
        let registered: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM listings")
            .fetch_one(&app.pool)
            .await
            .expect("listing count");
        assert_eq!(registered, 0, "{methods}: nothing registered");
    }

    let (status, body) = execute(
        &app,
        &seller.token,
        &register_locks_listing(&seller.pubky, json!(["digital"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
