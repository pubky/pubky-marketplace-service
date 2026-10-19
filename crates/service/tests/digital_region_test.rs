//! No digital sales to EU or UK buyers at launch (legal launch plan C3b),
//! behind `DIGITAL_REGION_BLOCK_ENABLED`, independent of the EU fence.

mod common;

use axum::http::StatusCode;
use marketplace_service::config::Config;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    as_digital_checkout, as_digital_locks_listing, checkout_command_with_id, create_offer_command,
    execute, indexed_command_id, lock_resource_for, new_actor, offer_action, register_command,
    spawn_fake_homeserver, test_app_with_config, with_shop_homeserver, TestActor, TestApp,
    OFFER_COMMAND_ID,
};

const MESSAGE: &str = "Digital items aren't available to buyers in the EU or UK yet.";
const KINDS: [&str; 5] = ["file", "link", "text", "email", "message"];

async fn app(pool: PgPool, digital_block: bool, fence: bool) -> TestApp {
    let mut config = Config::for_tests();
    config.digital_region_block_enabled = digital_block;
    config.eu_fence_enabled = fence;
    test_app_with_config(pool, config).await
}

async fn physical_seller(app: &TestApp) -> TestActor {
    let seller = new_actor(app).await;
    let (code, body) = execute(app, &seller.token, &register_command(&seller.pubky, 50)).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    seller
}

/// A digital-only listing of one delivery kind.
async fn digital_seller(app: &TestApp, kind: &str) -> TestActor {
    let seller = new_actor(app).await;
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["fulfillment_methods"] = json!(["digital"]);
    let (code, body) = execute(app, &seller.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    sqlx::query("UPDATE listings SET digital_delivery_kind = $2 WHERE seller_pubky = $1")
        .bind(&seller.pubky)
        .bind(kind)
        .execute(&app.pool)
        .await
        .expect("set the delivery kind");
    seller
}

async fn locks_seller(app: &TestApp) -> TestActor {
    let seller = new_actor(app).await;
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    as_digital_locks_listing(&mut listing);
    let (code, body) = execute(app, &seller.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    seller
}

fn digital_checkout(seller: &TestActor, number: u64, country: Option<&str>) -> Value {
    let mut command = as_digital_checkout(checkout_command_with_id(
        &seller.pubky,
        &indexed_command_id(0x7100, number),
    ));
    if let Some(country) = country {
        command["payload"]["buyer_country"] = json!(country);
    }
    command
}

fn physical_checkout(seller: &TestActor, number: u64, country: Option<&str>) -> Value {
    let mut command = checkout_command_with_id(&seller.pubky, &indexed_command_id(0x7200, number));
    if let Some(country) = country {
        command["payload"]["buyer_country"] = json!(country);
    }
    command
}

#[track_caller]
fn assert_blocked(result: &(StatusCode, Value), reason: &str) {
    let (code, body) = result;
    assert_eq!(*code, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["code"],
        json!("DIGITAL_UNAVAILABLE_IN_REGION")
    );
    assert_eq!(body["error"]["message"], json!(MESSAGE));
    assert_eq!(body["error"]["reason"], json!(reason), "{body}");
}

async fn orders(app: &TestApp) -> i64 {
    common::count(&app.pool, "SELECT COUNT(*) FROM orders").await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn every_digital_kind_is_refused_for_eu_and_gb_buyers(pool: PgPool) {
    let app = app(pool, true, false).await;
    let buyer = new_actor(&app).await;
    let mut number = 0;
    for kind in KINDS {
        let seller = digital_seller(&app, kind).await;
        for country in ["DE", "FR", "GR", "IE", "NL", "SE"] {
            number += 1;
            let result = execute(
                &app,
                &buyer.token,
                &digital_checkout(&seller, number, Some(country)),
            )
            .await;
            assert_blocked(&result, "eu_buyer");
        }
        number += 1;
        let gb = execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, number, Some("GB")),
        )
        .await;
        assert_blocked(&gb, "gb_buyer");
    }
    assert_eq!(orders(&app).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_locks_listing_is_refused_for_eu_and_gb_buyers_and_open_elsewhere(pool: PgPool) {
    let app = app(pool, true, false).await;
    let seller = locks_seller(&app).await;
    let buyer = new_actor(&app).await;
    assert_blocked(
        &execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, 1, Some("DE")),
        )
        .await,
        "eu_buyer",
    );
    assert_blocked(
        &execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, 2, Some("GB")),
        )
        .await,
        "gb_buyer",
    );
    // A shipping-shaped command (an address, no fulfillment choice) against a
    // Locks listing is the same sale.
    let mut shaped = physical_checkout(&seller, 3, Some("FR"));
    shaped["command_id"] = json!(indexed_command_id(0x7100, 3));
    assert_blocked(&execute(&app, &buyer.token, &shaped).await, "eu_buyer");
    assert_eq!(orders(&app).await, 0);

    for (number, country) in [(10, "US"), (11, "CH"), (12, "NO"), (13, "BR")] {
        let result = execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, number, Some(country)),
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{country}: {}", result.1);
    }
    assert_eq!(orders(&app).await, 4);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn physical_items_are_unaffected_for_every_country(pool: PgPool) {
    let app = app(pool, true, false).await;
    let seller = physical_seller(&app).await;
    let buyer = new_actor(&app).await;
    for (number, country) in [(1, "DE"), (2, "GB"), (3, "US"), (4, "FR")] {
        let result = execute(
            &app,
            &buyer.token,
            &physical_checkout(&seller, number, Some(country)),
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{country}: {}", result.1);
    }
    // This flag alone does not ask a physical order for a country.
    let result = execute(&app, &buyer.token, &physical_checkout(&seller, 5, None)).await;
    assert_eq!(result.0, StatusCode::OK, "{}", result.1);
    assert_eq!(orders(&app).await, 5);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_digital_line_needs_the_buyer_country(pool: PgPool) {
    let app = app(pool, true, false).await;
    let seller = locks_seller(&app).await;
    let buyer = new_actor(&app).await;
    let result = execute(&app, &buyer.token, &digital_checkout(&seller, 1, None)).await;
    assert_eq!(result.0, StatusCode::UNPROCESSABLE_ENTITY, "{}", result.1);
    assert_eq!(result.1["error"]["code"], json!("INVALID_COMMAND"));
    assert_eq!(
        result.1["error"]["issues"][0]["path"],
        json!("payload.buyer_country")
    );
    assert_eq!(orders(&app).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_mixed_listing_is_refused_only_for_the_digital_choice(pool: PgPool) {
    let app = app(pool, true, false).await;
    let seller = new_actor(&app).await;
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["fulfillment_methods"] = json!(["shipping", "digital"]);
    let (code, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let buyer = new_actor(&app).await;

    let shipped = execute(
        &app,
        &buyer.token,
        &physical_checkout(&seller, 1, Some("DE")),
    )
    .await;
    assert_eq!(shipped.0, StatusCode::OK, "{}", shipped.1);

    let mut digital = physical_checkout(&seller, 2, Some("DE"));
    digital["payload"]["lines"][0]["fulfillment"] = json!("digital");
    digital["payload"]
        .as_object_mut()
        .expect("payload")
        .remove("delivery_address");
    assert_blocked(&execute(&app, &buyer.token, &digital).await, "eu_buyer");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_off_digital_sells_to_everyone_without_a_country(pool: PgPool) {
    let app = app(pool, false, false).await;
    let seller = locks_seller(&app).await;
    let buyer = new_actor(&app).await;
    for (number, country) in [(1, Some("DE")), (2, Some("GB")), (3, None)] {
        let result = execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, number, country),
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{country:?}: {}", result.1);
    }
    assert_eq!(orders(&app).await, 3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_stay_refused_on_digital_items_and_physical_offer_checkouts_pass(pool: PgPool) {
    let app = app(pool, true, false).await;
    let digital = locks_seller(&app).await;
    let buyer = new_actor(&app).await;
    let mut offer = create_offer_command(&digital.pubky, 1);
    offer["payload"]["buyer_country"] = json!("DE");
    let refused = execute(&app, &buyer.token, &offer).await;
    assert_eq!(refused.0, StatusCode::CONFLICT, "{}", refused.1);
    assert_eq!(
        refused.1["error"]["reason"],
        json!("offers_unavailable_for_digital")
    );

    let seller = physical_seller(&app).await;
    let mut offer = create_offer_command(&seller.pubky, 1);
    offer["payload"]["buyer_country"] = json!("DE");
    let created = execute(&app, &buyer.token, &offer).await;
    assert_eq!(created.0, StatusCode::OK, "{}", created.1);
    let accepted = execute(
        &app,
        &seller.token,
        &offer_action("offer.accept", 1, "00000000-0000-4000-8000-000000001101"),
    )
    .await;
    assert_eq!(accepted.0, StatusCode::OK, "{}", accepted.1);
    let row: (uuid::Uuid, String, i64, String, String) = sqlx::query_as(
        "SELECT award_id, listing_aggregate_id, accepted_listing_revision, \
         accepted_listing_record_sha256, accepted_variant_id FROM offers WHERE id = $1",
    )
    .bind(uuid::Uuid::parse_str(OFFER_COMMAND_ID).expect("offer id"))
    .fetch_one(&app.pool)
    .await
    .expect("accepted offer row");
    let command = json!({
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
            "buyer_country": "DE",
            "delivery_address": {
                "name": "Alice Buyer", "line1": "1 Market Street", "line2": "",
                "city": "Berlin", "region": "", "postal_code": "10115", "country_code": "DE"
            },
        }
    });
    let order = execute(&app, &buyer.token, &command).await;
    assert_eq!(order.0, StatusCode::OK, "{}", order.1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_replay_is_answered_from_its_stored_result(pool: PgPool) {
    let mut config = Config::for_tests();
    config.digital_region_block_enabled = false;
    let app = test_app_with_config(pool, config).await;
    let seller = locks_seller(&app).await;
    let buyer = new_actor(&app).await;
    let command = digital_checkout(&seller, 1, Some("DE"));
    let first = execute(&app, &buyer.token, &command).await;
    assert_eq!(first.0, StatusCode::OK, "{}", first.1);

    let mut config = Config::for_tests();
    config.digital_region_block_enabled = true;
    let mut state = app.state.clone();
    state.config = std::sync::Arc::new(config);
    let router = marketplace_service::http::build_router(state);
    let replay = common::send(router, "POST", "/v1/commands", Some(&buyer.token), &command).await;
    assert_eq!(replay.0, StatusCode::OK, "{}", replay.1);
    assert_eq!(orders(&app).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_two_flags_are_independent_and_compose(pool: PgPool) {
    let homeserver = spawn_fake_homeserver().await;
    let app = with_shop_homeserver(app(pool, true, true).await, &homeserver);
    let seller = new_actor(&app).await;
    homeserver.put_shop_record(
        &seller.pubky,
        json!({
            "schemaVersion": 1, "recordType": "shop", "ownerPubky": seller.pubky,
            "name": "S", "sellerStatus": "private", "location": { "countryCode": "CH" },
        }),
    );
    let mut listing = register_command(&seller.pubky, 50);
    listing["payload"]["digital_lock"] = json!({
        "policyUri": lock_resource_for(&seller.pubky),
        "criterionId": "paykit",
    });
    as_digital_locks_listing(&mut listing);
    let (code, body) = execute(&app, &seller.token, &listing).await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let buyer = new_actor(&app).await;

    // A declared private seller is open to the EU under the fence, but the
    // digital rule still holds the line back.
    assert_blocked(
        &execute(
            &app,
            &buyer.token,
            &digital_checkout(&seller, 1, Some("DE")),
        )
        .await,
        "eu_buyer",
    );
}
