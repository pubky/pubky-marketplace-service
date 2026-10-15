//! Buyer opt-in address sharing with a connected store (LG5): the second
//! envelope, sealed to the connection's recipient key with the delivery
//! envelope's construction, stored with the buyer's consent, opened only for
//! the push, and deleted once the store order exists.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use common::connector_provider::*;
use common::{
    checkout_command_with_id, execute, listing_aggregate, new_actor, payment_command,
    register_command, sealed_checkout_command_with_id, TestActor, TestApp,
};
use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, OpModeS, Serializable};
use marketplace_service::connectors::worker;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

const CONTEXT: &str = "pubky-marketplace/connector-address/v1";
const COMMAND_ID: &str = "00000000-0000-4000-8000-000000001000";

fn address() -> Value {
    json!({
        "name": "Alice Buyer",
        "line1": "1 Market Street",
        "line2": "Floor 2",
        "city": "Zurich",
        "region": "ZH",
        "postalCode": "8001",
        "countryCode": "CH",
    })
}

/// What the buyer's client does: pad the address JSON to 2,048 bytes with
/// `0x80` then zeros and seal it, base mode, to the published key.
fn seal(public_key: &[u8], aad: &str, address: &Value) -> String {
    let recipient =
        <X25519HkdfSha256 as hpke::Kem>::PublicKey::from_bytes(public_key).expect("public key");
    let mut plaintext = serde_json::to_vec(address).expect("address");
    plaintext.push(0x80);
    plaintext.resize(2048, 0);
    let (encapped, ciphertext) =
        hpke::single_shot_seal::<ChaCha20Poly1305, HkdfSha256, X25519HkdfSha256>(
            &OpModeS::Base,
            &recipient,
            CONTEXT.as_bytes(),
            &plaintext,
            aad.as_bytes(),
        )
        .expect("seal");
    let mut wire = encapped.to_bytes().to_vec();
    wire.extend_from_slice(&ciphertext);
    assert_eq!(wire.len(), 2096);
    URL_SAFE_NO_PAD.encode(wire)
}

struct Published {
    key_id: String,
    public_key: Vec<u8>,
}

async fn published_key(app: &TestApp, seller: &str) -> Published {
    published_key_for(app, seller, SHOP).await
}

/// The recipient key the seller's public status publishes for one store.
async fn published_key_for(app: &TestApp, seller: &str, domain: &str) -> Published {
    let (_, status) = api(
        app,
        "GET",
        &format!("/v0/sellers/{seller}/connector-status"),
        None,
        &json!(null),
    )
    .await;
    let store = status["stores"]
        .as_array()
        .expect("stores")
        .iter()
        .find(|store| store["domain"] == domain)
        .unwrap_or_else(|| panic!("a published store for {domain}: {status}"));
    let key = &store["address_sharing"]["recipient_key"];
    Published {
        key_id: key["key_id"].as_str().expect("key id").to_string(),
        public_key: URL_SAFE_NO_PAD
            .decode(key["public_key"].as_str().expect("public key"))
            .expect("base64url"),
    }
}

fn share_entry(seller: &str, key: &Published, domain: &str, envelope: String) -> Value {
    json!({
        "seller_pubky": seller,
        "shop_domain": domain,
        "consent_version": "2026-10-a",
        "key_id": key.key_id,
        "envelope": envelope,
    })
}

struct Order {
    order_id: String,
    payment_id: String,
}

async fn checkout(
    app: &TestApp,
    buyer: &TestActor,
    command: Value,
) -> (StatusCode, Value, Option<Order>) {
    let (status, body) = execute(app, &buyer.token, &command).await;
    let order = (status == StatusCode::OK).then(|| Order {
        order_id: body["result"]["orders"][0]["id"]
            .as_str()
            .expect("order id")
            .to_string(),
        payment_id: body["result"]["payments"][0]["id"]
            .as_str()
            .expect("payment id")
            .to_string(),
    });
    (status, body, order)
}

async fn pay(app: &TestApp, buyer: &TestActor, order: &Order) {
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A connected store with a mapped listing, and the seller's published key.
async fn opted_in_world(
    pool: PgPool,
) -> (
    TestApp,
    Arc<TestStore>,
    TestActor,
    TestActor,
    Uuid,
    Published,
) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    let key = published_key(&app, &seller.pubky).await;
    (app, store, seller, buyer, connection, key)
}

fn sealed_with_share(
    seller: &TestActor,
    buyer: &TestActor,
    key: &Published,
    domain: &str,
    bind_buyer: &str,
) -> Value {
    let mut command = sealed_checkout_command_with_id(&seller.pubky, COMMAND_ID, 0);
    let aad = format!(
        "{CONTEXT}|{COMMAND_ID}|{bind_buyer}|{}|{}",
        seller.pubky, key.key_id
    );
    let envelope = seal(&key.public_key, &aad, &address());
    command["payload"]["connector_shares"] =
        json!([share_entry(&seller.pubky, key, domain, envelope)]);
    let _ = buyer;
    command
}

async fn columns(
    app: &TestApp,
    order: &str,
) -> (Option<String>, Option<String>, bool, bool, Option<String>) {
    sqlx::query_as(
        "SELECT connector_share_domain, connector_share_consent_version, \
                connector_share_consented_at IS NOT NULL, \
                connector_address_envelope IS NOT NULL, connector_key_id \
         FROM orders WHERE id = $1::uuid",
    )
    .bind(order)
    .fetch_one(&app.pool)
    .await
    .expect("order columns")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_opted_in_address_reaches_the_store_once_and_is_then_deleted(pool: PgPool) {
    let (app, store, seller, buyer, _connection, key) = opted_in_world(pool).await;
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &buyer.pubky);
    let (status, body, order) = checkout(&app, &buyer, command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = order.expect("order");

    // The consent and the envelope are on the order, and in no projection.
    assert_eq!(
        columns(&app, &order.order_id).await,
        (
            Some(SHOP.to_string()),
            Some("2026-10-a".to_string()),
            true,
            true,
            Some(key.key_id.clone())
        )
    );
    let (status, view) = api(
        &app,
        "GET",
        &format!("/v1/orders/{}", order.order_id),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let projected = view.to_string();
    assert!(!projected.contains("connector"), "{projected}");
    assert!(!projected.contains(&key.key_id));
    let (_, orders) = api(&app, "GET", "/v1/orders", Some(&buyer.token), &json!(null)).await;
    assert!(!orders.to_string().contains("connector"));

    pay(&app, &buyer, &order).await;
    worker::run_pass(&app.state).await.expect("pass");

    let pushed = store
        .last_order_push
        .lock()
        .expect("push lock")
        .clone()
        .expect("a push");
    let shipped = pushed
        .shipping_address
        .expect("the opted-in address arrives");
    assert_eq!(shipped.name, "Alice Buyer");
    assert_eq!(shipped.line1, "1 Market Street");
    assert_eq!(shipped.line2, "Floor 2");
    assert_eq!(shipped.city, "Zurich");
    assert_eq!(shipped.region, "ZH");
    assert_eq!(shipped.postal_code, "8001");
    assert_eq!(shipped.country_code, "CH");
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");

    // Once the store order exists the envelope is gone; the consent stays.
    assert_eq!(
        columns(&app, &order.order_id).await,
        (
            Some(SHOP.to_string()),
            Some("2026-10-a".to_string()),
            true,
            false,
            None
        )
    );
    // The seller's own envelope is untouched.
    let kept: bool = sqlx::query_scalar(
        "SELECT delivery_address_envelope IS NOT NULL FROM orders WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("delivery envelope");
    assert!(kept);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_envelope_survives_a_failed_push_and_goes_with_the_one_that_succeeds(pool: PgPool) {
    let (app, store, seller, buyer, connection, key) = opted_in_world(pool).await;
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &buyer.pubky);
    let (_, _, order) = checkout(&app, &buyer, command).await;
    let order = order.expect("order");
    pay(&app, &buyer, &order).await;

    store.script(PushScript::Rejected("validation"));
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(
        push_state(&app.pool, &order.order_id).await.0,
        "quarantined"
    );
    assert!(
        columns(&app, &order.order_id).await.3,
        "a push that failed keeps the envelope for the retry"
    );
    let first = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert!(first.shipping_address.is_some());

    let (status, _) = api(
        &app,
        "POST",
        &format!(
            "/v1/connectors/{connection}/pushes/{}/retry",
            order.order_id
        ),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    let retried = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert!(
        retried.shipping_address.is_some(),
        "the retry still carries the address"
    );
    assert!(!columns(&app, &order.order_id).await.3);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn without_an_opt_in_no_address_is_stored_or_sent(pool: PgPool) {
    let (app, store, seller, buyer, _connection, _key) = opted_in_world(pool).await;
    let command = sealed_checkout_command_with_id(&seller.pubky, COMMAND_ID, 0);
    let (status, body, order) = checkout(&app, &buyer, command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = order.expect("order");
    assert_eq!(
        columns(&app, &order.order_id).await,
        (None, None, false, false, None)
    );
    pay(&app, &buyer, &order).await;
    worker::run_pass(&app.state).await.expect("pass");
    let pushed = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert!(pushed.shipping_address.is_none());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_opt_in_the_seller_cannot_receive_is_refused_before_any_order_exists(pool: PgPool) {
    let (app, _store, seller, buyer, _connection, key) = opted_in_world(pool).await;
    let good_aad = |buyer: &str| {
        format!(
            "{CONTEXT}|{COMMAND_ID}|{buyer}|{}|{}",
            seller.pubky, key.key_id
        )
    };
    let envelope = seal(&key.public_key, &good_aad(&buyer.pubky), &address());

    let cases: Vec<(&str, Value)> = vec![
        (
            "a store the seller has not connected",
            share_entry(
                &seller.pubky,
                &key,
                "someone-else.test-store.example",
                envelope.clone(),
            ),
        ),
        (
            "a key the connection does not hold",
            json!({
                "seller_pubky": seller.pubky,
                "shop_domain": SHOP,
                "consent_version": "2026-10-a",
                "key_id": "ffffffffffffffffffffffffffffffff",
                "envelope": envelope,
            }),
        ),
    ];
    for (label, entry) in cases {
        let mut command = sealed_checkout_command_with_id(&seller.pubky, COMMAND_ID, 0);
        command["payload"]["connector_shares"] = json!([entry]);
        let (status, body, order) = checkout(&app, &buyer, command).await;
        assert!(order.is_none(), "{label}: {body}");
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{label}: {body}");
        assert_eq!(
            body["error"]["reason"], "connector_share_unavailable",
            "{label}"
        );
        assert!(
            !body.to_string().contains(&envelope),
            "the refusal never echoes an envelope"
        );
    }
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM orders").await,
        0
    );

    // A plaintext address cannot be shared with a store at all.
    let mut plaintext = checkout_command_with_id(&seller.pubky, COMMAND_ID);
    plaintext["payload"]["connector_shares"] =
        json!([share_entry(&seller.pubky, &key, SHOP, envelope.clone())]);
    let (status, body, _) = checkout(&app, &buyer, plaintext).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // After a disconnect the same opt-in is refused.
    let (_, list) = api(
        &app,
        "GET",
        "/v1/connectors",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    let id = list["connections"][0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    api(
        &app,
        "DELETE",
        &format!("/v1/connectors/{id}"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    let mut command = sealed_checkout_command_with_id(&seller.pubky, COMMAND_ID, 0);
    command["payload"]["connector_shares"] =
        json!([share_entry(&seller.pubky, &key, SHOP, envelope)]);
    let (status, body, order) = checkout(&app, &buyer, command).await;
    assert!(order.is_none(), "{body}");
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let _ = listing_aggregate(&seller.pubky);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_envelope_bound_to_another_buyer_does_not_open_and_the_order_still_ships_to_the_seller(
    pool: PgPool,
) {
    let (app, store, seller, buyer, _connection, key) = opted_in_world(pool).await;
    let other = new_actor(&app).await;
    // Sealed as if for another buyer: the AAD binding fails at open time.
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &other.pubky);
    let (status, body, order) = checkout(&app, &buyer, command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = order.expect("order");
    pay(&app, &buyer, &order).await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    let pushed = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert!(
        pushed.shipping_address.is_none(),
        "an envelope that does not open is not guessed at"
    );
    assert!(
        !columns(&app, &order.order_id).await.3,
        "and it is deleted all the same"
    );
    assert!(!format!("{pushed:?}").contains("Alice"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancelled_order_loses_the_envelope_with_the_final_state_purge(pool: PgPool) {
    let (app, _store, seller, buyer, _connection, key) = opted_in_world(pool).await;
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &buyer.pubky);
    let (_, _, order) = checkout(&app, &buyer, command).await;
    let order = order.expect("order");
    assert!(columns(&app, &order.order_id).await.3);
    // The unpaid order is cancelled by the buyer: a final state.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &common::order_command(
            "order.cancel_request",
            &order.order_id,
            1,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let purged =
        marketplace_service::handlers::delivery_envelope::purge_final_delivery_addresses(&app.pool)
            .await
            .expect("purge");
    assert_eq!(purged, 1);
    let (domain, version, consented, envelope, key_id) = columns(&app, &order.order_id).await;
    assert!(!envelope && key_id.is_none());
    assert_eq!(
        (domain.as_deref(), version.as_deref(), consented),
        (Some(SHOP), Some("2026-10-a"), true),
        "the record of consent stays"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_address_is_never_logged_or_projected_to_the_provider_debug_output(pool: PgPool) {
    common::install_log_capture();
    let (app, store, seller, buyer, _connection, key) = opted_in_world(pool).await;
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &buyer.pubky);
    let (_, _, order) = checkout(&app, &buyer, command).await;
    let order = order.expect("order");
    pay(&app, &buyer, &order).await;
    worker::run_pass(&app.state).await.expect("pass");
    let pushed = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert!(pushed.shipping_address.is_some());
    assert!(!format!("{pushed:?}").contains("Market Street"));
    let logs = common::captured_logs();
    for needle in ["Market Street", "Alice Buyer", "8001"] {
        assert!(!logs.contains(needle), "{needle} reached the logs");
    }
}

const WOO_SHOP: &str = "woo.test-store.example";

/// One order, one buyer opt-in for the Shopify store, and a push to each of
/// the seller's two stores. `chosen_first` picks which push lands first.
async fn two_stores_one_envelope(pool: PgPool, chosen_first: bool) {
    let (app, stores) = connector_app_full(pool, test_keys(), test_settings()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let shopify = connect_shop(&app, &seller, SHOP).await;
    let woo = connect_woo_shop(&app, &seller, WOO_SHOP).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 3)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    map_listing(&app, &seller, shopify, "boots_01", "prod-1", "var-1").await;
    map_listing(&app, &seller, woo, "boots_01", "woo-prod-1", "woo-var-1").await;
    let key = published_key_for(&app, &seller.pubky, SHOP).await;
    assert_ne!(
        key.key_id,
        published_key_for(&app, &seller.pubky, WOO_SHOP)
            .await
            .key_id,
        "each store has its own recipient key"
    );
    let command = sealed_with_share(&seller, &buyer, &key, SHOP, &buyer.pubky);
    let (status, body, order) = checkout(&app, &buyer, command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order = order.expect("order");
    pay(&app, &buyer, &order).await;
    worker::consume_events(&app.state).await.expect("consume");
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM connector_order_push").await,
        2
    );

    let (first, second) = if chosen_first {
        ("shopify", "woocommerce")
    } else {
        ("woocommerce", "shopify")
    };
    let defer = |provider: &'static str, at: &'static str| {
        let pool = app.pool.clone();
        async move {
            sqlx::query(&format!(
                "UPDATE connector_order_push SET next_attempt_at = '{at}'::timestamptz \
                 WHERE provider = $1"
            ))
            .bind(provider)
            .execute(&pool)
            .await
            .expect("reschedule");
        }
    };
    defer(second, "2100-01-01").await;
    assert_eq!(
        worker::drive_pushes(&app.state, 10).await.expect("first"),
        1
    );
    let state_of = |provider: &'static str| {
        let pool = app.pool.clone();
        let order_id = order.order_id.clone();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM connector_order_push \
                 WHERE pubky_order_id = $1::uuid AND provider = $2",
            )
            .bind(order_id)
            .bind(provider)
            .fetch_one(&pool)
            .await
            .expect("push state")
        }
    };
    assert_eq!(state_of(first).await, "created");
    assert_eq!(state_of(second).await, "pending");
    assert_eq!(
        columns(&app, &order.order_id).await.3,
        !chosen_first,
        "only the push to the store the buyer chose deletes the envelope"
    );

    defer(second, "2000-01-01").await;
    assert_eq!(
        worker::drive_pushes(&app.state, 10).await.expect("second"),
        1
    );
    assert_eq!(state_of(second).await, "created");
    assert!(
        !columns(&app, &order.order_id).await.3,
        "the chosen store's push has deleted it"
    );

    let to_shopify = stores
        .shopify
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("a push to the Shopify store");
    let to_woo = stores
        .woocommerce
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("a push to the WooCommerce store");
    assert_eq!(
        to_shopify
            .shipping_address
            .expect("the store the buyer chose receives the address")
            .city,
        "Zurich"
    );
    assert!(
        to_woo.shipping_address.is_none(),
        "the other store never receives it"
    );
    assert_eq!(
        columns(&app, &order.order_id).await.0.as_deref(),
        Some(SHOP),
        "the consent record stays"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_push_to_the_other_store_first_leaves_the_envelope_for_the_chosen_store(pool: PgPool) {
    two_stores_one_envelope(pool, false).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_push_to_the_chosen_store_first_deletes_the_envelope_and_the_other_store_gets_none(
    pool: PgPool,
) {
    two_stores_one_envelope(pool, true).await;
}
