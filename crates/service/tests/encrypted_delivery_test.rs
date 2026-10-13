//! Sealed delivery addresses (encrypted-delivery-address design §D2, §F).
//!
//! The buyer's client seals the address to the seller's delivery key and the
//! service stores the 2,096 envelope bytes without opening them. Only the
//! seller of a paid, shipped order receives the envelope back; the plaintext
//! column stays empty for sealed orders.

mod common;

use axum::http::StatusCode;
use serde_json::{json, Value};
use sqlx::PgPool;

use common::{
    checkout_command_with_id, create_sealed_paid_order, execute, listing_aggregate, new_actor,
    register_command, sealed_checkout_command, sealed_checkout_command_with_id, send,
    test_app_with_config, test_envelope, test_envelope_bytes, TestActor, TestApp,
    TEST_DELIVERY_KEY_ID,
};
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;

const SECOND_COMMAND_ID: &str = "00000000-0000-4000-8000-000000001001";

fn require_encrypted_config() -> Config {
    let mut config = Config::for_tests();
    config.require_encrypted_delivery_address = true;
    config
}

async fn get(app: &TestApp, uri: &str, actor: &TestActor) -> (StatusCode, Value) {
    send(
        app.router.clone(),
        "GET",
        uri,
        Some(&actor.token),
        &Value::Null,
    )
    .await
}

struct StoredRow {
    plaintext_is_null: bool,
    envelope: Option<Vec<u8>>,
    key_id: Option<String>,
    command_id: Option<String>,
}

async fn stored(app: &TestApp, order_id: &str) -> StoredRow {
    let (plaintext_is_null, envelope, key_id, command_id): (
        bool,
        Option<Vec<u8>>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT delivery_address IS NULL, delivery_address_envelope, delivery_key_id, \
         checkout_command_id::text FROM orders WHERE id = $1::uuid",
    )
    .bind(order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order row");
    StoredRow {
        plaintext_is_null,
        envelope,
        key_id,
        command_id,
    }
}

fn assert_refused(body: &Value, status: StatusCode, expected: StatusCode) {
    assert_eq!(status, expected, "{body}");
    assert_eq!(body["ok"], json!(false), "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sealed_checkout_stores_the_envelope_and_never_a_plaintext_address(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_sealed_paid_order(&app, &seller, &buyer).await;

    let row = stored(&app, &paid.order_id).await;
    assert!(row.plaintext_is_null);
    assert_eq!(row.envelope, Some(test_envelope_bytes(0)));
    assert_eq!(row.key_id.as_deref(), Some(TEST_DELIVERY_KEY_ID));
    assert_eq!(
        row.command_id.as_deref(),
        Some("00000000-0000-4000-8000-000000001000")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn only_the_seller_of_the_paid_order_receives_the_sealed_envelope(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let paid = create_sealed_paid_order(&app, &seller, &buyer).await;
    let uri = format!("/v1/orders/{}", paid.order_id);

    let (status, view) = get(&app, &uri, &seller).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(
        view["delivery_address"],
        json!({
            "format": "hpke_v1",
            "key_id": TEST_DELIVERY_KEY_ID,
            "command_id": "00000000-0000-4000-8000-000000001000",
            "envelope": test_envelope(0),
        }),
        "{view}"
    );

    let (status, view) = get(&app, &uri, &buyer).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert!(view.get("delivery_address").is_none(), "{view}");
    assert!(!view.to_string().contains(&test_envelope(0)), "{view}");

    let (status, view) = get(&app, &uri, &stranger).await;
    assert_ne!(status, StatusCode::OK, "{view}");
    assert!(!view.to_string().contains(&test_envelope(0)), "{view}");

    for actor in [&seller, &buyer] {
        let (status, list) = get(&app, "/v1/orders", actor).await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert!(
            !list.to_string().contains(&test_envelope(0)),
            "the list view must not carry an envelope: {list}"
        );
        assert!(list["orders"]
            .as_array()
            .expect("order list")
            .iter()
            .all(|order| order.get("delivery_address").is_none()));
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_command_result_and_its_replay_never_carry_the_envelope(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;
    let command = sealed_checkout_command(&seller.pubky);

    let (status, fresh) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{fresh}");
    let (status, replay) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{replay}");

    for body in [&fresh, &replay] {
        assert!(body["result"]["orders"][0]
            .get("delivery_address")
            .is_none());
        assert!(!body.to_string().contains(&test_envelope(0)), "{body}");
    }
    let (stored_result,): (Value,) =
        sqlx::query_as("SELECT result FROM command_results WHERE actor_pubky = $1")
            .bind(&buyer.pubky)
            .fetch_one(&app.pool)
            .await
            .expect("command result row");
    assert!(!stored_result.to_string().contains(&test_envelope(0)));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_multi_seller_checkout_stores_each_sellers_own_envelope(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller_a = new_actor(&app).await;
    let seller_b = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller_a.token, &register_command(&seller_a.pubky, 1)).await;
    execute(&app, &seller_b.token, &register_command(&seller_b.pubky, 1)).await;

    let mut command = checkout_command_with_id(&seller_a.pubky, SECOND_COMMAND_ID);
    let payload = command["payload"].as_object_mut().expect("payload");
    payload.remove("delivery_address");
    payload["lines"] = json!([
        {
            "listing_aggregate_id": listing_aggregate(&seller_a.pubky),
            "expected_revision": 1,
            "quantity": 1,
        },
        {
            "listing_aggregate_id": listing_aggregate(&seller_b.pubky),
            "expected_revision": 1,
            "quantity": 1,
        },
    ]);
    payload.insert(
        "delivery_address_envelopes".to_string(),
        json!([
            {
                "seller_pubky": seller_a.pubky,
                "key_id": TEST_DELIVERY_KEY_ID,
                "envelope": test_envelope(1),
            },
            {
                "seller_pubky": seller_b.pubky,
                "key_id": "fedcba9876543210fedcba9876543210",
                "envelope": test_envelope(2),
            },
        ]),
    );
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let orders = body["result"]["orders"].as_array().expect("orders");
    assert_eq!(orders.len(), 2, "{body}");

    for order in orders {
        let id = order["id"].as_str().expect("order id");
        let seller = order["seller_pubky"].as_str().expect("seller");
        let (seed, key_id) = if seller == seller_a.pubky {
            (1, TEST_DELIVERY_KEY_ID)
        } else {
            (2, "fedcba9876543210fedcba9876543210")
        };
        let row = stored(&app, id).await;
        assert_eq!(row.envelope, Some(test_envelope_bytes(seed)));
        assert_eq!(row.key_id.as_deref(), Some(key_id));
        assert_eq!(row.command_id.as_deref(), Some(SECOND_COMMAND_ID));
        assert!(row.plaintext_is_null);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn envelopes_that_do_not_match_the_shipped_sellers_are_refused(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller_a = new_actor(&app).await;
    let seller_b = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller_a.token, &register_command(&seller_a.pubky, 1)).await;
    execute(&app, &seller_b.token, &register_command(&seller_b.pubky, 1)).await;

    let mut command = sealed_checkout_command_with_id(&seller_a.pubky, SECOND_COMMAND_ID, 0);
    command["payload"]["lines"] = json!([
        {
            "listing_aggregate_id": listing_aggregate(&seller_a.pubky),
            "expected_revision": 1,
            "quantity": 1,
        },
        {
            "listing_aggregate_id": listing_aggregate(&seller_b.pubky),
            "expected_revision": 1,
            "quantity": 1,
        },
    ]);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["error"]["reason"],
        json!("delivery_envelope_sellers"),
        "{body}"
    );

    let mut command = sealed_checkout_command_with_id(&seller_a.pubky, SECOND_COMMAND_ID, 0);
    command["payload"]["delivery_address_envelopes"][0]["seller_pubky"] = json!(seller_b.pubky);
    let (status, body) = execute(&app, &buyer.token, &command).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["error"]["reason"],
        json!("delivery_envelope_sellers"),
        "{body}"
    );

    let (orders,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(&app.pool)
        .await
        .expect("count");
    assert_eq!(orders, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn malformed_and_doubled_address_payloads_are_refused_without_echoing_them(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let mut wrong_length = sealed_checkout_command(&seller.pubky);
    let short = &test_envelope(0)[..2000];
    wrong_length["payload"]["delivery_address_envelopes"][0]["envelope"] = json!(short);
    let (status, body) = execute(&app, &buyer.token, &wrong_length).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!body.to_string().contains(short), "{body}");

    let mut bad_key = sealed_checkout_command(&seller.pubky);
    bad_key["payload"]["delivery_address_envelopes"][0]["key_id"] = json!("NOT-A-KEY-ID");
    let (status, body) = execute(&app, &buyer.token, &bad_key).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);

    let mut both = sealed_checkout_command(&seller.pubky);
    both["payload"]["delivery_address"] =
        checkout_command_with_id(&seller.pubky, SECOND_COMMAND_ID)["payload"]["delivery_address"]
            .clone();
    let (status, body) = execute(&app, &buyer.token, &both).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);

    let mut neither = sealed_checkout_command(&seller.pubky);
    neither["payload"]
        .as_object_mut()
        .expect("payload")
        .remove("delivery_address_envelopes");
    let (status, body) = execute(&app, &buyer.token, &neither).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_on_plaintext_is_refused_and_sealed_is_accepted(pool: PgPool) {
    let app = test_app_with_config(pool, require_encrypted_config()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    execute(&app, &seller.token, &register_command(&seller.pubky, 1)).await;

    let plaintext = checkout_command_with_id(&seller.pubky, SECOND_COMMAND_ID);
    let (status, body) = execute(&app, &buyer.token, &plaintext).await;
    assert_refused(&body, status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        body["error"]["reason"],
        json!("encrypted_delivery_required"),
        "{body}"
    );
    assert!(!body.to_string().contains("1 Market Street"), "{body}");

    let (status, body) = execute(&app, &buyer.token, &sealed_checkout_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_the_flag_off_a_legacy_plaintext_address_still_works_and_projects_as_plaintext(
    pool: PgPool,
) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = common::create_paid_order(&app, &seller, &buyer).await;

    let row = stored(&app, &paid.order_id).await;
    assert!(!row.plaintext_is_null);
    assert!(row.envelope.is_none());
    assert!(row.key_id.is_none());

    let (status, view) = get(&app, &format!("/v1/orders/{}", paid.order_id), &seller).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(
        view["delivery_address"]["format"],
        json!("plaintext_v1"),
        "{view}"
    );
    assert_eq!(
        view["delivery_address"]["address"]["line1"],
        json!("1 Market Street")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_database_refuses_an_order_holding_a_plaintext_address_and_an_envelope(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = common::create_paid_order(&app, &seller, &buyer).await;

    let both = sqlx::query(
        "UPDATE orders SET delivery_address_envelope = $1, delivery_key_id = $2 \
         WHERE id = $3::uuid",
    )
    .bind(test_envelope_bytes(0))
    .bind(TEST_DELIVERY_KEY_ID)
    .bind(&paid.order_id)
    .execute(&app.pool)
    .await;
    assert!(both.is_err(), "plaintext and envelope must never coexist");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_database_refuses_a_malformed_envelope_row(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let paid = create_sealed_paid_order(&app, &seller, &buyer).await;

    for (envelope, key_id) in [
        (Some(vec![0u8; 100]), Some(TEST_DELIVERY_KEY_ID)),
        (Some(test_envelope_bytes(0)), Some("NOT-A-KEY-ID")),
        (Some(test_envelope_bytes(0)), None),
        (None, Some(TEST_DELIVERY_KEY_ID)),
    ] {
        let result = sqlx::query(
            "UPDATE orders SET delivery_address_envelope = $1, delivery_key_id = $2 \
             WHERE id = $3::uuid",
        )
        .bind(envelope)
        .bind(key_id)
        .bind(&paid.order_id)
        .execute(&app.pool)
        .await;
        assert!(result.is_err(), "a malformed envelope row must be refused");
    }
}

const FINAL_STATES: [&str; 4] = ["completed", "cancelled", "refunded_external", "closed"];
const OPEN_STATES: [&str; 2] = ["paid", "processing"];

async fn run_worker(app: &TestApp) -> marketplace_service::workers::WorkerSummary {
    marketplace_service::workers::run_once(&app.state, uuid::Uuid::new_v4(), app.clock.now())
        .await
        .expect("worker pass")
}

async fn address_columns_empty(app: &TestApp, order_id: &str) -> bool {
    let row = stored(app, order_id).await;
    row.plaintext_is_null && row.envelope.is_none() && row.key_id.is_none()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_clears_sealed_and_plaintext_addresses_at_each_final_state(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let mut cases = Vec::new();
    for state in FINAL_STATES {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let sealed = create_sealed_paid_order(&app, &seller, &buyer).await;
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let legacy = common::create_paid_order(&app, &seller, &buyer).await;
        cases.push((state, sealed.order_id, legacy.order_id));
    }

    for (_, sealed, legacy) in &cases {
        assert!(!address_columns_empty(&app, sealed).await);
        assert!(!address_columns_empty(&app, legacy).await);
    }
    let summary = run_worker(&app).await;
    assert_eq!(
        summary.delivery_addresses_purged, 0,
        "paid orders keep theirs"
    );

    for (state, sealed, legacy) in &cases {
        for order_id in [sealed, legacy] {
            sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
                .bind(order_id)
                .bind(state)
                .execute(&app.pool)
                .await
                .unwrap_or_else(|error| panic!("move order to {state}: {error}"));
        }
    }
    let summary = run_worker(&app).await;
    assert_eq!(summary.delivery_addresses_purged, 8);
    for (state, sealed, legacy) in &cases {
        assert!(address_columns_empty(&app, sealed).await, "sealed {state}");
        assert!(
            address_columns_empty(&app, legacy).await,
            "plaintext {state}"
        );
    }
    assert_eq!(run_worker(&app).await.delivery_addresses_purged, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_worker_keeps_the_address_of_every_order_that_has_not_ended(pool: PgPool) {
    let app = test_app_with_config(pool, Config::for_tests()).await;
    let mut kept = Vec::new();
    for state in OPEN_STATES {
        let seller = new_actor(&app).await;
        let buyer = new_actor(&app).await;
        let sealed = create_sealed_paid_order(&app, &seller, &buyer).await;
        sqlx::query("UPDATE orders SET state = $2 WHERE id = $1::uuid")
            .bind(&sealed.order_id)
            .bind(state)
            .execute(&app.pool)
            .await
            .unwrap_or_else(|error| panic!("move order to {state}: {error}"));
        kept.push(sealed.order_id);
    }
    assert_eq!(run_worker(&app).await.delivery_addresses_purged, 0);
    for order_id in kept {
        assert!(!address_columns_empty(&app, &order_id).await);
    }
}
