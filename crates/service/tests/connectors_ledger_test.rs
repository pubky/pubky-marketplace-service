//! The order-push ledger against PostgreSQL: duplicate events, a crash
//! between `sending` and `created`, concurrent workers, throttling,
//! quarantine and retry, and the cancel and refund mirrors. Everything is
//! driven through the real event feed (`events`) by the worker passes, with
//! a real in-test provider.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use common::connector_provider::*;
use common::{create_paid_order, execute, new_actor, order_command, PaidOrder, TestActor, TestApp};
use marketplace_service::clock::Clock;
use marketplace_service::connectors::ledger;
use marketplace_service::connectors::outbox;
use marketplace_service::connectors::worker;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

struct Scenario {
    app: TestApp,
    store: Arc<TestStore>,
    seller: TestActor,
    buyer: TestActor,
    connection: Uuid,
    order: PaidOrder,
}

/// A connected store, a mapped listing and one paid order whose
/// `payment.confirmed` event has not been consumed yet.
async fn scenario(pool: PgPool) -> Scenario {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    Scenario {
        app,
        store,
        seller,
        buyer,
        connection,
        order,
    }
}

impl Scenario {
    async fn pass(&self) {
        worker::run_pass(&self.app.state).await.expect("pass");
    }

    async fn push(&self) -> (String, Option<String>, Option<String>, i32) {
        push_state(&self.app.pool, &self.order.order_id).await
    }

    fn after(&self, seconds: i64) {
        self.app.clock.advance_seconds(seconds);
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_paid_order_is_pushed_once_with_no_buyer_or_address(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    let (state, external, reason, attempts) = s.push().await;
    assert_eq!(state, "created");
    assert_eq!(external.as_deref(), Some("ext-1"));
    assert_eq!(reason, None);
    assert_eq!(attempts, 1);
    assert_eq!(s.store.pushes(), 1);
    assert_eq!(
        s.store.find_calls.load(Ordering::SeqCst),
        0,
        "a first attempt has nothing to look up"
    );
    let pushed = s
        .store
        .last_order_push
        .lock()
        .expect("push lock")
        .clone()
        .expect("a push was recorded");
    assert_eq!(pushed.pubky_order_id.to_string(), s.order.order_id);
    assert_eq!(pushed.lines.len(), 1);
    assert_eq!(pushed.lines[0].provider_product_id, "prod-1");
    assert_eq!(pushed.lines[0].provider_variant_id, "var-1");
    assert_eq!(pushed.lines[0].quantity, 1);
    assert_eq!(pushed.total_minor, s.order.total_minor);
    assert!(
        pushed.shipping_address.is_none(),
        "no opt-in, so no address reaches the store"
    );
    assert!(!format!("{pushed:?}").contains(&s.buyer.pubky));
    assert_eq!(
        s.store.last_app.lock().expect("app lock").as_deref(),
        Some(TEST_CLIENT_ID),
        "the provider call carries the connection's own app"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn duplicate_event_delivery_creates_one_ledger_row_and_one_push(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    for _ in 0..2 {
        sqlx::query("UPDATE connector_event_cursor SET sequence = 0 WHERE name = 'events'")
            .execute(&s.app.pool)
            .await
            .expect("rewind cursor");
        s.pass().await;
    }
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_order_push").await,
        1
    );
    assert_eq!(s.store.pushes(), 1);
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn crash_after_the_remote_create_is_reconciled_by_a_lookup(pool: PgPool) {
    let s = scenario(pool).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    let claim = ledger::claim_next_push(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &["shopify".to_string()],
    )
    .await
    .expect("claim")
    .expect("a due push");
    assert!(!claim.lookup_first);
    s.store.plant_order(claim.row.pubky_order_id, "ext-crashed");
    assert_eq!(s.push().await.0, "sending");

    worker::drive_pushes(&s.app.state, 10).await.expect("drive");
    assert_eq!(s.store.pushes(), 0);
    assert_eq!(s.push().await.0, "sending");

    s.after(61);
    worker::drive_pushes(&s.app.state, 10).await.expect("drive");
    let (state, external, _, attempts) = s.push().await;
    assert_eq!(state, "created");
    assert_eq!(external.as_deref(), Some("ext-crashed"));
    assert_eq!(attempts, 2);
    assert_eq!(s.store.find_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.store.pushes(),
        0,
        "the order already existed; no second push"
    );
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn crash_before_the_remote_create_pushes_once_after_the_lookup_finds_nothing(pool: PgPool) {
    let s = scenario(pool).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    ledger::claim_next_push(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &["shopify".to_string()],
    )
    .await
    .expect("claim")
    .expect("a due push");
    s.after(61);
    worker::drive_pushes(&s.app.state, 10).await.expect("drive");
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.find_calls.load(Ordering::SeqCst), 1);
    assert_eq!(s.store.pushes(), 1);
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unknown_outcome_after_a_real_create_never_duplicates(pool: PgPool) {
    let s = scenario(pool).await;
    s.store.script(PushScript::CreateThenUnknown);
    s.pass().await;
    let (state, external, _, attempts) = s.push().await;
    assert_eq!(
        state, "sending",
        "the outcome is unknown, so the row stays sending"
    );
    assert_eq!(external, None);
    assert_eq!(attempts, 1);
    assert_eq!(s.store.remote_orders().len(), 1);

    s.after(5);
    s.pass().await;
    assert_eq!(s.store.pushes(), 1);

    s.after(10);
    s.pass().await;
    let (state, external, _, _) = s.push().await;
    assert_eq!(state, "created");
    assert_eq!(external.as_deref(), Some("ext-1"));
    assert_eq!(s.store.pushes(), 1, "found by lookup; not pushed again");
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_unknown_outcome_without_a_create_is_retried_after_the_lookup(pool: PgPool) {
    let s = scenario(pool).await;
    s.store.script(PushScript::UnknownWithoutCreate);
    s.pass().await;
    assert_eq!(s.push().await.0, "sending");
    s.after(11);
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.find_calls.load(Ordering::SeqCst), 1);
    assert_eq!(s.store.pushes(), 2);
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_provider_call_that_outlives_its_timeout_is_an_unknown_outcome(pool: PgPool) {
    let mut settings = test_settings();
    settings.provider_timeout = Duration::from_secs(1);
    let (app, store) = connector_app_with(pool, test_keys(), settings).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    *store.push_delay.lock().expect("delay lock") = Duration::from_secs(5);
    worker::run_pass(&app.state).await.expect("pass");
    let (state, _, _, _) = push_state(&app.pool, &order.order_id).await;
    assert_eq!(state, "sending");
    assert_eq!(
        store.remote_orders().len(),
        0,
        "the cancelled call never reached its create"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_workers_on_one_order_push_exactly_once(pool: PgPool) {
    let s = scenario(pool).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    *s.store.push_delay.lock().expect("delay lock") = Duration::from_millis(200);
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let state = s.app.state.clone();
        tasks.push(tokio::spawn(async move {
            worker::drive_pushes(&state, 5).await.expect("drive")
        }));
    }
    let mut handled = 0;
    for task in tasks {
        handled += task.await.expect("worker task");
    }
    assert_eq!(handled, 1, "one worker owned the row");
    assert_eq!(s.store.pushes(), 1);
    assert_eq!(s.store.remote_orders().len(), 1);
    assert_eq!(s.push().await.0, "created");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_second_worker_interleaved_with_an_open_push_does_nothing(pool: PgPool) {
    let s = scenario(pool).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    let gate = s.store.open_gate();

    let first = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::drive_pushes(&state, 5).await.expect("worker A") })
    };
    gate.entered.notified().await;
    assert_eq!(s.push().await.0, "sending");
    assert_eq!(s.store.remote_orders().len(), 0);

    assert_eq!(
        worker::drive_pushes(&s.app.state, 5)
            .await
            .expect("worker B"),
        0
    );
    s.after(30);
    assert_eq!(
        worker::drive_pushes(&s.app.state, 5)
            .await
            .expect("worker C"),
        0
    );
    assert_eq!(s.store.pushes(), 1);

    gate.release.notify_one();
    assert_eq!(first.await.expect("worker A task"), 1);
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.pushes(), 1);
    assert_eq!(s.store.remote_orders().len(), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_worker_that_lost_its_lease_cannot_overwrite_the_new_owner(pool: PgPool) {
    let s = scenario(pool).await;
    worker::consume_events(&s.app.state).await.expect("consume");
    let stale = ledger::claim_next_push(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &["shopify".to_string()],
    )
    .await
    .expect("claim")
    .expect("a due push");

    s.after(61);
    worker::drive_pushes(&s.app.state, 5)
        .await
        .expect("takeover");
    let (state, external, _, attempts) = s.push().await;
    assert_eq!((state.as_str(), attempts), ("created", 2));

    let now = s.app.clock.now();
    assert!(
        !ledger::mark_created(&s.app.pool, &stale.row, "ext-late", now)
            .await
            .expect("late created")
    );
    assert!(
        !ledger::release_unknown(&s.app.pool, &stale.row, now, "late", now)
            .await
            .expect("late release")
    );
    let connection =
        marketplace_service::connectors::store::get(&s.app.pool, stale.row.connection_id)
            .await
            .expect("connection read")
            .expect("connection");
    assert!(
        !ledger::quarantine_push(&s.app.pool, &stale.row, &connection, "late", "late", now)
            .await
            .expect("late quarantine")
    );
    let (state, after_external, _, _) = s.push().await;
    assert_eq!(state, "created");
    assert_eq!(after_external, external);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_throttled_push_waits_and_then_goes_without_a_lookup(pool: PgPool) {
    let s = scenario(pool).await;
    s.store
        .script(PushScript::RateLimited(Some(Duration::from_secs(120))));
    s.pass().await;
    let (state, _, _, attempts) = s.push().await;
    assert_eq!((state.as_str(), attempts), ("pending", 1));
    assert_eq!(s.store.remote_orders().len(), 0);

    s.after(60);
    s.pass().await;
    assert_eq!(s.store.pushes(), 1, "Retry-After is honoured");

    s.after(61);
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(
        s.store.find_calls.load(Ordering::SeqCst),
        0,
        "a throttle is a definite refusal, so no lookup is needed"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refused_push_is_quarantined_alerted_and_retried_after_a_lookup(pool: PgPool) {
    let s = scenario(pool).await;
    s.store.script(PushScript::Rejected("validation"));
    s.pass().await;
    let (state, external, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(external, None);
    assert_eq!(reason.as_deref(), Some("validation"));

    let (status, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts?unresolved=true",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(alerts["alerts"].as_array().expect("alerts").len(), 1);
    assert_eq!(alerts["alerts"][0]["kind"], "push_quarantined");
    assert_eq!(alerts["alerts"][0]["pubky_order_id"], s.order.order_id);

    let (status, pushes) = api(
        &s.app,
        "GET",
        &format!("/v1/connectors/{}/pushes", s.connection),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pushes["pushes"][0]["quarantine_reason"], "validation");
    assert_eq!(
        pushes["pushes"][0]["quarantine_severity"],
        "action_required"
    );

    let url = format!(
        "/v1/connectors/{}/pushes/{}/retry",
        s.connection, s.order.order_id
    );
    let (status, queued) = api(&s.app, "POST", &url, Some(&s.seller.token), &json!(null)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{queued}");
    assert_eq!(queued["push"]["state"], "sending");
    let (status, again) = api(&s.app, "POST", &url, Some(&s.seller.token), &json!(null)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(again["error"]["code"], "not_retryable");

    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(
        s.store.find_calls.load(Ordering::SeqCst),
        1,
        "a retry looks the order up first"
    );
    assert_eq!(s.store.remote_orders().len(), 1);

    let (status, done) = api(&s.app, "POST", &url, Some(&s.seller.token), &json!(null)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(done["error"]["code"], "already_created");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_mapping_removed_before_the_push_sets_the_order_aside_until_it_is_restored(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    worker::consume_events(&app.state).await.expect("consume");
    // The seller removes the mapping between the event and the push.
    api(
        &app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [] }),
    )
    .await;
    worker::drive_pushes(&app.state, 10).await.expect("drive");
    let (state, _, reason, _) = push_state(&app.pool, &order.order_id).await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("no_mapped_lines"));
    assert_eq!(store.pushes(), 0);
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM connector_alerts").await,
        0
    );
    let (_, pushes) = api(
        &app,
        "GET",
        &format!("/v1/connectors/{connection}/pushes"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(pushes["pushes"][0]["quarantine_severity"], "info");

    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
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
}

/// A second listing of the same seller, registered next to `boots_01`.
fn second_listing_command(seller: &str) -> serde_json::Value {
    let mut command = common::register_command(seller, 2);
    command["command_id"] = json!("018f47d2-6a27-7c23-a49d-6b21bb770121");
    command["aggregate_id"] = json!(format!("listing:{seller}_socks_01"));
    command["payload"]["listing_id"] = json!("socks_01");
    command
}

/// One checkout with a line for each of the seller's two listings, paid.
async fn two_line_order(app: &TestApp, seller: &TestActor, buyer: &TestActor) -> String {
    for command in [
        common::register_command(&seller.pubky, 3),
        second_listing_command(&seller.pubky),
    ] {
        let (status, body) = execute(app, &seller.token, &command).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let mut checkout = common::checkout_command(&seller.pubky);
    checkout["payload"]["lines"] = json!([
        { "listing_aggregate_id": common::listing_aggregate(&seller.pubky), "expected_revision": 1, "quantity": 1 },
        { "listing_aggregate_id": format!("listing:{}_socks_01", seller.pubky), "expected_revision": 1, "quantity": 1 },
    ]);
    let (status, body) = execute(app, &buyer.token, &checkout).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment")
        .to_string();
    let (status, body) = execute(
        app,
        &buyer.token,
        &common::payment_command(&payment_id, 1, "confirmed", 1, 1_050),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    order_id
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_with_no_mapped_line_is_a_pubky_only_sale_and_is_not_pushed(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    // The store maps some other listing, so the connection has mappings but
    // none for this order.
    let (status, body) = execute(&app, &seller.token, &second_listing_command(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    map_listing(&app, &seller, connection, "socks_01", "prod-9", "var-9").await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM connector_order_push").await,
        0,
        "{}",
        order.order_id
    );
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM connector_alerts").await,
        0
    );
    assert_eq!(store.pushes(), 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_with_only_some_lines_mapped_is_quarantined_with_an_alert(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let order_id = two_line_order(&app, &seller, &buyer).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    worker::run_pass(&app.state).await.expect("pass");
    let (state, _, reason, _) = push_state(&app.pool, &order_id).await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("unmapped_variant"));
    assert_eq!(store.pushes(), 0);
    let (_, alerts) = api(
        &app,
        "GET",
        "/v1/connectors/alerts?unresolved=true",
        Some(&seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"].as_array().expect("alerts").len(), 1);
    assert!(alerts["alerts"][0]["detail"]
        .as_str()
        .expect("detail")
        .contains("socks_01"));

    // Mapping the other line and retrying pushes the whole order.
    let (status, _) = api(
        &app,
        "PUT",
        &format!("/v1/connectors/{connection}/mappings"),
        Some(&seller.token),
        &json!({ "mappings": [
            { "listing_id": "boots_01", "provider_product_id": "prod-1", "provider_variant_id": "var-1" },
            { "listing_id": "socks_01", "provider_product_id": "prod-2", "provider_variant_id": "var-2" },
        ] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    api(
        &app,
        "POST",
        &format!("/v1/connectors/{connection}/pushes/{order_id}/retry"),
        Some(&seller.token),
        &json!(null),
    )
    .await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(push_state(&app.pool, &order_id).await.0, "created");
    let pushed = store
        .last_order_push
        .lock()
        .expect("lock")
        .clone()
        .expect("push");
    assert_eq!(pushed.lines.len(), 2);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn rejected_credentials_close_the_push_and_flag_the_connection(pool: PgPool) {
    let s = scenario(pool).await;
    s.store.script(PushScript::Unauthorized);
    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("credentials_rejected"));
    let (_, list) = api(
        &s.app,
        "GET",
        "/v1/connectors",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(list["connections"][0]["status"], "error");
    assert_eq!(
        list["connections"][0]["last_error"]["message"],
        "credentials_rejected"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn too_many_failed_attempts_quarantine_the_push(pool: PgPool) {
    let s = scenario(pool).await;
    for _ in 0..5 {
        s.store.script(PushScript::UnknownWithoutCreate);
    }
    for _ in 0..8 {
        s.pass().await;
        s.after(700);
    }
    let (state, _, reason, attempts) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("max_attempts_exceeded"));
    assert_eq!(attempts, 5);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_cancelled_before_the_push_is_set_aside_without_an_alert(pool: PgPool) {
    let s = scenario(pool).await;
    let order_id = s.order.order_id.as_str();
    let (status, body) = execute(
        &s.app,
        &s.buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = execute(
        &s.app,
        &s.seller.token,
        &order_command("order.cancel_approve", order_id, 3, json!({}), 1_402),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("order_cancelled_before_push"));
    assert_eq!(s.store.pushes(), 0);
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_alerts").await,
        0
    );
    let (_, pushes) = api(
        &s.app,
        "GET",
        &format!("/v1/connectors/{}/pushes?state=quarantined", s.connection),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(pushes["pushes"][0]["quarantine_severity"], "info");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_after_the_push_is_mirrored_once_and_never_refunds(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    let order_id = s.order.order_id.as_str();
    execute(
        &s.app,
        &s.buyer.token,
        &order_command(
            "order.cancel_request",
            order_id,
            2,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
    )
    .await;
    let (status, body) = execute(
        &s.app,
        &s.seller.token,
        &order_command("order.cancel_approve", order_id, 3, json!({}), 1_402),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    s.store.cancel_unknown_once.store(true, Ordering::SeqCst);
    s.pass().await;
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 1);
    assert!(
        !s.store.remote_orders()[0].cancelled,
        "the first attempt timed out"
    );
    s.after(11);
    s.pass().await;
    s.after(700);
    s.pass().await;
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 2);
    let remote = &s.store.remote_orders()[0];
    assert!(remote.cancelled);
    assert!(
        remote.refunds.is_empty(),
        "a cancel never records a store refund"
    );
    let action_state: String =
        sqlx::query_scalar("SELECT state FROM connector_order_actions WHERE kind = 'cancel'")
            .fetch_one(&s.app.pool)
            .await
            .expect("cancel action");
    assert_eq!(action_state, "done");

    sqlx::query("UPDATE connector_event_cursor SET sequence = 0 WHERE name = 'events'")
        .execute(&s.app.pool)
        .await
        .expect("rewind");
    s.after(700);
    s.pass().await;
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_order_actions").await,
        1
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_pubky_refund_is_mirrored_to_the_store_once(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    let order_id = s.order.order_id.as_str();
    for (seller, kind, revision, payload, number) in [
        (
            true,
            "fulfillment.ship",
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "T-1" }),
            1_210,
        ),
        (false, "fulfillment.confirm_delivery", 3, json!({}), 1_211),
        (
            false,
            "return.request",
            4,
            json!({ "reason": "wrong size", "requested_amount_minor": s.order.total_minor }),
            1_212,
        ),
        (true, "return.approve", 5, json!({}), 1_213),
        (true, "return.receive", 6, json!({}), 1_214),
        (
            true,
            "refund.record_external",
            7,
            json!({ "amount_minor": s.order.total_minor, "transaction_id": "bitcoin-tx-1" }),
            1_215,
        ),
    ] {
        let token = if seller {
            &s.seller.token
        } else {
            &s.buyer.token
        };
        let (status, body) = execute(
            &s.app,
            token,
            &order_command(kind, order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }
    s.pass().await;
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
    let refund = s
        .store
        .last_refund
        .lock()
        .expect("refund lock")
        .clone()
        .expect("a refund was recorded");
    assert_eq!(refund.amount_minor, Some(s.order.total_minor));
    assert_eq!(refund.transaction_id.as_deref(), Some("bitcoin-tx-1"));
    assert!(refund.full);
    assert_eq!(refund.pubky_order_id.to_string(), s.order.order_id);

    sqlx::query("UPDATE connector_event_cursor SET sequence = 0 WHERE name = 'events'")
        .execute(&s.app.pool)
        .await
        .expect("rewind");
    s.after(700);
    s.pass().await;
    assert_eq!(
        s.store.refund_calls.load(Ordering::SeqCst),
        1,
        "the refund id is the event id, so a replay makes no second action"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_paid_before_the_connection_existed_is_not_pushed(pool: PgPool) {
    let (app, store) = connector_app(pool).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_paid_order(&app, &seller, &buyer).await;
    app.clock.advance_seconds(60);
    let connection = connect_shop(&app, &seller, SHOP).await;
    map_listing(&app, &seller, connection, "boots_01", "prod-1", "var-1").await;
    worker::run_pass(&app.state).await.expect("pass");
    assert_eq!(
        common::count(&app.pool, "SELECT COUNT(*) FROM connector_order_push").await,
        0,
        "{}",
        order.order_id
    );
    assert_eq!(store.pushes(), 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_cursor_never_skips_an_event_whose_transaction_is_still_open(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let now = app.clock.now();
    let insert = "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
                  VALUES ($1, $2, $3, 1, 'actor', 'test.noop', $4)";

    let mut slow = app.pool.begin().await.expect("slow tx");
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind("noop:a")
        .bind(now)
        .execute(&mut *slow)
        .await
        .expect("slow insert");
    sqlx::query(insert)
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind("noop:b")
        .bind(now)
        .execute(&app.pool)
        .await
        .expect("fast insert");
    let committed_first: i64 =
        sqlx::query_scalar("SELECT sequence FROM events WHERE aggregate_id = 'noop:b'")
            .fetch_one(&app.pool)
            .await
            .expect("fast sequence");

    let blocked = outbox::consume_batch(&app.pool, now).await.expect("batch");
    assert_eq!(
        blocked.scanned, 0,
        "nothing is final past the open transaction"
    );
    let cursor: i64 =
        sqlx::query_scalar("SELECT sequence FROM connector_event_cursor WHERE name = 'events'")
            .fetch_one(&app.pool)
            .await
            .expect("cursor");
    assert!(cursor < committed_first - 1);

    slow.commit().await.expect("slow commit");
    let caught_up = outbox::consume_batch(&app.pool, now).await.expect("batch");
    assert_eq!(caught_up.scanned, 2);
    assert_eq!(caught_up.cursor, committed_first);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_consumers_do_not_read_the_feed_at_once(pool: PgPool) {
    let (app, _store) = connector_app(pool).await;
    let mut holder = app.pool.begin().await.expect("holder");
    sqlx::query("SELECT sequence FROM connector_event_cursor WHERE name = 'events' FOR UPDATE")
        .execute(&mut *holder)
        .await
        .expect("hold cursor");
    let other = outbox::consume_batch(&app.pool, app.clock.now())
        .await
        .expect("batch");
    assert!(other.busy);
    holder.rollback().await.expect("release");
    let free = outbox::consume_batch(&app.pool, app.clock.now())
        .await
        .expect("batch");
    assert!(!free.busy);
}
