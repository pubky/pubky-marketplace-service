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
        settled_pass(&self.app).await;
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
    settled_consume(&s.app).await;
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
    settled_consume(&s.app).await;
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
    settled_pass(&app).await;
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
    settled_consume(&s.app).await;
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
    settled_consume(&s.app).await;
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
    settled_consume(&s.app).await;
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
    settled_consume(&app).await;
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
    // Not sent, and never silently: one `listing_not_linked` alert on the
    // order names the listing, and no generic quarantine alert.
    let alerts: Vec<(String, Option<uuid::Uuid>, String)> =
        sqlx::query_as("SELECT kind, pubky_order_id, detail FROM connector_alerts")
            .fetch_all(&app.pool)
            .await
            .expect("alerts");
    assert_eq!(alerts.len(), 1, "{alerts:?}");
    assert_eq!(alerts[0].0, "listing_not_linked");
    assert_eq!(
        alerts[0].1.map(|id| id.to_string()).as_deref(),
        Some(order.order_id.as_str())
    );
    assert!(
        alerts[0].2.contains("listing boots_01 is not linked"),
        "{}",
        alerts[0].2
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
    settled_pass(&app).await;
    assert_eq!(push_state(&app.pool, &order.order_id).await.0, "created");
    assert_eq!(
        common::count(
            &app.pool,
            "SELECT COUNT(*) FROM connector_alerts \
             WHERE kind = 'listing_not_linked' AND resolved_at IS NULL"
        )
        .await,
        0,
        "the order reached the store, so its not-linked alert is resolved"
    );
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
    settled_pass(&app).await;
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
    settled_pass(&app).await;
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
    settled_pass(&app).await;
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
    assert_eq!(refund.amount_minor, s.order.total_minor);
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
    settled_pass(&app).await;
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

// === A quarantined push and the mirrors queued behind it =====================

async fn cancel_order(s: &Scenario) {
    let order_id = s.order.order_id.as_str();
    for (token, kind, revision, payload, number) in [
        (
            &s.buyer.token,
            "order.cancel_request",
            2,
            json!({ "reason": "changed my mind" }),
            1_401,
        ),
        (&s.seller.token, "order.cancel_approve", 3, json!({}), 1_402),
    ] {
        let (status, body) = execute(
            &s.app,
            token,
            &order_command(kind, order_id, revision, payload, number),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{kind}: {body}");
    }
}

/// The seller records an external refund of the whole order, through the
/// return flow.
async fn refund_order(s: &Scenario) {
    refund_order_of(s, s.order.total_minor).await;
}

/// The seller records an external refund of `amount_minor`, through the
/// return flow.
async fn refund_order_of(s: &Scenario, amount_minor: i64) {
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
            json!({ "amount_minor": amount_minor, "transaction_id": "bitcoin-tx-1" }),
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
}

async fn actions(s: &Scenario) -> Vec<(String, String, Option<String>)> {
    sqlx::query_as(
        "SELECT kind, state, quarantine_reason FROM connector_order_actions \
         ORDER BY kind, action_key",
    )
    .fetch_all(&s.app.pool)
    .await
    .expect("actions")
}

fn parked(kind: &str) -> (String, String, Option<String>) {
    (
        kind.to_string(),
        "quarantined".to_string(),
        Some("push_quarantined".to_string()),
    )
}

fn waiting(kind: &str) -> (String, String, Option<String>) {
    (kind.to_string(), "pending".to_string(), None)
}

/// The store rejects the push while it is in flight, so the push ends
/// quarantined. `meanwhile` runs between the push being claimed and the
/// store's answer, with the push `sending`.
async fn reject_a_push_in_flight<F, Fut>(s: &Scenario, meanwhile: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    settled_consume(&s.app).await;
    s.store.script(PushScript::Rejected("validation"));
    let gate = s.store.open_gate();
    let task = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::drive_pushes(&state, 5).await.expect("worker") })
    };
    gate.entered.notified().await;
    assert_eq!(s.push().await.0, "sending");
    meanwhile().await;
    gate.release.notify_one();
    assert_eq!(task.await.expect("worker task"), 1);
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("validation"));
}

async fn quarantine_a_push(s: &Scenario) {
    s.store.script(PushScript::Rejected("validation"));
    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("validation"));
}

/// The seller's retry. The earlier attempt in truth reached the store, so
/// the lookup before the push finds the order.
async fn retry_after_the_store_took_the_order(s: &Scenario) -> serde_json::Value {
    s.store
        .plant_order(s.order.order_id.parse().expect("order id"), "ext-planted");
    let (status, queued) = api(
        &s.app,
        "POST",
        &format!(
            "/v1/connectors/{}/pushes/{}/retry",
            s.connection, s.order.order_id
        ),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{queued}");
    queued
}

async fn pushes_view(s: &Scenario) -> serde_json::Value {
    let (status, pushes) = api(
        &s.app,
        "GET",
        &format!("/v1/connectors/{}/pushes", s.connection),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    pushes
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_queued_while_the_push_is_in_flight_is_parked_by_its_quarantine_and_runs_after_a_retry(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    reject_a_push_in_flight(&s, || async {
        cancel_order(&s).await;
        settled_consume(&s.app).await;
        assert_eq!(actions(&s).await, vec![waiting("cancel")]);
    })
    .await;

    assert_eq!(
        actions(&s).await,
        vec![parked("cancel")],
        "the cancel does not stay queued behind a push that can no longer create"
    );
    let view = pushes_view(&s).await;
    assert_eq!(view["pushes"][0]["actions"][0]["state"], "quarantined");
    assert_eq!(
        view["pushes"][0]["actions"][0]["quarantine_reason"],
        "push_quarantined"
    );
    assert_eq!(
        worker::drive_actions(&s.app.state, 5).await.expect("drive"),
        0
    );
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 0);

    let queued = retry_after_the_store_took_the_order(&s).await;
    assert_eq!(queued["push"]["actions"][0]["state"], "pending");
    assert_eq!(actions(&s).await, vec![waiting("cancel")]);
    s.pass().await;
    let (state, external, _, _) = s.push().await;
    assert_eq!(
        (state.as_str(), external.as_deref()),
        ("created", Some("ext-planted"))
    );
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 1);
    assert!(s.store.remote_orders()[0].cancelled);
    assert_eq!(
        actions(&s).await,
        vec![("cancel".to_string(), "done".to_string(), None)]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_recorded_while_the_push_is_in_flight_is_parked_by_its_quarantine_and_runs_after_a_retry(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    reject_a_push_in_flight(&s, || async {
        refund_order(&s).await;
        settled_consume(&s.app).await;
        assert_eq!(actions(&s).await, vec![waiting("refund")]);
    })
    .await;

    assert_eq!(actions(&s).await, vec![parked("refund")]);
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);

    retry_after_the_store_took_the_order(&s).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        actions(&s).await,
        vec![("refund".to_string(), "done".to_string(), None)]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_recorded_on_a_quarantined_push_waits_for_the_retry(pool: PgPool) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    cancel_order(&s).await;
    s.pass().await;
    assert_eq!(actions(&s).await, vec![parked("cancel")]);
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 0);

    retry_after_the_store_took_the_order(&s).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 1);
    assert!(s.store.remote_orders()[0].cancelled);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_recorded_on_a_quarantined_push_waits_for_the_retry(pool: PgPool) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    refund_order(&s).await;
    s.pass().await;
    assert_eq!(actions(&s).await, vec![parked("refund")]);
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);

    retry_after_the_store_took_the_order(&s).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_retry_that_finds_no_store_order_for_a_refunded_order_withholds_it_and_leaves_no_action(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    refund_order(&s).await;
    s.pass().await;
    assert_eq!(actions(&s).await, vec![parked("refund")]);

    let (status, _) = api(
        &s.app,
        "POST",
        &format!(
            "/v1/connectors/{}/pushes/{}/retry",
            s.connection, s.order.order_id
        ),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("order_cancelled_before_push"));
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new(),
        "no store order exists, so there is nothing to mirror"
    );
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.store.pushes(),
        1,
        "the refunded order is not pushed again"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_recorded_while_the_push_is_pending_never_leaves_an_action_behind(pool: PgPool) {
    let s = scenario(pool).await;
    settled_consume(&s.app).await;
    assert_eq!(s.push().await.0, "pending");
    refund_order(&s).await;
    settled_consume(&s.app).await;
    assert_eq!(
        actions(&s).await,
        vec![waiting("refund")],
        "a refund seen before the push starts is queued, not dropped"
    );

    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("order_cancelled_before_push"));
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new()
    );
    assert_eq!(s.store.pushes(), 0);
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        common::count(&s.app.pool, "SELECT COUNT(*) FROM connector_alerts").await,
        0
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_and_a_refund_recorded_before_the_push_start_leave_no_action(pool: PgPool) {
    let s = scenario(pool).await;
    settled_consume(&s.app).await;
    cancel_order(&s).await;
    let (status, body) = execute(
        &s.app,
        &s.seller.token,
        &order_command(
            "refund.record_external",
            &s.order.order_id,
            4,
            json!({ "amount_minor": s.order.total_minor, "transaction_id": "bitcoin-tx-2" }),
            1_403,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    s.pass().await;
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("order_cancelled_before_push"));
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new()
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_queued_on_a_pending_push_is_applied_once_the_push_creates_the_order(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    settled_consume(&s.app).await;
    assert_eq!(s.push().await.0, "pending");
    let order_id: Uuid = s.order.order_id.parse().expect("order id");
    let mut tx = s.app.pool.begin().await.expect("tx");
    assert!(ledger::has_mirrorable_push(&mut tx, order_id)
        .await
        .expect("mirrorable"));
    ledger::on_refund_recorded(
        &mut tx,
        s.app.clock.now(),
        order_id,
        "refund-1",
        &json!({
            "amount_minor": 1_000,
            "transaction_id": "bitcoin-tx-9",
            "order_total_minor": s.order.total_minor,
            "order_currency": "USD",
            "order_exponent": 2,
            "full": false,
        }),
        Uuid::new_v4(),
    )
    .await
    .expect("queue the refund");
    tx.commit().await.expect("commit");
    assert_eq!(actions(&s).await, vec![waiting("refund")]);
    assert_eq!(
        worker::drive_actions(&s.app.state, 5).await.expect("drive"),
        0,
        "the refund waits for the store order"
    );

    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
    let refund = s
        .store
        .last_refund
        .lock()
        .expect("refund lock")
        .clone()
        .expect("a refund was recorded");
    assert_eq!(refund.amount_minor, 1_000);
    assert_eq!(refund.transaction_id.as_deref(), Some("bitcoin-tx-9"));
    assert!(!refund.full);
    assert_eq!(
        actions(&s).await,
        vec![("refund".to_string(), "done".to_string(), None)]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_of_a_pending_push_drops_the_mirror_already_queued_for_it(pool: PgPool) {
    let s = scenario(pool).await;
    settled_consume(&s.app).await;
    let order_id: Uuid = s.order.order_id.parse().expect("order id");
    let now = s.app.clock.now();
    let mut tx = s.app.pool.begin().await.expect("tx");
    ledger::on_refund_recorded(
        &mut tx,
        now,
        order_id,
        "refund-1",
        &json!({ "amount_minor": 1_000 }),
        Uuid::new_v4(),
    )
    .await
    .expect("queue the refund");
    ledger::on_order_cancelled(&mut tx, now, order_id, "cancelled", Uuid::new_v4())
        .await
        .expect("cancel");
    tx.commit().await.expect("commit");
    let (state, _, reason, _) = s.push().await;
    assert_eq!(state, "quarantined");
    assert_eq!(reason.as_deref(), Some("order_cancelled_before_push"));
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new(),
        "a push set aside for good leaves no queued mirror"
    );
}

// === Events racing a retry or a quarantine of the push they mirror onto ======
//
// Two transactions are held in the interleaving that used to lose or strand a
// mirror: one reads the push as it was, the other is changing it. The
// handlers lock the push rows they read, so the later transaction waits for
// the earlier one and then sees what it did.

#[derive(Clone, Copy)]
enum Mirror {
    Cancel,
    Refund,
}

impl Mirror {
    fn kind(self) -> &'static str {
        match self {
            Mirror::Cancel => "cancel",
            Mirror::Refund => "refund",
        }
    }

    fn other(self) -> Mirror {
        match self {
            Mirror::Cancel => Mirror::Refund,
            Mirror::Refund => Mirror::Cancel,
        }
    }

    /// The Pubky event itself, through the command path, not yet consumed.
    async fn happens(self, s: &Scenario) {
        match self {
            Mirror::Cancel => cancel_order(s).await,
            Mirror::Refund => refund_order(s).await,
        }
    }

    /// The event consumer's handler, in a transaction the caller holds open.
    async fn queue_in(self, s: &Scenario, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) {
        let order_id: Uuid = s.order.order_id.parse().expect("order id");
        let now = s.app.clock.now();
        match self {
            Mirror::Cancel => {
                ledger::on_order_cancelled(tx, now, order_id, "cancelled", Uuid::new_v4())
                    .await
                    .expect("cancel handler")
            }
            Mirror::Refund => ledger::on_refund_recorded(
                tx,
                now,
                order_id,
                &Uuid::new_v4().to_string(),
                &json!({
                    "amount_minor": 1_000,
                    "order_currency": "USD",
                    "order_total_minor": s.order.total_minor,
                    "order_exponent": 2,
                }),
                Uuid::new_v4(),
            )
            .await
            .expect("refund handler"),
        }
    }

    async fn queue(self, s: &Scenario) {
        let mut tx = s.app.pool.begin().await.expect("tx");
        self.queue_in(s, &mut tx).await;
        tx.commit().await.expect("commit");
    }
}

async fn waiting_backends(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_stat_activity \
         WHERE datname = current_database() AND wait_event_type = 'Lock'",
    )
    .fetch_one(pool)
    .await
    .expect("waiting backends")
}

async fn wait_for_waiters(pool: &PgPool, count: i64) {
    for _ in 0..200 {
        if waiting_backends(pool).await >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected {count} backends waiting on a lock");
}

/// Holds the row lock of a mirror, so a statement that updates it waits.
async fn hold_the_mirror(pool: &PgPool, kind: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.expect("tx");
    sqlx::query("SELECT id FROM connector_order_actions WHERE kind = $1 FOR UPDATE")
        .bind(kind)
        .fetch_one(&mut *tx)
        .await
        .expect("lock the mirror");
    tx
}

/// Gives the task a chance to finish. A task that does not finish in this
/// time is waiting on a lock.
async fn finishes_unaided<T>(task: &mut tokio::task::JoinHandle<T>) -> bool {
    tokio::time::timeout(Duration::from_millis(1_500), task)
        .await
        .is_ok()
}

fn retry_task(s: &Scenario) -> tokio::task::JoinHandle<()> {
    let pool = s.app.pool.clone();
    let connection = s.connection;
    let order_id: Uuid = s.order.order_id.parse().expect("order id");
    let now = s.app.clock.now();
    tokio::spawn(async move {
        let outcome = ledger::retry_push(&pool, connection, order_id, now)
            .await
            .expect("retry");
        assert!(matches!(outcome, ledger::RetryOutcome::Queued(_)));
    })
}

/// The retry has run, and every mirror was queued behind the push waiting for
/// it: the push resolves to `created` and each mirror reaches the store.
async fn both_mirrors_run_after_the_retry(s: &Scenario) {
    assert_eq!(s.push().await.0, "sending");
    assert_eq!(
        actions(s).await,
        vec![waiting("cancel"), waiting("refund")],
        "no mirror is parked behind a push that is on its way to created"
    );
    s.store
        .plant_order(s.order.order_id.parse().expect("order id"), "ext-planted");
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        actions(s).await,
        vec![
            ("cancel".to_string(), "done".to_string(), None),
            ("refund".to_string(), "done".to_string(), None)
        ]
    );
}

/// A retry holding the push, stalled on another mirror, while the event is
/// consumed.
async fn a_retry_under_way_when_the_event_is_consumed(pool: PgPool, event: Mirror) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    event.other().queue(&s).await;
    assert_eq!(actions(&s).await, vec![parked(event.other().kind())]);
    event.happens(&s).await;

    let anchor = hold_the_mirror(&s.app.pool, event.other().kind()).await;
    let retry = retry_task(&s);
    wait_for_waiters(&s.app.pool, 1).await;
    let mut consume = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::consume_events(&state).await.expect("consume") })
    };
    if !finishes_unaided(&mut consume).await {
        wait_for_waiters(&s.app.pool, 2).await;
    }
    anchor.commit().await.expect("release");
    retry.await.expect("retry task");
    consume.await.expect("consume task");
    both_mirrors_run_after_the_retry(&s).await;
}

/// The event handler holding the push, uncommitted, when the retry arrives.
async fn an_event_under_way_when_the_retry_arrives(pool: PgPool, event: Mirror) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    event.other().queue(&s).await;

    let mut tx = s.app.pool.begin().await.expect("tx");
    event.queue_in(&s, &mut tx).await;
    let mut retry = retry_task(&s);
    finishes_unaided(&mut retry).await;
    tx.commit().await.expect("commit");
    retry.await.expect("retry task");
    both_mirrors_run_after_the_retry(&s).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_consumed_while_a_retry_is_under_way_is_queued_behind_the_new_attempt(
    pool: PgPool,
) {
    a_retry_under_way_when_the_event_is_consumed(pool, Mirror::Cancel).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_consumed_while_a_retry_is_under_way_is_queued_behind_the_new_attempt(
    pool: PgPool,
) {
    a_retry_under_way_when_the_event_is_consumed(pool, Mirror::Refund).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_retry_that_arrives_while_a_cancel_is_being_queued_re_queues_it(pool: PgPool) {
    an_event_under_way_when_the_retry_arrives(pool, Mirror::Cancel).await;
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_retry_that_arrives_while_a_refund_is_being_queued_re_queues_it(pool: PgPool) {
    an_event_under_way_when_the_retry_arrives(pool, Mirror::Refund).await;
}

fn both_parked() -> Vec<(String, String, Option<String>)> {
    vec![parked("cancel"), parked("refund")]
}

/// The claimed push, with a pending refund queued behind it.
async fn a_push_in_flight_with_a_mirror(s: &Scenario) -> ledger::ClaimedPush {
    settled_consume(&s.app).await;
    let claim = ledger::claim_next_push(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &["shopify".to_string()],
    )
    .await
    .expect("claim")
    .expect("a due push");
    Mirror::Refund.queue(s).await;
    assert_eq!(actions(s).await, vec![waiting("refund")]);
    claim
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_cancel_consumed_while_the_push_is_being_quarantined_is_parked_with_the_rest(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    let claim = a_push_in_flight_with_a_mirror(&s).await;
    cancel_order(&s).await;

    let anchor = hold_the_mirror(&s.app.pool, "refund").await;
    let quarantining = {
        let pool = s.app.pool.clone();
        let row = claim.row.clone();
        let now = s.app.clock.now();
        tokio::spawn(async move {
            let connection = marketplace_service::connectors::store::get(&pool, row.connection_id)
                .await
                .expect("connection read")
                .expect("connection");
            ledger::quarantine_push(&pool, &row, &connection, "validation", "refused", now)
                .await
                .expect("quarantine")
        })
    };
    wait_for_waiters(&s.app.pool, 1).await;
    let mut consume = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::consume_events(&state).await.expect("consume") })
    };
    if !finishes_unaided(&mut consume).await {
        wait_for_waiters(&s.app.pool, 2).await;
    }
    anchor.commit().await.expect("release");
    assert!(quarantining.await.expect("quarantine task"));
    consume.await.expect("consume task");

    assert_eq!(s.push().await.0, "quarantined");
    assert_eq!(
        actions(&s).await,
        both_parked(),
        "no mirror is left pending behind a quarantined push"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_quarantine_that_arrives_while_a_cancel_is_being_queued_parks_it(pool: PgPool) {
    let s = scenario(pool).await;
    let claim = a_push_in_flight_with_a_mirror(&s).await;

    let mut tx = s.app.pool.begin().await.expect("tx");
    Mirror::Cancel.queue_in(&s, &mut tx).await;
    let mut quarantining = {
        let pool = s.app.pool.clone();
        let row = claim.row.clone();
        let now = s.app.clock.now();
        tokio::spawn(async move {
            let connection = marketplace_service::connectors::store::get(&pool, row.connection_id)
                .await
                .expect("connection read")
                .expect("connection");
            ledger::quarantine_push(&pool, &row, &connection, "validation", "refused", now)
                .await
                .expect("quarantine")
        })
    };
    finishes_unaided(&mut quarantining).await;
    tx.commit().await.expect("commit");
    assert!(quarantining.await.expect("quarantine task"));

    assert_eq!(s.push().await.0, "quarantined");
    assert_eq!(actions(&s).await, both_parked());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_reversed_before_its_event_is_consumed_mirrors_nothing(pool: PgPool) {
    let s = scenario(pool).await;
    settled_consume(&s.app).await;
    assert_eq!(s.push().await.0, "pending");
    refund_order(&s).await;
    sqlx::query(
        "UPDATE orders SET state = 'return_received', external_refund = NULL \
         WHERE id = $1::uuid",
    )
    .bind(&s.order.order_id)
    .execute(&s.app.pool)
    .await
    .expect("the refund is reversed and the order reopened");

    s.pass().await;
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new(),
        "a refund the order no longer holds has nothing to mirror"
    );
    assert_eq!(s.push().await.0, "created", "the reopened order is pushed");
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
}

// === A refund reversed and recorded again ====================================

/// What a cancelled reversal leaves on the order, and the event it emits: the
/// order holds no refund and is open again. The revision moves on as it does
/// for any order change.
async fn reverse_the_refund(s: &Scenario) -> i64 {
    let revision: i64 = sqlx::query_scalar(
        "UPDATE orders SET state = 'return_received', external_refund = NULL, \
         revision = revision + 1 WHERE id = $1::uuid RETURNING revision",
    )
    .bind(&s.order.order_id)
    .fetch_one(&s.app.pool)
    .await
    .expect("reverse the refund");
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, $4, 'paypal', 'refund.reversal_cancelled', $5)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(format!("order:{}", s.order.order_id))
    .bind(revision)
    .bind(s.app.clock.now())
    .execute(&s.app.pool)
    .await
    .expect("reversal event");
    revision
}

async fn record_the_refund_again(s: &Scenario, revision: i64, amount_minor: i64, txn: &str) {
    let (status, body) = execute(
        &s.app,
        &s.seller.token,
        &order_command(
            "refund.record_external",
            &s.order.order_id,
            revision,
            json!({ "amount_minor": amount_minor, "transaction_id": txn }),
            1_216,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

fn refund_amounts(s: &Scenario) -> Vec<i64> {
    s.store
        .refund_amounts
        .lock()
        .expect("refund amounts")
        .clone()
}

async fn refund_actions(s: &Scenario) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT state, action_key FROM connector_order_actions WHERE kind = 'refund' \
         ORDER BY created_at, id",
    )
    .fetch_all(&s.app.pool)
    .await
    .expect("refund actions")
}

async fn reversal_alerts(pool: &PgPool) -> i64 {
    common::count(
        pool,
        "SELECT COUNT(*) FROM connector_alerts WHERE dedupe_key LIKE 'refund_reversed|%'",
    )
    .await
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_recorded_again_after_a_reversal_reaches_the_store_once_with_its_own_amount(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    refund_order(&s).await;
    let revision = reverse_the_refund(&s).await;
    let second = s.order.total_minor / 2;
    record_the_refund_again(&s, revision, second, "bitcoin-tx-2").await;

    s.pass().await;
    assert_eq!(
        refund_amounts(&s),
        vec![second],
        "the store gets the second amount, once"
    );
    let actions = refund_actions(&s).await;
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].0, "done");
    assert_eq!(reversal_alerts(&s.app.pool).await, 0);

    sqlx::query("UPDATE connector_event_cursor SET sequence = 0 WHERE name = 'events'")
        .execute(&s.app.pool)
        .await
        .expect("rewind");
    s.after(700);
    s.pass().await;
    assert_eq!(refund_amounts(&s), vec![second], "a replay adds none");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_already_mirrored_when_it_is_reversed_is_flagged_and_a_smaller_new_one_adds_nothing(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    s.pass().await;
    refund_order(&s).await;
    s.pass().await;
    assert_eq!(refund_amounts(&s), vec![s.order.total_minor]);

    let revision = reverse_the_refund(&s).await;
    s.pass().await;
    assert_eq!(reversal_alerts(&s.app.pool).await, 1);
    let second = s.order.total_minor / 2;
    record_the_refund_again(&s, revision, second, "bitcoin-tx-2").await;
    s.pass().await;
    assert_eq!(
        refund_amounts(&s),
        vec![s.order.total_minor],
        "the store already shows the first refund, which is the larger: the second adds nothing"
    );
    assert_eq!(reversal_alerts(&s.app.pool).await, 1);
    let (_, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts?unresolved=true",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"][0]["kind"], "refund_reversed_after_mirror");
    assert_eq!(alerts["alerts"][0]["pubky_order_id"], s.order.order_id);
    assert!(alerts["alerts"][0]["detail"]
        .as_str()
        .expect("detail")
        .contains("reversed or reduced"));
    let states: Vec<String> = refund_actions(&s)
        .await
        .into_iter()
        .map(|(state, _)| state)
        .collect();
    assert_eq!(states, vec!["done", "done"]);

    s.pass().await;
    assert_eq!(
        reversal_alerts(&s.app.pool).await,
        1,
        "one alert per mirror"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_reversed_before_it_reached_the_store_is_dropped_without_an_alert(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    refund_order(&s).await;
    settled_consume(&s.app).await;
    assert_eq!(refund_actions(&s).await.len(), 1);

    reverse_the_refund(&s).await;
    settled_consume(&s.app).await;
    assert_eq!(refund_actions(&s).await.len(), 0);
    s.pass().await;
    assert!(refund_amounts(&s).is_empty());
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(reversal_alerts(&s.app.pool).await, 0);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_retry_and_a_reversal_over_the_same_parked_mirrors_lock_them_in_one_order(pool: PgPool) {
    let s = scenario(pool).await;
    quarantine_a_push(&s).await;
    Mirror::Refund.queue(&s).await;
    Mirror::Refund.queue(&s).await;
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM connector_order_actions ORDER BY id")
        .fetch_all(&s.app.pool)
        .await
        .expect("mirror ids");
    assert_eq!(ids.len(), 2);
    reverse_the_refund(&s).await;

    // Another transaction holds the mirror that sorts last. The retry takes
    // the first and waits for the last; the reversal then wants the first.
    // Were the two to take the pair in different orders, each would hold the
    // row the other waits for as soon as the last is released.
    let mut holder = s.app.pool.begin().await.expect("tx");
    sqlx::query("SELECT id FROM connector_order_actions WHERE id = $1 FOR UPDATE")
        .bind(ids[1])
        .fetch_one(&mut *holder)
        .await
        .expect("hold the last mirror");
    let retry = retry_task(&s);
    wait_for_waiters(&s.app.pool, 1).await;
    let consume = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::consume_events(&state).await.expect("consume") })
    };
    wait_for_waiters(&s.app.pool, 2).await;
    holder.commit().await.expect("release");
    retry.await.expect("the retry is not a deadlock victim");
    consume
        .await
        .expect("the reversal is not a deadlock victim");

    assert_eq!(s.push().await.0, "sending");
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new(),
        "the reversal drops the mirrors the retry had re-queued"
    );
}

// === The refund model: unit, difference, full ===============================

fn refund_payload(s: &Scenario, amount_minor: i64) -> serde_json::Value {
    json!({
        "amount_minor": amount_minor,
        "transaction_id": "bitcoin-tx-9",
        "order_total_minor": s.order.total_minor,
        "order_currency": "USD",
        "order_exponent": 2,
        "full": amount_minor >= s.order.total_minor,
    })
}

async fn queue_refund(s: &Scenario, key: &str, amount_minor: i64) {
    let mut tx = s.app.pool.begin().await.expect("tx");
    ledger::on_refund_recorded(
        &mut tx,
        s.app.clock.now(),
        s.order.order_id.parse().expect("order id"),
        key,
        &refund_payload(s, amount_minor),
        Uuid::new_v4(),
    )
    .await
    .expect("queue the refund");
    tx.commit().await.expect("commit");
}

fn last_refund(s: &Scenario) -> marketplace_service::connectors::provider::RefundRecord {
    s.store
        .last_refund
        .lock()
        .expect("refund lock")
        .clone()
        .expect("a refund was recorded")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_partial_refund_is_recorded_in_the_order_currency_and_is_not_full(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    let half = s.order.total_minor / 2;
    refund_order_of(&s, half).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, half);
    assert_eq!(refund.recorded_total_minor, half);
    assert_eq!(refund.order_total_minor, s.order.total_minor);
    assert_eq!(refund.order_currency, "USD");
    assert!(!refund.full, "half of the order is not a full refund");
    assert_eq!(refund.basis, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_of_the_whole_order_is_full(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    refund_order(&s).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, s.order.total_minor);
    assert!(refund.full);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_later_larger_refund_mirrors_only_the_difference(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    queue_refund(&s, "refund-a", 3_000).await;
    s.pass().await;
    assert_eq!(
        s.store.refund_amounts.lock().expect("amounts").clone(),
        vec![3_000]
    );

    queue_refund(&s, "refund-b", 8_000).await;
    s.pass().await;
    assert_eq!(
        s.store.refund_amounts.lock().expect("amounts").clone(),
        vec![3_000, 5_000],
        "the store is told the 5,000 still to mirror, not the 8,000 total"
    );
    let refund = last_refund(&s);
    assert_eq!(refund.recorded_total_minor, 8_000);

    queue_refund(&s, "refund-c", 8_000).await;
    queue_refund(&s, "refund-d", 6_000).await;
    s.pass().await;
    assert_eq!(
        s.store.refund_amounts.lock().expect("amounts").len(),
        2,
        "a repeat, or a smaller total, adds nothing"
    );
    assert!(actions(&s)
        .await
        .iter()
        .all(|(_, state, _)| state == "done"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_the_merchant_already_made_in_the_store_counts(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    s.store.refund_in_store("ext-1", 2_000);
    queue_refund(&s, "refund-a", 5_000).await;
    s.pass().await;
    assert_eq!(
        s.store.refund_amounts.lock().expect("amounts").clone(),
        vec![3_000],
        "the store already shows 2,000 refunded"
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_refund_above_the_payment_is_quarantined_and_never_sent(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    queue_refund(&s, "refund-a", s.order.total_minor + 1).await;
    s.pass().await;
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        actions(&s).await,
        vec![(
            "refund".to_string(),
            "quarantined".to_string(),
            Some("refund_exceeds_payment".to_string())
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_store_order_in_another_currency_is_never_refunded_against(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    sqlx::query("UPDATE connector_order_push SET external_id = 'ext-other'")
        .execute(&s.app.pool)
        .await
        .expect("repoint");
    s.store.plant_order_paid(
        s.order.order_id.parse().expect("order id"),
        "ext-other",
        1_000_000,
        "CHF",
    );
    queue_refund(&s, "refund-a", 1_000).await;
    s.pass().await;
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        actions(&s).await,
        vec![(
            "refund".to_string(),
            "quarantined".to_string(),
            Some("currency_mismatch".to_string())
        )]
    );
}

/// Reprices the paid, unpushed order in EUR and records that it was paid with
/// USDT (`payment_amount_minor` USDT0 millionths for `cents` USD cents).
async fn paid_in_usdt0_for_a_eur_order(s: &Scenario, cents: Option<i64>) -> i64 {
    let payment_millionths = cents.unwrap_or(2 * s.order.total_minor) * 10_000;
    sqlx::query(
        "UPDATE orders SET currency = 'EUR', payment_method = 'usdt', payment_asset = 'USDT', \
         payment_network = 'arbitrum-one', payment_amount_minor = $2, payment_exponent = 6, \
         payment_quote_basis = 'usd-parity', paykit_total_sats = $2 WHERE id = $1::uuid",
    )
    .bind(&s.order.order_id)
    .bind(payment_millionths)
    .execute(&s.app.pool)
    .await
    .expect("pay in USDT");
    if let Some(cents) = cents {
        sqlx::query(
            "UPDATE orders SET usd_conversion_pair = 'EURUSD', usd_conversion_source = 'blocktank', \
             usd_conversion_usd_rate = 100000, usd_conversion_currency_rate = 92000, \
             usd_conversion_fetched_at = now(), usd_conversion_cents = $2 WHERE id = $1::uuid",
        )
        .bind(&s.order.order_id)
        .bind(cents)
        .execute(&s.app.pool)
        .await
        .expect("record the conversion");
    }
    payment_millionths
}

/// What `refund.record_external` leaves on a USDT-paid non-USD order: the
/// refund in USDT0 units, the order refunded, and the event.
async fn record_a_usdt0_refund(s: &Scenario, millionths: i64) {
    let revision: i64 = sqlx::query_scalar(
        "UPDATE orders SET state = 'refunded_external', revision = revision + 1, \
         external_refund = jsonb_build_object('amount_minor', $2::bigint, \
             'transaction_id', '0x' || repeat('ab', 32), 'recorded_at', '2026-10-15T12:00:00.000Z') \
         WHERE id = $1::uuid RETURNING revision",
    )
    .bind(&s.order.order_id)
    .bind(millionths)
    .fetch_one(&s.app.pool)
    .await
    .expect("record the refund");
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
         occurred_at) VALUES ($1, $2, $3, $4, $5, 'refund.recorded_external', $6)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(format!("order:{}", s.order.order_id))
    .bind(revision)
    .bind(&s.seller.pubky)
    .bind(s.app.clock.now())
    .execute(&s.app.pool)
    .await
    .expect("refund event");
}

/// Records that the paid order's 137.00 USD were paid as `received` sats
/// through Paykit.
async fn paid_in_sats(s: &Scenario, received: i64) {
    sqlx::query(
        "UPDATE orders SET payment_method = 'paykit', paykit_total_sats = $2, \
         paykit_observed_sats = $2 WHERE id = $1::uuid",
    )
    .bind(&s.order.order_id)
    .bind(received)
    .execute(&s.app.pool)
    .await
    .expect("pay in sats");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sats_refund_of_a_usd_priced_order_is_mirrored_in_proportion_in_cents(pool: PgPool) {
    let s = scenario(pool).await;
    assert_eq!(s.order.total_minor, 13_700);
    paid_in_sats(&s, 51_637).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    // Half the sats received, recorded by the seller in sats.
    refund_order_of(&s, 25_819).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, 6_850, "half of 137.00 USD");
    assert_eq!(refund.recorded_total_minor, 6_850);
    assert_eq!(refund.order_currency, "USD");
    assert!(!refund.full);
    let basis = refund.basis.expect("the basis is recorded for the note");
    assert!(
        basis.contains("25819 of the 51637 sats received for 137.00 USD"),
        "{basis}"
    );
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sats_refund_of_everything_received_is_the_whole_order(pool: PgPool) {
    let s = scenario(pool).await;
    paid_in_sats(&s, 51_637).await;
    s.pass().await;
    refund_order_of(&s, 51_637).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, s.order.total_minor);
    assert!(refund.full);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt0_refund_of_a_non_usd_order_is_converted_with_the_orders_own_conversion(
    pool: PgPool,
) {
    let s = scenario(pool).await;
    let total = s.order.total_minor;
    let payment = paid_in_usdt0_for_a_eur_order(&s, Some(2 * total)).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    assert_eq!(
        s.store.remote_orders()[0].currency,
        "EUR",
        "the store order is in the order currency"
    );

    // A quarter of the payment, in USDT0 millionths.
    record_a_usdt0_refund(&s, payment / 4).await;
    s.pass().await;
    let expected = (total + 2) / 4;
    let refund = last_refund(&s);
    assert_eq!(
        refund.amount_minor, expected,
        "a quarter of the order, rounded half up, in the order currency"
    );
    assert_eq!(refund.recorded_total_minor, expected);
    assert_eq!(refund.order_currency, "EUR");
    assert!(!refund.full);
    let basis = refund.basis.expect("the basis is recorded for the note");
    assert!(basis.contains("USDT0"), "{basis}");
    assert!(basis.contains("EURUSD"), "{basis}");
    assert!(basis.contains("rounded half up"), "{basis}");
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt0_refund_of_the_whole_payment_is_the_whole_order(pool: PgPool) {
    let s = scenario(pool).await;
    let total = s.order.total_minor;
    let payment = paid_in_usdt0_for_a_eur_order(&s, Some(2 * total)).await;
    s.pass().await;
    record_a_usdt0_refund(&s, payment).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, total);
    assert!(refund.full);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usdt0_refund_with_no_recorded_conversion_is_quarantined_not_sent(pool: PgPool) {
    // The order records USDT terms but no conversion (its columns are NULL).
    let s = scenario(pool).await;
    let payment = paid_in_usdt0_for_a_eur_order(&s, None).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    record_a_usdt0_refund(&s, payment / 4).await;
    s.pass().await;
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        actions(&s).await,
        vec![(
            "refund".to_string(),
            "quarantined".to_string(),
            Some("refund_amount_unconvertible".to_string())
        )]
    );
    let (_, alerts) = api(
        &s.app,
        "GET",
        "/v1/connectors/alerts?unresolved=true",
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(alerts["alerts"][0]["kind"], "action_quarantined");
    assert!(alerts["alerts"][0]["detail"]
        .as_str()
        .expect("detail")
        .contains("by hand"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn an_order_without_usdt_terms_is_unaffected_by_the_conversion_columns(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    let half = s.order.total_minor / 2;
    refund_order_of(&s, half).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, half);
    assert_eq!(refund.basis, None);
}

// === One mirror at a time per store order ====================================

fn refunded_at_the_store(s: &Scenario) -> i64 {
    s.store.remote_orders()[0].refunded_minor
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn two_workers_and_two_refund_mirrors_of_one_order_make_one_store_refund(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    // Two records of the same 5,000 refund (a refund recorded again), and a
    // cancel that is not due yet.
    queue_refund(&s, "refund-a", 5_000).await;
    queue_refund(&s, "refund-b", 5_000).await;
    Mirror::Cancel.queue(&s).await;
    sqlx::query(
        "UPDATE connector_order_actions SET next_attempt_at = next_attempt_at + interval '1 hour' \
         WHERE kind = 'cancel'",
    )
    .execute(&s.app.pool)
    .await
    .expect("delay the cancel");

    // Worker A takes a refund mirror and the store is slow to answer.
    let gate = s.store.open_refund_gate();
    let worker_a = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::drive_actions(&state, 1).await.expect("worker A") })
    };
    gate.entered.notified().await;
    assert_eq!(
        s.store.refund_calls.load(Ordering::SeqCst),
        1,
        "A is inside the store's refund call, which has not recorded anything yet"
    );
    assert_eq!(refunded_at_the_store(&s), 0);
    sqlx::query(
        "UPDATE connector_order_actions SET next_attempt_at = next_attempt_at - interval '2 hours' \
         WHERE kind = 'cancel'",
    )
    .execute(&s.app.pool)
    .await
    .expect("the cancel is due");

    // Worker B, another replica, finds the order busy: the other refund
    // mirror and the cancel both wait, so B neither reads the store's total
    // nor writes.
    let mut worker_b = {
        let state = s.app.state.clone();
        tokio::spawn(async move { worker::drive_actions(&state, 5).await.expect("worker B") })
    };
    let b_result = tokio::time::timeout(Duration::from_millis(1_500), &mut worker_b).await;
    // Let every store call finish, whether or not the code under test let B in.
    gate.release.notify_one();
    gate.release.notify_one();
    *s.store.refund_gate.lock().expect("gate") = None;
    assert_eq!(
        b_result
            .expect("worker B does not wait inside a store call for an order that is busy")
            .expect("worker B task"),
        0,
        "nothing of a busy order is claimed"
    );
    assert_eq!(worker_a.await.expect("worker A task"), 1);
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 1);
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 0);
    assert_eq!(refunded_at_the_store(&s), 5_000);

    // With A finished, B takes the rest: the second record finds the store
    // already shows 5,000 and records nothing; the cancel runs.
    assert_eq!(
        worker::drive_actions(&s.app.state, 5)
            .await
            .expect("worker B"),
        2
    );
    assert_eq!(
        s.store.refund_amounts.lock().expect("amounts").clone(),
        vec![5_000],
        "exactly one store refund, of the right amount"
    );
    assert_eq!(refunded_at_the_store(&s), 5_000);
    assert_eq!(s.store.cancel_calls.load(Ordering::SeqCst), 1);
    assert!(actions(&s)
        .await
        .iter()
        .all(|(_, state, _)| state == "done"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_worker_that_died_mid_call_leaves_the_order_busy_until_its_lease_lapses(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    queue_refund(&s, "refund-a", 5_000).await;
    queue_refund(&s, "refund-b", 5_000).await;

    // A worker claims a mirror, its refund reaches the store, and the worker
    // dies before it hears back.
    let _lost = ledger::claim_next_action(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &["shopify".to_string()],
    )
    .await
    .expect("claim")
    .expect("a due mirror");
    s.store.refund_in_store("ext-1", 5_000);

    assert_eq!(
        worker::drive_actions(&s.app.state, 5).await.expect("drive"),
        0,
        "while the lease holds, the order's other mirror waits"
    );

    // The lease lapses. The next claimant re-reads what the store shows, so
    // neither mirror writes the 5,000 again.
    s.after(61);
    assert_eq!(
        worker::drive_actions(&s.app.state, 5).await.expect("drive"),
        2
    );
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(refunded_at_the_store(&s), 5_000);
    assert!(actions(&s)
        .await
        .iter()
        .all(|(_, state, _)| state == "done"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn mirrors_of_one_order_in_different_stores_run_side_by_side(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    queue_refund(&s, "refund-a", 5_000).await;
    // The same order also pushed to a second store, with its own created push.
    let woo = connect_woo_shop(&s.app, &s.seller, "woo.test-store.example").await;
    let order_id: Uuid = s.order.order_id.parse().expect("order id");
    sqlx::query(
        "INSERT INTO connector_order_push \
         (id, connection_id, seller_pubky, pubky_order_id, provider, state, external_id, \
          attempts, next_attempt_at, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, 'woocommerce', 'created', 'woo-1', 1, now(), now(), now())",
    )
    .bind(Uuid::new_v4())
    .bind(woo)
    .bind(&s.seller.pubky)
    .bind(order_id)
    .execute(&s.app.pool)
    .await
    .expect("a push to the second store");
    let mut tx = s.app.pool.begin().await.expect("tx");
    ledger::on_refund_recorded(
        &mut tx,
        s.app.clock.now(),
        order_id,
        "refund-a",
        &refund_payload(&s, 5_000),
        Uuid::new_v4(),
    )
    .await
    .expect("queue the refund");
    tx.commit().await.expect("commit");
    assert_eq!(actions(&s).await.len(), 2, "one mirror per store");

    let providers = ["shopify".to_string(), "woocommerce".to_string()];
    let first = ledger::claim_next_action(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &providers,
    )
    .await
    .expect("claim")
    .expect("a mirror");
    let second = ledger::claim_next_action(
        &s.app.pool,
        s.app.clock.now(),
        Duration::from_secs(60),
        &providers,
    )
    .await
    .expect("claim")
    .expect("the other store's mirror, not held up by the first");
    assert_ne!(first.1.connection_id, second.1.connection_id);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_legacy_usdt_order_with_no_terms_to_convert_with_is_quarantined_not_sent(pool: PgPool) {
    // `payment_method = 'usdt'` with no asset terms and no conversion: the
    // refund is in USDT0 units by the rule main applies, and nothing here can
    // convert it.
    let s = scenario(pool).await;
    sqlx::query("UPDATE orders SET currency = 'EUR', payment_method = 'usdt' WHERE id = $1::uuid")
        .bind(&s.order.order_id)
        .execute(&s.app.pool)
        .await
        .expect("a legacy USDT order");
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    record_a_usdt0_refund(&s, 1_000).await;
    s.pass().await;
    assert_eq!(s.store.refund_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        actions(&s).await,
        vec![(
            "refund".to_string(),
            "quarantined".to_string(),
            Some("refund_amount_unconvertible".to_string())
        )]
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_usd_order_paid_in_usdt_records_in_the_order_currency(pool: PgPool) {
    // A USD price paid in USDT records its refund in cents, like any rail.
    let s = scenario(pool).await;
    sqlx::query(
        "UPDATE orders SET payment_method = 'usdt', payment_asset = 'USDT', \
         payment_network = 'arbitrum-one', payment_amount_minor = total_minor * 10000, \
         payment_exponent = 6, payment_quote_basis = 'usd-parity' WHERE id = $1::uuid",
    )
    .bind(&s.order.order_id)
    .execute(&s.app.pool)
    .await
    .expect("USDT terms");
    s.pass().await;
    let half = s.order.total_minor / 2;
    sqlx::query("UPDATE orders SET paykit_total_sats = total_minor * 10000 WHERE id = $1::uuid")
        .bind(&s.order.order_id)
        .execute(&s.app.pool)
        .await
        .expect("cap");
    record_a_usdt0_refund(&s, half).await;
    s.pass().await;
    let refund = last_refund(&s);
    assert_eq!(refund.amount_minor, half);
    assert_eq!(refund.basis, None);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_push_of_a_disconnected_connection_gets_no_mirror(pool: PgPool) {
    let s = scenario(pool).await;
    s.pass().await;
    assert_eq!(s.push().await.0, "created");
    let (status, _) = api(
        &s.app,
        "DELETE",
        &format!("/v1/connectors/{}", s.connection),
        Some(&s.seller.token),
        &json!(null),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let order_id: Uuid = s.order.order_id.parse().expect("order id");
    let mut tx = s.app.pool.begin().await.expect("tx");
    assert!(
        !ledger::has_mirrorable_push(&mut tx, order_id)
            .await
            .expect("mirrorable"),
        "nothing of that push can ever run"
    );
    tx.rollback().await.expect("rollback");
    queue_refund(&s, "refund-a", 5_000).await;
    Mirror::Cancel.queue(&s).await;
    assert_eq!(
        actions(&s).await,
        Vec::<(String, String, Option<String>)>::new(),
        "no mirror is queued behind a disconnected connection"
    );
}
