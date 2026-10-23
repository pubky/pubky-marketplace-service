//! The public drop read (`GET /v0/drops/{seller_pubky}/{drop_id}`): it takes
//! no row lock unless a scheduled transition is due, so steady-state reads no
//! longer queue behind checkout on the same drop. When a transition is due it
//! locks the drop row and applies it once, however many reads and sweeps meet
//! at the boundary. A found drop carries short public caching; a miss is
//! never cached.

mod common;

use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use axum::Router;
use chrono::{DateTime, Utc};
use marketplace_service::clock::{format_timestamp, Clock};
use marketplace_service::handlers::drops::{apply_time_transitions, fetch_drop_for_update};
use marketplace_service::model::DropRow;
use marketplace_service::workers::transition_due_drops;
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;

use common::{
    count, drop_aggregate, drop_record_json, execute, new_actor, register_command,
    sync_drop_command, test_app_with_homeserver, ts_after, TestActor, TestApp,
};

const DROP_ID: &str = "winter_drop";
/// Seconds after the fixture instant at which the drop starts.
const STARTS_IN: i64 = 600;
/// Seconds after the fixture instant at which a drop with an end closes.
const ENDS_IN: i64 = 1_200;
/// Upper bound for one read. A read stuck behind a row lock never returns,
/// so a bounded wait turns that into a failure instead of a hung test.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Concurrent reads at the start boundary, below the per-test pool size of 5.
const CONCURRENT_READS: usize = 4;
/// Seconds the clock moves on while a read waits on the drop row lock.
const LOCK_WAIT: i64 = 7;
/// Seconds the clock moves on while a read waits for a pool connection.
const CONNECTION_WAIT: i64 = 3;
/// Scheduler turns a spawned read gets to reach the pool wait.
const SCHEDULER_TURNS: usize = 50;

fn public_drop_uri(seller_pubky: &str) -> String {
    format!("/v0/drops/{seller_pubky}/{DROP_ID}")
}

async fn public_drop(router: Router, uri: &str) -> (StatusCode, HeaderMap, Value) {
    common::send_with_headers(router, "GET", uri, None, &json!(null)).await
}

/// Registers the `boots_01` listing and syncs one announced drop over it that
/// starts [`STARTS_IN`] seconds after the fixture instant and, when `ends_in`
/// is given, ends `ends_in` seconds after the fixture instant. Returns the
/// seller and the drop aggregate id.
async fn announced_drop(
    app: &TestApp,
    homeserver: &common::FakeHomeserver,
    ends_in: Option<i64>,
) -> (TestActor, String) {
    let seller = new_actor(app).await;
    let register = register_command(&seller.pubky, 5);
    homeserver.publish_registration(&register);
    let (status, body) = execute(app, &seller.token, &register).await;
    assert_eq!(status, StatusCode::OK, "register fixture failed: {body}");
    homeserver.put_drop_record(
        &seller.pubky,
        DROP_ID,
        drop_record_json(
            &seller.pubky,
            DROP_ID,
            1,
            &["boots_01"],
            &ts_after(STARTS_IN),
            ends_in.map(ts_after).as_deref(),
            5,
            2,
        ),
    );
    let (status, body) = execute(
        app,
        &seller.token,
        &sync_drop_command(&seller.pubky, DROP_ID, 1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "drop sync fixture failed: {body}");
    let aggregate_id = drop_aggregate(&seller.pubky, DROP_ID);
    (seller, aggregate_id)
}

async fn drop_started_events(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM events WHERE kind = 'drop.started'",
    )
    .await
}

async fn drop_ended_events(pool: &PgPool) -> i64 {
    count(
        pool,
        "SELECT COUNT(*) FROM events WHERE kind = 'drop.ended'",
    )
    .await
}

/// The persisted state and revision of the drop row.
async fn stored_drop(pool: &PgPool, aggregate_id: &str) -> (String, i64) {
    sqlx::query_as("SELECT state, revision FROM drops WHERE aggregate_id = $1")
        .bind(aggregate_id)
        .fetch_one(pool)
        .await
        .expect("the drop exists")
}

/// All bindings of the drop in `drop_listings`, and how many are active.
async fn binding_counts(pool: &PgPool, aggregate_id: &str) -> (i64, i64) {
    sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE active) FROM drop_listings \
         WHERE drop_aggregate_id = $1",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("binding counts load")
}

/// Reads the public drop while another transaction holds its row
/// `FOR UPDATE`, as a checkout on this drop would. Panics if the read does
/// not finish within [`READ_TIMEOUT`], that is, if it waits for the row lock.
async fn public_drop_while_row_is_locked(
    app: &TestApp,
    aggregate_id: &str,
    uri: &str,
) -> (StatusCode, HeaderMap, Value) {
    let mut holder = app.pool.begin().await.expect("holder begins");
    sqlx::query("SELECT 1 FROM drops WHERE aggregate_id = $1 FOR UPDATE")
        .bind(aggregate_id)
        .execute(&mut *holder)
        .await
        .expect("holder locks the drop row");

    let read = tokio::time::timeout(READ_TIMEOUT, public_drop(app.router.clone(), uri)).await;
    holder.rollback().await.expect("holder rolls back");
    read.expect("the read does not wait for the drop row lock")
}

/// Locks the drop row `FOR UPDATE` in a holder transaction, spawns a public
/// read, and returns once that read waits on the row lock. By then the read's
/// unlocked SELECT has already seen the row as it was before the holder
/// writes. Returns the holder, the row it locked, and the pending read.
async fn spawn_read_waiting_on_the_row_lock(
    app: &TestApp,
    aggregate_id: &str,
    uri: String,
) -> (
    Transaction<'static, Postgres>,
    DropRow,
    JoinHandle<(StatusCode, HeaderMap, Value)>,
) {
    let mut holder = app.pool.begin().await.expect("holder begins");
    let (holder_pid,): (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
        .fetch_one(&mut *holder)
        .await
        .expect("holder pid");
    let locked_row = fetch_drop_for_update(&mut holder, aggregate_id)
        .await
        .expect("holder locks the drop row")
        .expect("the drop exists");

    let router = app.router.clone();
    let read = tokio::spawn(async move { public_drop(router, &uri).await });

    let started = tokio::time::Instant::now();
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE datname = current_database() AND pid <> $1 \
             AND wait_event_type = 'Lock' \
             AND query ILIKE '%FROM drops%' AND query ILIKE '%FOR UPDATE%')",
        )
        .bind(holder_pid)
        .fetch_one(&app.pool)
        .await
        .expect("lock-wait probe");
        if waiting {
            break;
        }
        assert!(
            started.elapsed() < READ_TIMEOUT,
            "the read never waited on the drop row lock"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (holder, locked_row, read)
}

/// The occurrence time of the drop's one `drop.started` event and the drop
/// row's `updated_at`.
async fn start_event_and_row_times(
    pool: &PgPool,
    aggregate_id: &str,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let occurred_at: DateTime<Utc> = sqlx::query_scalar(
        "SELECT occurred_at FROM events WHERE aggregate_id = $1 AND kind = 'drop.started'",
    )
    .bind(aggregate_id)
    .fetch_one(pool)
    .await
    .expect("the drop.started event exists");
    let updated_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT updated_at FROM drops WHERE aggregate_id = $1")
            .bind(aggregate_id)
            .fetch_one(pool)
            .await
            .expect("the drop exists");
    (occurred_at, updated_at)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_drop_read_takes_no_row_lock_when_nothing_is_due(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, None).await;

    let (status, _, body) =
        public_drop_while_row_is_locked(&app, &aggregate_id, &public_drop_uri(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("announced"));
    assert_eq!(body["drop"]["revision"], json!(1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_read_of_a_live_drop_takes_no_row_lock(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, None).await;
    let uri = public_drop_uri(&seller.pubky);
    app.clock.advance_seconds(STARTS_IN + 1);

    // The first read past starts_at applies the start and persists it.
    let (status, _, body) = public_drop(app.router.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(
        stored_drop(&app.pool, &aggregate_id).await,
        ("live".to_owned(), 2)
    );

    // A live drop with no ends_at has nothing due, so the next read does
    // not touch the row lock.
    let (status, _, body) = public_drop_while_row_is_locked(&app, &aggregate_id, &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_public_reads_at_starts_at_record_one_start(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver, None).await;
    app.clock.advance_seconds(STARTS_IN + 1);

    let uri = public_drop_uri(&seller.pubky);
    let mut reads = JoinSet::new();
    for _ in 0..CONCURRENT_READS {
        let router = app.router.clone();
        let uri = uri.clone();
        reads.spawn(
            async move { tokio::time::timeout(READ_TIMEOUT, public_drop(router, &uri)).await },
        );
    }
    let mut finished = 0;
    while let Some(read) = reads.join_next().await {
        let (status, headers, body) = read
            .expect("read task runs")
            .expect("read finishes in time");
        assert_eq!(status, StatusCode::OK, "public read failed: {body}");
        assert_eq!(body["drop"]["state"], json!("live"));
        assert_eq!(body["drop"]["revision"], json!(2));
        assert_eq!(headers["cache-control"], "public, max-age=1");
        finished += 1;
    }
    assert_eq!(finished, CONCURRENT_READS);
    assert_eq!(drop_started_events(&app.pool).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_read_racing_the_drop_sweep_records_one_start(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver, None).await;
    app.clock.advance_seconds(STARTS_IN + 1);
    let now = app.clock.now();

    let uri = public_drop_uri(&seller.pubky);
    let ((status, _, body), swept) = tokio::time::timeout(READ_TIMEOUT, async {
        tokio::join!(
            public_drop(app.router.clone(), &uri),
            transition_due_drops(&app.pool, now)
        )
    })
    .await
    .expect("the read and the sweep both finish");
    swept.expect("the sweep succeeds");

    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));
    assert_eq!(drop_started_events(&app.pool).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_read_that_waits_on_the_lock_applies_nothing_the_holder_already_applied(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, None).await;
    app.clock.advance_seconds(STARTS_IN + 1);
    let now = app.clock.now();

    // Once the read waits on the row lock, its unlocked SELECT has already
    // seen the drop announced with the start due.
    let (mut holder, locked_row, read) =
        spawn_read_waiting_on_the_row_lock(&app, &aggregate_id, public_drop_uri(&seller.pubky))
            .await;
    app.clock.advance_seconds(LOCK_WAIT);

    // The holder applies the start first. A read that applied its stale
    // unlocked row would write revision 2 again and fail on the events
    // (aggregate_id, revision) unique constraint.
    let applied = apply_time_transitions(&mut holder, locked_row, Uuid::new_v4(), now)
        .await
        .expect("holder applies the start");
    assert_eq!(applied.revision, 2);
    holder.commit().await.expect("holder commits");

    let (status, _, body) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .expect("the read finishes once the holder commits")
        .expect("read task runs");
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));
    // The server time is taken once the lock is granted, not before the wait.
    assert_eq!(
        body["drop"]["server_time"],
        json!(ts_after(STARTS_IN + 1 + LOCK_WAIT))
    );
    assert_eq!(
        stored_drop(&app.pool, &aggregate_id).await,
        ("live".to_owned(), 2)
    );
    assert_eq!(drop_started_events(&app.pool).await, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_read_that_waits_on_the_lock_stamps_the_transition_after_the_wait(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, None).await;
    app.clock.advance_seconds(STARTS_IN + 1);

    let (holder, _, read) =
        spawn_read_waiting_on_the_row_lock(&app, &aggregate_id, public_drop_uri(&seller.pubky))
            .await;
    app.clock.advance_seconds(LOCK_WAIT);
    // The holder applies nothing, so the read applies the start itself.
    holder.rollback().await.expect("holder rolls back");

    let (status, _, body) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .expect("the read finishes once the holder rolls back")
        .expect("read task runs");
    let after_wait = ts_after(STARTS_IN + 1 + LOCK_WAIT);
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));
    assert_eq!(body["drop"]["server_time"], json!(after_wait));
    assert_eq!(
        stored_drop(&app.pool, &aggregate_id).await,
        ("live".to_owned(), 2)
    );
    assert_eq!(drop_started_events(&app.pool).await, 1);
    let (occurred_at, updated_at) = start_event_and_row_times(&app.pool, &aggregate_id).await;
    assert_eq!(format_timestamp(occurred_at), after_wait);
    assert_eq!(format_timestamp(updated_at), after_wait);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_public_read_takes_the_server_time_after_waiting_for_a_connection(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver, None).await;

    // The router shares this pool. Holding every connection makes the read
    // wait in the pool before its unlocked SELECT.
    let pool_size = app.pool.options().get_max_connections();
    let mut held = Vec::new();
    for _ in 0..pool_size {
        held.push(app.pool.acquire().await.expect("hold a pool connection"));
    }
    assert_eq!(app.pool.num_idle(), 0, "every connection is held");

    let router = app.router.clone();
    let uri = public_drop_uri(&seller.pubky);
    let read = tokio::spawn(async move { public_drop(router, &uri).await });
    // The test runtime is single-threaded, so the read runs only while this
    // task yields. It needs no I/O before the pool wait, so a few turns let
    // it reach that wait.
    for _ in 0..SCHEDULER_TURNS {
        tokio::task::yield_now().await;
    }
    assert!(!read.is_finished(), "the read waits for a pool connection");

    app.clock.advance_seconds(CONNECTION_WAIT);
    drop(held.pop());
    let (status, _, body) = tokio::time::timeout(READ_TIMEOUT, read)
        .await
        .expect("the read finishes once a connection is free")
        .expect("read task runs");
    drop(held);

    // Nothing is due, so the read takes no lock; its server time is the
    // instant it got the connection, not the instant it started waiting.
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("announced"));
    assert_eq!(
        body["drop"]["server_time"],
        json!(ts_after(CONNECTION_WAIT))
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_drop_read_has_short_public_caching_and_a_miss_is_not_cached(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver, None).await;

    let (status, headers, body) =
        public_drop(app.router.clone(), &public_drop_uri(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(headers["cache-control"], "public, max-age=1");

    let (status, headers, body) = public_drop(
        app.router.clone(),
        &format!("/v0/drops/{}/no_such_drop", seller.pubky),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(headers["cache-control"], "no-store");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_reads_past_ends_at_close_the_drop_once(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, Some(ENDS_IN)).await;
    let (state_before, revision_before) = stored_drop(&app.pool, &aggregate_id).await;
    assert_eq!(state_before, "announced");
    // The whole window elapses with no command or sweep touching the drop,
    // so the due transition goes straight from announced to ended_closed.
    app.clock.advance_seconds(ENDS_IN + 1);
    let now = app.clock.now();

    let uri = public_drop_uri(&seller.pubky);
    let mut reads = JoinSet::new();
    for _ in 0..CONCURRENT_READS {
        let router = app.router.clone();
        let uri = uri.clone();
        reads.spawn(async move { public_drop(router, &uri).await });
    }
    let (responses, swept) = tokio::time::timeout(READ_TIMEOUT, async {
        tokio::join!(
            async {
                let mut responses = Vec::new();
                while let Some(read) = reads.join_next().await {
                    responses.push(read.expect("read task runs"));
                }
                responses
            },
            transition_due_drops(&app.pool, now)
        )
    })
    .await
    .expect("the reads and the sweep all finish");
    swept.expect("the sweep succeeds");

    assert_eq!(responses.len(), CONCURRENT_READS);
    for (status, headers, body) in responses {
        assert_eq!(status, StatusCode::OK, "public read failed: {body}");
        assert_eq!(body["drop"]["state"], json!("ended_closed"));
        assert_eq!(body["drop"]["revision"], json!(revision_before + 1));
        assert_eq!(headers["cache-control"], "public, max-age=1");
    }

    let (state_after, revision_after) = stored_drop(&app.pool, &aggregate_id).await;
    assert_eq!(state_after, "ended_closed");
    assert_eq!(revision_after, revision_before + 1);
    assert_eq!(drop_ended_events(&app.pool).await, 1);
    assert_eq!(drop_started_events(&app.pool).await, 0);
    let (bindings, active_bindings) = binding_counts(&app.pool, &aggregate_id).await;
    assert_eq!(bindings, 1, "the drop binds boots_01");
    assert_eq!(active_bindings, 0, "a closed drop deactivates its bindings");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_public_read_alone_closes_a_live_drop_past_ends_at(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver, Some(ENDS_IN)).await;
    let uri = public_drop_uri(&seller.pubky);
    app.clock.advance_seconds(STARTS_IN + 1);

    let (status, _, body) = public_drop(app.router.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));

    // Live with ends_at still ahead has nothing due, so a read does not
    // touch the row lock.
    let (status, _, body) = public_drop_while_row_is_locked(&app, &aggregate_id, &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("live"));
    assert_eq!(body["drop"]["revision"], json!(2));

    // No sweep runs: the read past ends_at applies the close itself.
    app.clock.advance_seconds(ENDS_IN - STARTS_IN);
    let (status, headers, body) = public_drop(app.router.clone(), &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("ended_closed"));
    assert_eq!(body["drop"]["revision"], json!(3));
    assert_eq!(headers["cache-control"], "public, max-age=1");

    assert_eq!(
        stored_drop(&app.pool, &aggregate_id).await,
        ("ended_closed".to_owned(), 3)
    );
    assert_eq!(drop_started_events(&app.pool).await, 1);
    assert_eq!(drop_ended_events(&app.pool).await, 1);
    let (bindings, active_bindings) = binding_counts(&app.pool, &aggregate_id).await;
    assert_eq!(bindings, 1, "the drop binds boots_01");
    assert_eq!(active_bindings, 0, "a closed drop deactivates its bindings");

    // A closed drop has nothing due, so a later read does not touch the row
    // lock either.
    let (status, _, body) = public_drop_while_row_is_locked(&app, &aggregate_id, &uri).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("ended_closed"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_failed_public_drop_read_is_not_cached(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver, None).await;
    // The app shares this pool, so once it is closed the read cannot get a
    // connection and fails.
    app.pool.close().await;

    let (status, headers, body) =
        public_drop(app.router.clone(), &public_drop_uri(&seller.pubky)).await;
    assert_eq!(
        status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected: {body}"
    );
    assert_eq!(body["error"]["code"], json!("INTERNAL"));
    assert_eq!(headers["cache-control"], "no-store");
}
