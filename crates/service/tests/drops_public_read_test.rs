//! The public drop read (`GET /v0/drops/{seller_pubky}/{drop_id}`): it takes
//! no row lock unless a scheduled transition is due, so steady-state reads no
//! longer queue behind checkout on the same drop. When a transition is due it
//! locks the drop row and applies it once, however many reads and sweeps meet
//! at the boundary. A found drop carries short public caching; a miss is
//! never cached.

mod common;

use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use marketplace_service::clock::Clock;
use marketplace_service::workers::transition_due_drops;
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::task::JoinSet;

use common::{
    count, drop_aggregate, drop_record_json, execute, new_actor, register_command,
    sync_drop_command, test_app_with_homeserver, ts_after, TestActor, TestApp,
};

const DROP_ID: &str = "winter_drop";
/// Seconds after the fixture instant at which the drop starts.
const STARTS_IN: i64 = 600;
/// Upper bound for one read. A read stuck behind a row lock never returns,
/// so a bounded wait turns that into a failure instead of a hung test.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Concurrent reads at the start boundary, below the per-test pool size of 5.
const CONCURRENT_READS: usize = 4;

fn public_drop_uri(seller_pubky: &str) -> String {
    format!("/v0/drops/{seller_pubky}/{DROP_ID}")
}

async fn public_drop(app: &TestApp, uri: &str) -> (StatusCode, HeaderMap, Value) {
    common::send_with_headers(app.router.clone(), "GET", uri, None, &json!(null)).await
}

/// Registers the `boots_01` listing and syncs one announced drop over it that
/// starts [`STARTS_IN`] seconds after the fixture instant. Returns the seller
/// and the drop aggregate id.
async fn announced_drop(app: &TestApp, homeserver: &common::FakeHomeserver) -> (TestActor, String) {
    let seller = new_actor(app).await;
    homeserver.publish_registration(&register_command(&seller.pubky, 5));
    let (status, body) = execute(app, &seller.token, &register_command(&seller.pubky, 5)).await;
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
            None,
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

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_drop_read_takes_no_row_lock_when_nothing_is_due(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, aggregate_id) = announced_drop(&app, &homeserver).await;

    // Another transaction holds the drop row, as a checkout on this drop
    // would.
    let mut holder = app.pool.begin().await.expect("holder begins");
    sqlx::query("SELECT 1 FROM drops WHERE aggregate_id = $1 FOR UPDATE")
        .bind(&aggregate_id)
        .execute(&mut *holder)
        .await
        .expect("holder locks the drop row");

    let read = tokio::time::timeout(
        READ_TIMEOUT,
        public_drop(&app, &public_drop_uri(&seller.pubky)),
    )
    .await;
    holder.rollback().await.expect("holder rolls back");

    let (status, _, body) = read.expect("the read does not wait for the drop row lock");
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(body["drop"]["state"], json!("announced"));
    assert_eq!(body["drop"]["revision"], json!(1));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn concurrent_public_reads_at_starts_at_record_one_start(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver).await;
    app.clock.advance_seconds(STARTS_IN + 1);

    let uri = public_drop_uri(&seller.pubky);
    let mut reads = JoinSet::new();
    for _ in 0..CONCURRENT_READS {
        let router = app.router.clone();
        let uri = uri.clone();
        reads.spawn(async move {
            tokio::time::timeout(
                READ_TIMEOUT,
                common::send_with_headers(router, "GET", &uri, None, &json!(null)),
            )
            .await
        });
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
    let (seller, aggregate_id) = announced_drop(&app, &homeserver).await;
    app.clock.advance_seconds(STARTS_IN + 1);
    let now = app.clock.now();

    let uri = public_drop_uri(&seller.pubky);
    let ((status, _, body), swept) = tokio::time::timeout(READ_TIMEOUT, async {
        tokio::join!(
            public_drop(&app, &uri),
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
    let (state, revision): (String, i64) =
        sqlx::query_as("SELECT state, revision FROM drops WHERE aggregate_id = $1")
            .bind(&aggregate_id)
            .fetch_one(&app.pool)
            .await
            .expect("drop row exists");
    assert_eq!((state.as_str(), revision), ("live", 2));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn public_drop_read_has_short_public_caching_and_a_miss_is_not_cached(pool: PgPool) {
    let (app, homeserver) = test_app_with_homeserver(pool).await;
    let (seller, _) = announced_drop(&app, &homeserver).await;

    let (status, headers, body) = public_drop(&app, &public_drop_uri(&seller.pubky)).await;
    assert_eq!(status, StatusCode::OK, "public read failed: {body}");
    assert_eq!(headers["cache-control"], "public, max-age=1");

    let (status, headers, body) =
        public_drop(&app, &format!("/v0/drops/{}/no_such_drop", seller.pubky)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unexpected: {body}");
    assert_eq!(body["error"]["code"], json!("NOT_FOUND"));
    assert_eq!(headers["cache-control"], "no-store");
}
