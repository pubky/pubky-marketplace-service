//! 0099: event cursors name the homeserver they count on. Rows that predate
//! the file keep NULL (the configured homeserver) across a rerun; a follower
//! cursor never moves backwards on one homeserver and restarts at any cursor
//! on another; a revival records the superseded delete with its homeserver,
//! whose marker changes only with a revival; a delete older than the revival
//! marker is refused only on the marker's homeserver; and the documented
//! back-out restarts every cursor on the configured homeserver and clears the
//! listing markers that name another one, leaving the guard enabled. The follower
//! across a real homeserver move is exercised in `homeserver_pkarr_test.rs`.

use sqlx::PgPool;
use uuid::Uuid;

const AGGREGATE: &str = "listing:seller_boots_01";
const SELLER: &str = "seller";

fn homeserver(letter: char) -> String {
    letter.to_string().repeat(52)
}

async fn epoch(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT record_epoch FROM listings WHERE aggregate_id = $1")
        .bind(AGGREGATE)
        .fetch_one(pool)
        .await
        .expect("record epoch")
}

/// A tombstone at the current epoch, as the service writes it.
async fn tombstone(pool: &PgPool, cursor: &str, homeserver: Option<&str>) -> Result<(), String> {
    let observed = epoch(pool).await;
    let mut tx = pool.begin().await.expect("tombstone tx");
    sqlx::query("SELECT set_config('marketplace.listing_deletion_authority', 'command', true)")
        .execute(&mut *tx)
        .await
        .expect("authority");
    sqlx::query("SELECT set_config('marketplace.listing_deletion_observed_epoch', $1, true)")
        .bind(observed.to_string())
        .execute(&mut *tx)
        .await
        .expect("observed epoch");
    let written = sqlx::query(
        "UPDATE listings SET deleted_at = now(), deleted_event_cursor = $2, \
         deleted_event_homeserver = $3 WHERE aggregate_id = $1 AND deleted_at IS NULL",
    )
    .bind(AGGREGATE)
    .bind(cursor)
    .bind(homeserver)
    .execute(&mut *tx)
    .await;
    match written {
        Ok(_) => {
            tx.commit().await.expect("tombstone commits");
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

/// The re-registration that revives a tombstoned listing.
async fn revive(pool: &PgPool) {
    sqlx::query(
        "UPDATE listings SET deleted_at = NULL, deleted_event_cursor = NULL, \
         generation = generation + 1 WHERE aggregate_id = $1",
    )
    .bind(AGGREGATE)
    .execute(pool)
    .await
    .expect("revival");
}

type ListingMarkers = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// `deleted_event_cursor`, `deleted_event_homeserver`, `revived_from_cursor`
/// and `revived_from_homeserver`.
async fn markers(pool: &PgPool) -> ListingMarkers {
    sqlx::query_as(
        "SELECT deleted_event_cursor, deleted_event_homeserver, revived_from_cursor, \
         revived_from_homeserver FROM listings WHERE aggregate_id = $1",
    )
    .bind(AGGREGATE)
    .fetch_one(pool)
    .await
    .expect("listing markers")
}

/// A follower cursor write under the current lease.
async fn write_cursor(
    pool: &PgPool,
    authority: &str,
    cursor: Option<&str>,
    homeserver: Option<&str>,
) -> Result<(), String> {
    let mut tx = pool.begin().await.expect("cursor tx");
    sqlx::query("SELECT set_config('marketplace.listing_deletion_authority', $1, true)")
        .bind(authority)
        .execute(&mut *tx)
        .await
        .expect("authority");
    let written = sqlx::query(
        "INSERT INTO listing_deletion_cursors (seller_pubky, event_cursor, homeserver, polled_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (seller_pubky) DO UPDATE SET \
             event_cursor = EXCLUDED.event_cursor, homeserver = EXCLUDED.homeserver",
    )
    .bind(SELLER)
    .bind(cursor)
    .bind(homeserver)
    .execute(&mut *tx)
    .await;
    match written {
        Ok(_) => {
            tx.commit().await.expect("cursor commits");
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

async fn stored_cursor(pool: &PgPool) -> (Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT event_cursor, homeserver FROM listing_deletion_cursors WHERE seller_pubky = $1",
    )
    .bind(SELLER)
    .fetch_one(pool)
    .await
    .expect("cursor row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0099_scopes_deletion_cursors_to_their_homeserver_and_is_rerunnable(
    pool: PgPool,
) {
    let a = homeserver('a');
    let b = homeserver('b');
    sqlx::query(
        "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, listing_revision, \
         content_hash, server_revision, state, total_quantity, available_quantity, \
         reserved_quantity, sold_quantity, unit_price_amount_minor, unit_price_currency, \
         unit_price_exponent, shipping_minor, sale_format, fulfillment_methods, updated_at) \
         VALUES ($1, $2, 'boots_01', 'Winter boots', 1, repeat('a', 64), 1, 'available', \
         1, 1, 0, 0, 12500, 'USD', 2, 0, 'fixed_price', ARRAY['shipping'], now())",
    )
    .bind(AGGREGATE)
    .bind(SELLER)
    .execute(&pool)
    .await
    .expect("listing row");
    let holder = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_leases (task, holder, lease_until, fence) \
         VALUES ('listing_deletions', $1, now() + interval '1 minute', 1)",
    )
    .bind(holder)
    .execute(&pool)
    .await
    .expect("follower lease");
    let follower = format!("follower:{holder}:1");

    // Rows as a binary from before 0099 writes them: no homeserver.
    write_cursor(&pool, &follower, Some("500"), None)
        .await
        .expect("a cursor on the configured homeserver");
    tombstone(&pool, "500", None)
        .await
        .expect("a tombstone on the configured homeserver");
    revive(&pool).await;
    assert_eq!(
        markers(&pool).await,
        (None, None, Some("500".to_string()), None),
        "a revival on the configured homeserver records the delete as before"
    );

    sqlx::raw_sql(include_str!(
        "../migrations/0099_homeserver_scoped_deletion_cursors.sql"
    ))
    .execute(&pool)
    .await
    .expect("0099 must be directly rerunnable");
    assert_eq!(stored_cursor(&pool).await, (Some("500".to_string()), None));
    assert_eq!(
        markers(&pool).await,
        (None, None, Some("500".to_string()), None)
    );
    let triggers: Vec<String> = sqlx::query_scalar(
        "SELECT tgname::text FROM pg_trigger WHERE NOT tgisinternal \
         AND tgname IN ('listings_deletion_guard', 'listing_deletion_cursors_guard') \
         ORDER BY tgname",
    )
    .fetch_all(&pool)
    .await
    .expect("guard triggers");
    assert_eq!(
        triggers,
        ["listing_deletion_cursors_guard", "listings_deletion_guard"],
        "a rerun keeps one of each guard"
    );
    let checks: Vec<String> = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint WHERE conname IN \
         ('listing_deletion_cursors_homeserver_check', 'listings_deleted_event_homeserver_check', \
          'listings_revived_from_homeserver_check') ORDER BY conname",
    )
    .fetch_all(&pool)
    .await
    .expect("homeserver checks");
    assert_eq!(
        checks,
        [
            "listing_deletion_cursors_homeserver_check",
            "listings_deleted_event_homeserver_check",
            "listings_revived_from_homeserver_check",
        ],
        "a rerun keeps one of each check"
    );

    // The follower cursor: monotonic on one homeserver, restarted on another.
    let refused = write_cursor(&pool, &follower, Some("400"), None)
        .await
        .expect_err("backwards on the configured homeserver");
    assert!(refused.contains("would move backwards"), "{refused}");
    write_cursor(&pool, &follower, Some("3"), Some(&a))
        .await
        .expect("a lower cursor on another homeserver");
    let refused = write_cursor(&pool, &follower, Some("2"), Some(&a))
        .await
        .expect_err("backwards on that homeserver");
    assert!(refused.contains("would move backwards"), "{refused}");
    write_cursor(&pool, &follower, None, Some(&b))
        .await
        .expect("a restart on a third homeserver");
    write_cursor(&pool, &follower, Some("7"), Some(&b))
        .await
        .expect("forward on it");
    assert_eq!(
        stored_cursor(&pool).await,
        (Some("7".to_string()), Some(b.clone()))
    );
    let refused = write_cursor(&pool, &follower, Some("8"), Some("not-a-homeserver"))
        .await
        .expect_err("a homeserver that is not a pubky");
    assert!(
        refused.contains("listing_deletion_cursors_homeserver_check"),
        "{refused}"
    );
    let refused = write_cursor(&pool, "command", Some("8"), Some(&b))
        .await
        .expect_err("a cursor write without the follower lease");
    assert!(refused.contains("no current follower lease"), "{refused}");

    // The revival marker's homeserver moves only with a revival.
    let refused =
        sqlx::query("UPDATE listings SET revived_from_homeserver = $2 WHERE aggregate_id = $1")
            .bind(AGGREGATE)
            .bind(&a)
            .execute(&pool)
            .await
            .expect_err("a marker moved to another homeserver outside a revival")
            .to_string();
    assert!(
        refused.contains("its homeserver changes only with a revival"),
        "{refused}"
    );

    // The seller moved to `a`, whose stream is below the marker: its delete
    // retires the revival there; one older than the marker on the configured
    // homeserver is still refused.
    let refused = tombstone(&pool, "3", None)
        .await
        .expect_err("an older delete on the marker's homeserver");
    assert!(refused.contains("predates the revival"), "{refused}");
    tombstone(&pool, "3", Some(&a))
        .await
        .expect("a delete on another homeserver, below the marker");
    assert_eq!(
        markers(&pool).await,
        (
            Some("3".to_string()),
            Some(a.clone()),
            Some("500".to_string()),
            None
        )
    );

    // Reviving it records that delete, on its homeserver, as the marker.
    revive(&pool).await;
    assert_eq!(
        markers(&pool).await,
        (None, None, Some("3".to_string()), Some(a.clone())),
        "the marker moves to the superseded delete's homeserver"
    );
    let refused = tombstone(&pool, "2", Some(&a))
        .await
        .expect_err("an older delete on the marker's new homeserver");
    assert!(refused.contains("predates the revival"), "{refused}");
    let refused = tombstone(&pool, "4", Some("not-a-homeserver"))
        .await
        .expect_err("a tombstone naming no pubky");
    assert!(
        refused.contains("listings_deleted_event_homeserver_check"),
        "{refused}"
    );
    tombstone(&pool, "3", Some(&a))
        .await
        .expect("the superseded delete, at the current epoch");

    // Backing out after `pkarr` (docs/migrations.md): every cursor that names
    // a homeserver restarts on the configured one.
    sqlx::raw_sql(
        "BEGIN; \
         SELECT set_config('marketplace.listing_deletion_authority', \
             (SELECT 'follower:' || holder || ':' || fence \
              FROM worker_leases WHERE task = 'listing_deletions'), true); \
         UPDATE listing_deletion_cursors SET event_cursor = NULL, homeserver = NULL \
             WHERE homeserver IS NOT NULL; \
         COMMIT;",
    )
    .execute(&pool)
    .await
    .expect("the documented back-out restart");
    assert_eq!(stored_cursor(&pool).await, (None, None));

    // Then the listing markers that name a homeserver: kept aside, and
    // cleared with the guard disabled for the one transaction.
    assert_eq!(
        markers(&pool).await,
        (
            Some("3".to_string()),
            Some(a.clone()),
            Some("3".to_string()),
            Some(a.clone())
        )
    );
    sqlx::raw_sql(
        "BEGIN; \
         SET LOCAL lock_timeout = '10s'; \
         CREATE TABLE listing_homeserver_markers_0099 AS \
             SELECT aggregate_id, deleted_event_cursor, deleted_event_homeserver, \
                    revived_from_cursor, revived_from_homeserver \
             FROM listings \
             WHERE deleted_event_homeserver IS NOT NULL OR revived_from_homeserver IS NOT NULL; \
         ALTER TABLE listings DISABLE TRIGGER listings_deletion_guard; \
         UPDATE listings SET deleted_event_cursor = '0', deleted_event_homeserver = NULL \
             WHERE deleted_event_homeserver IS NOT NULL; \
         UPDATE listings SET revived_from_cursor = NULL, revived_from_homeserver = NULL \
             WHERE revived_from_homeserver IS NOT NULL; \
         ALTER TABLE listings ENABLE TRIGGER listings_deletion_guard; \
         COMMIT;",
    )
    .execute(&pool)
    .await
    .expect("the documented marker back-out");
    assert_eq!(
        markers(&pool).await,
        (Some("0".to_string()), None, None, None)
    );
    let kept: (String, String, String, String, String) = sqlx::query_as(
        "SELECT aggregate_id, deleted_event_cursor, deleted_event_homeserver, \
         revived_from_cursor, revived_from_homeserver FROM listing_homeserver_markers_0099",
    )
    .fetch_one(&pool)
    .await
    .expect("the kept markers");
    assert_eq!(
        kept,
        (
            AGGREGATE.to_string(),
            "3".to_string(),
            a.clone(),
            "3".to_string(),
            a.clone()
        )
    );
    let enabled: String = sqlx::query_scalar(
        "SELECT tgenabled::text FROM pg_trigger WHERE tgname = 'listings_deletion_guard'",
    )
    .fetch_one(&pool)
    .await
    .expect("guard trigger");
    assert_eq!(enabled, "O", "the guard is enabled again");

    // A binary from before 0099 revives and tombstones on the configured
    // homeserver, its own numbering, unhindered by the other homeserver's.
    revive(&pool).await;
    assert_eq!(
        markers(&pool).await,
        (None, None, Some("0".to_string()), None)
    );
    tombstone(&pool, "1", None)
        .await
        .expect("a delete on the configured homeserver after the back-out");
    let refused =
        sqlx::query("UPDATE listings SET revived_from_cursor = NULL WHERE aggregate_id = $1")
            .bind(AGGREGATE)
            .execute(&pool)
            .await
            .expect_err("the guard is back")
            .to_string();
    assert!(refused.contains("would move backwards"), "{refused}");
}
