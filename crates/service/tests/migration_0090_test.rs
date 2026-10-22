//! Migration 0090: the attestor publisher's two tables and the `returned`
//! annotation outcome, over a database that already holds annotations. Every
//! existing annotation survives and stays append-only, `returned` is accepted
//! only after the migration, any other outcome is still refused, and the file
//! reruns as a no-op that keeps what the publisher recorded.

use std::borrow::Cow;

use chrono::Utc;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0090: &str = include_str!("../migrations/0090_attestor_publisher.sql");

const ORDER_REF: &str = "5f1a3a1c2d0e4b8f9a7c6e5d4b3a29181716151413121110f0e0d0c0b0a09080";
const ATTESTOR: &str = "7jfgaa9nutjyixzikb7tgmsf9gkwq7iqz498zr1nd5ig1fng4esy";
const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            ALL_MIGRATIONS
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}

async fn annotate(pool: &PgPool, outcome: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO attestation_annotations (id, order_ref, outcome, reason, annotated_at) \
         VALUES ($1, $2, $3, NULL, $4)",
    )
    .bind(Uuid::new_v4())
    .bind(ORDER_REF)
    .bind(outcome)
    .bind(Utc::now())
    .execute(pool)
    .await
    .map(|_| ())
}

async fn outcomes(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT outcome FROM attestation_annotations ORDER BY outcome")
        .fetch_all(pool)
        .await
        .expect("outcomes")
}

fn constraint_violation(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|error| error.constraint().map(str::to_string))
}

#[sqlx::test(migrations = false)]
async fn returned_is_accepted_after_the_migration_and_existing_annotations_survive(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(89).run(&pool).await.expect("0001..0089 apply");

    annotate(&pool, "refunded").await.expect("refunded before");
    annotate(&pool, "refund_reversal_cancelled")
        .await
        .expect("reversal before");
    let refused = annotate(&pool, "returned")
        .await
        .expect_err("returned is not an outcome before 0090");
    assert_eq!(
        constraint_violation(&refused).as_deref(),
        Some("attestation_annotations_outcome_check")
    );

    ALL_MIGRATIONS.run(&pool).await.expect("0090 applies");

    assert_eq!(
        outcomes(&pool).await,
        vec!["refund_reversal_cancelled", "refunded"],
        "every existing annotation survives"
    );
    annotate(&pool, "returned").await.expect("returned after");
    let refused = annotate(&pool, "exchanged")
        .await
        .expect_err("any other outcome is still refused");
    assert_eq!(
        constraint_violation(&refused).as_deref(),
        Some("attestation_annotations_outcome_check")
    );
    let validated: bool = sqlx::query_scalar(
        "SELECT convalidated FROM pg_constraint \
         WHERE conname = 'attestation_annotations_outcome_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("constraint");
    assert!(validated, "the widened check is validated against the rows");

    let update = sqlx::query("UPDATE attestation_annotations SET outcome = 'returned'")
        .execute(&pool)
        .await;
    assert!(update.is_err(), "annotations stay append-only");
    let delete = sqlx::query("DELETE FROM attestation_annotations")
        .execute(&pool)
        .await;
    assert!(delete.is_err(), "annotations stay append-only");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_what_the_publisher_recorded(pool: PgPool) {
    sqlx::query(
        "INSERT INTO attestor_publications (attestor_pubky, path, revision, body_hash, published_at) \
         VALUES ($1, '/pub/pubky.app/marketplace/v1/attestor/profile', 2, $2, now())",
    )
    .bind(ATTESTOR)
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("publication");
    sqlx::query(
        "INSERT INTO attestor_publish_attempts \
         (attestor_pubky, path, revision, body_hash, attempts, next_attempt_at, last_error, updated_at) \
         VALUES ($1, '/pub/pubky.app/marketplace/v1/attestor/profile', 3, $2, 1, now(), 'down', now())",
    )
    .bind(ATTESTOR)
    .bind(HASH)
    .execute(&pool)
    .await
    .expect("attempt");
    annotate(&pool, "returned").await.expect("returned");

    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0090)
        .execute(&mut *tx)
        .await
        .expect("0090 must be directly rerunnable");
    tx.commit().await.expect("commit");

    let kept: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT revision FROM attestor_publications), \
                (SELECT revision FROM attestor_publish_attempts)",
    )
    .fetch_one(&pool)
    .await
    .expect("rows");
    assert_eq!(kept, (2, 3));
    assert_eq!(outcomes(&pool).await, vec!["returned"]);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_tables_refuse_an_unusable_row(pool: PgPool) {
    for table in ["attestor_publications", "attestor_publish_attempts"] {
        let columns = if table == "attestor_publications" {
            "(attestor_pubky, path, revision, body_hash, published_at)"
        } else {
            "(attestor_pubky, path, revision, body_hash, attempts, next_attempt_at, updated_at)"
        };
        let tail = if table == "attestor_publications" {
            "now()"
        } else {
            "1, now(), now()"
        };
        let insert = |revision: i64, hash: &str| {
            format!("INSERT INTO {table} {columns} VALUES ('{ATTESTOR}', '/p', {revision}, '{hash}', {tail})")
        };
        for (revision, hash) in [(0, HASH), (1, "ABC"), (1, &HASH.to_uppercase())] {
            sqlx::query(&insert(revision, hash))
                .execute(&pool)
                .await
                .expect_err(&format!(
                    "{table} refuses revision {revision} with hash {hash}"
                ));
        }
        sqlx::query(&insert(1, HASH))
            .execute(&pool)
            .await
            .expect("a usable row");
        sqlx::query(&insert(2, HASH))
            .execute(&pool)
            .await
            .expect_err(&format!("{table} holds one row per attestor and path"));
    }
}
