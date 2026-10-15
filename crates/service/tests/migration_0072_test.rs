//! Migration 0072: grant-session challenge records and the session source
//! columns. Additive, rerunnable, and invisible to existing session rows.

use sqlx::PgPool;

const MIGRATION: &str = include_str!("../migrations/0072_grant_sessions.sql");

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0072_is_rerunnable_and_leaves_existing_sessions_untouched(pool: PgPool) {
    sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, 'existing', '/:rw', now(), now() + INTERVAL '1 day')",
    )
    .bind(vec![1u8; 32])
    .execute(&pool)
    .await
    .expect("existing session");

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0072 must be directly rerunnable");

    let (grant_id, client_id): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT source_grant_id, source_client_id FROM auth_sessions WHERE pubky = 'existing'",
    )
    .fetch_one(&pool)
    .await
    .expect("existing session row");
    assert_eq!((grant_id, client_id), (None, None));

    let id = uuid::Uuid::new_v4();
    let insert = "INSERT INTO grant_session_challenges (challenge_id, pubky, grant_id, used_at) \
                  VALUES ($1, 'p', 'g', now()) ON CONFLICT DO NOTHING";
    let first = sqlx::query(insert)
        .bind(id)
        .execute(&pool)
        .await
        .expect("first use");
    let second = sqlx::query(insert)
        .bind(id)
        .execute(&pool)
        .await
        .expect("second use");
    assert_eq!((first.rows_affected(), second.rows_affected()), (1, 0));

    let bucket = "INSERT INTO automation_rate_limits \
                  (seller_pubky, endpoint_class, tokens, updated_at) VALUES ($1, $2, 1, now())";
    let pubky = "p".repeat(52);
    sqlx::query(bucket)
        .bind(&pubky)
        .bind("grant_session.mint")
        .execute(&pool)
        .await
        .expect("the grant-session class is admitted");
    sqlx::query(bucket)
        .bind(&pubky)
        .bind("session.admin")
        .execute(&pool)
        .await
        .expect("existing classes stay admitted");
    assert!(sqlx::query(bucket)
        .bind(&pubky)
        .bind("grant_session.other")
        .execute(&pool)
        .await
        .is_err());
}
