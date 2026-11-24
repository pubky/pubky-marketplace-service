//! Migration 0099: per-seller inventory rate buckets and the
//! `session.create` automation class. Additive and directly rerunnable.

use sqlx::PgPool;

const MIGRATION: &str = include_str!("../migrations/0099_inventory_actor_rate_limits.sql");

const AUTOMATION_CLASSES: [&str; 8] = [
    "session.admin",
    "export.listings",
    "export.orders",
    "events.read",
    "listing.sync_many",
    "webhook.admin",
    "grant_session.mint",
    "session.create",
];

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0099_is_rerunnable_and_leaves_session_buckets_in_place(pool: PgPool) {
    let token_hash = vec![7u8; 32];
    sqlx::query(
        "INSERT INTO auth_sessions (token_hash, pubky, capabilities, created_at, expires_at) \
         VALUES ($1, 'existing', '/:rw', now(), now() + INTERVAL '1 day')",
    )
    .bind(&token_hash)
    .execute(&pool)
    .await
    .expect("existing session");
    sqlx::query(
        "INSERT INTO inventory_rate_limits (session_hash, endpoint_class, tokens, updated_at) \
         VALUES ($1, 'inventory.read', 17, now())",
    )
    .bind(&token_hash)
    .execute(&pool)
    .await
    .expect("existing per-session bucket");
    sqlx::query(
        "INSERT INTO inventory_actor_rate_limits (actor_pubky, endpoint_class, tokens, updated_at) \
         VALUES ('existing', 'inventory.adjust', 5, now())",
    )
    .execute(&pool)
    .await
    .expect("existing per-seller bucket");

    for _ in 0..2 {
        sqlx::raw_sql(MIGRATION)
            .execute(&pool)
            .await
            .expect("0099 must be directly rerunnable");
    }

    let old: f64 =
        sqlx::query_scalar("SELECT tokens FROM inventory_rate_limits WHERE session_hash = $1")
            .bind(&token_hash)
            .fetch_one(&pool)
            .await
            .expect("per-session bucket left in place");
    assert_eq!(old, 17.0);
    let new: f64 = sqlx::query_scalar(
        "SELECT tokens FROM inventory_actor_rate_limits WHERE actor_pubky = 'existing'",
    )
    .fetch_one(&pool)
    .await
    .expect("per-seller bucket survives a rerun");
    assert_eq!(new, 5.0);

    // The seller's budget is not tied to any one session.
    sqlx::query("DELETE FROM auth_sessions WHERE token_hash = $1")
        .bind(&token_hash)
        .execute(&pool)
        .await
        .expect("session delete");
    let kept: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_actor_rate_limits WHERE actor_pubky = 'existing'",
    )
    .fetch_one(&pool)
    .await
    .expect("per-seller count");
    assert_eq!(kept, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn automation_endpoint_classes_stay_closed(pool: PgPool) {
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0099 reruns");

    let bucket = "INSERT INTO automation_rate_limits \
                  (seller_pubky, endpoint_class, tokens, updated_at) VALUES ($1, $2, 1, now())";
    let pubky = "p".repeat(52);
    for class in AUTOMATION_CLASSES {
        sqlx::query(bucket)
            .bind(&pubky)
            .bind(class)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{class} must be admitted: {error}"));
    }
    for class in ["session.other", "session.create ", ""] {
        assert!(
            sqlx::query(bucket)
                .bind(&pubky)
                .bind(class)
                .execute(&pool)
                .await
                .is_err(),
            "{class:?} must be refused"
        );
    }

    let actor_bucket = "INSERT INTO inventory_actor_rate_limits \
                        (actor_pubky, endpoint_class, tokens, updated_at) VALUES ($1, $2, 1, now())";
    for class in ["inventory.read", "inventory.adjust"] {
        sqlx::query(actor_bucket)
            .bind(&pubky)
            .bind(class)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{class} must be admitted: {error}"));
    }
    assert!(sqlx::query(actor_bucket)
        .bind(&pubky)
        .bind("session.create")
        .execute(&pool)
        .await
        .is_err());

    let definition: String = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conname = 'automation_rate_limits_endpoint_class_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("one class constraint");
    for class in AUTOMATION_CLASSES {
        assert!(
            definition.contains(&format!("'{class}'")),
            "{class} missing from {definition}"
        );
    }
}
