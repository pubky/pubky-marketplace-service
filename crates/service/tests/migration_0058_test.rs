use sqlx::PgPool;

const PURGE: &str = include_str!("../migrations/0058_purge_retired_stripe_values.sql");

async fn insert_config(
    pool: &PgPool,
    seller_pubky: &str,
    link: Option<&str>,
    ciphertext: Option<&[u8]>,
    paypal: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO seller_payment_configs (seller_pubky, bitcoin_enabled, stripe_payment_link, \
         stripe_restricted_key_ciphertext, paypal_merchant_email, created_at, updated_at) \
         VALUES ($1, TRUE, $2, $3, $4, '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z')",
    )
    .bind(seller_pubky)
    .bind(link)
    .bind(ciphertext)
    .bind(paypal)
    .execute(pool)
    .await
    .expect("config seeds");
}

#[test]
fn the_purge_is_one_statement() {
    let code = PURGE
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let statements: Vec<&str> = code
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .collect();
    assert_eq!(statements.len(), 1);
    assert!(statements[0].starts_with("UPDATE seller_payment_configs"));
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0058_clears_stripe_values_and_is_rerunnable(pool: PgPool) {
    insert_config(
        &pool,
        "both",
        Some("https://buy.stripe.com/test_abc123"),
        Some(&[1, 2, 3]),
        Some("both@example.com"),
    )
    .await;
    insert_config(
        &pool,
        "link-only",
        Some("https://buy.stripe.com/test_def456"),
        None,
        None,
    )
    .await;
    insert_config(
        &pool,
        "key-only",
        None,
        Some(&[4, 5]),
        Some("key@example.com"),
    )
    .await;
    insert_config(&pool, "paypal-only", None, None, Some("paypal@example.com")).await;

    let (updated_before,): (chrono::DateTime<chrono::Utc>,) =
        sqlx::query_as("SELECT updated_at FROM seller_payment_configs WHERE seller_pubky = 'both'")
            .fetch_one(&pool)
            .await
            .expect("row");

    let first = sqlx::raw_sql(PURGE)
        .execute(&pool)
        .await
        .expect("0058 applies to seeded rows");
    assert_eq!(first.rows_affected(), 3, "only rows holding a value change");

    let (remaining,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM seller_payment_configs \
         WHERE stripe_payment_link IS NOT NULL OR stripe_restricted_key_ciphertext IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(remaining, 0);

    let kept: Vec<(String, bool, Option<String>)> = sqlx::query_as(
        "SELECT seller_pubky, bitcoin_enabled, paypal_merchant_email \
         FROM seller_payment_configs ORDER BY seller_pubky",
    )
    .fetch_all(&pool)
    .await
    .expect("rows");
    assert_eq!(
        kept,
        vec![
            (
                "both".to_string(),
                true,
                Some("both@example.com".to_string())
            ),
            (
                "key-only".to_string(),
                true,
                Some("key@example.com".to_string())
            ),
            ("link-only".to_string(), true, None),
            (
                "paypal-only".to_string(),
                true,
                Some("paypal@example.com".to_string())
            ),
        ],
        "the purge leaves every other column alone"
    );
    let (updated_after,): (chrono::DateTime<chrono::Utc>,) =
        sqlx::query_as("SELECT updated_at FROM seller_payment_configs WHERE seller_pubky = 'both'")
            .fetch_one(&pool)
            .await
            .expect("row");
    assert_eq!(updated_after, updated_before, "updated_at is not touched");

    let second = sqlx::raw_sql(PURGE)
        .execute(&pool)
        .await
        .expect("0058 must be directly rerunnable");
    assert_eq!(second.rows_affected(), 0, "a rerun matches no row");
}
