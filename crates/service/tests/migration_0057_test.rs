//! 0057: sealed delivery addresses. An order holds the 2,096-byte envelope
//! and its key id together or not at all, never beside a plaintext address,
//! and the file removes the plaintext address from every order already in a
//! final state. Rerunning it changes nothing further.

use sqlx::PgPool;
use uuid::Uuid;

const MIGRATION: &str = include_str!("../migrations/0057_encrypted_delivery_address.sql");
const FINAL_STATES: [&str; 4] = ["completed", "cancelled", "refunded_external", "closed"];
const OPEN_STATES: [&str; 4] = ["pending_payment", "paid", "processing", "shipped"];

async fn seed_order(pool: &PgPool, state: &str, address: Option<&str>) -> Uuid {
    let order_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, \
         exponent, guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 1, $2, '[]', $3::jsonb, 100, 10, 110, 'EUR', \
         2, 1, $4, now(), now())",
    )
    .bind(order_id)
    .bind(state)
    .bind(address)
    .bind(Uuid::new_v4())
    .execute(pool)
    .await
    .unwrap_or_else(|error| panic!("seed {state} order: {error}"));
    order_id
}

async fn address_is_null(pool: &PgPool, order_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT delivery_address IS NULL FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order row")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0057_removes_plaintext_from_final_orders_only(pool: PgPool) {
    let address = r#"{"line1":"1 Market Street"}"#;
    let mut finals = Vec::new();
    for state in FINAL_STATES {
        finals.push(seed_order(&pool, state, Some(address)).await);
    }
    let mut opens = Vec::new();
    for state in OPEN_STATES {
        opens.push(seed_order(&pool, state, Some(address)).await);
    }
    let pickup = seed_order(&pool, "completed", None).await;

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0057 must be directly rerunnable");

    for order_id in finals {
        assert!(
            address_is_null(&pool, order_id).await,
            "a final order must lose its plaintext address"
        );
    }
    for order_id in opens {
        assert!(
            !address_is_null(&pool, order_id).await,
            "an open order keeps its address until it ends"
        );
    }
    assert!(address_is_null(&pool, pickup).await);

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("a second rerun");
}
