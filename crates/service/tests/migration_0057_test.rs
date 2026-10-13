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

async fn total_balance_validated(pool: &PgPool) -> Option<bool> {
    sqlx::query_scalar(
        "SELECT convalidated FROM pg_constraint \
         WHERE conname = 'orders_total_balance' AND conrelid = 'orders'::regclass",
    )
    .fetch_optional(pool)
    .await
    .expect("orders_total_balance")
}

// Staging's Aug 2026 orders carry totals that include the tax 0018 dropped,
// so they break `orders_total_balance`, which has been NOT VALID since 0018.
// The sweep is an UPDATE, which re-checks that constraint on every row it
// touches, so without the drop and re-add the first deploy of 0057 on staging
// rolled back with `orders_total_balance` violated.
#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0057_sweeps_final_orders_that_break_the_total_balance(pool: PgPool) {
    let address = r#"{"line1":"1 Market Street"}"#;
    let cancelled = seed_order(&pool, "cancelled", Some(address)).await;
    let open = seed_order(&pool, "paid", Some(address)).await;

    sqlx::raw_sql(
        "ALTER TABLE orders DROP CONSTRAINT orders_total_balance; \
         UPDATE orders SET total_minor = 125 WHERE id IN (SELECT id FROM orders); \
         ALTER TABLE orders ADD CONSTRAINT orders_total_balance \
         CHECK (total_minor = subtotal_minor + shipping_minor) NOT VALID",
    )
    .execute(&pool)
    .await
    .expect("seed orders that predate the narrowed total check");

    let unfixed = sqlx::raw_sql(
        "UPDATE orders SET delivery_address = NULL WHERE id IN (SELECT id FROM orders)",
    )
    .execute(&pool)
    .await;
    assert!(
        unfixed.is_err(),
        "the seeded rows must trip orders_total_balance on UPDATE, or this test proves nothing"
    );

    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("0057 applies over orders that break orders_total_balance");

    assert!(
        address_is_null(&pool, cancelled).await,
        "the unbalanced final order still loses its plaintext address"
    );
    assert!(
        !address_is_null(&pool, open).await,
        "an open order keeps its address until it ends"
    );
    let totals: Vec<i64> = sqlx::query_scalar("SELECT total_minor FROM orders ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("totals");
    assert!(
        totals.iter().all(|total| *total == 125),
        "what buyers paid is not rewritten"
    );
    assert_eq!(
        total_balance_validated(&pool).await,
        Some(false),
        "orders_total_balance is back, NOT VALID"
    );
    let new_unbalanced = sqlx::raw_sql(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, created_at, updated_at) \
         VALUES (gen_random_uuid(), 'buyer', 'seller', 1, 'paid', '[]', 100, 10, 125, \
         'EUR', 2, 1, gen_random_uuid(), now(), now())",
    )
    .execute(&pool)
    .await;
    assert!(
        new_unbalanced.is_err(),
        "the balance is still enforced for every new order"
    );
}
