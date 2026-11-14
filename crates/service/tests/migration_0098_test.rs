//! Migration 0098: a seller confirmation may rest on the seller's wallet
//! Receipt (`confirmation_basis = 'seller_wallet_receipt'`), and then, and
//! only then, records that Receipt (`wallet_receipt`). The file reruns.
//! Rows of both kinds are written by `seller_wallet_receipt_test`.

use sqlx::PgPool;

const MIGRATION_0098: &str =
    include_str!("../migrations/0098_seller_wallet_receipt_confirmation.sql");

async fn constraint(pool: &PgPool, name: &str) -> String {
    sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid = 'paykit_seller_confirmations'::regclass AND conname = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect(name)
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_basis_allows_the_wallet_receipt_and_pairs_it_with_the_receipt(pool: PgPool) {
    for _ in 0..2 {
        let basis = constraint(
            &pool,
            "paykit_seller_confirmations_confirmation_basis_check",
        )
        .await;
        assert!(basis.contains("'seller_attestation'"), "{basis}");
        assert!(basis.contains("'seller_wallet_receipt'"), "{basis}");
        let pair = constraint(&pool, "paykit_seller_confirmations_wallet_receipt_basis").await;
        assert!(pair.contains("wallet_receipt IS NOT NULL"), "{pair}");
        let mut tx = pool.begin().await.expect("tx");
        sqlx::raw_sql(MIGRATION_0098)
            .execute(&mut *tx)
            .await
            .expect("0098 must be directly rerunnable");
        tx.commit().await.expect("commit");
    }
}
