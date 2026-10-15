//! Migration 0060: the `paykit` payment method, the multi-asset request
//! terms, USDT finality, and the nullable satoshi total of a released
//! USDT-only attempt. Directly rerunnable, with the shapes checked in the
//! database.

mod common;

use common::paykit_review::{create_sat_order, enable_bitcoin};
use common::*;
use sqlx::PgPool;
use uuid::Uuid;

async fn update(pool: &PgPool, order_id: Uuid, set: &str) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("UPDATE orders SET {set} WHERE id = $1"))
        .bind(order_id)
        .execute(pool)
        .await
        .map(|_| ())
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0060_is_rerunnable_and_checks_the_new_shapes(pool: PgPool) {
    sqlx::raw_sql(include_str!("../migrations/0060_single_paykit_bind.sql"))
        .execute(&pool)
        .await
        .expect("0060 must be directly rerunnable");

    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_sat_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");

    update(&pool, order_id, "payment_method = 'paykit'")
        .await
        .expect("paykit is a payment method");
    for method in ["lightning", "PAYKIT", ""] {
        assert!(
            update(&pool, order_id, &format!("payment_method = '{method}'"))
                .await
                .is_err(),
            "{method:?} is not a payment method"
        );
    }

    let terms = |amount: &str, accepted: &str, amounts: &str| {
        format!(
            "paykit_amount_minor = {amount}, paykit_accepted_assets = {accepted}, \
             paykit_payment_amounts = {amounts}"
        )
    };
    update(
        &pool,
        order_id,
        &terms(
            "13700",
            "ARRAY['BTC','USDT']",
            "'{\"BTC\":1,\"USDT\":2}'::jsonb",
        ),
    )
    .await
    .expect("the three terms set together");
    for (set, why) in [
        (terms("13700", "NULL", "NULL"), "amount alone"),
        (terms("NULL", "ARRAY['BTC']", "NULL"), "assets alone"),
        (terms("NULL", "NULL", "'{}'::jsonb"), "amounts alone"),
        (
            terms("0", "ARRAY['BTC']", "'{\"BTC\":1}'::jsonb"),
            "a zero amount",
        ),
        (
            terms("-1", "ARRAY['BTC']", "'{\"BTC\":1}'::jsonb"),
            "a negative amount",
        ),
        (
            terms("1", "ARRAY[]::text[]", "'{}'::jsonb"),
            "an empty offer",
        ),
    ] {
        assert!(update(&pool, order_id, &set).await.is_err(), "{why}");
    }
    update(&pool, order_id, &terms("NULL", "NULL", "NULL"))
        .await
        .expect("the legacy request has none of them");

    update(
        &pool,
        order_id,
        "payment_finality = 'pending', payment_finality_at = now()",
    )
    .await
    .expect("pending");
    assert!(
        update(
            &pool,
            order_id,
            "payment_finality = 'done', payment_finality_at = now()"
        )
        .await
        .is_err(),
        "an unknown finality"
    );
    assert!(
        update(&pool, order_id, "payment_finality = NULL")
            .await
            .is_err(),
        "a time without finality"
    );
    update(
        &pool,
        order_id,
        "payment_finality = NULL, payment_finality_at = NULL",
    )
    .await
    .expect("neither");
    assert!(
        update(&pool, order_id, "payment_finality = 'final'")
            .await
            .is_err(),
        "finality without its time"
    );
    for finality in ["final", "reverted"] {
        update(
            &pool,
            order_id,
            &format!("payment_finality = '{finality}', payment_finality_at = now()"),
        )
        .await
        .unwrap_or_else(|error| panic!("{finality}: {error}"));
    }

    // A refund of a USDT-paid order is recorded in the paid asset's units, so
    // the refund bound admits the amount that was paid and no more.
    update(
        &pool,
        order_id,
        "payment_asset = 'USDT', payment_network = 'arbitrum-one', \
         payment_amount_minor = 40500000, payment_exponent = 6, \
         payment_quote_basis = 'fixed_rate'",
    )
    .await
    .expect("the paid rail");
    update(
        &pool,
        order_id,
        "external_refund = '{\"amount_minor\": 40500000}'::jsonb",
    )
    .await
    .expect("a refund up to the amount paid");
    assert!(
        update(
            &pool,
            order_id,
            "external_refund = '{\"amount_minor\": 40500001}'::jsonb"
        )
        .await
        .is_err(),
        "more than was paid"
    );
    update(&pool, order_id, "external_refund = NULL")
        .await
        .expect("cleared");

    // A released attempt that offered no Bitcoin has no satoshi total.
    let insert = |total: Option<i64>| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "INSERT INTO paykit_superseded_attempts \
                 (order_id, invoice_id, paykit_api, total_sats, released_at, payment_reference, \
                  operation_id, payment_window_seconds, asset) \
                 VALUES ($1, $2, 'upstream', $3, now(), $4, 'op', 1800, 'USD')",
            )
            .bind(order_id)
            .bind(Uuid::new_v4())
            .bind(total)
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
        }
    };
    insert(None)
        .await
        .expect("a USDT-only attempt has no total");
    insert(Some(5_000)).await.expect("a Bitcoin total");
    for bad in [0, -1] {
        assert!(insert(Some(bad)).await.is_err(), "total {bad}");
    }
}
