//! Migration 0061: per-currency FX reference samples and the recorded USD
//! conversion of a non-USD price. Directly rerunnable, with every shape
//! checked in the database.

mod common;

use common::*;
use sqlx::PgPool;
use uuid::Uuid;

const MIGRATION_0061: &str = include_str!("../migrations/0061_multi_currency_fx.sql");

async fn update(pool: &PgPool, order_id: Uuid, set: &str) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("UPDATE orders SET {set} WHERE id = $1"))
        .bind(order_id)
        .execute(pool)
        .await
        .map(|_| ())
}

const EUR_CONVERSION: &str =
    "usd_conversion_pair = 'EURUSD', usd_conversion_source = 'blocktank', \
     usd_conversion_usd_rate = 77197, usd_conversion_currency_rate = 66481, \
     usd_conversion_fetched_at = now(), usd_conversion_cents = 15909";

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_migration_is_rerunnable_and_keeps_the_existing_rows(pool: PgPool) {
    sqlx::query(
        "INSERT INTO fx_rate_samples (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
         VALUES ('USD', 77197, now(), now(), to_timestamp(1800000000), 'blocktank')",
    )
    .execute(&pool)
    .await
    .expect("a USD sample");
    sqlx::raw_sql(MIGRATION_0061)
        .execute(&pool)
        .await
        .expect("0061 must be directly rerunnable");
    let samples: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM fx_rate_samples")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(samples, 1);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_usd_conversion_is_all_or_nothing_and_matches_the_order_currency(pool: PgPool) {
    let (app, _paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");

    assert!(
        update(&pool, order_id, EUR_CONVERSION).await.is_err(),
        "a USD order cannot carry a EURUSD conversion"
    );
    update(&pool, order_id, "currency = 'EUR'")
        .await
        .expect("order currency");
    update(&pool, order_id, EUR_CONVERSION)
        .await
        .expect("a EUR order records its USD conversion");

    for (what, set) in [
        ("another pair", "usd_conversion_pair = 'CHFUSD'"),
        ("USDUSD", "usd_conversion_pair = 'USDUSD'"),
        ("another source", "usd_conversion_source = 'kraken'"),
        ("a zero USD rate", "usd_conversion_usd_rate = 0"),
        ("a negative rate", "usd_conversion_currency_rate = -1"),
        ("zero cents", "usd_conversion_cents = 0"),
        ("no time", "usd_conversion_fetched_at = NULL"),
        ("a missing pair", "usd_conversion_pair = NULL"),
        ("a missing source", "usd_conversion_source = NULL"),
        ("missing cents", "usd_conversion_cents = NULL"),
    ] {
        assert!(update(&pool, order_id, set).await.is_err(), "{what}");
    }
    update(
        &pool,
        order_id,
        "usd_conversion_pair = NULL, usd_conversion_source = NULL, usd_conversion_usd_rate = NULL, \
         usd_conversion_currency_rate = NULL, usd_conversion_fetched_at = NULL, \
         usd_conversion_cents = NULL",
    )
    .await
    .expect("all six cleared together");
    update(&pool, order_id, "currency = 'USD'")
        .await
        .expect("an order without a conversion has no constraint on its currency");
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_released_attempt_keeps_its_conversion_with_the_same_shape_rules(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    common::paykit_review::enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");

    let insert = |conversion: &str| {
        let pool = pool.clone();
        let statement = format!(
            "INSERT INTO paykit_superseded_attempts (order_id, invoice_id, paykit_api, \
             stack_id, stack_endpoint, total_sats, released_at, usd_conversion_pair, \
             usd_conversion_source, usd_conversion_usd_rate, usd_conversion_currency_rate, \
             usd_conversion_fetched_at, usd_conversion_cents) \
             VALUES ($1, $2, 'fork', 'stack', 'http://stack.test', 1000, now(), {conversion})"
        );
        async move {
            sqlx::query(&statement)
                .bind(order_id)
                .bind(Uuid::new_v4())
                .execute(&pool)
                .await
        }
    };
    insert("'EURUSD', 'blocktank', 77197, 66481, now(), 15909")
        .await
        .expect("a released attempt records its conversion");
    insert("NULL, NULL, NULL, NULL, NULL, NULL")
        .await
        .expect("or none");
    for bad in [
        "'USDUSD', 'blocktank', 77197, 66481, now(), 15909",
        "'eurusd', 'blocktank', 77197, 66481, now(), 15909",
        "'EURUSD', 'other', 77197, 66481, now(), 15909",
        "'EURUSD', 'blocktank', 0, 66481, now(), 15909",
        "'EURUSD', 'blocktank', 77197, 66481, now(), 0",
        "'EURUSD', 'blocktank', 77197, 66481, NULL, 15909",
        "'EURUSD', NULL, NULL, NULL, NULL, NULL",
    ] {
        assert!(insert(bad).await.is_err(), "{bad}");
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_bitcoin_quote_constraint_still_ties_a_quote_to_the_orders_currency(pool: PgPool) {
    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    common::paykit_review::enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");
    let quote = "bitcoin_quote_rate = 66481, bitcoin_quote_source = 'blocktank', \
         bitcoin_quote_fetched_at = now(), bitcoin_quoted_sats = 206074, \
         bitcoin_quote_expires_at = paykit_expires_at, bitcoin_quote_currency = 'EUR', \
         bitcoin_quote_exponent = 2, bitcoin_quote_spread_bps = 0, \
         paykit_expires_at = now() + interval '1 hour'";
    assert!(
        update(&pool, order_id, quote).await.is_err(),
        "a EUR quote on a USD order violates orders_fx_quote_check"
    );
    update(&pool, order_id, "currency = 'EUR'").await.unwrap();
    update(&pool, order_id, quote)
        .await
        .expect("a EUR quote on a EUR order is recorded");
}
