//! Migration 0103: removes static FX rate ceilings/floors and the 1.0 BTC
//! fiat-quote cap, and widens FX source constraints to the built-in
//! single-seller feed providers. Directly rerunnable.

mod common;

use common::*;
use marketplace_domain::currency::Currency;
use marketplace_service::fx::FxFeedProvider;
use sqlx::PgPool;
use uuid::Uuid;

const MIGRATION_0103: &str = include_str!("../migrations/0103_fx_remove_static_rate_ceilings.sql");

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0103_is_rerunnable_and_enforces_positive_finite_rates(pool: PgPool) {
    assert!(
        MIGRATION_0103.starts_with("-- no-transaction\n"),
        "0103 must run outside one implicit transaction so VALIDATE CONSTRAINT never holds ACCESS EXCLUSIVE"
    );
    sqlx::query(
        "INSERT INTO fx_rate_samples (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
         VALUES ('USD', 77197, now(), now(), to_timestamp(1800000000), 'blocktank')",
    )
    .execute(&pool)
    .await
    .expect("existing sample");

    sqlx::raw_sql(MIGRATION_0103)
        .execute(&pool)
        .await
        .expect("0103 must be directly rerunnable");

    // Every constraint updated by 0103 must be validated (convalidated = true).
    let unvalidated: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint \
         WHERE conname IN ( \
             'fx_rate_samples_rate_check', \
             'fx_rate_samples_source_check', \
             'orders_fx_quote_check', \
             'orders_usd_conversion_check', \
             'paykit_superseded_attempts_usd_conversion_check' \
         ) AND NOT convalidated",
    )
    .fetch_all(&pool)
    .await
    .expect("constraint catalog");
    assert!(
        unvalidated.is_empty(),
        "every 0103 constraint must be validated: {unvalidated:?}"
    );

    // 0102's store_seller_binding table remains intact alongside 0103.
    let binding_table_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
             SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name = 'store_seller_binding' \
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("binding table catalog");
    assert!(
        binding_table_exists,
        "0102 store_seller_binding table must coexist with 0103"
    );

    for (index, currency) in Currency::FIAT.into_iter().enumerate() {
        for (slot, rate) in [
            (1_i64, "0.00000001"),
            (2, "5000"),
            (3, "2500000"),
            (4, "500000000"),
        ] {
            sqlx::query(
                "INSERT INTO fx_rate_samples \
                 (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
                 VALUES ($1, $2::numeric, now(), now(), to_timestamp($3), 'blocktank')",
            )
            .bind(currency.code())
            .bind(rate)
            .bind(1_800_100_000 + index as i64 * 100 + slot)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("{currency} at {rate} must be admitted: {e}"));
        }
    }

    for (slot, bad_rate) in [
        (10_i64, "0"),
        (11, "-1"),
        (12, "NaN"),
        (13, "Infinity"),
        (14, "-Infinity"),
    ] {
        let rejected = sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', $1::numeric, now(), now(), to_timestamp($2), 'blocktank')",
        )
        .bind(bad_rate)
        .bind(1_800_200_000 + slot)
        .execute(&pool)
        .await;
        assert!(
            rejected.is_err(),
            "non-positive or non-finite rate {bad_rate} must be rejected"
        );
    }
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn migration_0103_admits_all_builtin_fx_providers_and_unbounded_quote_sats(pool: PgPool) {
    for (slot, provider) in FxFeedProvider::ALL.into_iter().enumerate() {
        sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', 77197, now(), now(), to_timestamp($1), $2)",
        )
        .bind(1_800_300_000 + slot as i64 * 60)
        .bind(provider.as_str())
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("provider {} must be admitted: {e}", provider.as_str()));
    }
    for bad_source in ["other", "", "BLOCKTANK"] {
        let rejected = sqlx::query(
            "INSERT INTO fx_rate_samples \
             (currency, rate, fetched_at, accepted_at, sample_bucket, source) \
             VALUES ('USD', 77197, now(), now(), to_timestamp(1800400000), $1)",
        )
        .bind(bad_source)
        .execute(&pool)
        .await;
        assert!(
            rejected.is_err(),
            "unknown fx source {bad_source:?} must be rejected"
        );
    }

    let (app, paykit) = test_app_with_payments(pool.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    common::paykit_review::enable_bitcoin(&app, &paykit, &seller).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    let order_id = Uuid::parse_str(&order.order_id).expect("order uuid");

    // Quotes below 1,000 sats and above 1.0 BTC (100,000,000 sats) are admitted
    // across every built-in provider.
    for (sats, provider) in [
        (1_i64, FxFeedProvider::Mempool),
        (500, FxFeedProvider::Kraken),
        (250_000_000, FxFeedProvider::Coinbase),
        (10_000_000_000, FxFeedProvider::Bitstamp),
    ] {
        sqlx::query(
            "UPDATE orders SET \
             paykit_expires_at = now() + interval '1 hour', \
             bitcoin_quote_rate = 2500000, \
             bitcoin_quote_source = $2, \
             bitcoin_quote_fetched_at = now(), \
             bitcoin_quoted_sats = $3, \
             bitcoin_quote_expires_at = now() + interval '1 hour', \
             bitcoin_quote_currency = currency, \
             bitcoin_quote_exponent = exponent, \
             bitcoin_quote_spread_bps = 0 \
             WHERE id = $1",
        )
        .bind(order_id)
        .bind(provider.as_str())
        .bind(sats)
        .execute(&pool)
        .await
        .unwrap_or_else(|e| panic!("sats={sats} provider={} failed: {e}", provider.as_str()));
    }

    // Non-positive sats and non-finite rates still fail closed.
    for (label, set) in [
        ("zero sats", "bitcoin_quoted_sats = 0"),
        ("NaN rate", "bitcoin_quote_rate = 'NaN'::numeric"),
        ("Infinity rate", "bitcoin_quote_rate = 'Infinity'::numeric"),
        ("unknown source", "bitcoin_quote_source = 'other'"),
    ] {
        let rejected = sqlx::query(&format!("UPDATE orders SET {set} WHERE id = $1"))
            .bind(order_id)
            .execute(&pool)
            .await;
        assert!(rejected.is_err(), "{label} must be rejected");
    }

    // Built-in providers are admitted on EUR USD-conversion rows and
    // superseded attempts too.
    sqlx::query(
        "UPDATE orders SET \
         bitcoin_quote_rate = NULL, bitcoin_quote_source = NULL, \
         bitcoin_quote_fetched_at = NULL, bitcoin_quoted_sats = NULL, \
         bitcoin_quote_expires_at = NULL, bitcoin_quote_currency = NULL, \
         bitcoin_quote_exponent = NULL, bitcoin_quote_spread_bps = NULL, \
         currency = 'EUR', \
         usd_conversion_pair = 'EURUSD', usd_conversion_source = 'kraken', \
         usd_conversion_usd_rate = 2500000, usd_conversion_currency_rate = 2100000, \
         usd_conversion_fetched_at = now(), usd_conversion_cents = 15909 \
         WHERE id = $1",
    )
    .bind(order_id)
    .execute(&pool)
    .await
    .expect("kraken conversion admitted on orders");

    sqlx::query(
        "INSERT INTO paykit_superseded_attempts (order_id, invoice_id, paykit_api, \
         stack_id, stack_endpoint, total_sats, released_at, usd_conversion_pair, \
         usd_conversion_source, usd_conversion_usd_rate, usd_conversion_currency_rate, \
         usd_conversion_fetched_at, usd_conversion_cents) \
         VALUES ($1, $2, 'fork', 'stack', 'http://stack.test', 250000000, now(), \
         'EURUSD', 'mempool', 2500000, 2100000, now(), 15909)",
    )
    .bind(order_id)
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await
    .expect("mempool conversion admitted on superseded attempts");
}
