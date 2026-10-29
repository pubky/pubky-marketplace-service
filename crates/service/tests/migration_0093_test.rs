//! Migration 0093: `orders_external_refund_within_total` admits the sats an
//! overpaid Bitcoin order received (`paykit_observed_sats`), over a database
//! that already holds refunds. A refund of the observed amount above the
//! payable is refused before the migration and recorded after it, anything
//! above every bound is still refused, every existing refund still satisfies
//! the constraint, and the file reruns as a no-op. Every refund recorded
//! before it is named in the unit its writer recorded (`amount_unit`).

use std::borrow::Cow;

use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::PgPool;
use uuid::Uuid;

mod common;

static ALL_MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

const MIGRATION_0093: &str = include_str!("../migrations/0093_bitcoin_refund_received_sats.sql");

const CONSTRAINT: &str = "orders_external_refund_within_total";
const PAYABLE_SATS: i64 = 51_637;
const OBSERVED_SATS: i64 = 53_000;

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

/// A cancelled 137.00 USD order paid in Bitcoin, overpaid by 1,363 sats.
async fn seed_overpaid_bitcoin_order(pool: &PgPool) -> Uuid {
    let order_id = Uuid::new_v4();
    let lines = json!([{ "listing_aggregate_id": "listing:seller_boots_01", "quantity": 1 }]);
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, payment_method, paykit_total_sats, \
         paykit_observed_sats, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 4, 'cancelled', $2, NULL, 12500, 1200, 13700, 'USD', 2, \
         1, $3, 'paykit', $4, $5, now(), now())",
    )
    .bind(order_id)
    .bind(lines)
    .bind(Uuid::new_v4())
    .bind(PAYABLE_SATS)
    .bind(OBSERVED_SATS)
    .execute(pool)
    .await
    .expect("seed order");
    order_id
}

async fn record_refund(
    pool: &PgPool,
    order_id: Uuid,
    amount_minor: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE orders SET state = 'refunded_external', external_refund = jsonb_build_object(\
         'amount_minor', $2::bigint, 'transaction_id', 'bitcoin-refund-1', \
         'recorded_at', '2026-10-29T12:00:00.000Z') WHERE id = $1",
    )
    .bind(order_id)
    .bind(amount_minor)
    .execute(pool)
    .await
    .map(|_| ())
}

fn constraint_violation(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|error| error.constraint().map(str::to_string))
}

async fn refunded(pool: &PgPool, order_id: Uuid) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT (external_refund->>'amount_minor')::bigint FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_one(pool)
    .await
    .expect("order")
}

#[sqlx::test(migrations = false)]
async fn the_sats_an_overpaid_order_received_are_refundable_after_the_migration(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(92)
        .run(&pool)
        .await
        .expect("every migration before 0093 applies");

    let existing = seed_overpaid_bitcoin_order(&pool).await;
    record_refund(&pool, existing, PAYABLE_SATS)
        .await
        .expect("the payable is refundable before 0093");
    let overpaid = seed_overpaid_bitcoin_order(&pool).await;
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect_err("the observed amount above the payable is refused before 0093");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));

    ALL_MIGRATIONS.run(&pool).await.expect("0093 applies");

    assert_eq!(
        refunded(&pool, existing).await,
        Some(PAYABLE_SATS),
        "an existing refund survives"
    );
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS + 1)
        .await
        .expect_err("above everything the order received is still refused");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));
    record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect("the observed amount is refundable after 0093");
    assert_eq!(refunded(&pool, overpaid).await, Some(OBSERVED_SATS));
    let refused = record_refund(&pool, existing, 0)
        .await
        .expect_err("an empty refund is still refused");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));

    sqlx::query(&format!(
        "ALTER TABLE orders VALIDATE CONSTRAINT {CONSTRAINT}"
    ))
    .execute(&pool)
    .await
    .expect("every existing refund satisfies the widened bound");
}

struct LegacyRefund {
    currency: &'static str,
    exponent: i32,
    total_minor: i64,
    payment_method: &'static str,
    /// `payment_asset` and the terms it settled on.
    settled: Option<(&'static str, i64, i16)>,
    amount_minor: i64,
    refunded_by_review: bool,
}

/// A cancelled order refunded before 0093, by `refund.record_external` or a
/// refunded manual-review resolution, its record naming no unit.
async fn seed_legacy_refund(pool: &PgPool, refund: &LegacyRefund) -> Uuid {
    let order_id = Uuid::new_v4();
    let payment_id = Uuid::new_v4();
    let (asset, network, amount, exponent, basis) = match refund.settled {
        Some((asset, amount, exponent)) => (
            Some(asset),
            Some("arbitrum-one"),
            Some(amount),
            Some(exponent),
            Some("request"),
        ),
        None => (None, None, None, None, None),
    };
    sqlx::query(
        "INSERT INTO orders (id, buyer_pubky, seller_pubky, revision, state, lines, \
         delivery_address, subtotal_minor, shipping_minor, total_minor, currency, exponent, \
         guarantee_policy_version, payment_id, payment_method, payment_asset, payment_network, \
         payment_amount_minor, payment_exponent, payment_quote_basis, paykit_observed_sats, \
         paykit_total_sats, external_refund, created_at, updated_at) \
         VALUES ($1, 'buyer', 'seller', 4, 'refunded_external', '[]', NULL, $2, 0, $2, $3, $4, \
         1, $5, $6, $7, $8, $9, $10, $11, $12, $12, \
         jsonb_build_object('amount_minor', $13::bigint, 'transaction_id', 'legacy-refund-1', \
         'recorded_at', '2026-10-01T12:00:00.000Z'), now(), now())",
    )
    .bind(order_id)
    .bind(refund.total_minor)
    .bind(refund.currency)
    .bind(refund.exponent)
    .bind(payment_id)
    .bind(refund.payment_method)
    .bind(asset)
    .bind(network)
    .bind(amount)
    .bind(exponent)
    .bind(basis)
    .bind(refund.refunded_by_review.then_some(refund.amount_minor))
    .bind(refund.amount_minor)
    .execute(pool)
    .await
    .expect("seed refunded order");
    if refund.refunded_by_review {
        sqlx::query(
            "INSERT INTO payments (id, order_id, buyer_pubky, seller_pubky, revision, adapter, \
             state, confirmations, amount_minor, currency, exponent, created_at, updated_at) \
             VALUES ($1, $2, 'buyer', 'seller', 1, 'paykit', 'confirmed', 0, $3, $4, $5, \
             now(), now())",
        )
        .bind(payment_id)
        .bind(order_id)
        .bind(refund.total_minor)
        .bind(refund.currency)
        .bind(refund.exponent)
        .execute(pool)
        .await
        .expect("seed payment");
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, \
             occurred_at) VALUES ($1, $2, $3, 1, 'seller', 'payment.resolved', now())",
        )
        .bind(event_id)
        .bind(Uuid::new_v4())
        .bind(format!("payment:{payment_id}"))
        .execute(pool)
        .await
        .expect("seed event");
        sqlx::query(
            "INSERT INTO paykit_manual_resolutions (order_id, payment_id, resolution_id, \
             outcome, basis, resolved_at, resolved_by_pubky, refund_reference, request_hash, \
             response, event_id, created_at) \
             VALUES ($1, $2, $3, 'refunded', 'seller_attestation', now(), 'seller', \
             'legacy-refund-1', 'h', '{}', $4, now())",
        )
        .bind(order_id)
        .bind(payment_id)
        .bind(Uuid::new_v4())
        .bind(event_id)
        .execute(pool)
        .await
        .expect("seed refunded resolution");
    }
    order_id
}

async fn refund_record(pool: &PgPool, order_id: Uuid) -> Option<serde_json::Value> {
    sqlx::query_scalar("SELECT external_refund FROM orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(pool)
        .await
        .expect("order")
}

#[sqlx::test(migrations = false)]
async fn every_earlier_refund_is_named_in_the_unit_its_writer_recorded(pool: PgPool) {
    common::restore_0032_retention_connlimit(&pool).await;
    through(92)
        .run(&pool)
        .await
        .expect("every migration before 0093 applies");

    let cases = [
        (
            "a refunded review recorded the Bitcoin order's observed sats",
            LegacyRefund {
                currency: "USD",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "paykit",
                settled: None,
                amount_minor: 13_000,
                refunded_by_review: true,
            },
            "sat",
        ),
        (
            "the seller recorded a Bitcoin order's refund in the order total's units",
            LegacyRefund {
                currency: "USD",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "paykit",
                settled: Some(("BTC", 51_637, 8)),
                amount_minor: 13_700,
                refunded_by_review: false,
            },
            "order",
        ),
        (
            "a USDT order priced in USD recorded in cents",
            LegacyRefund {
                currency: "USD",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "paykit",
                settled: Some(("USDT", 137_000_000, 6)),
                amount_minor: 13_700,
                refunded_by_review: true,
            },
            "order",
        ),
        (
            "a USDT order priced in EUR recorded in USDT0 units",
            LegacyRefund {
                currency: "EUR",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "paykit",
                settled: Some(("USDT", 148_540_000, 6)),
                amount_minor: 148_540_000,
                refunded_by_review: false,
            },
            "usdt0",
        ),
        (
            "the legacy USDT method priced in EUR recorded in USDT0 units",
            LegacyRefund {
                currency: "EUR",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "usdt",
                settled: None,
                amount_minor: 13_700,
                refunded_by_review: false,
            },
            "usdt0",
        ),
        (
            "a PayPal refund is in the order total's units",
            LegacyRefund {
                currency: "EUR",
                exponent: 2,
                total_minor: 13_700,
                payment_method: "paypal",
                settled: None,
                amount_minor: 5_000,
                refunded_by_review: false,
            },
            "order",
        ),
    ];
    let mut seeded = Vec::new();
    for (case, refund, unit) in &cases {
        seeded.push((
            *case,
            seed_legacy_refund(&pool, refund).await,
            *unit,
            refund.amount_minor,
        ));
    }
    let unrefunded = seed_overpaid_bitcoin_order(&pool).await;

    ALL_MIGRATIONS.run(&pool).await.expect("0093 applies");

    for (case, order_id, unit, amount_minor) in &seeded {
        let record = refund_record(&pool, *order_id).await.expect("a refund");
        assert_eq!(record["amount_unit"], json!(unit), "{case}");
        assert_eq!(record["amount_minor"], json!(amount_minor), "{case}");
        assert_eq!(record["transaction_id"], json!("legacy-refund-1"), "{case}");
        assert_eq!(
            record["recorded_at"],
            json!("2026-10-01T12:00:00.000Z"),
            "{case}: the mirrored refund's identity is unchanged"
        );
    }
    assert_eq!(
        refund_record(&pool, unrefunded).await,
        None,
        "an order with no refund gains none"
    );

    // A rerun leaves a named record as it is, even one it would name
    // otherwise.
    let (_, named, _, _) = seeded[1];
    sqlx::query(
        "UPDATE orders SET external_refund = external_refund || '{\"amount_unit\": \"sat\"}' \
         WHERE id = $1",
    )
    .bind(named)
    .execute(&pool)
    .await
    .expect("name the unit");
    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0093)
        .execute(&mut *tx)
        .await
        .expect("0093 reruns");
    tx.commit().await.expect("commit");
    assert_eq!(
        refund_record(&pool, named).await.expect("a refund")["amount_unit"],
        json!("sat")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn the_file_reruns_and_keeps_the_widened_bound(pool: PgPool) {
    let overpaid = seed_overpaid_bitcoin_order(&pool).await;
    record_refund(&pool, overpaid, OBSERVED_SATS)
        .await
        .expect("the observed amount is refundable");

    let mut tx = pool.begin().await.expect("tx");
    sqlx::raw_sql(MIGRATION_0093)
        .execute(&mut *tx)
        .await
        .expect("0093 must be directly rerunnable");
    tx.commit().await.expect("commit");

    assert_eq!(refunded(&pool, overpaid).await, Some(OBSERVED_SATS));
    let refused = record_refund(&pool, overpaid, OBSERVED_SATS + 1)
        .await
        .expect_err("still bounded");
    assert_eq!(constraint_violation(&refused).as_deref(), Some(CONSTRAINT));
}
