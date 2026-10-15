//! Store stock for a listing mapped to several store variants reaches the
//! Pubky variant each is mapped to, through the runtime's
//! [`VariantInventory`] (`ServiceVariantInventory`).
//!
//! Per-variant inventory is a separate service change. Until a build has it
//! (the table `listing_variant_inventory`), the seam skips such a listing like
//! the default does, and the first test pins that. With it, the second test
//! drives the real adjust path: each size follows its own store variation and
//! the listing's total stays the sum of the sizes. Both run on every build and
//! each asserts what its build can do.

mod common;

use std::sync::Arc;

use common::connector_provider::*;
use common::{execute, new_actor, register_command, TestActor, TestApp};
use marketplace_service::connectors::provider::ProviderRegistry;
use marketplace_service::connectors::stock::{StockOutcome, VariantInventory, VariantStockRequest};
use marketplace_service::connectors::store::{self, ConnectionRow, MappingRow};
use marketplace_service::connectors::variant_stock::{
    variant_inventory_present, ServiceVariantInventory,
};
use marketplace_service::connectors::ConnectorRuntime;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

struct World {
    app: TestApp,
    seller: TestActor,
    connection: ConnectionRow,
}

async fn world(pool: PgPool, variants: Option<&[(&str, i64)]>) -> World {
    let (app, _store) = connector_app(pool).await;
    let runtime = app.state.connectors.as_ref().expect("connectors enabled");
    let rebuilt = Arc::new(
        ConnectorRuntime::new(
            runtime.keys.clone(),
            ProviderRegistry::clone(&runtime.providers),
            runtime.settings.clone(),
        )
        .with_variant_inventory(Arc::new(ServiceVariantInventory)),
    );
    let state = app.state.clone().with_connectors(Some(rebuilt));
    let app = rebuild(app, state);
    let seller = new_actor(&app).await;
    let connection = connect_shop(&app, &seller, SHOP).await;
    let supported = variant_inventory_present(&app.state).await.expect("probe");
    let mut command = register_command(&seller.pubky, 8);
    if let (Some(variants), true) = (variants, supported) {
        command["payload"]["variants"] = json!(variants
            .iter()
            .map(|(id, quantity)| json!({ "id": id, "quantity": quantity }))
            .collect::<Vec<_>>());
        command["payload"]["quantity"] = json!(variants.iter().map(|(_, q)| q).sum::<i64>());
    }
    let (status, body) = execute(&app, &seller.token, &command).await;
    assert_eq!(status, axum::http::StatusCode::OK, "register: {body}");
    let row = store::get(&app.pool, connection)
        .await
        .expect("lookup")
        .expect("connection");
    World {
        app,
        seller,
        connection: row,
    }
}

async fn map_sizes(world: &World, sizes: &[(&str, &str)]) -> Vec<MappingRow> {
    let mappings: Vec<_> = sizes
        .iter()
        .map(|(variant, store_variant)| {
            json!({
                "listing_id": "boots_01",
                "variant_id": variant,
                "provider_product_id": "prod-1",
                "provider_variant_id": store_variant,
            })
        })
        .collect();
    let (status, body) = api(
        &world.app,
        "PUT",
        &format!("/v1/connectors/{}/mappings", world.connection.id),
        Some(&world.seller.token),
        &json!({ "mappings": mappings }),
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "map: {body}");
    store::list_mappings(&world.app.pool, world.connection.id)
        .await
        .expect("mappings")
}

async fn apply(
    world: &World,
    mapping: &MappingRow,
    available: i64,
    event: Option<&str>,
) -> StockOutcome {
    ServiceVariantInventory
        .apply_variant_stock(
            &world.app.state,
            VariantStockRequest {
                connection: &world.connection,
                mapping,
                available,
                event_id: event,
            },
        )
        .await
        .expect("apply")
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_build_without_per_variant_inventory_skips_a_multi_variant_listing_like_the_default(
    pool: PgPool,
) {
    let probe = world(pool, None).await;
    if variant_inventory_present(&probe.app.state)
        .await
        .expect("probe")
    {
        return;
    }
    let mappings = map_sizes(&probe, &[("S", "var-s"), ("M", "var-m")]).await;
    assert_eq!(
        apply(&probe, &mappings[0], 3, Some("evt-1")).await,
        StockOutcome::Ignored("multi_variant_listing")
    );
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn with_per_variant_inventory_each_size_follows_its_own_store_variation(pool: PgPool) {
    let sizes: &[(&str, i64)] = &[("S", 5), ("M", 3), ("L", 0)];
    let world = world(pool, Some(sizes)).await;
    if !variant_inventory_present(&world.app.state)
        .await
        .expect("probe")
    {
        return;
    }
    let mappings = map_sizes(&world, &[("S", "var-s"), ("M", "var-m"), ("L", "var-l")]).await;
    let by_variant = |variant: &str| {
        mappings
            .iter()
            .find(|m| m.variant_id == variant)
            .expect("mapping")
            .clone()
    };
    let rows = |app: &TestApp| {
        let pool = app.pool.clone();
        async move {
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT v.variant_id, v.available_quantity FROM listing_variant_inventory v \
                 JOIN listings l ON l.aggregate_id = v.listing_aggregate_id \
                 WHERE l.listing_id = 'boots_01' ORDER BY v.variant_id",
            )
            .fetch_all(&pool)
            .await
            .expect("rows");
            let total: i64 = sqlx::query_scalar(
                "SELECT available_quantity FROM listings WHERE listing_id = 'boots_01'",
            )
            .fetch_one(&pool)
            .await
            .expect("total");
            (rows, total)
        }
    };

    assert_eq!(
        apply(&world, &by_variant("S"), 2, Some("evt-s")).await,
        StockOutcome::Applied { delta: -3 }
    );
    let (rows_now, total) = rows(&world.app).await;
    assert_eq!(
        rows_now,
        [
            ("L".to_string(), 0),
            ("M".to_string(), 3),
            ("S".to_string(), 2)
        ]
    );
    assert_eq!(total, 5, "the listing's total is the sum of its sizes");

    assert_eq!(
        apply(&world, &by_variant("S"), 2, None).await,
        StockOutcome::Unchanged
    );
    assert_eq!(
        apply(&world, &by_variant("M"), 7, Some("evt-m")).await,
        StockOutcome::Applied { delta: 4 }
    );
    assert!(
        matches!(
            apply(&world, &by_variant("M"), 7, Some("evt-m")).await,
            StockOutcome::Unchanged | StockOutcome::AlreadyApplied
        ),
        "the same event again changes nothing"
    );
    assert_eq!(
        apply(&world, &by_variant("L"), 4, None).await,
        StockOutcome::Applied { delta: 4 }
    );
    let (rows_now, total) = rows(&world.app).await;
    assert_eq!(
        rows_now,
        [
            ("L".to_string(), 4),
            ("M".to_string(), 7),
            ("S".to_string(), 2)
        ]
    );
    assert_eq!(total, 13);

    // A negative store level is zero; a mapping to a size the service does not know is stale.
    assert_eq!(
        apply(&world, &by_variant("S"), -5, None).await,
        StockOutcome::Applied { delta: -2 }
    );
    let stale = MappingRow {
        variant_id: "XXL".into(),
        ..by_variant("S")
    };
    assert_eq!(
        apply(&world, &stale, 1, None).await,
        StockOutcome::Ignored("variant_mapping_stale")
    );
    let unmapped = MappingRow {
        variant_id: String::new(),
        ..by_variant("S")
    };
    assert_eq!(
        apply(&world, &unmapped, 1, None).await,
        StockOutcome::Ignored("unmapped_variant")
    );
    let _ = Uuid::nil();
}
