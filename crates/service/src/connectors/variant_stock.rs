//! Store stock for a listing mapped to several store variants, applied to the
//! Pubky variant each one is mapped to. This is the real
//! [`VariantInventory`] the runtime uses (`runtime_from_env`).
//!
//! The service tracks stock per variant when it has `listing_variant_inventory`
//! (per-variant inventory): a variant-tracked listing's `inventory.adjust`
//! must name the variant, and the delta is against **that variant's**
//! `available_quantity`, not the listing's, which is the sum over sizes. A
//! store variant maps one to one to a Pubky variant (a WooCommerce variation
//! id to the record's variant id), so the mapping row's `variant_id` is the
//! selector.
//!
//! On a service without that table the seam answers what the default does,
//! `Ignored("multi_variant_listing")`, so the connector behaves the same
//! before and after the service gains per-variant inventory, with no flag.
//!
//! Errors map as the service documents them: `variant_required` and
//! `variant_assertion_mismatch` mean the mapping is stale (permanent, resync
//! the mapping), `revision_conflict` and rate limits retry, `negative_stock`
//! and its kin are `stock_refused`, an external reference already used is
//! already applied.

use async_trait::async_trait;
use axum::body::Bytes;
use axum::http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::auth::{Actor, AuthSession, INVENTORY_SERVICE_SCOPE};
use crate::AppState;

use super::stock::{StockOutcome, VariantInventory, VariantStockRequest};

pub struct ServiceVariantInventory;

fn worker_session(seller_pubky: &str) -> AuthSession {
    AuthSession {
        actor: Actor(seller_pubky.to_string()),
        capabilities: format!("{INVENTORY_SERVICE_SCOPE}:rw"),
        token_hash: Sha256::digest(format!("connector-worker|stock|{seller_pubky}").as_bytes())
            .to_vec(),
    }
}

/// Whether this database has per-variant inventory.
pub async fn variant_inventory_present(state: &AppState) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT to_regclass('listing_variant_inventory') IS NOT NULL")
        .fetch_one(&state.pool)
        .await
}

#[async_trait]
impl VariantInventory for ServiceVariantInventory {
    async fn apply_variant_stock(
        &self,
        state: &AppState,
        request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error> {
        if !variant_inventory_present(state).await? {
            return Ok(StockOutcome::Ignored("multi_variant_listing"));
        }
        let connection = request.connection;
        let mapping = request.mapping;
        if mapping.variant_id.is_empty() {
            return Ok(StockOutcome::Ignored("unmapped_variant"));
        }
        let current: Option<(i64, i64)> = sqlx::query_as(
            "SELECT l.server_revision, v.available_quantity \
             FROM listing_variant_inventory v \
             JOIN listings l ON l.aggregate_id = v.listing_aggregate_id \
             WHERE l.seller_pubky = $1 AND l.listing_id = $2 AND l.deleted_at IS NULL \
               AND v.variant_id = $3",
        )
        .bind(&connection.seller_pubky)
        .bind(&mapping.listing_id)
        .bind(&mapping.variant_id)
        .fetch_optional(&state.pool)
        .await?;
        let Some((revision, available)) = current else {
            return Ok(StockOutcome::Ignored("variant_mapping_stale"));
        };
        let delta = request.available.max(0) - available;
        if delta == 0 {
            return Ok(StockOutcome::Unchanged);
        }
        if delta.unsigned_abs() > 1_000_000 {
            return Ok(StockOutcome::Ignored("delta_out_of_range"));
        }
        let key = super::deterministic_uuid(&format!(
            "connector-stock|{}|{}|{}|{revision}|{delta}",
            connection.id,
            mapping.variant_id,
            request.event_id.unwrap_or("poll"),
        ));
        let mut body = json!({
            "schema_version": 1,
            "kind": "inventory.adjust",
            "aggregate_id": marketplace_domain::ids::listing_aggregate_id(
                &connection.seller_pubky,
                &mapping.listing_id,
            ),
            "listing_id": mapping.listing_id,
            "expected_revision": revision,
            "delta": delta,
            "idempotency_key": key,
            "variant": { "id": mapping.variant_id },
        });
        if let Some(event_id) = request.event_id {
            body["external_ref"] = json!({
                "channel": connection.provider,
                "external_id": format!("{event_id}|{}", mapping.variant_id),
            });
        }
        let bytes = Bytes::from(serde_json::to_vec(&body).expect("adjust request serializes"));
        let response = crate::inventory::adjust_inventory_for(
            state,
            &worker_session(&connection.seller_pubky),
            Ok(bytes),
            false,
        )
        .await;
        let status = response.status();
        let payload = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap_or_default();
        let parsed: Value = serde_json::from_slice(&payload).unwrap_or(Value::Null);
        let code = parsed
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(match (status, code) {
            (StatusCode::OK, _) => StockOutcome::Applied { delta },
            (_, "external_reference_conflict") => StockOutcome::AlreadyApplied,
            (_, "listing_not_found") => StockOutcome::Ignored("listing_not_found"),
            (_, "variant_required" | "variant_assertion_mismatch") => {
                StockOutcome::Ignored("variant_mapping_stale")
            }
            (_, "revision_conflict" | "rate_limited") => StockOutcome::Retry(code.to_string()),
            (StatusCode::TOO_MANY_REQUESTS, _) => StockOutcome::Retry("rate_limited".to_string()),
            (_, "negative_stock" | "stock_overflow" | "inventory_invariant_violation") => {
                StockOutcome::Ignored("stock_refused")
            }
            _ => StockOutcome::Retry(if code.is_empty() {
                format!("status_{}", status.as_u16())
            } else {
                code.to_string()
            }),
        })
    }
}
