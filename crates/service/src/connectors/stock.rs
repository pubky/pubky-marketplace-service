//! Store stock into Pubky listing stock, through the existing
//! `inventory.adjust` handler.
//!
//! The store is the source of truth for stock a connector maps. A stock
//! level for a mapped variant becomes a delta against the listing's
//! `available` quantity and goes through the same handler a seller's own
//! automation uses: same validation, same revision check, same idempotency
//! table, same `inventory.adjusted` event. Only the per-session rate bucket is
//! skipped: it is keyed to a stored bearer session, and the worker is paced by
//! its own queue and backoff. The worker acts as the connection's seller
//! through an in-process session that carries only the inventory capability;
//! no seller bearer exists or is stored.
//!
//! Only mapped variants are applied. A listing the connection maps to exactly
//! one store variant (every `split` connection, and a `native` listing with a
//! single variant) syncs at listing level, which the service tracks. A
//! `native` listing mapped to several store variants needs per-variant stock
//! and goes through [`VariantInventory`]; until the service has that, the
//! default [`NoVariantInventory`] skips it. The service has no per-variant stock:
//! the seller's listing record carries a quantity per variant, but
//! registration sums them into the listing's `total_quantity`
//! (`registration_payload_from_record`), `listings` holds only total,
//! available, reserved and sold, and checkout, holds, cancellations, drops and
//! `inventory.adjust` all move that one listing-level number. Giving a store
//! variant a stock to land on would mean new per-variant inventory in the
//! service itself; see the connector contract's stock note for what that takes.

use axum::body::Bytes;
use axum::http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use async_trait::async_trait;

use crate::auth::{Actor, AuthSession, INVENTORY_SERVICE_SCOPE};
use crate::AppState;

use super::store::{self, ConnectionRow, MappingRow};

#[derive(Debug, PartialEq, Eq)]
pub enum StockOutcome {
    /// The listing's stock was moved by `delta`.
    Applied { delta: i64 },
    /// The listing already shows this level.
    Unchanged,
    /// The same event or plan was applied before.
    AlreadyApplied,
    /// Nothing to do, for a stated reason.
    Ignored(&'static str),
    /// Try again later: a concurrent change or a rate limit.
    Retry(String),
}

/// One store variant's sellable stock, to be applied to the Pubky variant it
/// is mapped to.
#[derive(Debug, Clone)]
pub struct VariantStockRequest<'a> {
    pub connection: &'a ConnectionRow,
    /// The mapping row: `listing_id` and `variant_id` name the Pubky side.
    pub mapping: &'a MappingRow,
    /// What the store can sell now (never negative).
    pub available: i64,
    /// The provider's webhook event id, or `None` for a poll. Use it as the
    /// adjustment's external reference so a replay cannot apply twice.
    pub event_id: Option<&'a str>,
}

/// Where per-variant store stock lands. The service has listing-level
/// inventory only today, so the default skips it. When per-variant inventory
/// exists, implement this once (set the Pubky variant's available quantity to
/// `available` through the new per-variant adjust path, idempotent on the
/// request's key) and pass it to
/// [`super::ConnectorRuntime::with_variant_inventory`]; nothing else in the
/// connector changes.
#[async_trait]
pub trait VariantInventory: Send + Sync + 'static {
    async fn apply_variant_stock(
        &self,
        state: &AppState,
        request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error>;
}

/// The default: no per-variant stock exists, so a listing mapped to several
/// store variants is skipped.
pub struct NoVariantInventory;

#[async_trait]
impl VariantInventory for NoVariantInventory {
    async fn apply_variant_stock(
        &self,
        _state: &AppState,
        _request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error> {
        Ok(StockOutcome::Ignored("multi_variant_listing"))
    }
}

fn worker_session(seller_pubky: &str) -> AuthSession {
    AuthSession {
        actor: Actor(seller_pubky.to_string()),
        capabilities: format!("{INVENTORY_SERVICE_SCOPE}:rw"),
        token_hash: Sha256::digest(format!("connector-worker|stock|{seller_pubky}").as_bytes())
            .to_vec(),
    }
}

/// Applies a store stock level to the mapped listing. `event_id` is the
/// provider's event id for a webhook (it also becomes the adjustment's
/// external reference, so a replay can never apply twice); `None` for a
/// poll.
pub async fn apply_stock_level(
    state: &AppState,
    connection: &ConnectionRow,
    mapping: &MappingRow,
    level: i64,
    event_id: Option<&str>,
) -> Result<StockOutcome, sqlx::Error> {
    if store::mapping_count_for_listing(&state.pool, connection.id, &mapping.listing_id).await? > 1
    {
        let Some(runtime) = state.connectors.as_ref() else {
            return Ok(StockOutcome::Ignored("multi_variant_listing"));
        };
        return runtime
            .variant_inventory
            .apply_variant_stock(
                state,
                VariantStockRequest {
                    connection,
                    mapping,
                    available: level.max(0),
                    event_id,
                },
            )
            .await;
    }
    let listing: Option<(i64, i64)> = sqlx::query_as(
        "SELECT server_revision, available_quantity FROM listings \
         WHERE seller_pubky = $1 AND listing_id = $2 AND deleted_at IS NULL",
    )
    .bind(&connection.seller_pubky)
    .bind(&mapping.listing_id)
    .fetch_optional(&state.pool)
    .await?;
    let Some((revision, available)) = listing else {
        return Ok(StockOutcome::Ignored("listing_not_found"));
    };
    let target = level.max(0);
    let delta = target - available;
    if delta == 0 {
        return Ok(StockOutcome::Unchanged);
    }
    if delta.unsigned_abs() > 1_000_000 {
        return Ok(StockOutcome::Ignored("delta_out_of_range"));
    }
    let key = super::deterministic_uuid(&format!(
        "connector-stock|{}|{}|{revision}|{delta}",
        connection.id,
        event_id.unwrap_or("poll"),
    ));
    let mut request = json!({
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
    });
    if let Some(event_id) = event_id {
        request["external_ref"] = json!({
            "channel": connection.provider,
            "external_id": event_id,
        });
    }
    let body = Bytes::from(serde_json::to_vec(&request).expect("adjust request serializes"));
    let response = crate::inventory::adjust_inventory_for(
        state,
        &worker_session(&connection.seller_pubky),
        Ok(body),
        false,
    )
    .await;
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap_or_default();
    let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let code = parsed
        .pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok(match (status, code) {
        (StatusCode::OK, _) => StockOutcome::Applied { delta },
        (_, "external_reference_conflict") => StockOutcome::AlreadyApplied,
        (_, "listing_not_found") => StockOutcome::Ignored("listing_not_found"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_worker_session_covers_only_the_inventory_capability() {
        let session = worker_session("seller");
        assert!(session.covers_inventory_service());
        assert_eq!(session.actor.0, "seller");
        assert_ne!(
            session.token_hash,
            worker_session("other").token_hash,
            "a session is identified per seller"
        );
    }
}
