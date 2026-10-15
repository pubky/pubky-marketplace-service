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
//! single variant) syncs at listing level. A `native` listing mapped to several
//! store variants needs per-variant stock and goes through
//! [`VariantInventory`]. The default, [`ServiceVariantInventory`], addresses the
//! Pubky variant row (`inventory.adjust` with a `variant` selector, the delta
//! being the target minus that variant's own `available`) when the service has
//! per-variant inventory (`listing_variant_inventory`, migration 0070 of the
//! variant-inventory work), and otherwise skips the listing
//! (`multi_variant_listing`), which is safe. [`NoVariantInventory`] always skips.
//!
//! A stock change that names a variant the service cannot address (the mapping
//! has no Pubky variant id, the variant is no longer on the listing, or the
//! service answers `variant_required` / `variant_assertion_mismatch`) is a
//! permanent mapping error, never a retry: the event settles as
//! `variant_mapping_error` and the seller gets one alert per variant to re-map.

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
    /// The store variant cannot be addressed on the Pubky side (a stale or
    /// incomplete mapping). Permanent until the seller re-maps: the framework
    /// raises an alert and never retries.
    MappingError(&'static str),
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
    let outcome =
        if store::mapping_count_for_listing(&state.pool, connection.id, &mapping.listing_id).await?
            > 1
        {
            match state.connectors.as_ref() {
                Some(runtime) => {
                    runtime
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
                        .await?
                }
                None => StockOutcome::Ignored("multi_variant_listing"),
            }
        } else {
            apply_listing_stock(state, connection, mapping, level, event_id).await?
        };
    if let StockOutcome::MappingError(reason) = &outcome {
        raise_mapping_alert(state, connection, mapping, reason).await?;
    }
    Ok(outcome)
}

async fn raise_mapping_alert(
    state: &AppState,
    connection: &ConnectionRow,
    mapping: &MappingRow,
    reason: &str,
) -> Result<(), sqlx::Error> {
    store::raise_alert(
        &state.pool,
        state.clock.now(),
        connection,
        store::AlertKind::VariantMappingError,
        None,
        &format!(
            "The store changed the stock of a variant Pubky cannot address ({reason}): listing {} \
             variant {:?}, store variant {}. Re-map it to one of the listing's current variants.",
            mapping.listing_id, mapping.variant_id, mapping.provider_variant_id
        ),
        &format!(
            "variant-mapping|{}|{}|{}|{reason}",
            connection.id, mapping.listing_id, mapping.variant_id
        ),
    )
    .await
}

/// Listing-level stock: the listing's `available` is the figure the store's
/// level is compared with.
async fn apply_listing_stock(
    state: &AppState,
    connection: &ConnectionRow,
    mapping: &MappingRow,
    level: i64,
    event_id: Option<&str>,
) -> Result<StockOutcome, sqlx::Error> {
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
    send_adjust(
        state,
        connection,
        mapping,
        revision,
        level.max(0) - available,
        event_id,
        None,
    )
    .await
}

/// Sends one `inventory.adjust` of `delta`, addressed to a variant when
/// `variant` names one, and classifies the answer.
async fn send_adjust(
    state: &AppState,
    connection: &ConnectionRow,
    mapping: &MappingRow,
    revision: i64,
    delta: i64,
    event_id: Option<&str>,
    variant: Option<&str>,
) -> Result<StockOutcome, sqlx::Error> {
    if delta == 0 {
        return Ok(StockOutcome::Unchanged);
    }
    if delta.unsigned_abs() > 1_000_000 {
        return Ok(StockOutcome::Ignored("delta_out_of_range"));
    }
    let request = adjust_request(connection, mapping, revision, delta, event_id, variant);
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
    Ok(classify_adjust(status, code, delta))
}

/// The `inventory.adjust` body for a stock change. A variant-addressed one
/// carries the Pubky variant id as the selector the service resolves against
/// the listing's variant rows.
fn adjust_request(
    connection: &ConnectionRow,
    mapping: &MappingRow,
    revision: i64,
    delta: i64,
    event_id: Option<&str>,
    variant: Option<&str>,
) -> Value {
    let key = super::deterministic_uuid(&format!(
        "connector-stock|{}|{}|{}|{revision}|{delta}",
        connection.id,
        variant.unwrap_or("-"),
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
    if let Some(variant) = variant {
        request["variant"] = json!({ "id": variant });
    }
    if let Some(event_id) = event_id {
        request["external_ref"] = json!({
            "channel": connection.provider,
            "external_id": event_id,
        });
    }
    request
}

/// What the service's answer to an adjust means. `variant_required` and
/// `variant_assertion_mismatch` say the mapping names no row the service has
/// (stale or missing): permanent, so a mapping error, not a retry.
fn classify_adjust(status: StatusCode, code: &str, delta: i64) -> StockOutcome {
    match (status, code) {
        (StatusCode::OK, _) => StockOutcome::Applied { delta },
        (_, "external_reference_conflict") => StockOutcome::AlreadyApplied,
        (_, "listing_not_found") => StockOutcome::Ignored("listing_not_found"),
        (_, "variant_required") => StockOutcome::MappingError("variant_required"),
        (_, "variant_assertion_mismatch") => {
            StockOutcome::MappingError("variant_assertion_mismatch")
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
    }
}

/// The default [`VariantInventory`]: per-variant stock through the service's
/// own variant rows when it has them, a skip when it does not.
///
/// The target is "the store can sell N of this variant", and the adjustment is
/// `N - that variant's available` (never the listing's `available`, which is
/// the sum across variants), sent as an `inventory.adjust` addressed to the
/// Pubky variant id the mapping stores. Reading the variant's `available` is a
/// query on `listing_variant_inventory`, so a service built without it is
/// detected (`to_regclass`) and the listing is skipped as before.
pub struct ServiceVariantInventory;

#[async_trait]
impl VariantInventory for ServiceVariantInventory {
    async fn apply_variant_stock(
        &self,
        state: &AppState,
        request: VariantStockRequest<'_>,
    ) -> Result<StockOutcome, sqlx::Error> {
        let present: bool =
            sqlx::query_scalar("SELECT to_regclass('listing_variant_inventory') IS NOT NULL")
                .fetch_one(&state.pool)
                .await?;
        if !present {
            return Ok(StockOutcome::Ignored("multi_variant_listing"));
        }
        let VariantStockRequest {
            connection,
            mapping,
            available: target,
            event_id,
        } = request;
        if mapping.variant_id.is_empty() {
            return Ok(StockOutcome::MappingError("variant_required"));
        }
        let aggregate = marketplace_domain::ids::listing_aggregate_id(
            &connection.seller_pubky,
            &mapping.listing_id,
        );
        let row: Option<(i64, Option<i64>)> = sqlx::query_as(
            "SELECT l.server_revision, v.available_quantity \
             FROM listings l \
             LEFT JOIN listing_variant_inventory v \
                    ON v.listing_aggregate_id = l.aggregate_id AND v.variant_id = $2 \
             WHERE l.aggregate_id = $1 AND l.deleted_at IS NULL",
        )
        .bind(&aggregate)
        .bind(&mapping.variant_id)
        .fetch_optional(&state.pool)
        .await?;
        let Some((revision, variant_available)) = row else {
            return Ok(StockOutcome::Ignored("listing_not_found"));
        };
        let Some(variant_available) = variant_available else {
            return Ok(StockOutcome::MappingError("variant_not_found"));
        };
        send_adjust(
            state,
            connection,
            mapping,
            revision,
            target.max(0) - variant_available,
            event_id,
            Some(&mapping.variant_id),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_errors_are_permanent_mapping_errors_not_retries() {
        let conflict = StatusCode::CONFLICT;
        assert_eq!(
            classify_adjust(conflict, "variant_required", 3),
            StockOutcome::MappingError("variant_required")
        );
        assert_eq!(
            classify_adjust(conflict, "variant_assertion_mismatch", 3),
            StockOutcome::MappingError("variant_assertion_mismatch")
        );
        assert_eq!(
            classify_adjust(conflict, "revision_conflict", 3),
            StockOutcome::Retry("revision_conflict".into())
        );
        assert_eq!(
            classify_adjust(StatusCode::OK, "", 3),
            StockOutcome::Applied { delta: 3 }
        );
        assert_eq!(
            classify_adjust(conflict, "negative_stock", -3),
            StockOutcome::Ignored("stock_refused")
        );
        assert_eq!(
            classify_adjust(conflict, "external_reference_conflict", 3),
            StockOutcome::AlreadyApplied
        );
        assert_eq!(
            classify_adjust(StatusCode::BAD_GATEWAY, "", 3),
            StockOutcome::Retry("status_502".into())
        );
    }

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
