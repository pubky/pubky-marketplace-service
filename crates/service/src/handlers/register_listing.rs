use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    validate_public_listing_payload, AuctionReserve, Command, CommandPayload,
    RegisterListingPayload, SaleFormat, VariantStock, UNLIMITED_STOCK_ON_PHYSICAL_LISTING,
};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::clock::format_timestamp;
use crate::executor::insert_event;
use crate::handlers::{
    current_listing_revision, fetch_auction_reserve, fetch_auction_reserve_for_update,
    fetch_listing, fetch_listing_for_update,
};
use crate::homeserver::{
    registration_payload_from_record, HomeserverFetchOutcome, HomeserverListingClient,
};
use crate::model::{money_json, AuctionReserveRow, AuctionState, ListingRow};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};
use crate::variant_stock::{self, StockPlan};

/// The refusal for a listing that ships or offers pickup at the unlimited
/// digital cap (`listing.register` and `listing.sync`, and so
/// `listing.sync_many` and the connectors that sync).
pub(crate) fn unlimited_cap_refusal() -> CommandFailure {
    CommandFailure::refused_with_reason(
        crate::refusal_audit::RefusalKind::InvalidState,
        ErrorCode::InvalidState,
        "1000000 is reserved for unlimited digital stock; a listing that ships or offers pickup cannot register it.",
        UNLIMITED_STOCK_ON_PHYSICAL_LISTING,
    )
}

/// The seller's public listing record for one `listing.register`, fetched
/// once before the executor opens its transaction or takes the command's
/// advisory lock, so a slow homeserver never holds a domain connection. It
/// is fetched only for a command that needs it (see `needs_public_record`),
/// passes the cheap actor and aggregate checks, and has no stored result;
/// the handler decides the auction authority from this one record.
pub enum RegisterRecordPrefetch {
    NotNeeded,
    Found(Value),
    NotFound,
    Unavailable,
    NoHomeserver,
}

/// An auction registers against its public authority.
fn needs_public_record(payload: &RegisterListingPayload) -> bool {
    payload.sale_format == SaleFormat::Auction
}

/// The seller-signed actor and the aggregate the command names; the handler
/// refuses anything else before it would read a record.
fn register_addresses_its_own_listing(
    actor: &str,
    command: &Command,
    payload: &RegisterListingPayload,
) -> bool {
    actor == payload.seller_pubky
        && command.aggregate_id
            == ids::listing_aggregate_id(&payload.seller_pubky, &payload.listing_id)
}

/// The `listing.register` payload whose public record the executor must read
/// before its transaction, or `None` for every other command. A register at
/// `expected_revision` 0 for a tombstoned listing would revive it, which
/// only the seller's re-published record authorizes. A listing tombstoned
/// after this read reaches the handler with no record, which refuses the
/// revival.
pub async fn public_record_request<'c>(
    pool: &PgPool,
    actor: &str,
    command: &'c Command,
) -> Result<Option<&'c RegisterListingPayload>, sqlx::Error> {
    let CommandPayload::RegisterListing(payload) = &command.payload else {
        return Ok(None);
    };
    if !register_addresses_its_own_listing(actor, command, payload) {
        return Ok(None);
    }
    if needs_public_record(payload) {
        return Ok(Some(payload));
    }
    if command.expected_revision != 0 {
        return Ok(None);
    }
    let tombstoned: Option<bool> =
        sqlx::query_scalar("SELECT deleted_at IS NOT NULL FROM listings WHERE aggregate_id = $1")
            .bind(&command.aggregate_id)
            .fetch_optional(pool)
            .await?;
    Ok(tombstoned.unwrap_or(false).then_some(payload))
}

/// A revival is a re-creation: the seller's homeserver must hold the record
/// again. A tombstone stands for a delete that is still the record's latest
/// event, and a register must not reverse it.
fn refuse_revival_without_record(prefetched: &RegisterRecordPrefetch) -> Option<CommandFailure> {
    match prefetched {
        RegisterRecordPrefetch::Found(_) => None,
        RegisterRecordPrefetch::NotFound => Some(record_not_found()),
        RegisterRecordPrefetch::Unavailable => Some(record_unavailable()),
        RegisterRecordPrefetch::NotNeeded | RegisterRecordPrefetch::NoHomeserver => {
            Some(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "This listing was deleted. Publish its record again to re-create it.",
            ))
        }
    }
}

pub async fn prefetch_public_record(
    homeserver: Option<&dyn HomeserverListingClient>,
    payload: &RegisterListingPayload,
) -> RegisterRecordPrefetch {
    let Some(homeserver) = homeserver else {
        return RegisterRecordPrefetch::NoHomeserver;
    };
    match homeserver
        .fetch_listing(&payload.seller_pubky, &payload.listing_id)
        .await
    {
        HomeserverFetchOutcome::Found(record) => RegisterRecordPrefetch::Found(record),
        HomeserverFetchOutcome::NotFound => RegisterRecordPrefetch::NotFound,
        HomeserverFetchOutcome::Unavailable => RegisterRecordPrefetch::Unavailable,
    }
}

fn record_not_found() -> CommandFailure {
    CommandFailure::refused(
        crate::refusal_audit::RefusalKind::NotFound,
        ErrorCode::NotFound,
        "The seller's homeserver has no such listing record.",
    )
}

fn record_unavailable() -> CommandFailure {
    CommandFailure::refused(
        crate::refusal_audit::RefusalKind::UpstreamUnavailable,
        ErrorCode::UpstreamUnavailable,
        "The seller's homeserver could not be reached. Try again shortly.",
    )
}

/// The registration the seller's record derives, validated as `listing.sync`
/// validates it.
fn public_candidate(
    payload: &RegisterListingPayload,
    record: &Value,
) -> Result<RegisterListingPayload, CommandFailure> {
    match registration_payload_from_record(&payload.seller_pubky, &payload.listing_id, record) {
        Ok(Some(candidate)) => validate_public_listing_payload(candidate).map_err(|issues| {
            CommandFailure::refused_with_issues(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's listing record does not satisfy registration invariants.",
                issues,
            )
        }),
        _ => Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The seller's listing record could not be interpreted for registration.",
        )),
    }
}

/// `listing.register` at the cap with a physical method. A Locks listing
/// registers as digital, so it is never at the cap with a physical method
/// unless its record publishes one.
fn refuse_unlimited_cap(payload: &RegisterListingPayload) -> Option<CommandFailure> {
    payload
        .registers_unlimited_cap_with_physical_methods()
        .then(unlimited_cap_refusal)
}

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RegisterListingPayload,
    prefetched: &RegisterRecordPrefetch,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    if actor != payload.seller_pubky {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the listing seller may register inventory.",
        )));
    }
    if !register_addresses_its_own_listing(actor, command, payload) {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The listing aggregate id does not match its seller and listing.",
        )));
    }
    if let Some(refusal) = refuse_unlimited_cap(payload) {
        return Ok(Err(refusal));
    }

    if payload.sale_format == SaleFormat::Auction {
        let public_record = match prefetched {
            RegisterRecordPrefetch::Found(record) => record,
            RegisterRecordPrefetch::NotFound => return Ok(Err(record_not_found())),
            RegisterRecordPrefetch::Unavailable => return Ok(Err(record_unavailable())),
            RegisterRecordPrefetch::NotNeeded | RegisterRecordPrefetch::NoHomeserver => {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidCommand,
                    ErrorCode::InvalidCommand,
                    "Listing registration is not enabled on this deployment.",
                )));
            }
        };
        let public_candidate = match public_candidate(payload, public_record) {
            Ok(candidate) => candidate,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let mut public_command = payload.clone();
        public_command.auction_reserve = None;
        if public_candidate != public_command {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::RevisionConflict,
                ErrorCode::RevisionConflict,
                "The public listing candidate does not match the registration command.",
            )));
        }
    }

    let current = fetch_listing_for_update(tx, &command.aggregate_id).await?;
    let current_revision = current.as_ref().map(|c| c.server_revision).unwrap_or(0);
    // A tombstoned listing reads as absent everywhere, so a client
    // re-creating the id registers it as new (`expected_revision` 0). The
    // deleted row's revision chain does not bind the re-created record.
    let reviving = current.as_ref().is_some_and(ListingRow::is_deleted);
    if command.expected_revision != current_revision
        && !(reviving && command.expected_revision == 0)
    {
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            current_revision,
        )));
    }
    if reviving
        && (payload.sale_format != SaleFormat::FixedPrice
            || current
                .as_ref()
                .is_some_and(|deleted| deleted.sale_format != "fixed_price"))
    {
        return Ok(Err(
            crate::handlers::sync_listing::recreated_auction_refused(),
        ));
    }
    if reviving {
        if let Some(refusal) = refuse_revival_without_record(prefetched) {
            return Ok(Err(refusal));
        }
    }
    if let Some(current) = current.as_ref().filter(|_| !reviving) {
        if payload.listing_revision <= current.listing_revision {
            return Ok(Err(CommandFailure::refused_with_revision(
                crate::refusal_audit::RefusalKind::RevisionConflict,
                ErrorCode::RevisionConflict,
                "The public listing revision must advance.",
                current_revision,
            )));
        }
        let requested_sale_format = match payload.sale_format {
            SaleFormat::FixedPrice => "fixed_price",
            SaleFormat::Auction => "auction",
        };
        if current.sale_format != requested_sale_format {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The listing sale format cannot change after registration.",
            )));
        }
    }
    let current_reserve = fetch_auction_reserve_for_update(tx, &command.aggregate_id).await?;
    match (
        current.as_ref(),
        current_reserve.as_ref(),
        payload.auction_reserve.as_ref(),
    ) {
        (None, None, Some(reserve)) => {
            if payload.listing_revision != 1
                || reserve.expected_record_revision != 0
                || reserve.record_revision != 1
            {
                return Ok(Err(CommandFailure::refused_with_revision(
                    crate::refusal_audit::RefusalKind::RevisionConflict,
                    ErrorCode::RevisionConflict,
                    "The reserve record revision is stale.",
                    0,
                )));
            }
        }
        (None, None, None) => {}
        (Some(listing), Some(stored), Some(reserve)) if listing.sale_format == "auction" => {
            if reserve.expected_record_revision != stored.record_revision
                || reserve.record_revision != stored.record_revision + 1
            {
                return Ok(Err(CommandFailure::refused_with_revision(
                    crate::refusal_audit::RefusalKind::RevisionConflict,
                    ErrorCode::RevisionConflict,
                    "The reserve record revision is stale.",
                    listing.server_revision,
                )));
            }
        }
        (Some(listing), None, None) if listing.sale_format == "fixed_price" => {}
        _ => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvariantViolation,
                ErrorCode::InvariantViolation,
                "The listing reserve authority is inconsistent.",
            )))
        }
    }

    if let Some(listing) = current.as_ref() {
        if let Some(failure) = validate_edit(listing, current_reserve.as_ref(), payload, now)? {
            return Ok(Err(failure));
        }
    }

    apply_registration(
        tx,
        actor,
        command.command_id,
        &command.aggregate_id,
        payload,
        current.as_ref(),
        current_reserve.as_ref(),
        "listing.registered",
        now,
    )
    .await
}

/// Writes (or refreshes) the listing aggregate from a validated registration
/// payload: the shared tail of `listing.register` and `listing.sync`. The
/// caller has already taken the row lock (`current` comes from
/// `fetch_listing_for_update`) and made its own authority and revision
/// decisions; this enforces only the inventory invariant that survives both
/// paths — quantity can never fall below committed (reserved + sold) stock.
///
/// A tombstoned `current` is revived: the tombstone clears, `recreated_at`
/// is stamped, the generation advances (drop bindings made against the
/// deleted generation stop gating), and available stock is the new record's quantity minus the
/// holds and sales the deleted listing still owes. Its earlier available
/// count (seller adjustments, restocks while deleted) is discarded.
// Eight positional facts of one write; both callers must supply all of them.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_registration(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command_id: Uuid,
    aggregate_id: &str,
    payload: &RegisterListingPayload,
    current: Option<&ListingRow>,
    current_reserve: Option<&AuctionReserveRow>,
    event_kind: &str,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let current_revision = current.as_ref().map(|c| c.server_revision).unwrap_or(0);
    if let Some(refusal) = refuse_unpublishable_preorder(payload, current, now) {
        return Ok(Err(refusal));
    }
    let variants: &[VariantStock] = if payload.sale_format == SaleFormat::FixedPrice {
        &payload.variants
    } else {
        &[]
    };
    let variant_rows = if current.is_some() {
        variant_stock::lock_rows(tx, aggregate_id).await?
    } else {
        Vec::new()
    };
    let plan = match variant_stock::plan(
        tx,
        aggregate_id,
        variants,
        &variant_rows,
        current.is_none(),
        current_revision,
        // A record at the revision the listing already holds is not an edit
        // (only `listing.sync` gets here with one), so it is not the authority
        // for a total the seller may have adjusted since.
        current
            .filter(|listing| {
                !listing.is_deleted() && payload.listing_revision <= listing.listing_revision
            })
            .map(|_| variant_stock::UnchangedRecord {
                enabled_sum: variants.iter().map(|variant| variant.quantity).sum(),
                all_variants_sum: payload.quantity,
            }),
    )
    .await?
    {
        Ok(plan) => plan,
        Err(failure) => return Ok(Err(failure)),
    };
    let reserved_now = current.as_ref().map(|c| c.reserved_quantity).unwrap_or(0);
    let sold_now = current.as_ref().map(|c| c.sold_quantity).unwrap_or(0);
    // The listing's quantity columns: the payload's quantity for a listing
    // that sells one variant, the sum of its variant rows otherwise.
    let (total, available, reserved, sold, state) = match &plan {
        StockPlan::Implicit
            if variant_rows
                .iter()
                .any(|row| row.is_implicit() && row.made_to_order) =>
        {
            // Made to order: the record's quantity is not a count, so the
            // listing keeps only its committed units and stays orderable.
            (
                reserved_now + sold_now,
                0,
                reserved_now,
                sold_now,
                variant_stock::listing_state(0, reserved_now, true),
            )
        }
        StockPlan::Implicit => {
            let committed = reserved_now + sold_now;
            // Stock is the enabled variants' units: a disabled variant is not
            // for sale. A client that sent no variants (or a record with no
            // enabled variant with an id) gives only `payload.quantity`, which
            // is the sum of every variant, as before.
            let enabled_stock = if variants.is_empty() {
                payload.quantity
            } else {
                variants.iter().map(|variant| variant.quantity).sum()
            };
            // Recomputing a listing that counted a disabled variant's stock
            // must not go below the units already reserved or sold. Only that
            // case clamps; an edit that lowers the sum of every variant below
            // committed units is refused, exactly as before.
            let clamped = enabled_stock < committed && payload.quantity >= committed;
            let quantity = if clamped { committed } else { enabled_stock };
            if quantity < committed {
                return Ok(Err(CommandFailure::refused_with_revision(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "Listing quantity cannot fall below committed inventory.",
                    current_revision,
                )));
            }
            // Nothing left to sell: `reserved` while a buyer's payment still
            // holds units (they may restock), `sold` once every committed unit
            // is sold.
            let state = if quantity == committed {
                if reserved_now > 0 {
                    "reserved"
                } else {
                    "sold"
                }
            } else {
                "available"
            };
            (
                quantity,
                quantity - committed,
                reserved_now,
                sold_now,
                state,
            )
        }
        StockPlan::Keep => {
            let (total, available) = current
                .as_ref()
                .map(|listing| (listing.total_quantity, listing.available_quantity))
                .unwrap_or_default();
            let made_to_order = current.is_some_and(|listing| listing.made_to_order);
            let state = variant_stock::listing_state(available, reserved_now, made_to_order);
            (total, available, reserved_now, sold_now, state)
        }
        StockPlan::PerVariant { rows, .. } => {
            let total: i64 = rows.iter().map(|row| row.total).sum();
            let available: i64 = rows.iter().map(|row| row.available).sum();
            let reserved: i64 = rows.iter().map(|row| row.reserved).sum();
            let sold: i64 = rows.iter().map(|row| row.sold).sum();
            let made_to_order = rows.iter().any(|row| row.enabled && row.made_to_order);
            let state = variant_stock::listing_state(available, reserved, made_to_order);
            (total, available, reserved, sold, state)
        }
    };
    let sale_format = match payload.sale_format {
        SaleFormat::FixedPrice => "fixed_price",
        SaleFormat::Auction => "auction",
    };
    let auction = payload.auction_terms.as_ref().map(|terms| {
        auction_json(
            terms,
            payload,
            current.as_ref().and_then(|c| c.auction.as_ref()),
            now,
        )
    });
    let new_revision = current_revision + 1;
    let variant_prices = variant_prices_json(variants);
    let preorder_ships_from = payload.preorder.map(|preorder| preorder.ships_from);
    let preorder_grace_days = payload
        .preorder
        .and_then(|preorder| i32::try_from(preorder.grace_days).ok());
    let fulfillment_methods: Vec<String> = payload
        .fulfillment_methods
        .iter()
        .map(|method| method.as_str().to_string())
        .collect();

    if current.is_some() {
        if let StockPlan::PerVariant { rows, convert } = &plan {
            variant_stock::write_plan(tx, aggregate_id, rows, *convert).await?;
        }
    }
    let written = if current.is_none() {
        let inserted = sqlx::query(
            "INSERT INTO listings (aggregate_id, seller_pubky, listing_id, title, \
             listing_revision, content_hash, server_revision, state, total_quantity, \
             available_quantity, reserved_quantity, sold_quantity, unit_price_amount_minor, \
             unit_price_currency, unit_price_exponent, shipping_minor, sale_format, auction, \
             fulfillment_methods, digital_lock_policy_uri, digital_lock_criterion_id, updated_at, \
             variant_prices, preorder_ships_from, preorder_grace_days) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25) \
             ON CONFLICT (aggregate_id) DO NOTHING",
        )
        .bind(aggregate_id)
        .bind(&payload.seller_pubky)
        .bind(&payload.listing_id)
        .bind(&payload.title)
        .bind(payload.listing_revision)
        .bind(&payload.content_hash)
        .bind(new_revision)
        .bind(state)
        .bind(total)
        .bind(available)
        .bind(reserved)
        .bind(sold)
        .bind(payload.unit_price.amount_minor)
        .bind(&payload.unit_price.currency)
        .bind(payload.unit_price.exponent)
        .bind(payload.shipping_minor)
        .bind(sale_format)
        .bind(&auction)
        .bind(&fulfillment_methods)
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.policy_uri))
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.criterion_id))
        .bind(now)
        .bind(&variant_prices)
        .bind(preorder_ships_from)
        .bind(preorder_grace_days)
        .execute(&mut **tx)
        .await?;
        inserted.rows_affected() == 1
    } else {
        let updated = sqlx::query(
            "UPDATE listings SET title = $3, listing_revision = $4, content_hash = $5, \
             server_revision = $6, state = $7, total_quantity = $8, available_quantity = $9, \
             unit_price_amount_minor = $10, unit_price_currency = $11, unit_price_exponent = $12, \
             shipping_minor = $13, sale_format = $14, auction = $15, fulfillment_methods = $16, \
             digital_lock_policy_uri = $17, digital_lock_criterion_id = $18, updated_at = $19, \
             variant_prices = $20, preorder_ships_from = $21, preorder_grace_days = $22, \
             recreated_at = CASE WHEN deleted_at IS NULL THEN recreated_at ELSE $19 END, \
             generation = CASE WHEN deleted_at IS NULL THEN generation ELSE generation + 1 END, \
             deleted_at = NULL, deleted_event_cursor = NULL \
             WHERE aggregate_id = $1 AND server_revision = $2",
        )
        .bind(aggregate_id)
        .bind(current_revision)
        .bind(&payload.title)
        .bind(payload.listing_revision)
        .bind(&payload.content_hash)
        .bind(new_revision)
        .bind(state)
        .bind(total)
        .bind(available)
        .bind(payload.unit_price.amount_minor)
        .bind(&payload.unit_price.currency)
        .bind(payload.unit_price.exponent)
        .bind(payload.shipping_minor)
        .bind(sale_format)
        .bind(&auction)
        .bind(&fulfillment_methods)
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.policy_uri))
        .bind(payload.digital_lock.as_ref().map(|lock| &lock.criterion_id))
        .bind(now)
        .bind(&variant_prices)
        .bind(preorder_ships_from)
        .bind(preorder_grace_days)
        .execute(&mut **tx)
        .await?;
        updated.rows_affected() == 1
    };
    if !written {
        let latest = current_listing_revision(tx, aggregate_id).await?;
        return Ok(Err(CommandFailure::refused_with_revision(
            crate::refusal_audit::RefusalKind::RevisionConflict,
            ErrorCode::RevisionConflict,
            "The listing revision is stale.",
            latest,
        )));
    }
    if current.is_none() {
        if let StockPlan::PerVariant { rows, .. } = &plan {
            variant_stock::write_plan(tx, aggregate_id, rows, true).await?;
        }
    }

    if let Some(reserve) = payload.auction_reserve.as_ref() {
        persist_reserve(
            tx,
            aggregate_id,
            payload.listing_revision,
            command_id,
            reserve,
            current_reserve,
            now,
        )
        .await?;
    }

    let event_id = insert_event(
        tx,
        command_id,
        aggregate_id,
        new_revision,
        actor,
        event_kind,
        now,
    )
    .await?;

    let listing = fetch_listing(tx, aggregate_id)
        .await?
        .expect("listing was just written in this transaction");
    let reserve = fetch_auction_reserve(tx, aggregate_id).await?;
    let projection = listing
        .projection_for_actor_with_auction(actor, reserve.as_ref(), None)
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    Ok(Ok(HandlerSuccess {
        revision: new_revision,
        event_ids: vec![event_id],
        result: json!({ "kind": "listing", "listing": projection }),
    }))
}

/// A preorder's ships-from date is checked against server time when it is
/// published: on a new or revived listing, or when the record changes it.
/// A listing whose date has passed still syncs and edits as long as the
/// date stays the same.
fn refuse_unpublishable_preorder(
    payload: &RegisterListingPayload,
    current: Option<&ListingRow>,
    now: DateTime<Utc>,
) -> Option<CommandFailure> {
    let preorder = payload.preorder?;
    let unchanged = current.is_some_and(|listing| {
        !listing.is_deleted()
            && listing.preorder_ships_from == Some(preorder.ships_from)
            && listing.preorder_grace_days.map(i64::from) == Some(preorder.grace_days)
    });
    if unchanged || preorder.ships_from_publishable(now) {
        return None;
    }
    Some(CommandFailure::refused_with_reason(
        crate::refusal_audit::RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "A preorder must ship from a date after today and at most 90 days out.",
        PREORDER_SHIPS_FROM_OUT_OF_RANGE,
    ))
}

/// The refusal reason for a preorder date in the past or too far out.
pub const PREORDER_SHIPS_FROM_OUT_OF_RANGE: &str = "preorder_ships_from_out_of_range";

/// The enabled variants that sell at their own price, as the listing row
/// stores and projects them; `None` when every variant sells at the
/// listing's price.
pub(crate) fn variant_prices_json(variants: &[VariantStock]) -> Option<Value> {
    let priced: Vec<Value> = variants
        .iter()
        .filter_map(|variant| {
            variant.unit_price.as_ref().map(|price| {
                json!({
                    "variant_id": variant.id,
                    "unit_price": money_json(
                        price.amount_minor,
                        &price.currency,
                        price.exponent,
                    ),
                })
            })
        })
        .collect();
    (!priced.is_empty()).then(|| Value::Array(priced))
}

async fn persist_reserve(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
    listing_revision: i64,
    command_id: Uuid,
    reserve: &AuctionReserve,
    current: Option<&AuctionReserveRow>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let reserve_price = reserve.reserve_price.as_ref();
    if current.is_none() {
        sqlx::query(
            "INSERT INTO listing_auction_reserves \
             (listing_aggregate_id, listing_revision, record_revision, reserve_amount_minor, \
              reserve_currency, reserve_exponent, last_command_id, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(aggregate_id)
        .bind(listing_revision)
        .bind(reserve.record_revision)
        .bind(reserve_price.map(|money| money.amount_minor))
        .bind(reserve_price.map(|money| &money.currency))
        .bind(reserve_price.map(|money| money.exponent))
        .bind(command_id)
        .bind(now)
        .execute(&mut **tx)
        .await?;
    } else {
        let updated = sqlx::query(
            "UPDATE listing_auction_reserves SET listing_revision = $2, record_revision = $3, \
             reserve_amount_minor = $4, reserve_currency = $5, reserve_exponent = $6, \
             last_command_id = $7, updated_at = $8 \
             WHERE listing_aggregate_id = $1 AND record_revision = $9",
        )
        .bind(aggregate_id)
        .bind(listing_revision)
        .bind(reserve.record_revision)
        .bind(reserve_price.map(|money| money.amount_minor))
        .bind(reserve_price.map(|money| &money.currency))
        .bind(reserve_price.map(|money| money.exponent))
        .bind(command_id)
        .bind(now)
        .bind(reserve.expected_record_revision)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(sqlx::Error::Protocol(
                "reserve compare-and-swap failed".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_edit(
    current: &ListingRow,
    current_reserve: Option<&AuctionReserveRow>,
    payload: &RegisterListingPayload,
    now: DateTime<Utc>,
) -> Result<Option<CommandFailure>, sqlx::Error> {
    if current.sale_format
        != match payload.sale_format {
            SaleFormat::FixedPrice => "fixed_price",
            SaleFormat::Auction => "auction",
        }
    {
        return Ok(Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The listing sale format cannot change after registration.",
        )));
    }
    if current.sale_format != "auction" {
        return Ok(None);
    }
    let Some(stored_reserve) = current_reserve else {
        return Ok(Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "The auction reserve authority is missing.",
        )));
    };
    let Some(candidate_reserve) = payload.auction_reserve.as_ref() else {
        return Ok(Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvariantViolation,
            ErrorCode::InvariantViolation,
            "The auction reserve authority is missing.",
        )));
    };
    let stored_auction = current
        .auction
        .as_ref()
        .ok_or_else(|| sqlx::Error::Protocol("auction state is missing".to_string()))
        .and_then(|value| {
            AuctionState::from_value(value)
                .map_err(|error| sqlx::Error::Protocol(error.to_string()))
        })?;
    let candidate_terms = payload
        .auction_terms
        .as_ref()
        .expect("validated auction payload has terms");
    let terms_changed = current.unit_price_amount_minor != payload.unit_price.amount_minor
        || current.unit_price_currency != payload.unit_price.currency
        || current.unit_price_exponent != payload.unit_price.exponent
        || stored_auction.starts_at != candidate_terms.starts_at
        || stored_auction.ends_at != candidate_terms.ends_at
        || stored_auction.minimum_increment != candidate_terms.minimum_increment
        || stored_auction.anti_sniping_window_seconds
            != candidate_terms.anti_sniping_window_seconds
        || stored_auction.anti_sniping_extension_seconds
            != candidate_terms.anti_sniping_extension_seconds;
    if terms_changed {
        return Ok(Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Auction terms cannot change after registration.",
        )));
    }
    let stored_price = stored_reserve.reserve_price();
    let candidate_price = candidate_reserve.reserve_price.as_ref();
    if stored_price.as_ref() == candidate_price {
        return Ok(None);
    }
    let allowed_decrease = match (stored_price.as_ref(), candidate_price) {
        (Some(stored), Some(candidate)) => {
            candidate.same_asset(stored)
                && candidate.amount_minor < stored.amount_minor
                && stored_auction.status == "scheduled"
                && now < stored_auction.starts_at
                && stored_auction.bid_count == 0
        }
        _ => false,
    };
    if !allowed_decrease {
        return Ok(Some(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The auction reserve change is not permitted.",
        )));
    }
    Ok(None)
}

fn auction_json(
    terms: &marketplace_domain::commands::AuctionTerms,
    payload: &RegisterListingPayload,
    current: Option<&Value>,
    now: DateTime<Utc>,
) -> Value {
    let existing = |field: &str| current.and_then(|value| value.get(field)).cloned();
    let status = existing("status").unwrap_or_else(|| {
        if terms.starts_at > now {
            json!("scheduled")
        } else {
            json!("active")
        }
    });
    let unit_price = money_json(
        payload.unit_price.amount_minor,
        &payload.unit_price.currency,
        payload.unit_price.exponent,
    );
    json!({
        "starts_at": format_timestamp(terms.starts_at),
        "ends_at": format_timestamp(terms.ends_at),
        "minimum_increment": money_json(
            terms.minimum_increment.amount_minor,
            &terms.minimum_increment.currency,
            terms.minimum_increment.exponent,
        ),
        "anti_sniping_window_seconds": terms.anti_sniping_window_seconds,
        "anti_sniping_extension_seconds": terms.anti_sniping_extension_seconds,
        "status": status,
        "current_price": existing("current_price").unwrap_or(unit_price),
        "leader_pubky": existing("leader_pubky").unwrap_or(Value::Null),
        "bid_count": existing("bid_count").unwrap_or(json!(0)),
    })
}

#[cfg(test)]
mod tests {
    use super::{refuse_revival_without_record, RegisterRecordPrefetch};
    use marketplace_domain::ErrorCode;
    use serde_json::json;

    // A listing tombstoned after the executor decided no record was needed
    // reaches the handler with `NotNeeded`; only a found record revives.
    #[test]
    fn only_a_found_record_revives_a_tombstoned_listing() {
        assert!(refuse_revival_without_record(&RegisterRecordPrefetch::Found(json!({}))).is_none());
        for (prefetched, code) in [
            (RegisterRecordPrefetch::NotFound, ErrorCode::NotFound),
            (
                RegisterRecordPrefetch::Unavailable,
                ErrorCode::UpstreamUnavailable,
            ),
            (RegisterRecordPrefetch::NotNeeded, ErrorCode::InvalidState),
            (
                RegisterRecordPrefetch::NoHomeserver,
                ErrorCode::InvalidState,
            ),
        ] {
            let refusal = refuse_revival_without_record(&prefetched).expect("refused");
            assert_eq!(refusal.code(), code);
        }
    }
}
