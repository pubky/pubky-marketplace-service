//! `listing.sync`: service-side registration from the canonical homeserver
//! record, issuable by ANY authenticated actor.
//!
//! `listing.register` requires the seller because the client supplies the
//! registration fields. Here the service fetches the seller-signed record
//! from the seller-owned homeserver path itself, so the record's provenance
//! substitutes for the actor check: whoever asks, the data registered is
//! exactly what the seller published. This heals listings published before
//! durable-mode registration existed (no aggregate → buyers dead-end at
//! checkout, and only the seller could previously fix it).
//!
//! Sync is convergent, not optimistic: callers pass `expected_revision` 0
//! without knowing the aggregate's state, a pre-existing aggregate is never
//! a conflict, and a record whose revision does not advance past the
//! aggregate's is a successful no-op returning the current server revision.
//!
//! The homeserver reads (the record and, when it is missing, the deletion
//! proof) run in [`prefetch`], before the command's transaction, so no
//! database connection is held while the homeserver answers.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    validate_public_listing_payload, Command, CommandPayload, SaleFormat, SyncListingPayload,
};
use marketplace_domain::{ids, ErrorCode};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};

use crate::handlers::register_listing::apply_registration;
use crate::handlers::{fetch_auction_reserve_for_update, fetch_listing, fetch_listing_for_update};
use crate::homeserver::{
    registration_payload_from_record, HomeserverFetchOutcome, HomeserverListingClient,
    MalformedDigitalLock,
};
use crate::listing_deletion::{
    confirm_missing_record_deleted, observe_live, reread_missing_record, tombstone,
    DeletionAuthority, DeletionCheck, LiveListing, RecordAfterMiss,
};
use crate::result::{CommandFailure, HandlerResult, HandlerSuccess};

/// What a `listing.sync` read from the seller's homeserver before its
/// transaction.
#[derive(Debug)]
pub enum SyncPrefetch {
    /// No read: not a sync, or one the handler refuses before reading.
    NotNeeded,
    Found(Value),
    /// The record is missing. With the listing as it was live before the
    /// deletion proof was read, and that proof; `None` when no live listing
    /// was observed.
    NotFound(Option<(LiveListing, DeletionCheck)>),
    Unavailable,
}

/// The `listing.sync` payload whose record the executor must read before
/// the transaction, or `None` for every other command and for a sync the
/// handler refuses without reading (no homeserver, or an aggregate id that
/// is not its seller's and listing's).
pub fn record_request(command: &Command, has_homeserver: bool) -> Option<&SyncListingPayload> {
    let CommandPayload::SyncListing(payload) = &command.payload else {
        return None;
    };
    (has_homeserver
        && command.aggregate_id
            == ids::listing_aggregate_id(&payload.seller_pubky, &payload.listing_id))
    .then_some(payload)
}

/// Reads the seller's record. When it is missing, under `pkarr` the seller's
/// homeserver is looked up anew and the record read again, so a record on the
/// homeserver the seller moved to is synced, and a homeserver that cannot be
/// read is `Unavailable`. A record still missing, with the listing live, gets
/// its deletion proof. The handler tombstones only if the listing's
/// `record_epoch` is still the observed one, so a register, sync or revival
/// accepted meanwhile wins.
pub async fn prefetch(
    pool: &PgPool,
    homeserver: &dyn HomeserverListingClient,
    command: &Command,
    payload: &SyncListingPayload,
) -> Result<SyncPrefetch, sqlx::Error> {
    Ok(
        match homeserver
            .fetch_listing(&payload.seller_pubky, &payload.listing_id)
            .await
        {
            HomeserverFetchOutcome::Found(record) => SyncPrefetch::Found(record),
            HomeserverFetchOutcome::Unavailable => SyncPrefetch::Unavailable,
            HomeserverFetchOutcome::NotFound => {
                // Observed before any further read, so the tombstone below
                // is fenced on what was live before the proof was read.
                let observed = observe_live(pool, &command.aggregate_id).await?;
                match reread_missing_record(homeserver, &payload.seller_pubky, &payload.listing_id)
                    .await
                {
                    RecordAfterMiss::Found(record) => SyncPrefetch::Found(record),
                    RecordAfterMiss::Unavailable => SyncPrefetch::Unavailable,
                    RecordAfterMiss::Missing {
                        homeserver: record_homeserver,
                    } => {
                        let proof = match observed {
                            Some(observed) => Some((
                                observed,
                                confirm_missing_record_deleted(
                                    homeserver,
                                    &payload.seller_pubky,
                                    &payload.listing_id,
                                    record_homeserver,
                                )
                                .await,
                            )),
                            None => None,
                        };
                        SyncPrefetch::NotFound(proof)
                    }
                }
            }
        },
    )
}

pub async fn handle(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SyncListingPayload,
    has_homeserver: bool,
    prefetched: &SyncPrefetch,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let mut outcome = sync(tx, actor, command, payload, has_homeserver, prefetched, now).await?;
    // A sync that converged the listing to its record but could not convert it
    // to per-variant stock says so: `variant_conversion` names the state
    // (`deferred`, or `blocked` when the record itself has to change) and why.
    if let Ok(success) = outcome.as_mut() {
        let marker: Option<(String, i32)> = sqlx::query_as(
            "SELECT reason, attempts FROM listing_variant_conversion_blocks \
             WHERE listing_aggregate_id = $1",
        )
        .bind(&command.aggregate_id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some((reason, attempts)) = marker {
            success.result["variant_conversion"] = json!({
                "state": crate::variant_stock::conversion_state(&reason),
                "reason": reason,
                "attempts": attempts,
            });
        }
    }
    Ok(outcome)
}

async fn sync(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &SyncListingPayload,
    has_homeserver: bool,
    prefetched: &SyncPrefetch,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    // Fail closed: without a configured homeserver there is no canonical
    // record to derive registration from.
    if !has_homeserver {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "Listing sync is not enabled on this deployment.",
        )));
    }

    let expected_aggregate_id =
        ids::listing_aggregate_id(&payload.seller_pubky, &payload.listing_id);
    if command.aggregate_id != expected_aggregate_id {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidCommand,
            ErrorCode::InvalidCommand,
            "The listing aggregate id does not match its seller and listing.",
        )));
    }

    let record = match prefetched {
        SyncPrefetch::Found(record) => record,
        SyncPrefetch::NotFound(proof) => {
            return settle_missing_record(tx, actor, command, proof.as_ref(), now).await;
        }
        // `NotNeeded` reaches here only if the executor and the refusals
        // above disagree; nothing was read, so nothing is registered.
        SyncPrefetch::Unavailable | SyncPrefetch::NotNeeded => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::UpstreamUnavailable,
                ErrorCode::UpstreamUnavailable,
                "The seller's homeserver could not be reached. Try again shortly.",
            )));
        }
    };

    let registration = match registration_payload_from_record(
        &payload.seller_pubky,
        &payload.listing_id,
        record,
    ) {
        Ok(Some(registration)) => registration,
        Ok(None) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's listing record could not be interpreted for registration.",
            )))
        }
        // A present-but-invalid digitalLock refuses the sync rather than
        // healing the listing to no lock.
        Err(MalformedDigitalLock) => {
            return Ok(Err(CommandFailure::refused(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's listing record carries an invalid Locks payment lock.",
            )))
        }
    };
    if registration.registers_unlimited_cap_with_physical_methods() {
        return Ok(Err(
            crate::handlers::register_listing::unlimited_cap_refusal(),
        ));
    }
    // The derived payload must satisfy exactly the invariants
    // `listing.register` enforces; a record that fails them cannot back a
    // registered aggregate.
    let registration = match validate_public_listing_payload(registration) {
        Ok(registration) => registration,
        Err(issues) => {
            return Ok(Err(CommandFailure::refused_with_issues(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The seller's listing record does not satisfy registration invariants.",
                issues,
            )));
        }
    };

    let current = fetch_listing_for_update(tx, &command.aggregate_id).await?;
    if let Some(deleted) = current.as_ref().filter(|listing| listing.is_deleted()) {
        // The record was re-created at a deleted id. It revives the
        // aggregate with stock derived from this record alone; the
        // tombstoned row's revision chain does not apply to it.
        if deleted.sale_format != "fixed_price"
            || registration.sale_format != SaleFormat::FixedPrice
        {
            return Ok(Err(recreated_auction_refused()));
        }
        return apply_registration(
            tx,
            actor,
            command.command_id,
            &command.aggregate_id,
            &registration,
            Some(deleted),
            None,
            "listing.synced",
            now,
        )
        .await;
    }
    if let Some(current_auction) = current
        .as_ref()
        .filter(|listing| listing.sale_format == "auction")
    {
        let reserve = fetch_auction_reserve_for_update(tx, &command.aggregate_id).await?;
        let Some(reserve) = reserve.as_ref() else {
            return Ok(Err(seller_registration_required()));
        };
        if registration.sale_format != SaleFormat::Auction {
            return Ok(Err(seller_registration_required()));
        }
        if registration.listing_revision > current_auction.listing_revision {
            return Ok(Err(seller_registration_required()));
        }
        confirm_record(tx, &command.aggregate_id).await?;
        let viewer_bid =
            crate::handlers::auction::viewer_bid_projection(tx, current_auction, actor).await?;
        let projection = current_auction
            .projection_for_actor_with_auction(actor, Some(reserve), viewer_bid)
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
        return Ok(Ok(HandlerSuccess {
            revision: current_auction.server_revision,
            event_ids: vec![],
            result: json!({ "kind": "listing", "listing": projection }),
        }));
    }
    if registration.sale_format == SaleFormat::Auction {
        return Ok(Err(seller_registration_required()));
    }
    if let Some(current) = &current {
        // Convergent no-op: the aggregate already reflects this record
        // revision (or a newer one) — with narrow healing exceptions. When
        // the service's own derivation evolves (shipping was added after
        // listings were first registered; fulfillment_methods came with
        // local pickup), the record hasn't changed but what the service
        // reads out of it has. An equal-revision sync heals exactly those
        // derived fields: both are inventory-neutral, so nothing else
        // (quantity, price, state) is touched. Sync carries no pickup
        // DETAILS and can never null them (§A4).
        if registration.listing_revision <= current.listing_revision {
            // A listing registered before per-variant stock (or by a client
            // that sent no variants) keeps one shared total while its record
            // lists several enabled variants. An equal-revision sync moves it
            // to per-variant rows as soon as its committed stock can be
            // attributed to variants exactly.
            if registration.listing_revision == current.listing_revision
                && registration.sale_format == SaleFormat::FixedPrice
                && registration.variants.len() >= 2
            {
                let rows = crate::variant_stock::lock_rows(tx, &command.aggregate_id).await?;
                if !crate::variant_stock::tracks_variants(&rows) {
                    let plan = crate::variant_stock::plan(
                        tx,
                        &command.aggregate_id,
                        &registration.variants,
                        &rows,
                        false,
                        current.server_revision,
                        Some(crate::variant_stock::UnchangedRecord {
                            enabled_sum: registration
                                .variants
                                .iter()
                                .map(|variant| variant.quantity)
                                .sum(),
                            all_variants_sum: registration.quantity,
                        }),
                    )
                    .await?;
                    if matches!(plan, Ok(crate::variant_stock::StockPlan::PerVariant { .. })) {
                        return apply_registration(
                            tx,
                            actor,
                            command.command_id,
                            &command.aggregate_id,
                            &registration,
                            Some(current),
                            None,
                            "listing.synced",
                            now,
                        )
                        .await;
                    }
                }
            }
            // A listing that sells one variant counted the stock of the
            // record's disabled variants in its total. At the same record
            // revision the total is recomputed from the enabled variants when
            // it is still exactly that legacy sum (a seller's adjustment has
            // moved it otherwise, and stays), never below the units already
            // reserved or sold.
            if registration.listing_revision == current.listing_revision
                && registration.sale_format == SaleFormat::FixedPrice
                && !registration.variants.is_empty()
                && current.total_quantity == registration.quantity
            {
                let enabled: i64 = registration
                    .variants
                    .iter()
                    .map(|variant| variant.quantity)
                    .sum();
                let committed = current.reserved_quantity + current.sold_quantity;
                let target = enabled.max(committed);
                if target != current.total_quantity
                    && !crate::variant_stock::tracks_variants(
                        &crate::variant_stock::lock_rows(tx, &command.aggregate_id).await?,
                    )
                {
                    return apply_registration(
                        tx,
                        actor,
                        command.command_id,
                        &command.aggregate_id,
                        &registration,
                        Some(current),
                        None,
                        "listing.synced",
                        now,
                    )
                    .await;
                }
            }
            let registration_methods: Vec<String> = registration
                .fulfillment_methods
                .iter()
                .map(|method| method.as_str().to_string())
                .collect();
            // The healing exception covers the derived fields the service
            // reads out of the record after listings were first registered:
            // shipping (added earlier), fulfillment_methods (added with local
            // pickup) and the variants' own prices (migration 0075). All are
            // inventory-neutral, so nothing else (quantity, listing price,
            // state) is touched.
            let registration_variant_prices =
                crate::handlers::register_listing::variant_prices_json(&registration.variants);
            let registration_zones =
                crate::handlers::register_listing::shipping_zones_json(&registration);
            if registration.listing_revision == current.listing_revision
                && (registration.shipping_minor != current.shipping_minor
                    || registration.shipping_profile_id != current.shipping_profile_id
                    || registration_zones != current.shipping_zones
                    || registration_variant_prices != current.variant_prices
                    || registration_methods != current.fulfillment_methods
                    || registration
                        .digital_lock
                        .as_ref()
                        .map(|lock| &lock.policy_uri)
                        != current.digital_lock_policy_uri.as_ref()
                    || registration
                        .digital_lock
                        .as_ref()
                        .map(|lock| &lock.criterion_id)
                        != current.digital_lock_criterion_id.as_ref())
            {
                let healed: crate::model::ListingRow = sqlx::query_as(&format!(
                    "UPDATE listings SET server_revision = server_revision + 1, \
                     shipping_minor = $2, fulfillment_methods = $3, digital_lock_policy_uri = $4, \
                     digital_lock_criterion_id = $5, updated_at = $6, variant_prices = $7, \
                     shipping_profile_id = $8, shipping_zones = $9 \
                     WHERE aggregate_id = $1 \
                     RETURNING {}",
                    crate::handlers::LISTING_COLUMNS
                ))
                .bind(&command.aggregate_id)
                .bind(registration.shipping_minor)
                .bind(&registration_methods)
                .bind(
                    registration
                        .digital_lock
                        .as_ref()
                        .map(|lock| &lock.policy_uri),
                )
                .bind(
                    registration
                        .digital_lock
                        .as_ref()
                        .map(|lock| &lock.criterion_id),
                )
                .bind(now)
                .bind(&registration_variant_prices)
                .bind(&registration.shipping_profile_id)
                .bind(&registration_zones)
                .fetch_one(&mut **tx)
                .await?;
                let event_id = crate::executor::insert_event(
                    tx,
                    command.command_id,
                    &command.aggregate_id,
                    healed.server_revision,
                    actor,
                    "listing.synced",
                    now,
                )
                .await?;
                return Ok(Ok(HandlerSuccess {
                    revision: healed.server_revision,
                    event_ids: vec![event_id],
                    result: json!({
                        "kind": "listing",
                        "listing": healed
                            .projection_for_actor_with_auction(actor, None, None)
                            .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
                    }),
                }));
            }
            confirm_record(tx, &command.aggregate_id).await?;
            return Ok(Ok(HandlerSuccess {
                revision: current.server_revision,
                event_ids: vec![],
                result: json!({
                    "kind": "listing",
                    "listing": current
                        .projection_for_actor_with_auction(actor, None, None)
                        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?
                }),
            }));
        }
    }

    apply_registration(
        tx,
        actor,
        command.command_id,
        &command.aggregate_id,
        &registration,
        current.as_ref(),
        None,
        "listing.synced",
        now,
    )
    .await
}

/// A sync that found the seller's record and changes nothing still proves
/// the record exists after any delete confirmed before it: it advances the
/// listing's `record_epoch` on the row it holds locked, so such a delete
/// cannot tombstone the listing afterwards (0051).
async fn confirm_record(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE listings SET record_epoch = record_epoch + 1 WHERE aggregate_id = $1")
        .bind(aggregate_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The fetch found no record. A registered listing whose deletion the
/// seller's event stream confirms ([`prefetch`]) is tombstoned; an already
/// tombstoned one is a convergent no-op. Anything else keeps the definitive
/// 404.
async fn settle_missing_record(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    proof: Option<&(LiveListing, DeletionCheck)>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let not_found = || {
        CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The seller's homeserver has no such listing record.",
        )
    };
    let Some(current) = fetch_listing(tx, &command.aggregate_id).await? else {
        return Ok(Err(not_found()));
    };
    if current.is_deleted() {
        return Ok(Ok(deleted_success(&current, vec![])));
    }
    let Some((
        observed,
        DeletionCheck::Deleted {
            cursor,
            homeserver: event_homeserver,
        },
    )) = proof
    else {
        return Ok(Err(not_found()));
    };
    match tombstone(
        tx,
        DeletionAuthority::Command,
        &command.aggregate_id,
        cursor,
        event_homeserver.as_deref(),
        observed.record_epoch,
        actor,
        command.command_id,
        now,
    )
    .await?
    {
        Some((deleted, event_id)) => Ok(Ok(deleted_success(&deleted, vec![event_id]))),
        None => match fetch_listing(tx, &command.aggregate_id).await? {
            Some(deleted) if deleted.is_deleted() => Ok(Ok(deleted_success(&deleted, vec![]))),
            _ => Ok(Err(not_found())),
        },
    }
}

fn deleted_success(
    listing: &crate::model::ListingRow,
    event_ids: Vec<uuid::Uuid>,
) -> HandlerSuccess {
    HandlerSuccess {
        revision: listing.server_revision,
        event_ids,
        result: json!({ "kind": "listing_deleted", "listing": listing.deleted_projection() }),
    }
}

pub(crate) fn recreated_auction_refused() -> CommandFailure {
    CommandFailure::refused(
        crate::refusal_audit::RefusalKind::InvalidState,
        ErrorCode::InvalidState,
        "This listing id was deleted. Publish the auction under a new id.",
    )
}

fn seller_registration_required() -> CommandFailure {
    CommandFailure::refused(
        crate::refusal_audit::RefusalKind::InvalidState,
        ErrorCode::SellerRegistrationRequired,
        "The seller must register this auction with its private reserve authority.",
    )
}
