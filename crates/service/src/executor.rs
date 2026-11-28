//! Command envelope execution per ADR-0019 §3.
//!
//! Exactly one accepted result exists per actor + command id. An exact
//! replay returns the stored result without re-executing; a replay with a
//! different canonical payload is a conflict. Failures are never stored, so
//! a retried command that previously failed re-executes (matching the
//! TypeScript prototype engine).

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use marketplace_domain::commands::{parse_command, validate_actor, Command, CommandPayload};
use marketplace_domain::ErrorCode;
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::time::Instant;
use uuid::Uuid;

use crate::logging::{log_command, log_invalid_command};
use crate::model::redact_command_result;
use crate::refusal_audit::{CommandKind, RefusalKind, SurfaceKind};
use crate::result::{success_body, CommandFailure, HandlerResult};
use crate::AppState;

pub async fn execute(
    state: &AppState,
    actor: &str,
    raw: &Value,
) -> Result<(StatusCode, Value), sqlx::Error> {
    execute_with_edge(
        state,
        actor,
        raw,
        &crate::digital_region::EdgeContext::default(),
    )
    .await
}

/// [`execute`] for a command that arrived over HTTP, with what the trusted
/// edge said about the request (the client's country).
pub async fn execute_with_edge(
    state: &AppState,
    actor: &str,
    raw: &Value,
    edge: &crate::digital_region::EdgeContext,
) -> Result<(StatusCode, Value), sqlx::Error> {
    let started = Instant::now();
    if let Err(issues) = validate_actor(actor) {
        log_invalid_command(None, started.elapsed().as_millis() as u64);
        return Ok(failure_response(&CommandFailure::invalid_envelope(issues)));
    }
    let command = match parse_command(raw) {
        Ok(command) => command,
        Err(issues) => {
            log_invalid_command(Some(actor), started.elapsed().as_millis() as u64);
            let failure = CommandFailure::invalid_envelope(issues);
            let response = failure_response(&failure);
            enqueue_refusal(
                state,
                actor,
                state.clock.now(),
                SurfaceKind::V1Command,
                CommandKind::InvalidEnvelope,
                RefusalKind::InvalidEnvelope,
                None,
            );
            return Ok(response);
        }
    };
    let request_hash = command.request_hash();

    // In single-seller mode a command for any other seller is refused before
    // any other gate. A command id that already has a stored result is a
    // replay and is answered from it.
    if let Some(failure) = crate::store_seller::command_refusal(state, &command).await? {
        if stored_command_result(&state.pool, actor, command.command_id)
            .await?
            .is_none()
        {
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                command.expected_revision,
                started.elapsed().as_millis() as u64,
            );
            if failure.is_audited() {
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
            }
            return Ok(failure_response(&failure));
        }
    }

    // Reject commands whose currency is outside what the configured FX feed
    // quotes (for example CHF/JPY/CAD/AUD when configured with Bitstamp, or
    // non-USD currencies on a USD-only single-pair URL) upfront with 422
    // INVALID_COMMAND before any upstream IO or writes. Commands whose payload
    // omits the currency (`offer.accept`, `offer.checkout`, `checkout.create`)
    // resolve it locally from PostgreSQL while preserving auth, state, and
    // revision precedence. A command id with a stored result is still answered
    // as a replay.
    if let Some(failure) = resolve_provider_currency_refusal(state, actor, &command).await? {
        if stored_command_result(&state.pool, actor, command.command_id)
            .await?
            .is_none()
        {
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                command.expected_revision,
                started.elapsed().as_millis() as u64,
            );
            enqueue_refusal(
                state,
                actor,
                state.clock.now(),
                SurfaceKind::V1Command,
                command_kind(&command.payload),
                failure.refusal_kind(),
                Some(command.command_id),
            );
            return Ok(failure_response(&failure));
        }
    }

    // A command that sells a listing is refused when the index says the
    // listing, or its seller, is moderated, and when the seller's current
    // record says it is not for sale (any non-active state for a new
    // commitment, a removal for a continuation). Both are read before the
    // transaction, so a slow index or homeserver never holds a domain
    // connection. A command id that already has a stored result is a replay
    // and is answered from it.
    let moderation = state.listing_moderation.as_deref();
    let lifecycle = state.homeserver.clone().and_then(|homeserver| {
        crate::listing_lifecycle::gate_for(&command.payload).map(|gate| (homeserver, gate))
    });
    if moderation.is_some() || lifecycle.is_some() {
        let listings =
            crate::listing_moderation::listings_a_command_sells(&state.pool, actor, &command)
                .await?;
        let lifecycle_listings: Vec<(String, String)> = match (&lifecycle, &command.payload) {
            (None, _) => Vec::new(),
            (Some(_), CommandPayload::CloseAuction(_)) => {
                crate::listing_lifecycle::auction_being_closed(&state.pool, &command.aggregate_id)
                    .await?
            }
            (Some(_), _) => listings
                .iter()
                .filter(|listing| listing.live)
                .map(|listing| (listing.seller_pubky.clone(), listing.listing_id.clone()))
                .collect(),
        };
        if (!listings.is_empty() || !lifecycle_listings.is_empty())
            && stored_command_result(&state.pool, actor, command.command_id)
                .await?
                .is_none()
        {
            let (moderated, not_on_sale) = tokio::join!(
                async {
                    let gate = moderation?;
                    for listing in &listings {
                        if gate.check(&listing.seller_pubky, &listing.listing_id).await
                            == crate::listing_moderation::ModerationVerdict::Moderated
                        {
                            return Some(CommandFailure::refused(
                                crate::refusal_audit::RefusalKind::InvalidState,
                                ErrorCode::ListingModerated,
                                crate::listing_moderation::LISTING_MODERATED_MESSAGE,
                            ));
                        }
                    }
                    None
                },
                async {
                    let (homeserver, gate) = lifecycle?;
                    crate::listing_lifecycle::check(homeserver, &lifecycle_listings, gate).await
                },
            );
            if let Some(failure) = moderated.or(not_on_sale) {
                log_command(
                    actor,
                    command.kind(),
                    &command.command_id.to_string(),
                    &command.aggregate_id,
                    "refused",
                    Some(failure.code()),
                    Some(failure.message()),
                    command.expected_revision,
                    started.elapsed().as_millis() as u64,
                );
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return Ok(failure_response(&failure));
            }
        }
    }

    // Commerce in a blocked region (`REGION_MODES`): buying and selling are
    // refused for a blocked declared/shop country, a blocked IP country, or
    // an unknown IP country when `UNKNOWN=blocked` is explicitly set;
    // starting a payment on an already-admitted order checks its stored
    // declared buyer, destination and seller shop country without
    // re-checking the live request IP. Only commands that start new commerce
    // are checked; a replay is answered from its stored result.
    if state.config.region_policy().commerce_active()
        && crate::commerce_region::is_gated(&command.payload)
        && stored_command_result(&state.pool, actor, command.command_id)
            .await?
            .is_none()
    {
        if let Err(failure) = crate::commerce_region::gate(state, actor, &command, edge).await {
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                command.expected_revision,
                started.elapsed().as_millis() as u64,
            );
            enqueue_refusal(
                state,
                actor,
                state.clock.now(),
                SurfaceKind::V1Command,
                command_kind(&command.payload),
                failure.refusal_kind(),
                Some(command.command_id),
            );
            return Ok(failure_response(&failure));
        }
    }

    // The EU scope fence (legal launch plan C2-C4), when `EU_FENCE_ENABLED`
    // is on or `REGION_MODES` restricts a region: a command that sells needs
    // the buyer's country, a buyer in a nuanced region buys only from a
    // declared private seller, and a seller in a blocked region sells to
    // nobody. The sellers' shop records are read here for the same reason as
    // above. A replay is answered from its stored result.
    let mut fence = crate::region_fence::FencePlan::off();
    if state.config.region_policy().fence_active()
        && crate::region_fence::buyer_country_of(&command.payload).is_some()
        && stored_command_result(&state.pool, actor, command.command_id)
            .await?
            .is_none()
    {
        let listings =
            crate::listing_moderation::listings_a_command_sells(&state.pool, actor, &command)
                .await?;
        match crate::region_fence::gate(state, &command, &listings).await {
            Ok(plan) => fence = plan,
            Err(failure) => {
                log_command(
                    actor,
                    command.kind(),
                    &command.command_id.to_string(),
                    &command.aggregate_id,
                    "refused",
                    Some(failure.code()),
                    Some(failure.message()),
                    command.expected_revision,
                    started.elapsed().as_millis() as u64,
                );
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return Ok(failure_response(&failure));
            }
        }
    }

    // No digital sales to EU or UK buyers (legal launch plan C3b), when
    // `DIGITAL_REGION_BLOCK_ENABLED` is on or `REGION_MODES` has a nuanced
    // region. A replay is answered from its stored result.
    if state.config.region_policy().digital_active()
        && stored_command_result(&state.pool, actor, command.command_id)
            .await?
            .is_none()
    {
        if let Err(failure) = crate::digital_region::gate(state, &command, edge).await {
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                command.expected_revision,
                started.elapsed().as_millis() as u64,
            );
            enqueue_refusal(
                state,
                actor,
                state.clock.now(),
                SurfaceKind::V1Command,
                command_kind(&command.payload),
                failure.refusal_kind(),
                Some(command.command_id),
            );
            return Ok(failure_response(&failure));
        }
    }

    // Zone-priced shipping reads the sellers' shipping profiles and shop
    // records here, before the transaction, for the same reason: a slow
    // homeserver never holds a listing row lock. A replay is answered from
    // its stored result.
    let mut shipping = crate::shipping::ShippingPlan::off();
    if matches!(
        command.payload,
        CommandPayload::CreateCheckout(_)
            | CommandPayload::CreateOffer(_)
            | CommandPayload::AcceptOffer(_)
            | CommandPayload::PlaceBid(_)
    ) && stored_command_result(&state.pool, actor, command.command_id)
        .await?
        .is_none()
    {
        shipping = crate::shipping::ShippingPlan::read(state, &command).await?;
    }

    // A `listing.register` that needs the seller's public record, every
    // `listing.sync` (the record and, when it is missing, the deletion
    // proof), a `digital_delivery.set` of a file (the file, read back) and
    // the [`RecordReads`] commands (a drop record, an offer's listing record,
    // a Locks document, the seller's terms a payment start freezes) read the
    // seller's homeserver here, before the transaction and the command's
    // advisory lock, so a slow homeserver never holds a domain connection, a
    // row lock or a duplicate. No handler reads a homeserver inside its
    // transaction. The command enters its flight first,
    // so a concurrent duplicate waits for this submission instead of reading
    // the homeserver too, and a replay or a conflicting reuse of the command
    // id is answered from its stored result without the homeserver.
    // `command_results` rows are never updated or deleted, so that result is
    // the one the locked lookup below would find.
    let register_request =
        crate::handlers::register_listing::public_record_request(&state.pool, actor, &command)
            .await?;
    let sync_request =
        crate::handlers::sync_listing::record_request(&command, state.homeserver.is_some());
    let file_request = crate::handlers::digital::file_request(
        &state.pool,
        actor,
        &command,
        state.digital.is_some(),
        state.config.digital_delivery_max_bytes,
    )
    .await?;
    let reads_records = RecordReads::wanted(state, &command);
    let mut prefetched = crate::handlers::register_listing::RegisterRecordPrefetch::NotNeeded;
    let mut synced = crate::handlers::sync_listing::SyncPrefetch::NotNeeded;
    let mut file = crate::handlers::digital::FilePrefetch::NotNeeded;
    let mut reads = RecordReads::none(state.homeserver.is_some());
    let _flight = if register_request.is_some()
        || sync_request.is_some()
        || file_request.is_some()
        || reads_records
    {
        let flight = state.command_flights.enter(actor, command.command_id).await;
        if let Some((stored_hash, stored_result)) =
            stored_command_result(&state.pool, actor, command.command_id).await?
        {
            return Ok(stored_result_response(
                state,
                actor,
                &command,
                &request_hash,
                &stored_hash,
                stored_result,
                started,
            ));
        }
        if let Some(payload) = register_request {
            prefetched = crate::handlers::register_listing::prefetch_public_record(
                state.homeserver.as_deref(),
                payload,
            )
            .await;
        }
        if let (Some(payload), Some(homeserver)) = (sync_request, state.homeserver.as_deref()) {
            synced =
                crate::handlers::sync_listing::prefetch(&state.pool, homeserver, &command, payload)
                    .await?;
        }
        if let Some(request) = file_request {
            file = crate::handlers::digital::prefetch_file(
                state.homeserver.as_deref(),
                actor,
                request,
            )
            .await;
        }
        if reads_records {
            reads = RecordReads::read(state, actor, &command).await?;
        }
        Some(flight)
    } else {
        None
    };

    let mut tx = state.pool.begin().await?;

    // Serialize concurrent submissions of the same actor + command id so a
    // duplicate cannot execute twice before either result is stored.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 42))")
        .bind(format!("{actor}:{}", command.command_id))
        .execute(&mut *tx)
        .await?;

    if let Some((stored_hash, stored_result)) =
        stored_command_result(&mut *tx, actor, command.command_id).await?
    {
        tx.commit().await?;
        return Ok(stored_result_response(
            state,
            actor,
            &command,
            &request_hash,
            &stored_hash,
            stored_result,
            started,
        ));
    }

    let now = state.clock.now();
    let outcome = dispatch(
        state,
        &mut tx,
        actor,
        &command,
        &reads,
        &prefetched,
        &synced,
        file,
        &fence,
        &shipping,
        now,
    )
    .await;
    match outcome {
        Ok(Ok(success)) => {
            let body = success_body(&command, &success);
            // The minted Locks client reference exists in plaintext only in
            // the authenticated response: the durable copy of a prepare
            // result is sealed to the payment and buyer (ADR-0019 §8 —
            // persist only ciphertext).
            let stored_body = if command.kind() == "payment.prepare_locks" {
                let protected = state
                    .locks
                    .as_deref()
                    .and_then(|locks| seal_prepare_locks_result(locks, &command, actor, &body));
                let Some(stored_body) = protected else {
                    let failure = CommandFailure::refused(
                        crate::refusal_audit::RefusalKind::InvariantViolation,
                        ErrorCode::InvariantViolation,
                        "The command result could not be protected.",
                    );
                    let response = failure_response(&failure);
                    let descriptor = state.refusal_audit.as_ref().and_then(|audit| {
                        audit
                            .envelope(
                                now,
                                SurfaceKind::V1Command,
                                command_kind(&command.payload),
                                failure.refusal_kind(),
                                actor,
                                Some(command.command_id),
                            )
                            .ok()
                    });
                    tx.rollback().await?;
                    if let (Some(audit), Some(descriptor)) = (&state.refusal_audit, descriptor) {
                        audit.try_send(descriptor);
                    }
                    return Ok(response);
                };
                stored_body
            } else {
                body.clone()
            };
            sqlx::query(
                "INSERT INTO command_results (actor_pubky, command_id, request_hash, result, created_at) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(actor)
            .bind(command.command_id)
            .bind(&request_hash)
            .bind(&stored_body)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "accepted",
                None,
                None,
                success.revision,
                started.elapsed().as_millis() as u64,
            );
            Ok((StatusCode::OK, body))
        }
        Ok(Err(failure)) => {
            let response = failure_response(&failure);
            let descriptor = state.refusal_audit.as_ref().and_then(|audit| {
                audit
                    .envelope(
                        now,
                        SurfaceKind::V1Command,
                        command_kind(&command.payload),
                        failure.refusal_kind(),
                        actor,
                        Some(command.command_id),
                    )
                    .ok()
            });
            tx.rollback().await?;
            if let (Some(audit), Some(descriptor)) = (&state.refusal_audit, descriptor) {
                audit.try_send(descriptor);
            }
            log_command(
                actor,
                command.kind(),
                &command.command_id.to_string(),
                &command.aggregate_id,
                "refused",
                Some(failure.code()),
                Some(failure.message()),
                failure
                    .current_revision()
                    .unwrap_or(command.expected_revision),
                started.elapsed().as_millis() as u64,
            );
            Ok(response)
        }
        Err(error) => {
            tx.rollback().await?;
            if is_unique_violation(&error) {
                let failure = CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "A uniqueness constraint rejected the command.",
                );
                log_command(
                    actor,
                    command.kind(),
                    &command.command_id.to_string(),
                    &command.aggregate_id,
                    "conflict",
                    Some(failure.code()),
                    Some(failure.message()),
                    command.expected_revision,
                    started.elapsed().as_millis() as u64,
                );
                let response = failure_response(&failure);
                enqueue_refusal(
                    state,
                    actor,
                    now,
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return Ok(response);
            }
            Err(error)
        }
    }
}

async fn stored_command_result<'e, E>(
    executor: E,
    actor: &str,
    command_id: Uuid,
) -> Result<Option<(String, Value)>, sqlx::Error>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_as(
        "SELECT request_hash, result FROM command_results \
         WHERE actor_pubky = $1 AND command_id = $2",
    )
    .bind(actor)
    .bind(command_id)
    .fetch_optional(executor)
    .await
}

/// The response to a command whose actor + command id already has a stored
/// result: an exact replay returns it, and different input is a conflict.
fn stored_result_response(
    state: &AppState,
    actor: &str,
    command: &Command,
    request_hash: &str,
    stored_hash: &str,
    stored_result: Value,
    started: Instant,
) -> (StatusCode, Value) {
    if stored_hash == request_hash {
        log_command(
            actor,
            command.kind(),
            &command.command_id.to_string(),
            &command.aggregate_id,
            "idempotent_replay",
            None,
            None,
            stored_result["revision"].as_i64().unwrap_or_default(),
            started.elapsed().as_millis() as u64,
        );
        if command.kind() == "checkout.create" {
            let Some(stored_result) = redact_command_result(stored_result) else {
                let failure = CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "The stored command result could not be processed.",
                );
                let response = failure_response(&failure);
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return response;
            };
            return (StatusCode::OK, stored_result);
        }
        if command.kind() == "payment.prepare_locks" {
            // The stored result is sealed to this payment and buyer; it
            // is opened only for the same authenticated actor (the
            // command_results lookup is actor-scoped).
            let Some(locks) = state.locks.as_deref() else {
                let failure = CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "The stored command result could not be processed.",
                );
                let response = failure_response(&failure);
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return response;
            };
            let Some(stored_result) =
                unseal_prepare_locks_result(locks, command, actor, &stored_result)
            else {
                let failure = CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvariantViolation,
                    ErrorCode::InvariantViolation,
                    "The stored command result could not be processed.",
                );
                let response = failure_response(&failure);
                enqueue_refusal(
                    state,
                    actor,
                    state.clock.now(),
                    SurfaceKind::V1Command,
                    command_kind(&command.payload),
                    failure.refusal_kind(),
                    Some(command.command_id),
                );
                return response;
            };
            return (StatusCode::OK, stored_result);
        }
        return (StatusCode::OK, stored_result);
    }
    let failure = CommandFailure::refused(
        crate::refusal_audit::RefusalKind::IdempotencyConflict,
        ErrorCode::IdempotencyConflict,
        "The command id was already used with different input.",
    );
    log_command(
        actor,
        command.kind(),
        &command.command_id.to_string(),
        &command.aggregate_id,
        "conflict",
        Some(failure.code()),
        Some(failure.message()),
        command.expected_revision,
        started.elapsed().as_millis() as u64,
    );
    let response = failure_response(&failure);
    enqueue_refusal(
        state,
        actor,
        state.clock.now(),
        SurfaceKind::V1Command,
        command_kind(&command.payload),
        failure.refusal_kind(),
        Some(command.command_id),
    );
    response
}

fn enqueue_refusal(
    state: &AppState,
    actor: &str,
    now: DateTime<Utc>,
    surface: SurfaceKind,
    command_kind: CommandKind,
    refusal_kind: RefusalKind,
    command_id: Option<Uuid>,
) {
    if let Some(audit) = &state.refusal_audit {
        if let Ok(descriptor) =
            audit.envelope(now, surface, command_kind, refusal_kind, actor, command_id)
        {
            audit.try_send(descriptor);
        }
    }
}

/// The refusal-audit kind a command is recorded under.
pub fn command_kind(payload: &CommandPayload) -> CommandKind {
    match payload {
        CommandPayload::RegisterListing(_) => CommandKind::RegisterListing,
        CommandPayload::SyncListing(_) => CommandKind::SyncListing,
        CommandPayload::SyncDrop(_) => CommandKind::SyncDrop,
        CommandPayload::CancelDrop(_) => CommandKind::CancelDrop,
        CommandPayload::ReleaseDropListings(_) => CommandKind::ReleaseDropListings,
        CommandPayload::ReserveInventory(_) => CommandKind::ReserveInventory,
        CommandPayload::CreateCheckout(_) => CommandKind::CreateCheckout,
        CommandPayload::CreateOffer(_) => CommandKind::CreateOffer,
        CommandPayload::CounterOffer(_) => CommandKind::CounterOffer,
        CommandPayload::AcceptOffer(_) => CommandKind::AcceptOffer,
        CommandPayload::OfferCheckout(_) => CommandKind::OfferCheckout,
        CommandPayload::RejectOffer(_) => CommandKind::RejectOffer,
        CommandPayload::WithdrawOffer(_) => CommandKind::WithdrawOffer,
        CommandPayload::PlaceBid(_) => CommandKind::PlaceBid,
        CommandPayload::CloseAuction(_) => CommandKind::CloseAuction,
        CommandPayload::AdvanceSandboxPayment(_) => CommandKind::AdvanceSandboxPayment,
        CommandPayload::PrepareLocks(_) => CommandKind::PrepareLocks,
        CommandPayload::RegisterLocks(_) => CommandKind::RegisterLocks,
        CommandPayload::RequestCancellation(_) => CommandKind::RequestCancellation,
        CommandPayload::ApproveCancellation(_) => CommandKind::ApproveCancellation,
        CommandPayload::ShipOrder(_) => CommandKind::ShipOrder,
        CommandPayload::UpdateTracking(_) => CommandKind::UpdateTracking,
        CommandPayload::ConfirmDelivery(_) => CommandKind::ConfirmDelivery,
        CommandPayload::SetPickupDetails(_) => CommandKind::SetPickupDetails,
        CommandPayload::ClearPickupDetails(_) => CommandKind::ClearPickupDetails,
        CommandPayload::SetDigitalDelivery(_) => CommandKind::SetDigitalDelivery,
        CommandPayload::ClearDigitalDelivery(_) => CommandKind::ClearDigitalDelivery,
        CommandPayload::SetDeliveryEmail(_) => CommandKind::SetDeliveryEmail,
        CommandPayload::DeliverDigital(_) => CommandKind::DeliverDigital,
        CommandPayload::ReleaseDigital(_) => CommandKind::ReleaseDigital,
        CommandPayload::MarkReadyForPickup(_) => CommandKind::MarkReadyForPickup,
        CommandPayload::ConfirmPickup(_) => CommandKind::ConfirmPickup,
        CommandPayload::RequestReturn(_) => CommandKind::RequestReturn,
        CommandPayload::ApproveReturn(_) => CommandKind::ApproveReturn,
        CommandPayload::CancelReturn(_) => CommandKind::CancelReturn,
        CommandPayload::ReceiveReturn(_) => CommandKind::ReceiveReturn,
        CommandPayload::RecordExternalRefund(_) => CommandKind::RecordExternalRefund,
        CommandPayload::ConfirmRefundDestination(_) => CommandKind::ConfirmRefundDestination,
        CommandPayload::CreateReview(_) => CommandKind::CreateReview,
        CommandPayload::UpdateReview(_) => CommandKind::UpdateReview,
        CommandPayload::WithdrawReview(_) => CommandKind::WithdrawReview,
        CommandPayload::SetBandConsent(_) => CommandKind::SetBandConsent,
    }
}

/// The seller records a `drop.sync`, an offer command, a
/// `payment.sandbox_advance` or a `payment.prepare_locks` read before its
/// transaction: the drop record, the offer's listing record, the Locks
/// document, and the seller's terms an order freezes when its payment
/// starts.
struct RecordReads {
    drop_record: Option<crate::homeserver::HomeserverFetchOutcome>,
    offer_record: crate::handlers::offers::RecordPrefetch,
    locks: crate::handlers::locks::LocksPrefetch,
    terms: crate::seller_terms::TermsRead,
}

impl RecordReads {
    fn none(homeserver_configured: bool) -> Self {
        Self {
            drop_record: None,
            offer_record: crate::handlers::offers::RecordPrefetch::NotNeeded,
            locks: crate::handlers::locks::LocksPrefetch::NotNeeded,
            terms: crate::seller_terms::TermsRead::not_read(homeserver_configured),
        }
    }

    /// Whether `command` reads any of them.
    fn wanted(state: &AppState, command: &Command) -> bool {
        state.homeserver.is_some()
            && match &command.payload {
                CommandPayload::SyncDrop(_)
                | CommandPayload::CreateOffer(_)
                | CommandPayload::CounterOffer(_)
                | CommandPayload::AcceptOffer(_)
                | CommandPayload::PrepareLocks(_) => true,
                CommandPayload::AdvanceSandboxPayment(_) => state.config.sandbox_payments_enabled,
                _ => false,
            }
    }

    /// Reads them, each only for a command that passes the checks its
    /// handler makes before using it.
    async fn read(state: &AppState, actor: &str, command: &Command) -> Result<Self, sqlx::Error> {
        let homeserver = state.homeserver.as_deref();
        let mut reads = Self::none(homeserver.is_some());
        match &command.payload {
            CommandPayload::SyncDrop(_) => {
                reads.drop_record = crate::handlers::drops::prefetch(homeserver, command).await;
            }
            CommandPayload::CreateOffer(_)
            | CommandPayload::CounterOffer(_)
            | CommandPayload::AcceptOffer(_) => {
                reads.offer_record = crate::handlers::offers::record_prefetch(
                    &state.pool,
                    homeserver,
                    actor,
                    command,
                    state.clock.now(),
                )
                .await?;
            }
            CommandPayload::PrepareLocks(_) => {
                reads.locks = crate::handlers::locks::prefetch(
                    &state.pool,
                    state.locks.as_deref(),
                    homeserver,
                    actor,
                    command,
                )
                .await?;
            }
            CommandPayload::AdvanceSandboxPayment(payload) => {
                reads.terms = crate::handlers::payment::terms_read(
                    &state.pool,
                    homeserver,
                    actor,
                    command,
                    payload,
                )
                .await?;
            }
            _ => {}
        }
        Ok(reads)
    }
}

async fn resolve_provider_currency_refusal(
    state: &AppState,
    actor: &str,
    command: &Command,
) -> Result<Option<CommandFailure>, sqlx::Error> {
    let fx = crate::fx::FxFeedAdmission::from_config(&state.config);
    if fx.supports_all_fiat() {
        return Ok(None);
    }
    match &command.payload {
        CommandPayload::RegisterListing(payload) => Ok(crate::handlers::fx_currency_refusal(
            fx,
            payload.unit_price.currency.as_str(),
            "payload.unit_price.currency",
        )),
        CommandPayload::CreateOffer(payload) => Ok(crate::handlers::fx_currency_refusal(
            fx,
            payload.amount.currency.as_str(),
            "payload.amount.currency",
        )),
        CommandPayload::CounterOffer(payload) => Ok(crate::handlers::fx_currency_refusal(
            fx,
            payload.amount.currency.as_str(),
            "payload.amount.currency",
        )),
        CommandPayload::PlaceBid(payload) => Ok(crate::handlers::fx_currency_refusal(
            fx,
            payload.maximum_amount.currency.as_str(),
            "payload.maximum_amount.currency",
        )),
        CommandPayload::AcceptOffer(payload) => {
            crate::handlers::offers::accept_currency_refusal(
                &state.pool,
                state.clock.as_ref(),
                fx,
                actor,
                command,
                payload,
            )
            .await
        }
        CommandPayload::OfferCheckout(payload) => {
            crate::handlers::offer_checkout::checkout_currency_refusal(
                &state.pool,
                state.clock.as_ref(),
                fx,
                actor,
                command,
                payload,
            )
            .await
        }
        CommandPayload::CreateCheckout(payload) => {
            crate::handlers::checkout::checkout_currency_refusal(
                &state.pool,
                fx,
                actor,
                command,
                payload,
            )
            .await
        }
        _ => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    state: &AppState,
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    reads: &RecordReads,
    prefetched: &crate::handlers::register_listing::RegisterRecordPrefetch,
    synced: &crate::handlers::sync_listing::SyncPrefetch,
    file: crate::handlers::digital::FilePrefetch,
    fence: &crate::region_fence::FencePlan,
    shipping: &crate::shipping::ShippingPlan,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    match &command.payload {
        CommandPayload::RegisterListing(payload) => {
            crate::handlers::register_listing::handle(tx, actor, command, payload, prefetched, now)
                .await
        }
        CommandPayload::SyncListing(payload) => {
            crate::handlers::sync_listing::handle(
                tx,
                actor,
                command,
                payload,
                state.homeserver.is_some(),
                crate::fx::FxFeedAdmission::from_config(&state.config),
                synced,
                now,
            )
            .await
        }
        CommandPayload::SyncDrop(payload) => {
            crate::handlers::drops::sync(
                tx,
                actor,
                command,
                payload,
                state.homeserver.is_some(),
                reads.drop_record.as_ref(),
                now,
            )
            .await
        }
        CommandPayload::CancelDrop(payload) => {
            crate::handlers::drops::cancel(tx, actor, command, payload, now).await
        }
        CommandPayload::ReleaseDropListings(payload) => {
            crate::handlers::drops::release_listings(tx, actor, command, payload, now).await
        }
        CommandPayload::ReserveInventory(payload) => {
            crate::handlers::reserve_inventory::handle(tx, actor, command, payload, now).await
        }
        CommandPayload::CreateCheckout(payload) => {
            crate::handlers::checkout::handle(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref().map(|runtime| &runtime.keys),
                crate::handlers::checkout::CheckoutContext {
                    drop_claim_seconds: state.config.drop_claim_window_seconds,
                    digital: state.digital.as_deref(),
                    require_encrypted_delivery: state.config.require_encrypted_delivery_address,
                    fence,
                    shipping,
                    fx: crate::fx::FxFeedAdmission::from_config(&state.config),
                },
                now,
            )
            .await
        }
        CommandPayload::CreateOffer(payload) => {
            crate::handlers::offers::create(
                tx,
                actor,
                command,
                payload,
                state.homeserver.is_some(),
                crate::fx::FxFeedAdmission::from_config(&state.config),
                &reads.offer_record,
                shipping,
                now,
            )
            .await
        }
        CommandPayload::CounterOffer(payload) => {
            crate::handlers::offers::counter(
                tx,
                actor,
                command,
                payload,
                state.homeserver.is_some(),
                crate::fx::FxFeedAdmission::from_config(&state.config),
                &reads.offer_record,
                now,
            )
            .await
        }
        CommandPayload::AcceptOffer(payload) => {
            crate::handlers::offers::accept(
                tx,
                actor,
                command,
                payload,
                state.homeserver.is_some(),
                crate::fx::FxFeedAdmission::from_config(&state.config),
                &reads.offer_record,
                shipping,
                now,
            )
            .await
        }
        CommandPayload::OfferCheckout(payload) => {
            let hold_window_seconds = state
                .config
                .locks_payment_window_seconds
                .max(state.config.fiat_payment_window_seconds)
                .max(state.config.sandbox_payment_window_seconds);
            crate::handlers::offer_checkout::handle(
                tx,
                actor,
                command,
                payload,
                state.clock.as_ref(),
                hold_window_seconds,
                state.config.require_encrypted_delivery_address,
                crate::fx::FxFeedAdmission::from_config(&state.config),
                fence,
            )
            .await
        }
        CommandPayload::RejectOffer(payload) => {
            crate::handlers::offers::reject(tx, actor, command, payload, now).await
        }
        CommandPayload::WithdrawOffer(payload) => {
            crate::handlers::offers::withdraw(tx, actor, command, payload, now).await
        }
        CommandPayload::PlaceBid(payload) => {
            crate::handlers::auction::place_bid(
                tx,
                actor,
                command,
                payload,
                crate::fx::FxFeedAdmission::from_config(&state.config),
                shipping,
                now,
            )
            .await
        }
        CommandPayload::CloseAuction(payload) => {
            crate::handlers::auction::close(tx, actor, command, payload, now).await
        }
        CommandPayload::AdvanceSandboxPayment(payload) => {
            // Deployment boundary, not client courtesy: on a durable
            // deployment the buyer must never be able to drive a payment to
            // `paid` by command (ADR-0019 §7).
            if !state.config.sandbox_payments_enabled {
                return Ok(Err(CommandFailure::refused(
                    crate::refusal_audit::RefusalKind::InvalidCommand,
                    ErrorCode::InvalidCommand,
                    "Sandbox payment commands are disabled on this deployment.",
                )));
            }
            crate::handlers::payment::advance(
                tx,
                actor,
                command,
                payload,
                state.config.sandbox_payment_window_seconds,
                &reads.terms,
                state.confirm_keys(),
                now,
            )
            .await
        }
        CommandPayload::RegisterLocks(payload) => {
            crate::handlers::locks::register(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::PrepareLocks(payload) => {
            crate::handlers::locks::prepare(
                tx,
                actor,
                command,
                payload,
                state.locks.as_deref(),
                state.homeserver.is_some(),
                &reads.locks,
                state.config.locks_payment_window_seconds,
                state.config.region_policy().fence_modes(),
                now,
            )
            .await
        }
        CommandPayload::RequestCancellation(payload) => {
            crate::handlers::cancellation::request(tx, actor, command, payload, now).await
        }
        CommandPayload::ApproveCancellation(payload) => {
            crate::handlers::cancellation::approve(tx, actor, command, payload, now).await
        }
        CommandPayload::ShipOrder(payload) => {
            crate::handlers::fulfillment::ship(tx, actor, command, payload, now).await
        }
        CommandPayload::UpdateTracking(payload) => {
            crate::handlers::fulfillment::update_tracking(tx, actor, command, payload, now).await
        }
        CommandPayload::ConfirmDelivery(payload) => {
            crate::handlers::fulfillment::confirm_delivery(tx, actor, command, payload, now).await
        }
        CommandPayload::SetPickupDetails(payload) => {
            crate::handlers::pickup::set(
                tx,
                actor,
                command,
                payload,
                state.pickup.as_deref(),
                state.config.sandbox_payments_enabled,
                now,
            )
            .await
        }
        CommandPayload::ClearPickupDetails(payload) => {
            crate::handlers::pickup::clear(
                tx,
                actor,
                command,
                payload,
                state.pickup.as_deref(),
                state.config.sandbox_payments_enabled,
                now,
            )
            .await
        }
        CommandPayload::SetDigitalDelivery(payload) => {
            crate::handlers::digital::set(
                tx,
                actor,
                command,
                payload,
                state.digital.as_deref(),
                file,
                state.config.digital_delivery_max_bytes,
                now,
            )
            .await
        }
        CommandPayload::ClearDigitalDelivery(payload) => {
            crate::handlers::digital::clear(
                tx,
                actor,
                command,
                payload,
                state.digital.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::SetDeliveryEmail(payload) => {
            crate::handlers::digital_manual::set_delivery_email(
                tx,
                actor,
                command,
                payload,
                state.digital.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::DeliverDigital(payload) => {
            crate::handlers::digital_manual::deliver_digital(tx, actor, command, payload, now).await
        }
        CommandPayload::ReleaseDigital(payload) => {
            crate::handlers::digital_manual::release_digital(tx, actor, command, payload, now).await
        }
        CommandPayload::MarkReadyForPickup(payload) => {
            crate::handlers::fulfillment::mark_ready(tx, actor, command, payload, now).await
        }
        CommandPayload::ConfirmPickup(payload) => {
            crate::handlers::fulfillment::confirm_pickup(tx, actor, command, payload, now).await
        }
        CommandPayload::RequestReturn(payload) => {
            crate::handlers::returns::request(tx, actor, command, payload, now).await
        }
        CommandPayload::ApproveReturn(payload) => {
            crate::handlers::returns::approve(tx, actor, command, payload, now).await
        }
        CommandPayload::ReceiveReturn(payload) => {
            crate::handlers::returns::receive(
                tx,
                actor,
                command,
                payload,
                state.attestor.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::CancelReturn(payload) => {
            crate::handlers::returns::cancel(tx, actor, command, payload, now).await
        }
        CommandPayload::RecordExternalRefund(payload) => {
            crate::handlers::returns::record_external_refund(
                tx,
                actor,
                command,
                payload,
                state.attestor.as_deref(),
                state.refund_chain.as_deref(),
                now,
            )
            .await
        }
        CommandPayload::ConfirmRefundDestination(payload) => {
            crate::handlers::refund_destination::confirm(tx, actor, command, payload, now).await
        }
        CommandPayload::CreateReview(payload) => {
            crate::handlers::reviews::create(
                tx,
                actor,
                command,
                payload,
                state.attestor.as_deref(),
                state.config.review_blind_window_days,
                now,
            )
            .await
        }
        CommandPayload::UpdateReview(payload) => {
            crate::handlers::reviews::update(tx, actor, command, payload, now).await
        }
        CommandPayload::WithdrawReview(payload) => {
            crate::handlers::reviews::withdraw(tx, actor, command, payload, now).await
        }
        CommandPayload::SetBandConsent(payload) => {
            crate::handlers::attestation::set_band_consent(tx, actor, command, payload, now).await
        }
    }
}

fn failure_response(failure: &CommandFailure) -> (StatusCode, Value) {
    let status = StatusCode::from_u16(failure.http_status())
        .expect("error codes map to valid HTTP status codes");
    (status, failure.body())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(db) if db.code().as_deref() == Some("23505")
    )
}

/// The durable form of a `payment.prepare_locks` result: the plaintext
/// `client_reference` is replaced by its payment-and-buyer-bound seal, so
/// the stored row holds ciphertext only.
fn seal_prepare_locks_result(
    locks: &crate::locks::LocksRuntime,
    command: &Command,
    actor: &str,
    body: &Value,
) -> Option<Value> {
    let CommandPayload::PrepareLocks(payload) = &command.payload else {
        return None;
    };
    let reference = body.get("result")?.get("client_reference")?.as_str()?;
    let sealed = locks
        .keys
        .seal_result_client_reference(payload.payment_id, actor, reference);
    let mut stored = body.clone();
    let result = stored.get_mut("result")?.as_object_mut()?;
    result.remove("client_reference");
    result.insert(
        "client_reference_sealed".to_string(),
        Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            sealed,
        )),
    );
    Some(stored)
}

/// Reverses [`seal_prepare_locks_result`] for the same authenticated buyer
/// replaying their own command; any tamper, transplant, or key mismatch
/// fails closed.
fn unseal_prepare_locks_result(
    locks: &crate::locks::LocksRuntime,
    command: &Command,
    actor: &str,
    stored: &Value,
) -> Option<Value> {
    let CommandPayload::PrepareLocks(payload) = &command.payload else {
        return None;
    };
    let sealed = stored
        .get("result")?
        .get("client_reference_sealed")?
        .as_str()?;
    let sealed = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, sealed).ok()?;
    let reference = locks
        .keys
        .open_result_client_reference(payload.payment_id, actor, &sealed)?;
    let mut body = stored.clone();
    let result = body.get_mut("result")?.as_object_mut()?;
    result.remove("client_reference_sealed");
    result.insert("client_reference".to_string(), Value::String(reference));
    Some(body)
}

/// Appends one immutable domain event and returns its id.
pub async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    command_id: Uuid,
    aggregate_id: &str,
    revision: i64,
    actor: &str,
    kind: &str,
    occurred_at: DateTime<Utc>,
) -> Result<Uuid, sqlx::Error> {
    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO events (id, command_id, aggregate_id, revision, actor_pubky, kind, occurred_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(event_id)
    .bind(command_id)
    .bind(aggregate_id)
    .bind(revision)
    .bind(actor)
    .bind(kind)
    .bind(occurred_at)
    .execute(&mut **tx)
    .await?;
    Ok(event_id)
}
