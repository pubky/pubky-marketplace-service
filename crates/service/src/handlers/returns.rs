//! Return commands (`return.request`, `return.approve`, `return.receive`)
//! and the return resolution (`refund.record_external`), ported from the
//! TypeScript prototype engine. `return.request` follows the listing's return
//! policy frozen on the order ([`crate::return_policy`]).
//!
//! `refund.record_external` records independently supplied seller evidence
//! of a refund executed outside this service (an external transaction id).
//! A USDT refund's transaction is read on Arbitrum when an RPC is configured
//! ([`crate::refund_chain`]): what the buyer received is stored with it.
//! The service never claims to spend, custody, escrow, release, or refund
//! funds (ADR-0019 §7); it records the evidence and advances the order to
//! `refunded_external`.

use chrono::{DateTime, Utc};
use marketplace_domain::commands::{
    OrderActionPayload, RecordExternalRefundPayload, RequestReturnPayload,
};
use marketplace_domain::state_machines::{can_transition, order_machine, return_machine};
use marketplace_domain::{Command, ErrorCode};
use serde_json::json;
use sqlx::{Postgres, Transaction};

use crate::clock::format_timestamp;
use crate::handlers::{fetch_order_for_update, finish_order_action, guard_order_action};
use crate::model::OrderRow;
use crate::queries::ORDER_COLUMNS;
use crate::refund_chain::{RefundChain, RefundChainOutcome};
use crate::refund_destination::{self, is_transaction_hash, is_usdt_order};
use crate::result::{CommandFailure, HandlerResult};
use crate::return_policy::{self, ReturnPolicy, ReturnRefusal};

pub const REASON_REFUND_DESTINATION_REQUIRED: &str = "refund_destination_required";
pub const REASON_INVALID_REFUND_REFERENCE: &str = "invalid_refund_reference";
pub const REASON_REFUND_TRANSACTION_NOT_FOUND: &str = "refund_transaction_not_found";
pub const REASON_REFUND_TRANSACTION_REVERTED: &str = "refund_transaction_reverted";
pub const REASON_REFUND_TRANSACTION_WRONG_RECIPIENT: &str = "refund_transaction_wrong_recipient";
pub const REASON_REFUND_TRANSACTION_ALREADY_RECORDED: &str = "refund_transaction_already_recorded";
pub const REASON_RETURNS_NOT_ACCEPTED: &str = "returns_not_accepted";
pub const REASON_RETURN_WINDOW_CLOSED: &str = "return_window_closed";

/// When the order reached the buyer: the pickup handover's server instant,
/// or the shipment's `delivered_at`. `None` when neither is recorded.
async fn delivered_at(
    tx: &mut Transaction<'_, Postgres>,
    order: &OrderRow,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    if order.fulfillment == "pickup" {
        return sqlx::query_scalar("SELECT confirmed_at FROM pickup_handovers WHERE order_id = $1")
            .bind(order.id)
            .fetch_optional(&mut **tx)
            .await;
    }
    Ok(order
        .shipment
        .as_ref()
        .and_then(|shipment| shipment["delivered_at"].as_str())
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|instant| instant.with_timezone(&Utc)))
}

pub async fn request(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RequestReturnPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.buyer_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the buyer may request a return.",
        )));
    }
    if order.fulfillment == "digital" {
        return Ok(Err(CommandFailure::refused_with_reason(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "Digital purchases cannot be returned.",
            "returns_unavailable_for_digital",
        )));
    }
    if !matches!(order.state.as_str(), "delivered" | "completed")
        || payload.requested_amount_minor > order.total_minor
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The order is not eligible for this return amount.",
        )));
    }
    // The listing's return policy, frozen on the order at its payment start:
    // a final-sale order takes no return, and none after the return window.
    let policy = order
        .return_policy
        .as_ref()
        .and_then(ReturnPolicy::from_json);
    let delivered_at = delivered_at(tx, &order).await?;
    match return_policy::check_request(policy, delivered_at, now) {
        Ok(()) => {}
        Err(ReturnRefusal::FinalSale) => {
            return Ok(Err(CommandFailure::refused_with_reason(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "This item was sold as final sale: the seller doesn't accept returns.",
                REASON_RETURNS_NOT_ACCEPTED,
            )));
        }
        Err(ReturnRefusal::WindowClosed) => {
            return Ok(Err(CommandFailure::refused_with_reason(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The return window for this order has closed.",
                REASON_RETURN_WINDOW_CLOSED,
            )));
        }
    }
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        "return_requested"
    ));

    let return_request = json!({
        "state": "requested",
        "reason": payload.reason,
        "requested_amount_minor": payload.requested_amount_minor,
        "requested_at": format_timestamp(now),
        "updated_at": format_timestamp(now),
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'return_requested', \
         return_request = $3, updated_at = $4 WHERE id = $1 AND revision = $2 \
         RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.seller_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "return.requested",
        ("return_updated", &recipient),
        now,
    )
    .await
}

pub async fn approve(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    advance_return(
        tx,
        actor,
        command,
        payload,
        ReturnStep {
            from_order_state: "return_requested",
            to_order_state: "return_approved",
            to_return_state: "approved",
            invalid_message: "No return is pending approval.",
            event_kind: "return.approved",
        },
        now,
    )
    .await
}

pub async fn receive(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    advance_return(
        tx,
        actor,
        command,
        payload,
        ReturnStep {
            from_order_state: "return_approved",
            to_order_state: "return_received",
            to_return_state: "received",
            invalid_message: "The return is not approved.",
            event_kind: "return.received",
        },
        now,
    )
    .await
}

struct ReturnStep {
    from_order_state: &'static str,
    to_order_state: &'static str,
    to_return_state: &'static str,
    invalid_message: &'static str,
    event_kind: &'static str,
}

/// Seller-driven return progression shared by approve and receive: the order
/// state and the return sub-state advance together under the canonical
/// order and return machines, and the buyer is notified.
async fn advance_return(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &OrderActionPayload,
    step: ReturnStep,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            if step.to_return_state == "approved" {
                "Only the seller may approve this return."
            } else {
                "Only the seller may receive this return."
            },
        )));
    }
    let Some(mut return_request) = order
        .return_request
        .clone()
        .filter(|_| order.state == step.from_order_state)
    else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            step.invalid_message,
        )));
    };
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        step.to_order_state
    ));
    debug_assert!(can_transition(
        &return_machine(),
        return_request["state"]
            .as_str()
            .expect("return has a state"),
        step.to_return_state
    ));

    return_request["state"] = json!(step.to_return_state);
    return_request["updated_at"] = json!(format_timestamp(now));
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = $3, return_request = $4, \
         updated_at = $5 WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(step.to_order_state)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        step.event_kind,
        ("return_updated", &recipient),
        now,
    )
    .await
}

pub async fn record_external_refund(
    tx: &mut Transaction<'_, Postgres>,
    actor: &str,
    command: &Command,
    payload: &RecordExternalRefundPayload,
    attestor: Option<&crate::attestor::Attestor>,
    refund_chain: Option<&dyn RefundChain>,
    now: DateTime<Utc>,
) -> Result<HandlerResult, sqlx::Error> {
    let Some(order) = fetch_order_for_update(tx, payload.order_id).await? else {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::NotFound,
            ErrorCode::NotFound,
            "The order was not found.",
        )));
    };
    if let Some(failure) = guard_order_action(actor, command, &order) {
        return Ok(Err(failure));
    }
    if order.seller_pubky != actor {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::Unauthorized,
            ErrorCode::Unauthorized,
            "Only the seller may record a refund.",
        )));
    }
    // An `external_refund` recorded from PayPal refund notifications is a
    // running partial sum: the seller may close the order with a record
    // that covers at least what PayPal already returned. Any other existing
    // record refuses a second one.
    let recorded_minor = match order.external_refund.as_ref() {
        None => Some(0),
        Some(existing) => {
            let gateway: Option<(String,)> = sqlx::query_as(
                "SELECT refund_txn_id FROM order_gateway_refunds \
                 WHERE order_id = $1 AND refund_txn_id = $2",
            )
            .bind(order.id)
            .bind(existing["transaction_id"].as_str().unwrap_or_default())
            .fetch_optional(&mut **tx)
            .await?;
            gateway.and(existing["amount_minor"].as_i64())
        }
    };
    // A digital order never enters the return states: the seller's refund
    // after delivery is recorded straight from `delivered` or `completed`
    // (digital delivery design §3.6, §6 E9).
    let refundable_state = matches!(order.state.as_str(), "return_received" | "cancelled")
        || (order.fulfillment == "digital"
            && matches!(order.state.as_str(), "delivered" | "completed"));
    if !refundable_state
        || payload.amount_minor > refund_destination::refund_cap_minor(&order)
        || recorded_minor.is_none_or(|recorded| payload.amount_minor < recorded)
    {
        return Ok(Err(CommandFailure::refused(
            crate::refusal_audit::RefusalKind::InvalidState,
            ErrorCode::InvalidState,
            "The external refund cannot be recorded.",
        )));
    }
    // A USDT refund goes to the address the buyer confirmed and is evidenced
    // by its Arbitrum transaction hash. The address is copied into the
    // record, so a later change cannot rewrite what was recorded.
    let usdt_destination = if is_usdt_order(&order) {
        let Some(destination) = refund_destination::fetch(tx, order.id).await? else {
            return Ok(Err(CommandFailure::refused_with_reason(
                crate::refusal_audit::RefusalKind::InvalidState,
                ErrorCode::InvalidState,
                "The buyer has not confirmed a refund address.",
                REASON_REFUND_DESTINATION_REQUIRED,
            )));
        };
        if !is_transaction_hash(&payload.transaction_id) {
            return Ok(Err(CommandFailure::refused_with_reason(
                crate::refusal_audit::RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A USDT refund needs the Arbitrum transaction hash.",
                REASON_INVALID_REFUND_REFERENCE,
            )));
        }
        Some(destination)
    } else {
        None
    };
    // One Arbitrum transaction is evidence for one order's refund: a hash
    // already recorded on another order would read as checked there too.
    if usdt_destination.is_some() {
        let recorded_elsewhere: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM orders WHERE id <> $1 \
             AND lower(external_refund->>'transaction_id') = lower($2))",
        )
        .bind(order.id)
        .bind(&payload.transaction_id)
        .fetch_one(&mut **tx)
        .await?;
        if recorded_elsewhere {
            return Ok(Err(CommandFailure::refused_with_reason(
                crate::refusal_audit::RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "This transaction is already recorded as the refund of another order.",
                REASON_REFUND_TRANSACTION_ALREADY_RECORDED,
            )));
        }
    }
    // With an Arbitrum RPC configured, the transaction must have sent USDT to
    // that address; the record keeps what the buyer actually received.
    let received_units = match (refund_chain, &usdt_destination) {
        (Some(chain), Some(destination)) => {
            let refusal = |message: &'static str, reason: &'static str| {
                Ok(Err(CommandFailure::refused_with_reason(
                    crate::refusal_audit::RefusalKind::InvalidCommand,
                    ErrorCode::InvalidCommand,
                    message,
                    reason,
                )))
            };
            match chain
                .check(&payload.transaction_id, &destination.address)
                .await
            {
                RefundChainOutcome::Transferred { units } if units > 0 => Some(units),
                RefundChainOutcome::Transferred { .. } => {
                    return refusal(
                        "This transaction sent no USDT to the buyer's refund address.",
                        REASON_REFUND_TRANSACTION_WRONG_RECIPIENT,
                    );
                }
                RefundChainOutcome::NotFound => {
                    return refusal(
                        "Arbitrum doesn't show this transaction yet.",
                        REASON_REFUND_TRANSACTION_NOT_FOUND,
                    );
                }
                RefundChainOutcome::Reverted => {
                    return refusal(
                        "This transaction failed on Arbitrum.",
                        REASON_REFUND_TRANSACTION_REVERTED,
                    );
                }
                RefundChainOutcome::Unavailable => None,
            }
        }
        _ => None,
    };
    debug_assert!(can_transition(
        &order_machine(),
        &order.state,
        "refunded_external"
    ));

    let mut external_refund = json!({
        "amount_minor": payload.amount_minor,
        "transaction_id": payload.transaction_id,
        "recorded_at": format_timestamp(now),
    });
    if let Some(destination) = &usdt_destination {
        external_refund["destination_address"] = json!(destination.address);
    }
    if let Some(units) = received_units {
        external_refund["received_units"] = json!(units);
        external_refund["checked_at"] = json!(format_timestamp(now));
    }
    let return_request = order.return_request.clone().map(|mut request| {
        request["state"] = json!("refunded");
        request["updated_at"] = json!(format_timestamp(now));
        request
    });
    let updated: OrderRow = sqlx::query_as(&format!(
        "UPDATE orders SET revision = revision + 1, state = 'refunded_external', \
         external_refund = $3, return_request = $4, updated_at = $5 \
         WHERE id = $1 AND revision = $2 RETURNING {ORDER_COLUMNS}"
    ))
    .bind(order.id)
    .bind(order.revision)
    .bind(&external_refund)
    .bind(&return_request)
    .bind(now)
    .fetch_one(&mut **tx)
    .await?;

    // Attestor annotation (ADR 0024 §5): the refund annotates the
    // order_ref; the attestation itself is never revoked.
    if let Some(attestor) = attestor {
        crate::handlers::attestation::insert_annotation(
            tx, attestor, updated.id, "refunded", None, now,
        )
        .await?;
    }

    let recipient = updated.buyer_pubky.clone();
    finish_order_action(
        tx,
        actor,
        command,
        &updated,
        "refund.recorded_external",
        ("refund_recorded", &recipient),
        now,
    )
    .await
}
