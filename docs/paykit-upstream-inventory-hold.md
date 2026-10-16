# Upstream Paykit inventory hold boundary

Status: accepted consumer design for H1 (`pubky/paykit-server#26`).

## Invariant

Marketplace must not publish an upstream Paykit payment request after inventory has been released. One bind creates one inventory deadline. Activation retries never move it later; a successful activation only shortens it to the request's own deadline.

Fork Paykit behavior is unchanged: its prepare request already carries Marketplace's hold deadline. This design applies only to `PAYKIT_SERVER_API=upstream`.

## Bound

Upstream prepare returns its authoritative `prepare_expires_at`. Paykit atomically refuses the first activation when its database `clock_timestamp() >= prepare_expires_at`. A successful first activation fixes:

`payment_deadline = Paykit activation clock + requested payment_window_seconds`

Marketplace accepts that deadline only when its remaining TTL is no more than
`PAYKIT_MAX_PREPARE_TTL_SECONDS` (default 900, closed range 60–86400), plus the
same 60-second cross-service clock-skew allowance. Operators must align this
setting with Paykit's `prepare_ttl_seconds`. A longer response is treated as
contract corruption: Marketplace rolls back the bind and inventory hold and
best-effort voids the prepared invoice.

Marketplace therefore fixes the original hold after successful prepare to:

`prepare_expires_at + bitcoin_payment_window_seconds + 60 seconds`

The extra 60 seconds is a bounded clock/reaper margin. It is not sent to Paykit and does not alter Paykit's immutable payment window. It only prevents Marketplace's clock or expiry sweep from releasing stock at the producer's latest valid payment deadline.

Marketplace persists this deadline in the same transaction as the prepared invoice and activation outbox row. Every activation response must fit inside the original hold.

## After activation

Once activation succeeds, Paykit's `payment_deadline` is known and immutable, and the wallet refuses the request after it. Marketplace then shortens the hold to:

`min(original hold, payment_deadline + 60 seconds)`

so the order is released when its request expires, not up to `prepare_expires_at - activated_at` later (about 15 minutes with the defaults, since activation normally lands seconds after the bind). The Shop's "Pay by" and the seller's "restocks" time show this hold, so they match the wallet. Auction orders keep their reservation hold.

One exception keeps the original hold: an order whose payment Paykit has already seen (`paykit_request_state = 'detected'`). Bitcoin is on time by its first sighting, so a payment seen before the deadline can still confirm and pay after it. Marking the order `detected` restores its hold to `prepare_expires_at + payment_window_seconds + 60 seconds`, the original hold, recomputed from the persisted columns, so order reads show the time the sweep uses.

## Auction orders

An auction winner's hold is its reservation (`AUCTION_HOLD_SECONDS` from close), not a bind hold, and that deadline never extends. The bind therefore caps the requested window:

`payment_window_seconds = min(bitcoin_payment_window_seconds, reservation expires_at - bind time - 2 × 60 seconds - AUCTION_ACTIVATION_SLACK_SECONDS)`

`AUCTION_ACTIVATION_SLACK_SECONDS` (180) is the time the activation has to land after the bind, so one failed activate and its outbox lease retry fit.

It persists that window and caps the shown hold at `reservation expires_at - 60 seconds`. A winner with less than `AUCTION_MIN_PAYMENT_WINDOW_SECONDS` (300) left, or no active reservation, is refused the bitcoin bind (`auction_payment_window_too_short`). The activation guard requires the active reservation and `Marketplace time + persisted window + 2 × 60 seconds <= reservation expires_at`, so activation passes for the slack after the bind, and Paykit's deadline (its activation clock, at most 60 seconds ahead, plus the window) still fits inside the shown hold and never trips the activation-bounds alert. An activation that lands later is refused and the bind voided; the reservation stays active and the winner can bind again with the time left.

Order reads project the activated request's deadline as `paykit_expires_at` (null until activation, and again after a voided activation is re-bound). Marketplace Shop shows it as an auction winner's "Pay by" once present, so the shown deadline equals the wallet's; before activation it shows the reservation-based `hold_expires_at`. `paykit_expires_at` never exceeds `hold_expires_at` for an auction order.

## Activation guard and races

Before each signed upstream activation, worker locks payment then order and requires all of:

- order remains `pending_payment`;
- activation remains `preparing`;
- inventory remains held and the original hold has not elapsed (for an auction order: the active reservation outlasts the payment window plus 2 × 60 seconds);
- Marketplace time plus 60-second skew margin is before authoritative `prepare_expires_at`.

A failed guard never sends activation. Existing prepared-invoice release handling records attempt for reconciliation, releases inventory atomically, and leaves producer prepare reaper/void handling to close unpublished invoice.

Pre-call check is defense in depth, not cross-service atomicity. H1 relies on Paykit's reviewed atomic activation transaction: it locks preparation, samples database time, refuses at/after `prepare_expires_at`, and creates publication outbox work only in same transaction as activation. Since Marketplace hold extends beyond producer's latest possible payment deadline until activation, local expiry cannot race a valid publication; after activation the hold still ends after the producer's actual deadline.

Activation transport failure or lost response leaves fixed original hold unchanged and activation row retryable. It never extends hold. Existing `paykit_superseded_attempts` records attempts released after ambiguous delivery and polls signed status; confirmed money follows late-money/manual-review path. Marketplace never blindly voids a known active invoice.

## Timing assumptions and fail-closed checks

Accepted producer is reviewed Paykit commit `cce84b127febdc0411cd709df2b7cc913f6c4726` (tree `faa5a49144fc27667360d15f6a0a306e58188c18`). Consumer assumes:

1. `prepare_expires_at` is producer database time and immutable on replay.
2. First activation is rejected when producer DB time is at or past that timestamp.
3. Successful first activation returns immutable `activated_at` and `payment_deadline`, with `payment_deadline = activated_at + requested payment window`.
4. Activation and publication-outbox insertion commit atomically.
5. Worst tolerated Marketplace/Paykit clock plus expiry-scheduling skew is 60 seconds.

Consumer rejects a prepare whose remaining TTL is already inside margin or exceeds the configured maximum plus margin. With the largest permitted config, remaining preparation time is bounded to 86,460 seconds; with the default it is bounded to 960 seconds. The final hold adds only the configured payment window and one further 60-second expiry-scheduling margin. Consumer treats an activation response outside persisted bounds as contract corruption: it must not silently change Paykit terms or extend original hold. Such corruption requires operator alert and fresh producer/consumer review; 60 seconds is explicit operational assumption, not proof against unbounded clock faults.
