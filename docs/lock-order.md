# Row lock order

Two transactions that lock the same rows in opposite orders can deadlock.
Postgres detects the cycle after `deadlock_timeout` and aborts one of them
with `40P01`; that transaction rolls back completely.

## Seller counters before everything

With `EU_FENCE_ENABLED` on (or `REGION_MODES` restricting a region),
`checkout.create` and `offer.checkout` take one
transaction-scoped advisory lock per seller, `pg_advisory_xact_lock(
hashtextextended(seller_pubky, 6381))`, in ascending seller order, as the first
locks of the transaction: before the drop row, the listing rows and the
variant rows. The annual-limit count (`region_fence::usage`) then runs under
that lock, so two checkouts of one seller serialize at the lock and never hold a
listing the other needs. `eu_fence_test` runs checkouts over the same two
sellers in opposite order.

The annual limit and the private seller's re-confirmation point count paid
orders only, so they are decided where payments are taken. `region_fence::recheck_at_payment` takes the same lock, again before any
drop or listing row: at payment start (the bind, Locks preparation and the
sandbox payment call it before `acquire_payment_hold`) and as the first step of
`handlers::payment::confirm_order`. The late-money fork and the seller's
"resolve as paid" call it before they reacquire a hold, so they never wait for
the seller lock while holding a listing. Two payments confirming at once for
one seller therefore serialize at the lock: `eu_fence_test` pays eight open
orders at the last slot concurrently, and `annual_cap_at_payment_test` polls two
settled Bitcoin payments at once.

## A refund hash after its order

`refund.record_external` on a USDT order locks the order row, then takes
`pg_advisory_xact_lock(hashtextextended('refund-tx|' || lower(hash), 0))`
before it checks that no other order recorded the same Arbitrum transaction
hash. Two records of one hash on two orders hold different order rows and
serialize at the hash lock; the second sees the first's record and is refused
with `refund_transaction_already_recorded`. Nothing waits for an order row
while holding the hash lock.

## Drops before listings

A transaction that touches a drop and its listings locks the drop row, then
its bindings in `drop_listings`, then the listing rows. Checkout and
reservation gating (`lock_bound_drop`), drop sell-out confirmation
(`record_paid_unit`), drop credits on release, `drop.sync`, and the listing
tombstone all follow it.

## Listings in `aggregate_id` order

Listing rows are locked in ascending `aggregate_id`.

- `drop.sync` share-locks every listing it binds in one statement,
  `ORDER BY l.aggregate_id FOR SHARE OF l`. Postgres applies `ORDER BY`
  before the locking clause, so the rows are locked in that order.
- A transaction that locks or updates several listings calls
  `handlers::lock_listings_in_order` first. That locks all of them in one
  statement, `ORDER BY aggregate_id FOR UPDATE`. Its per-line locks and
  updates after that re-lock rows it already holds. The callers:

  | Path | Function |
  |---|---|
  | `checkout.create` | `handlers::checkout::handle`, after drop gating |
  | payment start (hold) | `handlers::holds::acquire_payment_hold` |
  | hold release (cancel, window expiry, void, refund routes) | `handlers::holds::release_lines` |
  | payment confirmation | `handlers::payment::confirm_order` |
  | late bitcoin settlement | `bitcoin_review::reacquire_hold` |

A multi-line order whose lines name listings in any order therefore never
holds one listing while it waits for a lower one that a `drop.sync` or
another of these transactions holds. `listing_deletion_test.rs` runs each
path above against a `drop.sync` over the same two listings, with the
higher listing first in line order.

## Listing, then its variant rows

A transaction that moves stock locks the listing row first and then the
listing's `listing_variant_inventory` rows (`variant_stock::lock_rows` does
both, so a caller that skipped the first lock still takes it). The roll-up
trigger rewrites the listing row after every variant-row change, so a path that
locked a variant row before the listing would wait for a listing a concurrent
path holds while it waits for that path's variant row. Every path above
(checkout, hold, release, payment confirmation, reserve, offer accept, award
expiry, reservation expiry, the Bitcoin re-hold, `inventory.adjust`,
registration and sync) takes the listing row first.

## Where a retry is enough

`workers::expire_due_payment_windows` and `expiry::expire_due_reservations`
release many orders in one transaction, in expiry order. Each order's
listings follow the order above, but across orders they do not, and drop
credits for a later order come after listing locks for an earlier one.
Sorting every listing of the batch up front would take listing locks
before the order rows these sweeps lock per order, which inverts the
order-then-listing order that cancellation and payment commands use.

These sweeps stay as they are because a deadlock costs one retry and loses
nothing:

- A sweep chosen as the victim rolls back. `workers::run_once` releases its
  lease and returns the error, so that tick's remaining tasks wait for the
  next tick, which reruns the sweep: its due rows are still due, and it
  selects them with `SKIP LOCKED`.
- A `drop.sync` chosen as the victim fails with nothing written. The sync is
  convergent (expected revision 0, and it re-reads the seller's record), so
  resubmitting it applies the same record.
- A command chosen as the victim (a checkout or payment step) fails with
  nothing written, and resubmitting it runs it again.
- A cycle needs a sweep that releases two orders whose listings another
  transaction locks in the opposite order at the same moment.

## Worker leases

The listing deletion follower share-locks its `worker_leases` row at the
start of each write transaction and takes no other lease row. A takeover
updates that row in a single statement that holds no other lock, so
nothing waiting on the lease row holds a lock the follower could wait on.
Migration 0050's guard triggers share-lock the same row again inside the
follower's own transaction, which already holds it; a `listing.sync`
tombstone declares `command` and does not touch the lease row.
