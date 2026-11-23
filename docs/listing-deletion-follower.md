# Listing deletion follower

The follower (`crates/service/src/listing_deletion.rs`) turns a seller's
homeserver `DEL` into a listing tombstone. It runs as a worker task under
the `listing_deletions` lease.

## Guarantees

- **Bounded by the lease.** Each database unit is one transaction whose
  statements the server cancels 100 ms before the lease deadline, awaited
  by the caller no longer than the deadline, connection acquisition
  included. Both deadlines count from before the lease acquisition.
- **Fenced.** `worker_leases.fence` grows with every acquisition. Each
  follower write share-locks its lease row and proceeds only while the row
  names the pass's holder and fence and has not expired; a takeover waits
  for a write in flight.
- **A cursor counts on one homeserver.** A homeserver numbers its events
  in a sequence of its own, so each seller's cursor names the homeserver it
  was read from (`listing_deletion_cursors.homeserver`, migration 0100;
  `NULL` is the homeserver `HOMESERVER_URL` names). On that homeserver the
  cursor never moves backwards. When the seller is read from another
  homeserver (under `HOMESERVER_RESOLUTION=pkarr`, after the seller moved
  and the cached record expired; or once after a deployment switches
  modes), the same pass reads that homeserver's stream from the start and
  the cursor restarts there, so a `DEL` on the new homeserver numbered
  below the old cursor is not skipped. Replaying the new stream removes
  nothing on its own: every `DEL` is still proven before a tombstone.
- **A delete is proven where the seller lives now.** Under `pkarr` every
  deletion proof (the follower's and `listing.sync`'s) first reads the
  seller's Pkarr record from the network, past the cache, and reads the
  record and the event stream from the homeserver it names. The record's
  404 and the `DEL` must come from the same homeserver; if the seller's
  homeserver changes between the two reads the proof is unavailable and
  is tried again later. A cached record that still names a homeserver the
  seller left never proves a delete there; a record that cannot be read
  leaves the listing as it is. `listing.sync` makes these reads before its
  database transaction and settles the result inside it.
- **Revivals are confirmed.** Each pass first confirms up to five revived
  listings not yet confirmed at their current generation
  (`listing_revival_checks`, 0051), least recently tried first, within the
  first half of the pass; the forward read keeps its own 20 settles. A
  revival whose record is gone and whose superseded `DEL` is still the
  record's latest event is tombstoned, even when that `DEL` is behind the
  seller's cursor. Every attempt is recorded, so a revival the homeserver
  could not answer for moves behind the ones not yet tried and stays due.
- **A rejected read is not a poll.** When the homeserver answers the event
  stream (or the record fetch that confirms a `DEL`) with a `429`, a `5xx`,
  or nothing, the seller's `polled_at` stays its last successful poll, its
  consecutive `failures` count grows, and it is not read again before
  `retry_not_before` (migration 0059): 60 s doubling to 15 min, or the
  homeserver's `Retry-After` when that is longer, capped at one hour. The
  cursor still advances over what was settled. Only that seller waits: due
  sellers inside their backoff are left out of the pass, so they take none
  of its seller slots, and the rest are served least recently attempted
  first. A successful poll clears the backoff.
- **An unresolvable seller is not unhosted.** Under
  `HOMESERVER_RESOLUTION=pkarr` each seller is read from the homeserver
  their Pkarr record names. A seller with no record, a lookup that fails or
  times out, or a homeserver that does not answer is a rejected read as
  above, never the 404 of a seller the homeserver does not host.
- **A record accepted after a confirmation wins.** A delete that the
  service accepted a record for after confirming it is settled again on a
  later pass; the seller's cursor stops before it.

## Revivals

A tombstoned listing is revived only by its re-published record:
`listing.sync` revives from the record it fetched, and `listing.register`
at `expected_revision` 0 for a tombstoned id reads the seller's record
before its transaction and refuses the revival unless the record is there
(`NOT_FOUND`, `UPSTREAM_UNAVAILABLE`, or `INVALID_STATE` when the listing
was tombstoned after that read).

`listings.record_epoch` (0051) counts the record-derived writes the service
accepted for a row: a revival, a change to a field the seller's record
supplies (a register, a sync, a sync's healing), or a sync that found the
record and changed nothing, which advances it by one explicitly. A trigger
maintains it for every writer; a statement can advance it by exactly one
and never set it otherwise. Every tombstone caller reads the epoch before it confirms the
delete against the homeserver, and the tombstone commits only while the
locked row still has that epoch. So a register or sync that lands between
the confirmation and the tombstone, including a re-publish after a delete,
is never retired by the delete confirmed before it.

A revival records the `DEL` it superseded in `revived_from_cursor`, and
its homeserver in `revived_from_homeserver` (0100). A later tombstone on
that homeserver needs that `DEL` or a newer one, confirmed at the current
epoch. The revival advanced the epoch, so the superseded `DEL` retires the
row only when it was confirmed after the revival and was then still the
record's latest event: nothing backs the revival. A `DEL` on another
homeserver is not compared with the marker: it was confirmed after the
revival, at the current epoch, on the homeserver the seller's record names.

## Database guards (migration 0050)

The guarantees above do not depend on which binary runs. Triggers refuse,
for every writer:

| Write | Refused when |
|---|---|
| Tombstone (`deleted_at` NULL to set) | the transaction declares no `marketplace.listing_deletion_authority`, or declares `follower:<holder>:<fence>` that is not the current lease row |
| Tombstone | the transaction does not declare the row's current `record_epoch` in `marketplace.listing_deletion_observed_epoch` (0051); a binary before 0051 declares none |
| Tombstone | its `DEL` cursor is older than the delete a revival of the listing superseded (`revived_from_cursor`), on the same homeserver (`deleted_event_homeserver`, `revived_from_homeserver`; 0100) |
| `record_epoch` | set by a statement to anything but one more: the trigger keeps it unless a revival or a record field changes |
| Revival marker | it would move backwards on its homeserver, or its homeserver changes other than by a revival (0100) |
| Follower cursor insert or update | no current follower authority, or the cursor would move backwards on the same homeserver (0100: a write that changes `homeserver` may set any cursor) |
| Follower cursor delete | always |

The service declares `command` for a `listing.sync` that confirmed the
delete itself and `follower:<holder>:<fence>` for a follower pass
(`DeletionAuthority`). A binary built before 0050 declares nothing, so none
of its tombstones or cursor writes commit once 0050 is applied, and a
refused tombstone rolls back its drop-binding release and its
`listing.deleted` event with it.

`listing_deletion_test.rs` runs the pre-0050 follower SQL against the new
schema: a tombstone held across lease expiry and a new-image takeover, and
a cursor write that checks its lease before the takeover commits. Neither
commits.

## Deploying 0050

**Deploy mode: rolling.** A rolling replacement is safe for correctness:

- 0050's `CREATE TRIGGER` statements wait for every transaction already
  writing `listings` or `listing_deletion_cursors`, so a write that started
  under the old binary before the guards exist commits before any new
  follower starts. The migration gives up after `lock_timeout` 10 s; the
  replica then fails to start, and the next start retries.
- After 0050 is applied and until the old replica stops, the old replica
  refuses its own deletion work:
  - its follower passes fail with `listing tombstone refused` or
    `listing deletion cursor write refused` and release the lease each
    tick, so the new replica's follower takes it;
  - a `listing.sync` it serves for a deleted record fails with an internal
    error and changes nothing. The new replica, or a later sync, tombstones
    the listing.

Those errors in the old replica's logs during the overlap are expected.
Rollback to an image without 0050 is not possible once it is applied
(sqlx refuses a binary missing an applied migration); fix forward.

## Deploying 0051

**Deploy mode: rolling**, on the same terms as 0050. Once 0051 is applied,
an old replica declares no record epoch, so its tombstones are refused with
`the record changed after the delete was confirmed`: its follower passes
fail and release the lease, and a `listing.sync` it serves for a deleted
record fails with an internal error and changes nothing. The new replica
tombstones those listings. Rollback is fix-forward.

## Deploying 0100

**Deploy mode: rolling.** 0100 adds three nullable columns
(`listing_deletion_cursors.homeserver`, `listings.deleted_event_homeserver`,
`listings.revived_from_homeserver`) and replaces both guard functions; every
existing row keeps `NULL`, the configured homeserver, so a `fixed`
deployment compares cursors exactly as before. An old replica in the overlap
writes no homeserver: its cursor writes stay monotonic on the stored
homeserver and its tombstones name the configured one, which is what
`fixed` writes. That holds only while every instance runs `fixed`: an old
replica beside a `pkarr` one writes its configured homeserver's cursor
into a row naming the seller's own homeserver, which the `pkarr` follower
then trusts. Switch to `pkarr` in a later rollout, once every instance runs
0100. The rollout order and backing out are in
[migrations.md](migrations.md#homeserver-scoped-deletion-cursors-0100).
