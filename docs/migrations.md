# Migrations

Files under `crates/service/migrations/` are additive-only, carry no DOWN
migration, and are directly rerunnable unless a rule below says otherwise.
An applied file is immutable: sqlx checksums every applied migration, so a
changed byte stops the next start.

## A ledger ahead of the build

Startup tolerates applied versions that are newer than every migration in the
running build: a newer image, or a branch image on staging, migrated first.
The build logs them (`database has migrations newer than this build`) and
starts, so rolling an image back across an additive migration works without
editing `_sqlx_migrations`. An applied version the build lacks at or below its
newest migration means a file was removed or renumbered, and startup is
refused as before. Checksums of the versions the build knows are always
verified, so a branch migration that later lands under the same number with
different bytes still stops startup; delete that branch row first.

## Index builds on existing tables

A plain `CREATE INDEX` holds a `SHARE` lock on its table for the whole
build, which blocks every `INSERT`, `UPDATE`, and `DELETE`. On `listings`,
`orders`, `payments`, or `events` that pauses checkout, payment
confirmation, and the workers until the build ends. A plain `DROP INDEX`
takes an `ACCESS EXCLUSIVE` lock, which also blocks reads. A `UNIQUE`,
`PRIMARY KEY`, or `EXCLUDE` constraint added to an existing table, as a
table constraint or inline on a new column, builds its index the same way.

Every migration after 0049 that builds, drops, or rebuilds an index on a
table the same file did not create uses this shape:

```sql
-- no-transaction
CREATE INDEX CONCURRENTLY listings_example_idx
    ON listings (seller_pubky, updated_at)
    WHERE deleted_at IS NULL
```

- The first line is `-- no-transaction`, so sqlx runs the file outside a
  transaction.
- The file holds exactly one statement. Postgres runs a query string of
  several statements as one implicit transaction and refuses
  `CONCURRENTLY` inside it, and sqlx sends a no-transaction file as one
  query string.
- A concurrent build never uses `IF NOT EXISTS`. A concurrent build that
  fails (uniqueness violation, deadlock, cancellation, crash) leaves an
  `INVALID` index behind, and sqlx records a no-transaction migration only
  after it succeeds, so the next start reruns the file. `IF NOT EXISTS`
  would skip the invalid index and record the migration as applied. Without
  it the start fails on the leftover name and the deployment stops.
- Drops use `DROP INDEX CONCURRENTLY [IF EXISTS] name`, also alone in a
  no-transaction file. Rebuilds are a concurrent build under a new name,
  then a concurrent drop of the old one, in two files.
- A unique constraint on an existing table is a concurrent unique index
  first, then `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE USING INDEX name`
  in a later, ordinary migration.
- An index on a table the same file creates may use a plain build: the
  table is empty.

These files are the exception to "directly rerunnable": sqlx never reruns
a recorded migration, and a rerun after a failure fails loudly instead of
recording an invalid index.

### Recovering from a failed concurrent build

1. List invalid indexes:
   `SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;`
2. Drop the one the failed migration names:
   `DROP INDEX CONCURRENTLY <name>;`
3. Redeploy. The migration reruns from the start.

### What a concurrent build waits for

A concurrent build waits for the transactions writing the table when it
starts and again between its scans, and at the end for every transaction
whose snapshot predates the build. Writes continue meanwhile. During a rolling deployment the old
replica keeps serving; a long transaction on either replica makes the build
take longer, not the writes.

### Enforcement

`crates/service/tests/migration_rules_test.rs` checks every migration after
0049 against these rules, shows the checker refusing each unsafe shape, and
runs the fixtures in `crates/service/tests/fixtures/migration_rules/`
through the sqlx migrator against Postgres: a concurrent build lets a write
through while it waits, and the same build inside a transaction or beside a
second statement fails before creating anything.

0001–0049 predate the rule and are applied. 0047 built
`listings_live_seller_idx` and rebuilt `drop_listings_one_active_per_listing`
with plain builds.

### Indexes the listing tombstones use

| Query | Index |
|---|---|
| Deletion follower: sellers with live listings | `listings_live_seller_idx` (0047) |
| Tombstone and drop gating: bindings of one listing | `drop_listings_listing_idx` (0010) |
| One active drop per listing generation | `drop_listings_one_active_per_listing` (0047) |
| Follower cursors, leases | primary keys of `listing_deletion_cursors`, `worker_leases` |

0050 (fenced listing deletion) adds `worker_leases.fence`,
`listings.revived_from_cursor`, and two guard triggers, and no index. It
sets `lock_timeout` so its brief `listings` locks cannot queue behind a
long transaction.

0051 (listing revival proof) adds `listings.record_epoch` with a constant
default (no rewrite), a `listings` trigger that maintains it, replaces the
`listings` guard function, and creates `listing_revival_checks` with its
primary key. It builds no index on an existing table and sets
`lock_timeout` like 0050. No index serves the follower's revival read (live
listings with `revived_from_cursor` set); it filters the live listings.

0053 (payment assets) widens `orders.payment_method` to `usdt`, adds five
nullable `orders` columns for what the buyer sends (`payment_asset`,
`payment_network`, `payment_amount_minor`, `payment_exponent`,
`payment_quote_basis`), and creates `seller_accepted_payment_options` with its
primary key. The two `orders` CHECKs (the method list; the five columns all
set or all NULL with a positive amount) are `NOT VALID`: new and updated rows
are checked, no table scan runs under the lock. No default, no rewrite, no
index on an existing table; it sets `lock_timeout` like 0050. A binary from
before it lists its `orders` columns and ignores them. The asset, network and
quote basis have no CHECK on purpose: a later value is not a migration.

0054 (refund destinations) creates `order_refund_destinations` with its
primary key and a foreign key to `orders`, and appends one refusal-audit
command kind (`confirm_refund_destination`, 39). The table holds the
Arbitrum One address a buyer confirmed for a USDT refund, one row per order.
Its CHECKs fix the asset (`USDT`), the network (`arbitrum-one`), the address
shape and the source (`buyer_entered`); a later source is a widened CHECK in
a later migration. It builds no index on an existing table and sets
`lock_timeout` like 0050.

0060 (single Paykit bind) widens `orders.payment_method` to `paykit` and adds
six nullable `orders` columns: the multi-asset request terms
(`paykit_amount_minor`, `paykit_accepted_assets`, `paykit_payment_amounts`,
set together or all NULL), a USDT payment's `payment_finality` with
`payment_finality_at`, and `paykit_second_rail_at`. It drops `NOT NULL` from
`paykit_superseded_attempts.total_sats` (a released USDT-only attempt has no
satoshi total) and replaces its positive check with one that allows NULL. It
also widens `orders_external_refund_within_total` to admit the paid amount
(`payment_amount_minor`): a refund of a USDT-paid order priced in satoshis is
recorded in USDT0 millionths; the bound only widens, so existing rows satisfy
it. The
`orders` CHECKs are `NOT VALID` like 0053's; no default, no rewrite, no index.
A binary from before it lists its `orders` columns and ignores them. It sets
`lock_timeout` like 0050.

0055 (priv data key custody releases) creates `user_priv_key_custody_releases`
with its primary key and a key-count check. It builds no index on an existing
table and takes no lock on one. Roll forward rather than back once any row
exists: a binary that predates 0055 would create a new data key for a released
owner (see [private data key custody](priv-key-custody.md)).

0058 (purge retired Stripe values) is one `UPDATE` that sets
`seller_payment_configs.stripe_payment_link` and
`stripe_restricted_key_ciphertext` to `NULL` for the rows that hold a value.
It changes no schema and builds no index, and a second run matches no row.
The two columns stay: 0009 created them and dropping a column is not
additive. Orders and payments bound to Stripe are not touched.

0059 (listing deletion follower backoff) drops `NOT NULL` from
`listing_deletion_cursors.polled_at` and adds `attempted_at`, `failures`
(`CHECK (failures >= 0)`) and `retry_not_before`. It builds no index. A
binary from before 0059 ignores the new columns and its inserts take their
defaults, so a rolling replacement is safe.

## Store connectors (0062 to 0067)

The connector framework uses 0062 to 0064 and 0066 of the 0062 to 0069 range reserved
for connectors (0065 and 0067 are the Shopify provider's). They follow 0061 (multi-currency
FX, merged) and read two of its `orders` columns only as optional values
(`usd_conversion_pair`, `usd_conversion_cents`). All are additive and
rerunnable.

No connector row exists on any deployment before these migrations run: the
connector tables are created only by 0062 and 0063, which are in this pull
request and unmerged, and `CONNECTORS_ENABLED` defaults off, so the worker
never writes or reads one until it is switched on. A refund mirror queued with
an older payload shape (an `amount_unit` of `sat` or `usdt`) therefore cannot
exist, and the worker reads only the payload `connectors::outbox` writes today
(amounts in the order currency, `blocked_code` when a refund cannot be
converted).

- 0062 creates `connector_connections` (one row per seller, provider and
  shop: a `draft` that expires after 24 hours, then `connected`,
  `needs_attention` with the missing scopes, `error` or `disconnected`; its
  `app_kind` is `none`, `custom` or the reserved `public`),
  `connector_app_credentials` (a merchant's own app client id and secret,
  sealed per connection, with an optional `retain_until` after a disconnect),
  `connector_credentials` (the sealed store secret), `connector_recipient_keys` (the sealed X25519
  key a buyer's opted-in address is sealed to, and its public half),
  `connector_variant_map` and `connector_product_selection`. Every index is
  built on a table the same file creates, so none blocks writes. One live
  (connected or error) connection per store is a unique partial index.
- 0063 creates `connector_order_push` (the idempotent push ledger, unique per
  seller, order and provider), `connector_order_actions` (cancel and refund
  mirrors), `connector_event_cursor` (started at the newest event when the
  file runs, so earlier orders are never pushed), `connector_inbound_events`
  (webhook dedupe) and `connector_alerts`. It reads `events` once, for the
  cursor's starting point, and builds indexes only on its own tables.
- 0064 adds five nullable columns to `orders` (`connector_share_domain`,
  `connector_share_consent_version`, `connector_share_consented_at`,
  `connector_address_envelope`, `connector_key_id`) and one `NOT VALID`
  CHECK (consent columns together, envelope and key id together and only with
  consent, envelope exactly 2,096 bytes). No default, no rewrite, no index; it
  sets `lock_timeout` like 0053. A binary from before 0064 ignores the columns.
  The envelope is deleted when the store order exists or when the order
  reaches a final state; the consent columns stay.
- 0066 widens the `connector_alerts` kind check from 0063 by one value,
  `refund_reversed_after_mirror` (a refund already mirrored to a store was
  reversed or reduced in Pubky). The table is small and new; the constraint is
  dropped and re-added under a short `lock_timeout`. A binary from before 0066
  never writes the value.

- 0065 adds `connector_connections.variant_mode` (`native` or `split`, default
  `native`, a CHECK on the two values). A constant default needs no table rewrite;
  it sets `lock_timeout` like 0053. A binary from before 0065 ignores the column
  and treats every connection as `native`, which is what every existing row holds.

- 0067 widens the `connector_alerts` kind check from 0066 by three values
  (`listings_unmapped`, `variant_mapping_error`, `push_total_mismatch`). Additive
  and rerunnable; the table is small, so the constraint is replaced in one pass
  under a short `lock_timeout`. A binary from before 0067 never writes the new
  kinds.

Roll forward rather than back once a connection exists: a binary that predates
0062 does not know the tables, but nothing in it reads them.

0071 (update tracking command kind) appends one refusal-audit command kind
(`update_tracking`, 40) for `fulfillment.update_tracking`. It creates no table
or column and builds no index; the tracking history rides in the existing
`orders.shipment` JSON. The catalog is append-only and a test keeps the source
enum and the table in exact parity. Refused commands are recorded against the
catalog row, so the migration must run before a binary that can refuse
`fulfillment.update_tracking`; migrations run at start, so a deployment gets
both together. The insert is `ON CONFLICT DO NOTHING` so the file is directly
rerunnable, and a closing check fails the migration if id 40 already holds a
different name. The number jumps from 0061 to 0071 on purpose (0062 to 0070 are
claimed by other branches); sqlx applies pending migrations in version order
and does not need the numbers to be contiguous, which `migration_0033_test`
already exercises with a missing 0032.

0073 (made-to-order stock) adds `listing_variant_inventory.made_to_order` and
`listings.made_to_order` (`BOOLEAN NOT NULL DEFAULT false`, a constant default,
so no rewrite), a `NOT VALID` CHECK that a made-to-order row has
`available_quantity = 0` (every existing row is `false`), replaces the 0070
roll-up function so it maintains `listings.made_to_order` and marks
made-to-order entries of `variant_availability`, and widens
`connector_alerts_kind` with `listing_not_linked` (small table, one pass). It
builds no index and sets `lock_timeout` like 0050. A binary from before 0073
never sets the flag, and on a made-to-order row its writes fail closed: a claim
would take `available` below 0, which the existing CHECK refuses.

0074 (connector stock refresh) adds the nullable
`connector_connections.stock_refresh_requested_at`: saving a connection's
mappings sets it and the connector worker reads that connection's stock on its
next tick, then clears it. No default, no index; `lock_timeout` like 0050. A
binary from before 0074 never sets or reads it.

0072 (grant sessions) creates `grant_session_challenges` with its primary key,
two CHECKs, and plain indexes on `expires_at` and on `pubky` for unused rows
(a new, empty table), adds the nullable `auth_sessions.source_grant_id` and
`source_client_id` columns, and widens the `automation_rate_limits`
endpoint-class CHECK with `grant_session.mint` as `NOT VALID`. It builds no
index on an existing table and sets `lock_timeout` like 0050. A binary from
before 0072 never reads the new table and its session inserts leave the new
columns NULL.

## Numbers claimed by open pull requests

`main` has 0001 to 0054 and 0056 to 0059; 0055 is #92, so the catalog is
gapless 0001 to 0059 once it lands; 0060 is the single Paykit bind. Pull requests that are open when this was
written claim numbers, so a new migration must check the open list
(`gh pr list`) and take the next free number (0060 or later) rather than
assume it is free:

| Number | File | Pull request |
| --- | --- | --- |
| 0055 | `0055_user_priv_key_custody_releases.sql` | #92 (`/priv` Phase 4) |
| 0071 | `0071_update_tracking_command_kind.sql` | optional tracking (`cursor/optional-tracking-85f6`) |
| 0072 | `0072_grant_sessions.sql` | #137 (grant sessions) |

This table describes unmerged work and goes stale as pull requests land; trust
`crates/service/migrations/` on `main` over it.

## Sealed delivery addresses (0057)

`0057_encrypted_delivery_address.sql` adds `orders.delivery_address_envelope`
and `orders.delivery_key_id`, two `NOT VALID`-then-`VALIDATE` constraints
(the pair travels together at exactly 2,096 bytes and a 32-hex key id; a
plaintext address and an envelope never share a row), and clears
`delivery_address` on orders already in a final state. The sweep is an UPDATE,
so it drops `orders_total_balance` and re-adds it `NOT VALID` around itself, as
0043 does: staging's tax-era orders break that constraint and would otherwise
abort the file. Number 0055 belongs to the owner-held key custody release
table (#92).

## Multi-currency FX (0061)

`0061_multi_currency_fx.sql` is additive and directly rerunnable. It widens
`fx_rate_samples` from USD only to the seven fiat currencies (the currency
check) with one sanity band per currency (`fx_rate_samples_rate_check`,
repeated from `fx::rate_bounds` and kept equal by a test), and adds the six
`usd_conversion_*` columns with an all-or-nothing, currency-matching check to
`orders` and `paykit_superseded_attempts`. Constraints are `NOT VALID` then
validated. See `docs/multi-currency.md`.

It follows 0060 (the single Paykit bind) in number and in the branch it is
stacked on, and reads or writes nothing 0060 creates. A binary from before
0061 ignores the new columns, and its sampler inserts only USD rows, which
both constraints admit, so a rolling replacement is safe. After 0061 the new
sampler needs about three minutes of samples before a non-USD price can be
quoted.

## Per-variant inventory (0070)

`0070_variant_inventory.sql` is additive and directly rerunnable. It takes
number 0070 and reads and writes nothing from 0060 to 0069 (0062 to 0069 are
the connectors'): it depends only on `listings` and `reservations`. See [per-variant stock](api.md#per-variant-stock).

- `listing_variant_inventory` (new table): one row per variant,
  `(listing_aggregate_id, variant_id)` primary key, `enabled`, `position`,
  `sku`, and `total`/`available`/`reserved`/`sold` quantities with the same
  balance check as `listings`. `variant_id = ''` is the implicit row of a
  listing that sells one variant (no sku, enabled, position 0).
- `listings.variant_availability` (nullable `JSONB`, no default, no rewrite):
  the public per-variant view, written only by the roll-up trigger.
- `reservations.variant_id` (nullable `TEXT`): the variant row a reservation
  holds. `NULL` is the implicit row, which is every reservation that exists.
- `listing_variant_conversion_blocks` (new table, one row per listing): the
  operator-visible marker of a listing whose record lists two or more enabled
  variants but that cannot convert yet. `reason` is `open_holds`,
  `unattributable_sales`, `adjusted_total` or `below_committed` (see
  [api.md](api.md#per-variant-stock)); `attempts`, `first_blocked_at` and `last_attempt_at`
  show how long and how often. A sync upserts it while the listing is blocked
  and deletes it when the listing converts or its record stops listing several
  variants. `scripts/variant-conversion-sweep.sh` (read-only, like
  `listing-record-sweep.sh`) lists it and prints the `sync-many` body that
  retries those listings; `sync-many` reports the state per item.
- Backfill: one implicit row per listing that has none, equal to the listing's
  totals. A rerun adds nothing to a listing that already has rows, so it never
  re-adds an implicit row beside keyed rows. The statement reads `listings`
  and inserts into the new table; it takes no lock on `listings`.
- Three triggers keep the listing's quantity columns the sum of its rows in
  the same statement as every change: `listing_variant_inventory_roll_up`
  (rows to listing), `listings_variant_rows_follow` (a direct write of the
  listing's quantity columns, to the implicit row) and
  `listings_implicit_variant_row` (every new listing has its implicit row).

No index is built on an existing table. `ALTER TABLE ... ADD COLUMN` adds
nullable columns without a default, and the file sets `lock_timeout` like 0050,
so creating the triggers on `listings` fails the start instead of queueing
behind a long transaction.

**Deploy mode: rolling.** A binary from before 0070 keeps working on a listing
that sells one variant: its write of the listing's totals is copied into the
implicit row, and its `INSERT INTO listings` gets an implicit row from the
insert trigger. It cannot sell a listing that tracks several variants: its
write of that listing's totals raises `check_violation` (`listing ... tracks
stock per variant`) and rolls that transaction back, instead of selling units
the variant rows do not know about. A listing becomes variant-tracked only when
a new binary reads a record with two or more enabled variants, so during the
overlap an old replica refuses only those listings. Roll forward rather than
back once a listing tracks variants: a binary that predates 0070 would refuse
every sale of it.

The backfill gives every existing listing one implicit row, because a
migration cannot read the seller's homeserver record. A listing whose record
lists two or more enabled variants becomes variant-tracked at its next sync.
After the deploy, run `sync-many` over the live listings that have several
variants so they convert at once (a listing with open holds, or with sales that
do not attribute to the record's variants, converts at a later sync and shows
in `scripts/variant-conversion-sweep.sh` until then).

**Totals that counted a disabled variant.** Before 0070 a listing that sold one
variant counted the stock of the record's disabled variants in its total. The
first sync of the record after the deploy recomputes the total from the enabled
variants: at the same record revision while the total is still exactly the old
sum (a seller's `inventory.adjust` has moved it otherwise, and stays), and at
any newer revision as for every edit. The new total is never below the units
already reserved or sold. For a listing whose enabled stock is below its sold
units the total becomes the sold units and the listing reads `sold`. Listings
with no disabled variant are untouched. `sync-many` over the live listings
applies it at once.

`crates/service/tests/migration_0070_test.rs` (serial: it replays 0032 on a
scratch database) pins the backfill on a database migrated through 0061 (the last migration
before 0070 in this tree; 0062 to 0069 are the connectors' and are not here), the
rerun, each trigger, the old-binary write, and the table's checks.

