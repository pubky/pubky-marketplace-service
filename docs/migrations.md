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
