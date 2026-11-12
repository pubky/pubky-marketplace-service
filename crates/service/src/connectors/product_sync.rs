//! Store product changes, queued for the seller's Shop.
//!
//! A listing linked to a store product (`connector_variant_map`) can follow
//! the store for three field groups: **content** (title, description,
//! images), **price** (each variant's price) and **variants** (options and
//! the variant set). Each group has a sync direction per connection, which a
//! listing may override: `from_store` (the default) or `off`. A fourth group,
//! **status**, is not configurable: a product the store no longer sells is
//! always queued, because a paid order for it could not reach the store.
//!
//! For every linked product the service keeps a **base** per group: the
//! store's value as last applied by the Shop, or as first seen. Whenever the
//! product is observed (a product webhook, or the periodic stock poll that
//! reads the whole catalog anyway) the store's current value is compared with
//! the base. A difference becomes one **pending change** per product and
//! group; a newer store value replaces the proposal of a change still pending,
//! so the Shop only ever sees the latest. A store value that returns to the
//! base withdraws the pending change, and one the seller already dismissed is
//! not raised again. Each observation carries the time its read started; one
//! older than the product's last read is dropped, so a slow read can never
//! replace what a newer one found.
//!
//! The service never writes a listing: listing records live on the seller's
//! homeserver and only the seller's Shop signs them. The Shop reads the
//! pending changes, compares each with the listing as it stands (a field the
//! seller edited since the base is a conflict there), applies or dismisses
//! it, and resolves it here with the hash of the proposal it saw. Applying
//! moves the base to that proposal; a proposal replaced in the meantime is
//! refused as stale.
//!
//! A product follows a group while any listing linked to it does; its base
//! and pending change for the group are shared by those listings. Changing a
//! direction (for the connection or a listing) so that a product starts or
//! stops following a group withdraws the product's pending change for that
//! group and drops its base, so the next observation takes the store as it
//! then stands: nothing reviewed under the old direction can be applied under
//! the new one, and nothing that changed while a group was off is replayed
//! when it is turned back on. A product another listing keeps following
//! keeps its base and pending change. Each group's base is dropped at its own
//! instant: only a read that started after it may take the group's new base,
//! and a change to one group never voids a read for another.
//!
//! Unlinking a product forgets it: its base, its changes and the overrides of
//! listings that no longer link anything go with the mapping. Its snapshot
//! row stays, emptied, as the instant of the unlink: a read that started
//! before it was made for the previous link and is dropped, also when the
//! product is linked again before the read completes.
//!
//! Only what is still needed is kept: a withdrawn change is deleted, a
//! product and group keep at most their pending change and their latest
//! resolution (the dismissal a later observation is compared with), and an
//! unlinked product keeps only its read fences.
//!
//! Lock order, everywhere: the connection row, then the connection's sync
//! lock (direction changes and unlinking), then snapshot rows (several in
//! product-id order), then change and override rows. A change's foreign key
//! is its snapshot, so queueing one never touches the connection row.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::provider::CatalogProduct;

/// A part of a listing that can follow the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldGroup {
    Content,
    Price,
    Variants,
    Status,
}

impl FieldGroup {
    /// The groups with a sync direction.
    pub const SYNCED: [FieldGroup; 3] =
        [FieldGroup::Content, FieldGroup::Price, FieldGroup::Variants];

    pub fn as_str(self) -> &'static str {
        match self {
            FieldGroup::Content => "content",
            FieldGroup::Price => "price",
            FieldGroup::Variants => "variants",
            FieldGroup::Status => "status",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "content" => Some(FieldGroup::Content),
            "price" => Some(FieldGroup::Price),
            "variants" => Some(FieldGroup::Variants),
            "status" => Some(FieldGroup::Status),
            _ => None,
        }
    }

    /// The group's snapshot column; the names are fixed, never user input.
    fn column(self) -> &'static str {
        self.as_str()
    }

    /// The snapshot column of the group's last direction change; status has
    /// no direction.
    fn rebased_column(self) -> Option<&'static str> {
        match self {
            FieldGroup::Content => Some("content_rebased_at"),
            FieldGroup::Price => Some("price_rebased_at"),
            FieldGroup::Variants => Some("variants_rebased_at"),
            FieldGroup::Status => None,
        }
    }
}

/// Where a field group's truth lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// The store is the truth: its changes are queued for the Shop.
    FromStore,
    /// Not synced.
    Off,
}

impl Direction {
    pub const DEFAULT: Direction = Direction::FromStore;

    pub fn as_str(self) -> &'static str {
        match self {
            Direction::FromStore => "from_store",
            Direction::Off => "off",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "from_store" => Some(Direction::FromStore),
            "off" => Some(Direction::Off),
            _ => None,
        }
    }
}

/// A product's value for each field group, in a stable shape: lists are
/// sorted where the store's order carries no meaning, so a reordering alone is
/// never a change.
#[derive(Debug, Clone, PartialEq)]
pub struct ProductValues {
    pub content: Value,
    pub price: Value,
    pub variants: Value,
    pub status: Value,
}

impl ProductValues {
    fn get(&self, group: FieldGroup) -> &Value {
        match group {
            FieldGroup::Content => &self.content,
            FieldGroup::Price => &self.price,
            FieldGroup::Variants => &self.variants,
            FieldGroup::Status => &self.status,
        }
    }
}

pub fn values_of(product: &CatalogProduct) -> ProductValues {
    let mut prices: Vec<Value> = product
        .variants
        .iter()
        .map(|variant| {
            json!({
                "provider_variant_id": variant.provider_variant_id,
                "amount_minor": variant.price.amount_minor,
                "currency": variant.price.currency,
                "exponent": variant.price.exponent,
            })
        })
        .collect();
    prices.sort_by(|a, b| {
        a["provider_variant_id"]
            .as_str()
            .cmp(&b["provider_variant_id"].as_str())
    });
    let mut variants: Vec<Value> = product
        .variants
        .iter()
        .map(|variant| {
            json!({
                "provider_variant_id": variant.provider_variant_id,
                "title": variant.title,
                "sku": variant.sku,
                "option_values": variant
                    .option_values
                    .iter()
                    .map(|value| json!({ "name": value.name, "value": value.value }))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    variants.sort_by(|a, b| {
        a["provider_variant_id"]
            .as_str()
            .cmp(&b["provider_variant_id"].as_str())
    });
    ProductValues {
        content: json!({
            "title": product.title,
            "description": product.description,
            "image_urls": product.image_urls,
        }),
        price: json!({ "variants": prices }),
        variants: json!({
            "options": product
                .options
                .iter()
                .map(|option| json!({ "name": option.name, "values": option.values }))
                .collect::<Vec<_>>(),
            "variants": variants,
        }),
        status: available(true),
    }
}

fn available(on_sale: bool) -> Value {
    json!({ "available": on_sale })
}

/// SHA-256 of the RFC 8785 canonical form, so equal values hash equally
/// whatever their key order.
pub fn value_hash(value: &Value) -> Vec<u8> {
    let canonical = serde_json_canonicalizer::to_vec(value).expect("JSON values canonicalize");
    Sha256::digest(canonical).to_vec()
}

/// What one observation did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Observed {
    /// The read started before the product's last read, or before it was
    /// last unlinked: nothing was done.
    pub stale: bool,
    /// Groups taken as the base (first sight, or after a rebase).
    pub based: u32,
    /// Changes queued or whose proposal was replaced.
    pub queued: u32,
    /// Pending changes withdrawn because the store returned to the base or
    /// to a value the seller dismissed.
    pub withdrawn: u32,
}

/// Per field group with a direction: does any listing linked to the product
/// follow the store?
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Follows {
    content: bool,
    price: bool,
    variants: bool,
}

impl Follows {
    fn group(&self, group: FieldGroup) -> bool {
        match group {
            FieldGroup::Content => self.content,
            FieldGroup::Price => self.price,
            FieldGroup::Variants => self.variants,
            FieldGroup::Status => true,
        }
    }
}

/// The connection's directions, the listing overrides and the listings
/// linked to each product.
#[derive(Debug, Default, Clone)]
pub struct SyncConfig {
    pub settings: BTreeMap<FieldGroup, Direction>,
    pub overrides: BTreeMap<(String, FieldGroup), Direction>,
}

impl SyncConfig {
    pub fn connection_direction(&self, group: FieldGroup) -> Direction {
        self.settings
            .get(&group)
            .copied()
            .unwrap_or(Direction::DEFAULT)
    }

    pub fn listing_direction(&self, listing_id: &str, group: FieldGroup) -> Direction {
        self.overrides
            .get(&(listing_id.to_string(), group))
            .copied()
            .unwrap_or_else(|| self.connection_direction(group))
    }

    fn follows(&self, listings: &BTreeSet<String>) -> Follows {
        let any = |group| {
            listings
                .iter()
                .any(|listing| self.listing_direction(listing, group) == Direction::FromStore)
        };
        Follows {
            content: any(FieldGroup::Content),
            price: any(FieldGroup::Price),
            variants: any(FieldGroup::Variants),
        }
    }
}

pub async fn load_config(
    conn: &mut PgConnection,
    connection_id: Uuid,
) -> Result<SyncConfig, sqlx::Error> {
    let settings: Vec<(String, String)> = sqlx::query_as(
        "SELECT field_group, direction FROM connector_sync_settings WHERE connection_id = $1",
    )
    .bind(connection_id)
    .fetch_all(&mut *conn)
    .await?;
    let overrides: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT listing_id, field_group, direction FROM connector_sync_overrides \
         WHERE connection_id = $1",
    )
    .bind(connection_id)
    .fetch_all(&mut *conn)
    .await?;
    let mut config = SyncConfig::default();
    for (group, direction) in settings {
        if let (Some(group), Some(direction)) =
            (FieldGroup::parse(&group), Direction::parse(&direction))
        {
            config.settings.insert(group, direction);
        }
    }
    for (listing, group, direction) in overrides {
        if let (Some(group), Some(direction)) =
            (FieldGroup::parse(&group), Direction::parse(&direction))
        {
            config.overrides.insert((listing, group), direction);
        }
    }
    Ok(config)
}

/// The listings linked to a store product on a connection.
async fn linked_listings(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
    provider_product_id: &str,
) -> Result<BTreeSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT listing_id FROM connector_variant_map \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(|(listing,)| listing).collect())
}

/// Every store product linked to a listing on the connection.
pub async fn linked_products(
    pool: &PgPool,
    connection_id: Uuid,
) -> Result<BTreeSet<String>, sqlx::Error> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT provider_product_id FROM connector_variant_map WHERE connection_id = $1",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(product,)| product).collect())
}

/// Whether a read of the product started after `at`: a webhook received
/// before that adds nothing, because that read already saw the store as it
/// was then, or later. A read that started at the same instant may have
/// preceded the delivery, so it does not count.
pub async fn checked_since(
    pool: &PgPool,
    connection_id: Uuid,
    provider_product_id: &str,
    at: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let checked: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT checked_at FROM connector_product_snapshots \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .fetch_optional(pool)
    .await?;
    Ok(checked.is_some_and(|checked| checked > at))
}

#[derive(sqlx::FromRow)]
struct SnapshotRow {
    checked_at: DateTime<Utc>,
    unlinked_at: Option<DateTime<Utc>>,
    content_rebased_at: Option<DateTime<Utc>>,
    price_rebased_at: Option<DateTime<Utc>>,
    variants_rebased_at: Option<DateTime<Utc>>,
    content: Option<Value>,
    price: Option<Value>,
    variants: Option<Value>,
    status: Option<Value>,
}

impl SnapshotRow {
    fn get(&self, group: FieldGroup) -> Option<&Value> {
        match group {
            FieldGroup::Content => self.content.as_ref(),
            FieldGroup::Price => self.price.as_ref(),
            FieldGroup::Variants => self.variants.as_ref(),
            FieldGroup::Status => self.status.as_ref(),
        }
    }

    fn rebased_at(&self, group: FieldGroup) -> Option<DateTime<Utc>> {
        match group {
            FieldGroup::Content => self.content_rebased_at,
            FieldGroup::Price => self.price_rebased_at,
            FieldGroup::Variants => self.variants_rebased_at,
            FieldGroup::Status => None,
        }
    }

    /// Whether a read that started at `read_at` is older than the product's
    /// last read, or than its last unlink (a read made for a previous link).
    fn is_stale(&self, read_at: DateTime<Utc>) -> bool {
        self.checked_at > read_at || self.unlinked_at.is_some_and(|unlinked| read_at < unlinked)
    }
}

/// Ensures each product has a snapshot row (an empty one, never read, for a
/// product without one) and locks the rows in product-id order. The insert
/// waits for a concurrent first insert of the same row to commit, so the lock
/// is always taken on the row that transaction wrote.
async fn lock_snapshots(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
    products: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO connector_product_snapshots \
         (connection_id, provider_product_id, checked_at, created_at) \
         SELECT $1, product, 'epoch'::timestamptz, $3 \
         FROM unnest($2::text[]) AS product ORDER BY product \
         ON CONFLICT (connection_id, provider_product_id) DO NOTHING",
    )
    .bind(connection_id)
    .bind(products)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "SELECT provider_product_id FROM connector_product_snapshots \
         WHERE connection_id = $1 AND provider_product_id = ANY($2) \
         ORDER BY provider_product_id FOR UPDATE",
    )
    .bind(connection_id)
    .bind(products)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Locks one product's snapshot row (see [`lock_snapshots`]) and reads it.
async fn lock_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
    provider_product_id: &str,
) -> Result<SnapshotRow, sqlx::Error> {
    lock_snapshots(tx, now, connection_id, &[provider_product_id.to_string()]).await?;
    sqlx::query_as(
        "SELECT checked_at, unlinked_at, content_rebased_at, price_rebased_at, \
                variants_rebased_at, content, price, variants, status \
         FROM connector_product_snapshots \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .fetch_one(&mut **tx)
    .await
}

async fn set_base(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
    provider_product_id: &str,
    group: FieldGroup,
    value: Option<&Value>,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "UPDATE connector_product_snapshots SET {} = $3 \
         WHERE connection_id = $1 AND provider_product_id = $2",
        group.column()
    ))
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(value)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Withdraws the product's pending change for the group. Returns how many
/// were withdrawn (0 or 1).
async fn withdraw(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
    provider_product_id: &str,
    group: FieldGroup,
) -> Result<u32, sqlx::Error> {
    let withdrawn = sqlx::query(
        "DELETE FROM connector_product_changes \
         WHERE connection_id = $1 AND provider_product_id = $2 AND field_group = $3 \
           AND state = 'pending'",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(group.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(u32::try_from(withdrawn.rows_affected()).unwrap_or(u32::MAX))
}

/// The proposal hash the seller last dismissed for the group, when the
/// latest resolution was a dismissal.
async fn dismissed_hash(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
    provider_product_id: &str,
    group: FieldGroup,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    let latest: Option<(String, Vec<u8>)> = sqlx::query_as(
        "SELECT state, proposed_hash FROM connector_product_changes \
         WHERE connection_id = $1 AND provider_product_id = $2 AND field_group = $3 \
           AND state IN ('applied', 'dismissed') \
         ORDER BY seq DESC LIMIT 1",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(group.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    Ok(latest.and_then(|(state, hash)| (state == "dismissed").then_some(hash)))
}

/// Queues `proposed` for the group, or replaces the proposal of the change
/// already pending. Returns whether anything was written.
#[allow(clippy::too_many_arguments)]
async fn queue(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
    provider_product_id: &str,
    group: FieldGroup,
    base: &Value,
    proposed: &Value,
    hash: &[u8],
) -> Result<bool, sqlx::Error> {
    let pending: Option<(Uuid, Vec<u8>)> = sqlx::query_as(
        "SELECT id, proposed_hash FROM connector_product_changes \
         WHERE connection_id = $1 AND provider_product_id = $2 AND field_group = $3 \
           AND state = 'pending' FOR UPDATE",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(group.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    match pending {
        Some((_, current)) if current == hash => Ok(false),
        Some((id, _)) => {
            sqlx::query(
                "UPDATE connector_product_changes \
                 SET proposed = $2, proposed_hash = $3, updated_at = $4 WHERE id = $1",
            )
            .bind(id)
            .bind(proposed)
            .bind(hash)
            .bind(now)
            .execute(&mut **tx)
            .await?;
            Ok(true)
        }
        None => {
            sqlx::query(
                "INSERT INTO connector_product_changes \
                 (id, connection_id, provider_product_id, field_group, base, proposed, \
                  proposed_hash, state, detected_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $8)",
            )
            .bind(Uuid::new_v4())
            .bind(connection_id)
            .bind(provider_product_id)
            .bind(group.as_str())
            .bind(base)
            .bind(proposed)
            .bind(hash)
            .bind(now)
            .execute(&mut **tx)
            .await?;
            Ok(true)
        }
    }
}

/// Compares the store's current values with the product's base and queues
/// the differences. `now` is when the store was read: an observation older
/// than the product's last read is dropped, and a newer one becomes its
/// `checked_at`, which [`checked_since`] compares deliveries with.
async fn compare(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    provider_product_id: &str,
    current: &[(FieldGroup, Value)],
) -> Result<Observed, sqlx::Error> {
    let mut observed = Observed::default();
    let mut tx = pool.begin().await?;
    let snapshot = lock_snapshot(&mut tx, now, connection_id, provider_product_id).await?;
    if snapshot.is_stale(now) {
        observed.stale = true;
        tx.rollback().await?;
        return Ok(observed);
    }
    let listings = linked_listings(&mut tx, connection_id, provider_product_id).await?;
    if listings.is_empty() {
        // Unlinked since the read was planned; the unlink forgot the product.
        tx.rollback().await?;
        return Ok(observed);
    }
    let follows = load_config(&mut tx, connection_id)
        .await?
        .follows(&listings);
    for (group, value) in current {
        let group = *group;
        let Some(base) = snapshot.get(group) else {
            // A group dropped by a change of its own direction is based only
            // by a read that started after it: an older read may carry a
            // value from before the change.
            if snapshot
                .rebased_at(group)
                .is_some_and(|rebased| now < rebased)
            {
                continue;
            }
            // A linked product the service has not seen yet was on sale when
            // it was linked: its removal is a change, not a base.
            if group == FieldGroup::Status && *value != available(true) {
                let base = available(true);
                set_base(
                    &mut tx,
                    connection_id,
                    provider_product_id,
                    group,
                    Some(&base),
                )
                .await?;
                queue(
                    &mut tx,
                    now,
                    connection_id,
                    provider_product_id,
                    group,
                    &base,
                    value,
                    &value_hash(value),
                )
                .await?;
                observed.queued += 1;
            } else {
                set_base(
                    &mut tx,
                    connection_id,
                    provider_product_id,
                    group,
                    Some(value),
                )
                .await?;
                observed.based += 1;
            }
            continue;
        };
        if !follows.group(group) {
            continue;
        }
        let hash = value_hash(value);
        if hash == value_hash(base)
            || dismissed_hash(&mut tx, connection_id, provider_product_id, group).await?
                == Some(hash.clone())
        {
            observed.withdrawn +=
                withdraw(&mut tx, connection_id, provider_product_id, group).await?;
            continue;
        }
        if queue(
            &mut tx,
            now,
            connection_id,
            provider_product_id,
            group,
            base,
            value,
            &hash,
        )
        .await?
        {
            observed.queued += 1;
        }
    }
    sqlx::query(
        "UPDATE connector_product_snapshots SET checked_at = $3 \
         WHERE connection_id = $1 AND provider_product_id = $2",
    )
    .bind(connection_id)
    .bind(provider_product_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(observed)
}

/// Compares a product the store sells, read at `now`, with its base.
pub async fn observe(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    product: &CatalogProduct,
) -> Result<Observed, sqlx::Error> {
    let values = values_of(product);
    let current: Vec<(FieldGroup, Value)> = [
        FieldGroup::Content,
        FieldGroup::Price,
        FieldGroup::Variants,
        FieldGroup::Status,
    ]
    .into_iter()
    .map(|group| (group, values.get(group).clone()))
    .collect();
    compare(
        pool,
        now,
        connection_id,
        &product.provider_product_id,
        &current,
    )
    .await
}

/// Records that the store no longer sells a product, as known at `now`: when
/// the delivery saying so arrived, or when the catalog walk that missed it
/// started. Only its status is compared; the other groups keep their base.
pub async fn observe_removed(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    provider_product_id: &str,
) -> Result<Observed, sqlx::Error> {
    compare(
        pool,
        now,
        connection_id,
        provider_product_id,
        &[(FieldGroup::Status, available(false))],
    )
    .await
}

// === The seller's routes ======================================================

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ChangeRow {
    pub id: Uuid,
    pub provider_product_id: String,
    pub field_group: String,
    pub base: Option<Value>,
    pub proposed: Value,
    pub proposed_hash: Vec<u8>,
    pub detected_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// The pending changes of the connection's linked products, oldest first.
pub async fn list_pending(
    pool: &PgPool,
    connection_id: Uuid,
    limit: i64,
) -> Result<Vec<ChangeRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT c.id, c.provider_product_id, c.field_group, c.base, c.proposed, \
                c.proposed_hash, c.detected_at, c.updated_at \
         FROM connector_product_changes c \
         WHERE c.connection_id = $1 AND c.state = 'pending' \
           AND EXISTS (SELECT 1 FROM connector_variant_map m \
                       WHERE m.connection_id = c.connection_id \
                         AND m.provider_product_id = c.provider_product_id) \
         ORDER BY c.detected_at, c.seq LIMIT $2",
    )
    .bind(connection_id)
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolution {
    /// The Shop wrote the proposal to the listing.
    Applied,
    /// The seller kept the listing as it is.
    Dismissed,
}

impl Resolution {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "applied" => Some(Resolution::Applied),
            "dismissed" => Some(Resolution::Dismissed),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResolveOutcome {
    Resolved,
    NotFound,
    /// The change is no longer pending, or its proposal is not the one the
    /// Shop saw.
    Stale,
}

/// Resolves a pending change with the hash of the proposal the Shop acted
/// on. Applying moves the group's base to that proposal.
pub async fn resolve(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    change_id: Uuid,
    resolution: Resolution,
    seen_hash: &[u8],
) -> Result<ResolveOutcome, sqlx::Error> {
    let product: Option<String> = sqlx::query_scalar(
        "SELECT provider_product_id FROM connector_product_changes \
         WHERE id = $1 AND connection_id = $2",
    )
    .bind(change_id)
    .bind(connection_id)
    .fetch_optional(pool)
    .await?;
    let Some(product) = product else {
        return Ok(ResolveOutcome::NotFound);
    };
    let mut tx = pool.begin().await?;
    lock_snapshots(&mut tx, now, connection_id, std::slice::from_ref(&product)).await?;
    let change: Option<(String, String, Value, Vec<u8>)> = sqlx::query_as(
        "SELECT state, field_group, proposed, proposed_hash FROM connector_product_changes \
         WHERE id = $1 FOR UPDATE",
    )
    .bind(change_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((state, group, proposed, hash)) = change else {
        return Ok(ResolveOutcome::NotFound);
    };
    if state != "pending" || hash != seen_hash {
        return Ok(ResolveOutcome::Stale);
    }
    let group = FieldGroup::parse(&group)
        .ok_or_else(|| sqlx::Error::Decode(format!("unknown field group {group}").into()))?;
    let final_state = match resolution {
        Resolution::Applied => {
            set_base(&mut tx, connection_id, &product, group, Some(&proposed)).await?;
            "applied"
        }
        Resolution::Dismissed => "dismissed",
    };
    sqlx::query(
        "UPDATE connector_product_changes SET state = $2, resolved_at = $3, updated_at = $3 \
         WHERE id = $1",
    )
    .bind(change_id)
    .bind(final_state)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    // Only the latest resolution is ever read again.
    sqlx::query(
        "DELETE FROM connector_product_changes \
         WHERE connection_id = $1 AND provider_product_id = $2 AND field_group = $3 \
           AND state IN ('applied', 'dismissed') AND id <> $4",
    )
    .bind(connection_id)
    .bind(&product)
    .bind(group.as_str())
    .bind(change_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ResolveOutcome::Resolved)
}

/// Withdraws the group's pending changes and drops its base for every
/// product given, so the next observation takes the store as it then stands.
/// A fixed number of statements, however many products.
async fn rebase(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
    group: FieldGroup,
    products: &BTreeSet<String>,
) -> Result<(), sqlx::Error> {
    let Some(rebased_column) = group.rebased_column() else {
        return Ok(());
    };
    if products.is_empty() {
        return Ok(());
    }
    let products: Vec<String> = products.iter().cloned().collect();
    lock_snapshots(tx, now, connection_id, &products).await?;
    sqlx::query(
        "DELETE FROM connector_product_changes \
         WHERE connection_id = $1 AND field_group = $2 AND state = 'pending' \
           AND provider_product_id = ANY($3)",
    )
    .bind(connection_id)
    .bind(group.as_str())
    .bind(&products)
    .execute(&mut **tx)
    .await?;
    sqlx::query(&format!(
        "UPDATE connector_product_snapshots SET {} = NULL, {rebased_column} = $3 \
         WHERE connection_id = $1 AND provider_product_id = ANY($2)",
        group.column()
    ))
    .bind(connection_id)
    .bind(&products)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Forgets what the connection no longer links, in the transaction that
/// changed its mappings, at `now`: the bases and changes of products no
/// listing links any more, and the overrides of listings that link nothing. A
/// product linked again later is based afresh; a listing linked again follows
/// the connection's directions.
///
/// An unlinked product's snapshot row stays, emptied, with `unlinked_at` set
/// to `now`: a read that started before it was made for the previous link,
/// and is dropped even when it completes after the product is linked again.
/// Every call moves the fence of every product still unlinked, which only
/// drops reads that started while nothing linked the product.
pub async fn forget_unlinked(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
) -> Result<(), sqlx::Error> {
    // The same lock as the direction changes, taken before any snapshot or
    // override row: both touch overrides and snapshots, in opposite orders.
    lock_config(tx, connection_id).await?;
    let orphans: Vec<String> = sqlx::query_scalar(
        "SELECT s.provider_product_id FROM connector_product_snapshots s \
         WHERE s.connection_id = $1 AND NOT EXISTS ( \
             SELECT 1 FROM connector_variant_map m \
             WHERE m.connection_id = s.connection_id \
               AND m.provider_product_id = s.provider_product_id) \
         ORDER BY s.provider_product_id FOR UPDATE",
    )
    .bind(connection_id)
    .fetch_all(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM connector_product_changes c \
         WHERE c.connection_id = $1 AND NOT EXISTS ( \
             SELECT 1 FROM connector_variant_map m \
             WHERE m.connection_id = c.connection_id \
               AND m.provider_product_id = c.provider_product_id)",
    )
    .bind(connection_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE connector_product_snapshots \
         SET content = NULL, price = NULL, variants = NULL, status = NULL, \
             unlinked_at = GREATEST(unlinked_at, $3) \
         WHERE connection_id = $1 AND provider_product_id = ANY($2)",
    )
    .bind(connection_id)
    .bind(&orphans)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM connector_sync_overrides o \
         WHERE o.connection_id = $1 AND NOT EXISTS ( \
             SELECT 1 FROM connector_variant_map m \
             WHERE m.connection_id = o.connection_id AND m.listing_id = o.listing_id)",
    )
    .bind(connection_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Rebases the group for those of `products` whose base it changes: a
/// product is followed while any listing linked to it follows the group, and
/// its base and pending change are shared by those listings. A product still
/// followed (or still not) under `after` keeps both, so a listing whose own
/// direction did not change keeps the change it has not applied yet.
async fn rebase_where_following_changes(
    tx: &mut Transaction<'_, Postgres>,
    now: DateTime<Utc>,
    connection_id: Uuid,
    group: FieldGroup,
    before: &SyncConfig,
    after: &SyncConfig,
    products: &BTreeSet<String>,
) -> Result<(), sqlx::Error> {
    if products.is_empty() {
        return Ok(());
    }
    let candidates: Vec<String> = products.iter().cloned().collect();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT provider_product_id, listing_id FROM connector_variant_map \
         WHERE connection_id = $1 AND provider_product_id = ANY($2)",
    )
    .bind(connection_id)
    .bind(&candidates)
    .fetch_all(&mut **tx)
    .await?;
    let mut listings: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (product, listing) in rows {
        listings.entry(product).or_default().insert(listing);
    }
    let changed: BTreeSet<String> = listings
        .into_iter()
        .filter(|(_, listings)| {
            before.follows(listings).group(group) != after.follows(listings).group(group)
        })
        .map(|(product, _)| product)
        .collect();
    rebase(tx, now, connection_id, group, &changed).await
}

/// Products linked to any of `listings`, in name order (the lock order of
/// [`rebase`]).
async fn products_of_listings(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
    listings: &BTreeSet<String>,
) -> Result<BTreeSet<String>, sqlx::Error> {
    let listings: Vec<String> = listings.iter().cloned().collect();
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT provider_product_id FROM connector_variant_map \
         WHERE connection_id = $1 AND listing_id = ANY($2)",
    )
    .bind(connection_id)
    .bind(&listings)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(|(product,)| product).collect())
}

/// Serialises direction changes and unlinking per connection. The
/// connection row comes first, in the mode the rows written next take through
/// their foreign keys anyway: a mapping save locks that row before this lock,
/// so taking them in the other order could deadlock with it.
async fn lock_config(
    tx: &mut Transaction<'_, Postgres>,
    connection_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1 FROM connector_connections WHERE id = $1 FOR KEY SHARE")
        .bind(connection_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('connector-sync|' || $1::text, 0))")
        .bind(connection_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Sets the connection's direction for each group given. A group whose
/// direction changes is rebased for every product of a listing that follows
/// the connection's setting for it (no override), unless another listing of
/// the product keeps it followed or unfollowed.
pub async fn put_settings(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    changes: &[(FieldGroup, Direction)],
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_config(&mut tx, connection_id).await?;
    let config = load_config(&mut tx, connection_id).await?;
    let all_listings: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT listing_id FROM connector_variant_map WHERE connection_id = $1",
    )
    .bind(connection_id)
    .fetch_all(&mut *tx)
    .await?;
    for (group, direction) in changes {
        if config.connection_direction(*group) == *direction {
            continue;
        }
        sqlx::query(
            "INSERT INTO connector_sync_settings (connection_id, field_group, direction, updated_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (connection_id, field_group) \
             DO UPDATE SET direction = EXCLUDED.direction, updated_at = EXCLUDED.updated_at",
        )
        .bind(connection_id)
        .bind(group.as_str())
        .bind(direction.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let following: BTreeSet<String> = all_listings
            .iter()
            .map(|(listing,)| listing.clone())
            .filter(|listing| !config.overrides.contains_key(&(listing.clone(), *group)))
            .collect();
        let products = products_of_listings(&mut tx, connection_id, &following).await?;
        let mut after = config.clone();
        after.settings.insert(*group, *direction);
        rebase_where_following_changes(
            &mut tx,
            now,
            connection_id,
            *group,
            &config,
            &after,
            &products,
        )
        .await?;
    }
    tx.commit().await
}

/// Sets (or, with `None`, removes) a linked listing's own direction for each
/// group given. A group whose effective direction changes is rebased for the
/// listing's products. Returns `false`, changing nothing, when the listing is
/// not linked: checked under the lock an unlink takes, so an override can
/// never outlive its listing's link.
pub async fn put_override(
    pool: &PgPool,
    now: DateTime<Utc>,
    connection_id: Uuid,
    listing_id: &str,
    changes: &[(FieldGroup, Option<Direction>)],
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    lock_config(&mut tx, connection_id).await?;
    let listing: BTreeSet<String> = [listing_id.to_string()].into_iter().collect();
    if products_of_listings(&mut tx, connection_id, &listing)
        .await?
        .is_empty()
    {
        return Ok(false);
    }
    let config = load_config(&mut tx, connection_id).await?;
    for (group, direction) in changes {
        let before = config.listing_direction(listing_id, *group);
        match direction {
            Some(direction) => {
                sqlx::query(
                    "INSERT INTO connector_sync_overrides \
                     (connection_id, listing_id, field_group, direction, updated_at) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (connection_id, listing_id, field_group) \
                     DO UPDATE SET direction = EXCLUDED.direction, updated_at = EXCLUDED.updated_at",
                )
                .bind(connection_id)
                .bind(listing_id)
                .bind(group.as_str())
                .bind(direction.as_str())
                .bind(now)
                .execute(&mut *tx)
                .await?;
            }
            None => {
                sqlx::query(
                    "DELETE FROM connector_sync_overrides \
                     WHERE connection_id = $1 AND listing_id = $2 AND field_group = $3",
                )
                .bind(connection_id)
                .bind(listing_id)
                .bind(group.as_str())
                .execute(&mut *tx)
                .await?;
            }
        }
        let mut after = config.clone();
        let key = (listing_id.to_string(), *group);
        match direction {
            Some(direction) => after.overrides.insert(key, *direction),
            None => after.overrides.remove(&key),
        };
        if before != after.listing_direction(listing_id, *group) {
            let products = products_of_listings(&mut tx, connection_id, &listing).await?;
            rebase_where_following_changes(
                &mut tx,
                now,
                connection_id,
                *group,
                &config,
                &after,
                &products,
            )
            .await?;
        }
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::provider::{CatalogOption, CatalogOptionValue, CatalogVariant, Price};

    fn product() -> CatalogProduct {
        CatalogProduct {
            provider_product_id: "gid://shopify/Product/1".into(),
            handle: None,
            title: "Hat".into(),
            description: Some("Wool".into()),
            url: None,
            options: vec![CatalogOption {
                name: "Size".into(),
                values: vec!["S".into(), "M".into()],
            }],
            image_urls: vec!["https://cdn.example/hat.png".into()],
            variants: [
                "gid://shopify/ProductVariant/2",
                "gid://shopify/ProductVariant/1",
            ]
            .iter()
            .enumerate()
            .map(|(index, id)| CatalogVariant {
                provider_variant_id: (*id).into(),
                sku: None,
                title: Some(if index == 0 { "M" } else { "S" }.into()),
                option_values: vec![CatalogOptionValue {
                    name: "Size".into(),
                    value: if index == 0 { "M" } else { "S" }.into(),
                }],
                price: Price {
                    amount_minor: 2100,
                    currency: "USD".into(),
                    exponent: 2,
                },
                stock: Some(3),
                made_to_order: false,
                image_url: None,
            })
            .collect(),
            categories: vec![],
            attributes: vec![],
        }
    }

    #[test]
    fn stock_and_variant_order_are_not_part_of_any_group() {
        let mut moved = product();
        moved.variants.reverse();
        for variant in &mut moved.variants {
            variant.stock = Some(99);
        }
        assert_eq!(values_of(&product()), values_of(&moved));
        let mut repriced = product();
        repriced.variants[1].price.amount_minor = 2500;
        let (before, after) = (values_of(&product()), values_of(&repriced));
        assert_ne!(before.price, after.price);
        assert_eq!(before.content, after.content);
        assert_eq!(before.variants, after.variants);
    }

    #[test]
    fn the_hash_ignores_key_order() {
        let a = json!({ "title": "Hat", "description": null });
        let b: Value = serde_json::from_str(r#"{"description":null,"title":"Hat"}"#).expect("json");
        assert_eq!(value_hash(&a), value_hash(&b));
        assert_ne!(
            value_hash(&a),
            value_hash(&json!({ "title": "Cap", "description": null }))
        );
    }

    #[test]
    fn a_listing_override_wins_over_the_connection() {
        let mut config = SyncConfig::default();
        assert_eq!(
            config.listing_direction("l", FieldGroup::Price),
            Direction::FromStore
        );
        config.settings.insert(FieldGroup::Price, Direction::Off);
        assert_eq!(
            config.listing_direction("l", FieldGroup::Price),
            Direction::Off
        );
        config
            .overrides
            .insert(("l".into(), FieldGroup::Price), Direction::FromStore);
        assert_eq!(
            config.listing_direction("l", FieldGroup::Price),
            Direction::FromStore
        );
        let listings: BTreeSet<String> = ["l".to_string(), "m".to_string()].into_iter().collect();
        let follows = config.follows(&listings);
        assert!(follows.price, "one following listing is enough");
        assert!(follows.content && follows.variants);
    }
}
