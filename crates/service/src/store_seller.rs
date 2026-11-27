//! Single-seller mode: `STORE_SELLER_PUBKY` binds the service to one seller.
//!
//! Every command and every signed-in route that acts for a seller names that
//! seller through what it touches: a listing or drop aggregate, a seller
//! pubky, an offer, an order, a payment or a receipt. [`serves`] resolves
//! each to its seller and admits the request only when every one is the
//! store seller. The executor runs it for each command before any other gate
//! ([`command_refusal`]); [`require_store_seller`] runs it for each signed-in
//! route from the route's entry in [`ROUTES`], and [`refuse_unlisted_routes`]
//! refuses any route missing from it wherever it is mounted. All refuse with
//! `SELLER_NOT_SERVED` (HTTP 409) and write a `seller_not_served` refusal
//! audit row for an authenticated actor. Sessions are still issued to any
//! identity: buyers need them.
//! At boot, [`refuse_other_sellers`] refuses a database that already holds
//! another seller's listings, orders, holds, store connections, payment
//! configurations, webhook endpoints or seller settings.
//!
//! With `STORE_SELLER_PUBKY` unset the service is a marketplace and nothing
//! here runs.

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{FromRequestParts, MatchedPath, RawPathParams, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use marketplace_domain::commands::{Command, CommandPayload};
use marketplace_domain::ErrorCode;
use serde_json::json;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::auth::Actor;
use crate::result::CommandFailure;
use crate::store_claim::ClaimCode;
use crate::AppState;

/// The optional pre-pinned seller.
pub const ENV_STORE_SELLER_PUBKY: &str = "STORE_SELLER_PUBKY";
pub const ENV_STORE_MODE: &str = "STORE_MODE";
/// Shared by every boot binding read, the env insert, claims and release.
pub const STORE_BINDING_LOCK_KEY: i64 = 4_713_371_337_102;
pub const STORE_UNCLAIMED_MESSAGE: &str = "This store has not been claimed by its seller yet.";
pub const STORE_ALREADY_CLAIMED_MESSAGE: &str = "This store has already been claimed.";

/// `off` turns the index moderation gate off. Allowed only with
/// [`ENV_STORE_SELLER_PUBKY`]; unset (or `on`) keeps it on.
pub const ENV_LISTING_MODERATION: &str = "LISTING_MODERATION";

pub const SELLER_NOT_SERVED_MESSAGE: &str = "This service sells only its store seller's listings.";

/// Boot-time store configuration. A missing pin is claim mode, not an open
/// marketplace; runtime decisions use [`Active`] instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreMode {
    Marketplace,
    SingleSeller { pinned: Option<String> },
}

impl StoreMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Marketplace => "marketplace",
            Self::SingleSeller { .. } => "single_seller",
        }
    }

    pub fn pinned(&self) -> Option<&str> {
        match self {
            Self::Marketplace => None,
            Self::SingleSeller { pinned } => pinned.as_deref(),
        }
    }

    pub const fn is_claim_mode(&self) -> bool {
        matches!(self, Self::SingleSeller { pinned: None })
    }
}

/// The exhaustive runtime answer. Off and Claimed are terminal for a process;
/// only Unclaimed consults the binding row again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Active {
    Off,
    Unclaimed,
    Claimed(String),
}

#[derive(Debug)]
pub struct StoreBinding {
    active: RwLock<Active>,
    code: RwLock<Option<ClaimCode>>,
    last_read: Mutex<Option<Instant>>,
    refresh: tokio::sync::Mutex<()>,
}

impl StoreBinding {
    pub fn new(active: Active, code: Option<ClaimCode>) -> Self {
        Self {
            active: RwLock::new(active),
            code: RwLock::new(code),
            last_read: Mutex::new(None),
            refresh: tokio::sync::Mutex::new(()),
        }
    }

    pub fn from_mode(mode: &StoreMode, code: Option<ClaimCode>) -> Self {
        let active = match mode {
            StoreMode::Marketplace => Active::Off,
            StoreMode::SingleSeller {
                pinned: Some(seller),
            } => Active::Claimed(seller.clone()),
            StoreMode::SingleSeller { pinned: None } => Active::Unclaimed,
        };
        Self::new(active, code)
    }

    pub fn active(&self) -> Active {
        self.active.read().expect("store binding read").clone()
    }

    /// Re-reads an unclaimed binding at most once a second, single flight.
    pub async fn refresh(&self, pool: &PgPool, force: bool) -> Result<Active, sqlx::Error> {
        let current = self.active();
        if !matches!(current, Active::Unclaimed) {
            return Ok(current);
        }
        if !force
            && self
                .last_read
                .lock()
                .expect("store binding clock")
                .is_some_and(|read| read.elapsed() < Duration::from_secs(1))
        {
            return Ok(current);
        }
        let _flight = self.refresh.lock().await;
        let current = self.active();
        if !matches!(current, Active::Unclaimed) {
            return Ok(current);
        }
        if !force
            && self
                .last_read
                .lock()
                .expect("store binding clock")
                .is_some_and(|read| read.elapsed() < Duration::from_secs(1))
        {
            return Ok(current);
        }
        let binding = read_binding(pool).await?;
        *self.last_read.lock().expect("store binding clock") = Some(Instant::now());
        if let Some(binding) = binding {
            *self.active.write().expect("store binding write") =
                Active::Claimed(binding.seller_pubky.clone());
            *self.code.write().expect("claim code write") = None;
            return Ok(Active::Claimed(binding.seller_pubky));
        }
        Ok(Active::Unclaimed)
    }

    pub fn claim_code_matches(&self, supplied: &str) -> bool {
        self.code
            .read()
            .expect("claim code read")
            .as_ref()
            .is_some_and(|expected| expected.matches(supplied))
    }

    pub fn mark_claimed(&self, seller: String) {
        *self.active.write().expect("store binding write") = Active::Claimed(seller);
        *self.code.write().expect("claim code write") = None;
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BindingRow {
    pub seller_pubky: String,
    pub bound_by: String,
    pub claimed_at: chrono::DateTime<chrono::Utc>,
    pub claim_session_id: Option<Uuid>,
    pub service_commit: Option<String>,
}

pub async fn read_binding(pool: &PgPool) -> Result<Option<BindingRow>, sqlx::Error> {
    sqlx::query_as::<_, BindingRow>(
        "SELECT seller_pubky, bound_by, claimed_at, claim_session_id, service_commit \
         FROM store_seller_binding WHERE singleton = TRUE",
    )
    .fetch_optional(pool)
    .await
}

async fn read_binding_on(connection: &mut PgConnection) -> Result<Option<BindingRow>, sqlx::Error> {
    sqlx::query_as::<_, BindingRow>(
        "SELECT seller_pubky, bound_by, claimed_at, claim_session_id, service_commit \
         FROM store_seller_binding WHERE singleton = TRUE",
    )
    .fetch_optional(connection)
    .await
}

pub async fn take_binding_lock(connection: &mut PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(STORE_BINDING_LOCK_KEY)
        .execute(connection)
        .await
        .map(|_| ())
}

/// Resolves the boot state in one transaction. Every mode reads the binding
/// while holding the shared lock, so release and boot are ordered.
pub async fn resolve_store(
    pool: &PgPool,
    mode: &StoreMode,
    code: Option<ClaimCode>,
) -> anyhow::Result<Arc<StoreBinding>> {
    resolve_store_inner(pool, mode, code, true).await
}

pub async fn resolve_store_without_code(
    pool: &PgPool,
    mode: &StoreMode,
    code: Option<ClaimCode>,
) -> anyhow::Result<Arc<StoreBinding>> {
    resolve_store_inner(pool, mode, code, false).await
}

async fn resolve_store_inner(
    pool: &PgPool,
    mode: &StoreMode,
    code: Option<ClaimCode>,
    generate_ephemeral: bool,
) -> anyhow::Result<Arc<StoreBinding>> {
    let mut tx = pool.begin().await?;
    take_binding_lock(&mut tx).await?;
    let existing = read_binding_on(&mut tx).await?;
    let active = match mode {
        StoreMode::Marketplace => {
            if let Some(binding) = existing {
                anyhow::bail!(
                    "this database is bound to a single-seller store (seller `{}...`); \
                     set `STORE_MODE=single_seller`",
                    crate::logging::actor_prefix(&binding.seller_pubky)
                );
            }
            Active::Off
        }
        StoreMode::SingleSeller {
            pinned: Some(seller),
        } => {
            if let Some(binding) = &existing {
                if &binding.seller_pubky != seller {
                    anyhow::bail!(
                        "STORE_SELLER_PUBKY `{}...` does not match the database binding `{}...`",
                        crate::logging::actor_prefix(seller),
                        crate::logging::actor_prefix(&binding.seller_pubky)
                    );
                }
            }
            let rows = other_seller_rows_on(&mut tx, seller).await?;
            refuse_rows_for_pin(seller, &rows)?;
            sqlx::query(
                "INSERT INTO store_seller_binding \
                 (singleton, seller_pubky, bound_by, service_commit) \
                 VALUES (TRUE, $1, 'env', $2) ON CONFLICT (singleton) DO NOTHING",
            )
            .bind(seller)
            .bind(crate::http::build_commit())
            .execute(&mut *tx)
            .await?;
            let stored = read_binding_on(&mut tx)
                .await?
                .ok_or_else(|| anyhow::anyhow!("store binding insert returned no binding"))?;
            if &stored.seller_pubky != seller {
                anyhow::bail!("store binding changed while the env pin was being written");
            }
            Active::Claimed(seller.clone())
        }
        StoreMode::SingleSeller { pinned: None } => match existing {
            Some(binding) => {
                let rows = other_seller_rows_on(&mut tx, &binding.seller_pubky).await?;
                refuse_rows_for_pin(&binding.seller_pubky, &rows)?;
                if code.is_some() {
                    tracing::warn!("store is claimed; remove STORE_CLAIM_TOKEN");
                }
                Active::Claimed(binding.seller_pubky)
            }
            None => {
                let rows = other_seller_rows_on(&mut tx, "").await?;
                if !rows.is_empty() {
                    anyhow::bail!(
                        "single-seller mode has no seller but the database holds rows of {} sellers; \
                         set STORE_SELLER_PUBKY or use an empty database",
                        rows.sellers.len()
                    );
                }
                Active::Unclaimed
            }
        },
    };
    tx.commit().await?;
    let code = if matches!(active, Active::Unclaimed) {
        match code {
            Some(code) => Some(code),
            None if generate_ephemeral => {
                let (code, rendered_code) = ClaimCode::ephemeral();
                tracing::info!(
                    "store unclaimed: setup code {} (valid until this process restarts)",
                    rendered_code
                );
                Some(code)
            }
            None => None,
        }
    } else {
        None
    };
    Ok(Arc::new(StoreBinding::new(active, code)))
}

/// Whether the index moderation gate is constructed and asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListingModeration {
    On,
    Off,
}

impl ListingModeration {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::On => "on",
            Self::Off => "off",
        }
    }
}

/// `STORE_SELLER_PUBKY` from a variable lookup: unset or empty is
/// marketplace mode; any other value must be a 52-character z-base-32
/// pubky. Whitespace alone is refused, not read as unset: a template that
/// renders blanks must not fall back to serving every seller.
pub fn store_mode_from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<StoreMode> {
    let pinned = match get(ENV_STORE_SELLER_PUBKY).filter(|value| !value.is_empty()) {
        None => None,
        Some(seller) if marketplace_domain::pubky::is_valid_pubky(&seller) => Some(seller),
        Some(_) => anyhow::bail!(
            "{ENV_STORE_SELLER_PUBKY} must be a 52-character z-base-32 pubky, without a \
             `pubky` prefix or surrounding whitespace"
        ),
    };
    let configured = get(ENV_STORE_MODE).filter(|value| !value.is_empty());
    match configured.as_deref() {
        None => Ok(match pinned {
            Some(seller) => StoreMode::SingleSeller {
                pinned: Some(seller),
            },
            None => StoreMode::Marketplace,
        }),
        Some("single_seller") => Ok(StoreMode::SingleSeller { pinned }),
        Some("marketplace") if pinned.is_none() => Ok(StoreMode::Marketplace),
        Some("marketplace") => anyhow::bail!(
            "STORE_MODE=marketplace contradicts STORE_SELLER_PUBKY; unset the seller pin"
        ),
        Some(_) => anyhow::bail!("STORE_MODE must be marketplace or single_seller"),
    }
}

/// `LISTING_MODERATION` from a variable lookup, in the deployment's store
/// mode. Unset, empty or `on` keeps moderation on and `NEXUS_URL` required.
/// `off` is refused in marketplace mode; in single-seller mode it needs
/// `NEXUS_URL` unset, so a deployment cannot believe an index it no longer
/// asks still moderates its listings. Any other value is refused.
pub fn moderation_from_lookup(
    get: &dyn Fn(&str) -> Option<String>,
    store: &StoreMode,
) -> anyhow::Result<ListingModeration> {
    let set = |name: &str| get(name).filter(|value| !value.trim().is_empty());
    let nexus = crate::listing_moderation::ENV_NEXUS_URL;
    match set(ENV_LISTING_MODERATION).as_deref().map(str::trim) {
        None | Some("on") => {
            if set(nexus).is_none() {
                anyhow::bail!("{nexus} must be set");
            }
            Ok(ListingModeration::On)
        }
        Some("off") => {
            if *store == StoreMode::Marketplace {
                anyhow::bail!(
                    "{ENV_LISTING_MODERATION}=off is allowed only in single-seller mode \
                     ({ENV_STORE_SELLER_PUBKY} set)"
                );
            }
            if set(nexus).is_some() {
                anyhow::bail!(
                    "{nexus} is not read when {ENV_LISTING_MODERATION} is `off`; unset it"
                );
            }
            Ok(ListingModeration::Off)
        }
        Some(_) => anyhow::bail!("{ENV_LISTING_MODERATION} must be `on` or `off`"),
    }
}

/// What the database holds for sellers other than the store seller. Rows
/// are counted whatever their state: soft-deleted listings and webhook
/// endpoints and disconnected store connections included.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtherSellerRows {
    pub listings: i64,
    pub orders: i64,
    /// Stock holds (`reservations`) on those sellers' listings.
    pub holds: i64,
    /// Store connections (`connector_connections`), pending ones included.
    pub connector_connections: i64,
    /// Sellers with a payment configuration (`seller_payment_configs`) or
    /// an accepted payment option (`seller_accepted_payment_options`).
    pub payment_configs: i64,
    /// Automation webhook endpoints (`webhook_endpoints`).
    pub webhook_endpoints: i64,
    /// Seller settings: the band consent (`attestation_band_consents`) a
    /// `seller_settings:` aggregate writes.
    pub seller_settings: i64,
    /// Up to three of those sellers, by the first eight characters of their
    /// pubky.
    pub sellers: Vec<String>,
}

impl OtherSellerRows {
    pub fn is_empty(&self) -> bool {
        self.listings == 0
            && self.orders == 0
            && self.holds == 0
            && self.connector_connections == 0
            && self.payment_configs == 0
            && self.webhook_endpoints == 0
            && self.seller_settings == 0
    }
}

/// Counts what the database holds for a seller other than `store`: their
/// listings, orders, holds, store connections, payment configurations,
/// webhook endpoints and seller settings.
pub async fn other_seller_rows(pool: &PgPool, store: &str) -> Result<OtherSellerRows, sqlx::Error> {
    let mut connection = pool.acquire().await?;
    other_seller_rows_on(&mut connection, store).await
}

pub async fn other_seller_rows_on(
    connection: &mut PgConnection,
    store: &str,
) -> Result<OtherSellerRows, sqlx::Error> {
    type Counts = (i64, i64, i64, i64, i64, i64, i64);
    let (
        listings,
        orders,
        holds,
        connector_connections,
        payment_configs,
        webhook_endpoints,
        seller_settings,
    ): Counts = sqlx::query_as(
        "SELECT \
             (SELECT COUNT(*) FROM listings WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM orders WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM reservations r \
                  JOIN listings l ON l.aggregate_id = r.listing_aggregate_id \
              WHERE l.seller_pubky <> $1), \
             (SELECT COUNT(*) FROM connector_connections WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM ( \
                  SELECT seller_pubky FROM seller_payment_configs WHERE seller_pubky <> $1 \
                  UNION SELECT seller_pubky FROM seller_accepted_payment_options \
                        WHERE seller_pubky <> $1 \
              ) configured), \
             (SELECT COUNT(*) FROM webhook_endpoints WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM attestation_band_consents WHERE seller_pubky <> $1)",
    )
    .bind(store)
    .fetch_one(&mut *connection)
    .await?;
    let sellers: Vec<String> = sqlx::query_scalar(
        "SELECT seller_pubky FROM ( \
             SELECT seller_pubky FROM listings WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM orders WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM connector_connections WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM seller_payment_configs WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM seller_accepted_payment_options \
                   WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM webhook_endpoints WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM attestation_band_consents WHERE seller_pubky <> $1 \
         ) other ORDER BY seller_pubky LIMIT 3",
    )
    .bind(store)
    .fetch_all(connection)
    .await?;
    Ok(OtherSellerRows {
        listings,
        orders,
        holds,
        connector_connections,
        payment_configs,
        webhook_endpoints,
        seller_settings,
        sellers: sellers
            .iter()
            .map(|seller| format!("{}…", crate::logging::actor_prefix(seller)))
            .collect(),
    })
}

/// Refuses to start single-seller mode on a database that holds anything of
/// another seller ([`other_seller_rows`]): the mode would refuse every route
/// and command for them, leaving their buyers' orders and holds unserved,
/// while their store connections, payment rails and webhooks stayed live.
pub async fn refuse_other_sellers(pool: &PgPool, mode: &StoreMode) -> anyhow::Result<()> {
    let Some(store) = mode.pinned() else {
        return Ok(());
    };
    let rows = other_seller_rows(pool, store).await?;
    refuse_rows_for_pin(store, &rows)
}

pub fn refuse_rows_for_pin(store: &str, rows: &OtherSellerRows) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let _ = store;
    anyhow::bail!(
        "{ENV_STORE_SELLER_PUBKY} is set, but the database holds {} listings, {} orders, {} \
         holds, {} store connections, {} payment configurations, {} webhook endpoints and {} \
         seller settings of other sellers ({}); single-seller mode needs a database that only \
         its store seller has used; refusing to start",
        rows.listings,
        rows.orders,
        rows.holds,
        rows.connector_connections,
        rows.payment_configs,
        rows.webhook_endpoints,
        rows.seller_settings,
        rows.sellers.join(", ")
    )
}

/// Something a command or route acts on, named by the seller it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    Seller(String),
    Offer(Uuid),
    Order(Uuid),
    Payment(Uuid),
    Receipt(Uuid),
}

impl Subject {
    /// The subject an aggregate id names: the seller in a listing, drop or
    /// seller-settings id, the record in an offer, order or payment id.
    /// `None` for an aggregate that belongs to no seller yet (a checkout).
    pub fn of_aggregate(aggregate_id: &str) -> Option<Self> {
        let (kind, rest) = aggregate_id.split_once(':')?;
        match kind {
            // `{seller}_{id}`; z-base-32 has no `_`, so the seller ends at
            // the first one.
            "listing" | "drop" => {
                let seller = rest.split_once('_').map_or(rest, |(seller, _)| seller);
                Some(Self::Seller(seller.to_string()))
            }
            "seller_settings" => Some(Self::Seller(rest.to_string())),
            "offer" => Uuid::parse_str(rest).ok().map(Self::Offer),
            "order" => Uuid::parse_str(rest).ok().map(Self::Order),
            "payment" => Uuid::parse_str(rest).ok().map(Self::Payment),
            _ => None,
        }
    }
}

/// The seller a subject belongs to; `None` for a record that does not exist,
/// which the handler refuses itself.
async fn seller_of(pool: &PgPool, subject: &Subject) -> Result<Option<String>, sqlx::Error> {
    let (query, id) = match subject {
        Subject::Seller(seller) => return Ok(Some(seller.clone())),
        Subject::Offer(id) => ("SELECT seller_pubky FROM offers WHERE id = $1", id),
        Subject::Order(id) => ("SELECT seller_pubky FROM orders WHERE id = $1", id),
        Subject::Payment(id) => ("SELECT seller_pubky FROM payments WHERE id = $1", id),
        Subject::Receipt(id) => (
            "SELECT o.seller_pubky FROM receipts r JOIN orders o ON o.id = r.order_id \
             WHERE r.id = $1",
            id,
        ),
    };
    sqlx::query_scalar(query)
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// THE check: whether every subject belongs to the store seller. Always true
/// in marketplace mode.
pub async fn serves(
    pool: &PgPool,
    binding: &StoreBinding,
    subjects: &[Subject],
) -> Result<bool, sqlx::Error> {
    match binding.refresh(pool, false).await? {
        Active::Off => Ok(true),
        Active::Unclaimed => Ok(false),
        Active::Claimed(store) => {
            for subject in subjects {
                if let Some(seller) = seller_of(pool, subject).await? {
                    if seller != store {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
    }
}

/// Everything a command acts on: its aggregate and whatever its payload
/// names. The match has no wildcard, so a new command does not compile until
/// it says what it touches.
pub fn command_subjects(command: &Command) -> Vec<Subject> {
    let mut subjects: Vec<Subject> = Subject::of_aggregate(&command.aggregate_id)
        .into_iter()
        .collect();
    match &command.payload {
        CommandPayload::RegisterListing(payload) => {
            subjects.push(Subject::Seller(payload.seller_pubky.clone()));
        }
        CommandPayload::SyncListing(payload) => {
            subjects.push(Subject::Seller(payload.seller_pubky.clone()));
        }
        CommandPayload::SyncDrop(payload) => {
            subjects.push(Subject::Seller(payload.seller_pubky.clone()));
        }
        CommandPayload::CreateCheckout(payload) => {
            subjects.extend(
                payload
                    .lines
                    .iter()
                    .filter_map(|line| Subject::of_aggregate(&line.listing_aggregate_id)),
            );
        }
        CommandPayload::CounterOffer(payload) => subjects.push(Subject::Offer(payload.offer_id)),
        CommandPayload::AcceptOffer(payload)
        | CommandPayload::RejectOffer(payload)
        | CommandPayload::WithdrawOffer(payload) => subjects.push(Subject::Offer(payload.offer_id)),
        CommandPayload::OfferCheckout(payload) => {
            subjects.push(Subject::Offer(payload.offer_id));
            subjects.extend(Subject::of_aggregate(&payload.listing_aggregate_id));
        }
        CommandPayload::AdvanceSandboxPayment(payload) => {
            subjects.push(Subject::Payment(payload.payment_id));
        }
        CommandPayload::PrepareLocks(payload) => {
            subjects.push(Subject::Payment(payload.payment_id))
        }
        CommandPayload::RegisterLocks(payload) => {
            subjects.push(Subject::Payment(payload.payment_id));
        }
        CommandPayload::RequestCancellation(payload) => {
            subjects.push(Subject::Order(payload.order_id));
        }
        CommandPayload::ShipOrder(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::UpdateTracking(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::ApproveCancellation(payload)
        | CommandPayload::ConfirmDelivery(payload)
        | CommandPayload::MarkReadyForPickup(payload)
        | CommandPayload::ConfirmPickup(payload)
        | CommandPayload::ApproveReturn(payload)
        | CommandPayload::CancelReturn(payload)
        | CommandPayload::ReceiveReturn(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::SetDeliveryEmail(payload) => {
            subjects.push(Subject::Order(payload.order_id));
        }
        CommandPayload::DeliverDigital(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::ReleaseDigital(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::RequestReturn(payload) => subjects.push(Subject::Order(payload.order_id)),
        CommandPayload::RecordExternalRefund(payload) => {
            subjects.push(Subject::Order(payload.order_id));
        }
        CommandPayload::ConfirmRefundDestination(payload) => {
            subjects.push(Subject::Order(payload.order_id));
        }
        CommandPayload::CreateReview(payload) | CommandPayload::UpdateReview(payload) => {
            subjects.push(Subject::Order(payload.order_id));
        }
        CommandPayload::WithdrawReview(payload) => subjects.push(Subject::Order(payload.order_id)),
        // The aggregate names everything these act on: the listing
        // (inventory, offers, bids, pickup and digital details), the drop,
        // or the actor's own seller settings.
        CommandPayload::CancelDrop(_)
        | CommandPayload::ReleaseDropListings(_)
        | CommandPayload::ReserveInventory(_)
        | CommandPayload::CreateOffer(_)
        | CommandPayload::PlaceBid(_)
        | CommandPayload::CloseAuction(_)
        | CommandPayload::SetPickupDetails(_)
        | CommandPayload::ClearPickupDetails(_)
        | CommandPayload::SetDigitalDelivery(_)
        | CommandPayload::ClearDigitalDelivery(_)
        | CommandPayload::SetBandConsent(_) => {}
    }
    subjects
}

/// The refusal of a command for a seller the deployment does not serve, or
/// `None` when it is served (always in marketplace mode).
pub(crate) async fn command_refusal(
    state: &AppState,
    command: &Command,
) -> Result<Option<CommandFailure>, sqlx::Error> {
    match state.store.refresh(&state.pool, false).await? {
        Active::Off => Ok(None),
        Active::Unclaimed => Ok(Some(CommandFailure::unaudited(
            ErrorCode::StoreUnclaimed,
            STORE_UNCLAIMED_MESSAGE,
        ))),
        Active::Claimed(_) => {
            if serves(&state.pool, &state.store, &command_subjects(command)).await? {
                Ok(None)
            } else {
                Ok(Some(not_served()))
            }
        }
    }
}

fn not_served() -> CommandFailure {
    CommandFailure::refused(
        crate::refusal_audit::RefusalKind::SellerNotServed,
        ErrorCode::SellerNotServed,
        SELLER_NOT_SERVED_MESSAGE,
    )
}

/// What a route acts on, for the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteScope {
    /// `/v1/commands` and the batch sync: each command is checked by the
    /// executor.
    Commands,
    /// A seller's own surface (stock, automation, webhooks, connectors,
    /// payment configuration): the session's actor must be the store seller.
    StoreSeller,
    /// The path parameter is a seller pubky.
    Seller(&'static str),
    /// The path parameter is a listing or drop aggregate id.
    Aggregate(&'static str),
    /// The path parameter is an order id.
    Order(&'static str),
    /// The path parameter is a payment id.
    Payment(&'static str),
    /// The path parameter is a receipt id.
    Receipt(&'static str),
    /// Signed in, but acts for no seller; the reason says why.
    Open(&'static str),
    /// Needs no session and is not guarded; the reason says why it acts for
    /// no other seller.
    Public(&'static str),
}

/// One route of [`crate::http::build_router`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteRule {
    pub method: &'static str,
    pub path: &'static str,
    pub scope: RouteScope,
}

const fn rule(method: &'static str, path: &'static str, scope: RouteScope) -> RouteRule {
    RouteRule {
        method,
        path,
        scope,
    }
}

use RouteScope::{Aggregate, Commands, Open, Order, Payment, Public, Receipt, Seller, StoreSeller};

const SIGN_IN: &str = "sign-in: sessions are issued to any identity, since buyers need them";
const OWN: &str = "the actor's own records, whatever their role";
const PROVIDER: &str = "authenticated by the provider for a connection only the store seller \
                        can create";
const STORED: &str = "answers only from what the guarded writes stored";

/// Every route the router serves, and what it acts on. In single-seller
/// mode a route missing here is refused wherever it is mounted
/// ([`refuse_unlisted_routes`]); `tests/store_seller_test.rs` fails when this
/// table and `http.rs` disagree.
pub const ROUTES: &[RouteRule] = &[
    rule("GET", "/health", Public("deployment health")),
    rule("GET", "/ready", Public("deployment readiness")),
    rule("GET", "/v1/store", Public("public store claim state")),
    rule("POST", "/v1/auth/sessions", Public(SIGN_IN)),
    rule("GET", "/v1/auth/sessions", Public(SIGN_IN)),
    rule("POST", "/v1/auth/grant-session-challenges", Public(SIGN_IN)),
    rule("POST", "/v1/auth/grant-sessions", Public(SIGN_IN)),
    rule("POST", "/v1/auth/grant-flows", Public(SIGN_IN)),
    rule("GET", "/v1/auth/grant-flows/{flow_id}", Public(SIGN_IN)),
    rule(
        "POST",
        "/v1/auth/grant-flows/{flow_id}/cancel",
        Public(SIGN_IN),
    ),
    rule(
        "POST",
        "/v1/auth/grant-flows/{flow_id}/result-nonces",
        Public(SIGN_IN),
    ),
    rule(
        "POST",
        "/v1/auth/grant-flows/{flow_id}/result-ticket",
        Public(SIGN_IN),
    ),
    rule(
        "POST",
        "/v1/auth/grant-flows/{flow_id}/claim",
        Public(SIGN_IN),
    ),
    rule("GET", "/v0/sellers/{pubky}/payment-config", Public(STORED)),
    rule(
        "GET",
        "/v0/payment-methods/{method}/sellers",
        Public(STORED),
    ),
    rule(
        "GET",
        "/v0/sellers/{pubky}/connector-status",
        Public(STORED),
    ),
    rule("GET", "/v0/drops/{seller_pubky}/{drop_id}", Public(STORED)),
    rule(
        "GET",
        "/v0/listings/{seller_pubky}/{listing_id}/availability",
        Public(STORED),
    ),
    rule(
        "POST",
        "/v0/address/suggest",
        Public("address autocomplete, bound to no seller"),
    ),
    rule(
        "POST",
        "/v0/paypal/ipn",
        Public("PayPal's notification for an order only a guarded checkout can create"),
    ),
    rule(
        "POST",
        "/v1/connectors/webhooks/woocommerce/c/{connection_id}",
        Public(PROVIDER),
    ),
    rule(
        "POST",
        "/v1/connectors/shopify/c/{connection_id}/webhooks",
        Public(PROVIDER),
    ),
    rule(
        "GET",
        "/v1/connectors/shopify/c/{connection_id}/launch",
        Public(PROVIDER),
    ),
    rule(
        "GET",
        "/v1/connectors/shopify/c/{connection_id}/oauth/callback",
        Public(PROVIDER),
    ),
    rule(
        "POST",
        "/v1/connectors/woocommerce/oauth/callback",
        Public(PROVIDER),
    ),
    // Inventory capability.
    rule("POST", "/v1/inventory/adjust", StoreSeller),
    rule("GET", "/v1/inventory/listings/{aggregate_id}", StoreSeller),
    // Automation.
    rule(
        "PATCH",
        "/v1/auth/sessions/{id}",
        Open("the actor's own session"),
    ),
    rule(
        "DELETE",
        "/v1/auth/sessions/{id}",
        Open("the actor's own session"),
    ),
    rule("GET", "/v1/sellers/{pubky}/listings", StoreSeller),
    rule("GET", "/v1/listings/{seller}/{listing_id}", StoreSeller),
    rule("GET", "/v1/sellers/{pubky}/orders", StoreSeller),
    rule("GET", "/v1/sellers/{pubky}/events", StoreSeller),
    rule("POST", "/v1/listings/sync-many", Commands),
    rule("POST", "/v1/webhooks", StoreSeller),
    rule("POST", "/v1/webhooks/{id}/rotate", StoreSeller),
    rule("DELETE", "/v1/webhooks/{id}", StoreSeller),
    // Store connectors.
    rule("GET", "/v1/connectors", StoreSeller),
    rule("POST", "/v1/connectors", StoreSeller),
    rule("POST", "/v1/connectors/resolve", StoreSeller),
    rule("GET", "/v1/connectors/alerts", StoreSeller),
    rule("POST", "/v1/connectors/alerts/{id}/resolve", StoreSeller),
    rule("DELETE", "/v1/connectors/{id}", StoreSeller),
    rule("PUT", "/v1/connectors/{id}/credentials", StoreSeller),
    rule("PUT", "/v1/connectors/{id}/settings", StoreSeller),
    rule("GET", "/v1/connectors/{id}/check", StoreSeller),
    rule("GET", "/v1/connectors/{id}/catalog", StoreSeller),
    rule("GET", "/v1/connectors/{id}/mappings", StoreSeller),
    rule("PUT", "/v1/connectors/{id}/mappings", StoreSeller),
    rule("GET", "/v1/connectors/{id}/selection", StoreSeller),
    rule("PUT", "/v1/connectors/{id}/selection", StoreSeller),
    rule("GET", "/v1/connectors/{id}/sync", StoreSeller),
    rule("PUT", "/v1/connectors/{id}/sync", StoreSeller),
    rule(
        "PUT",
        "/v1/connectors/{id}/sync/listings/{listing_id}",
        StoreSeller,
    ),
    rule("GET", "/v1/connectors/{id}/changes", StoreSeller),
    rule(
        "POST",
        "/v1/connectors/{id}/changes/{change_id}/resolve",
        StoreSeller,
    ),
    rule("PUT", "/v1/connectors/{id}/woocommerce/keys", StoreSeller),
    rule("GET", "/v1/connectors/{id}/pushes", StoreSeller),
    rule(
        "POST",
        "/v1/connectors/{id}/pushes/{order}/retry",
        StoreSeller,
    ),
    // Signed in.
    rule(
        "POST",
        "/v1/store/claim",
        Open("signed in; the actor claims the unclaimed store for itself"),
    ),
    rule("POST", "/v1/commands", Commands),
    rule(
        "GET",
        "/v1/listings/{aggregate_id}",
        Aggregate("aggregate_id"),
    ),
    rule(
        "GET",
        "/v1/listings/{aggregate_id}/bids",
        Aggregate("aggregate_id"),
    ),
    rule(
        "GET",
        "/v1/sellers/{seller_pubky}/annual-limit",
        Seller("seller_pubky"),
    ),
    rule("GET", "/v1/drops/{aggregate_id}", Aggregate("aggregate_id")),
    rule(
        "GET",
        "/v1/drops/{aggregate_id}/me",
        Aggregate("aggregate_id"),
    ),
    rule("GET", "/v1/offers", Open(OWN)),
    rule("GET", "/v1/orders", Open(OWN)),
    rule("GET", "/v1/orders/{id}", Order("id")),
    rule("GET", "/v1/orders/{id}/pickup-details", Order("id")),
    rule(
        "GET",
        "/v1/listings/{aggregate_id}/pickup-details",
        Aggregate("aggregate_id"),
    ),
    rule("GET", "/v1/orders/{id}/delivery-email", Order("id")),
    rule("GET", "/v1/orders/{id}/digital-evidence", Order("id")),
    rule(
        "GET",
        "/v1/orders/{id}/digital-delivery/{line_index}",
        Order("id"),
    ),
    rule(
        "GET",
        "/v1/listings/{aggregate_id}/digital-delivery",
        Aggregate("aggregate_id"),
    ),
    rule("GET", "/v1/orders/{id}/review-attestation", Order("id")),
    rule("GET", "/v1/orders/{id}/review-attestations", Order("id")),
    rule("GET", "/v1/sellers/{pubky}/band-consent", Seller("pubky")),
    rule("GET", "/v1/payments/{id}", Payment("id")),
    rule("GET", "/v1/receipts/{id}", Receipt("id")),
    rule("GET", "/v1/receipts/{id}/attestation", Receipt("id")),
    rule(
        "GET",
        "/v1/receipts/{id}/edition-attestation",
        Receipt("id"),
    ),
    rule("GET", "/v1/notifications", Open(OWN)),
    rule(
        "GET",
        "/v1/me/priv-keys",
        Open("the actor's own /priv data keys"),
    ),
    rule(
        "POST",
        "/v1/me/priv-keys/release",
        Open("the actor's own /priv data keys"),
    ),
    rule("PUT", "/v0/sellers/me/payment-config", StoreSeller),
    rule("GET", "/v0/sellers/me/payment-config", StoreSeller),
    rule("POST", "/v0/orders/{id}/payment-method", Order("id")),
    rule("POST", "/v0/orders/{id}/fiat/mark-paid", Order("id")),
    rule("POST", "/v0/orders/{id}/fiat/confirm-received", Order("id")),
    rule(
        "POST",
        "/v0/orders/{id}/confirm-bitcoin-payment",
        Order("id"),
    ),
    rule("POST", "/v0/orders/{id}/bitcoin/resolve", Order("id")),
];

/// The scope of a route; `HEAD` is answered by the `GET` route.
pub fn route_scope(method: &str, path: &str) -> Option<RouteScope> {
    let method = if method == "HEAD" { "GET" } else { method };
    ROUTES
        .iter()
        .find(|rule| rule.method == method && rule.path == path)
        .map(|rule| rule.scope)
}

/// The subjects of a signed-in route, `None` when the route is not in
/// [`ROUTES`], is listed as public although it has a session, or misses its
/// parameter (refused: fail closed). A parameter that does not parse as an
/// id names nothing; the handler refuses it.
fn route_subjects(
    scope: Option<RouteScope>,
    actor: &str,
    param: impl Fn(&str) -> Option<String>,
) -> Option<Vec<Subject>> {
    let id = |name: &str| param(name).map(|value| Uuid::parse_str(&value).ok());
    Some(match scope? {
        Public(_) => return None,
        Commands | Open(_) => Vec::new(),
        StoreSeller => vec![Subject::Seller(actor.to_string())],
        Seller(name) => vec![Subject::Seller(param(name)?)],
        Aggregate(name) => Subject::of_aggregate(&param(name)?).into_iter().collect(),
        Order(name) => id(name)?.map(Subject::Order).into_iter().collect(),
        Payment(name) => id(name)?.map(Subject::Payment).into_iter().collect(),
        Receipt(name) => id(name)?.map(Subject::Receipt).into_iter().collect(),
    })
}

/// The route guard, layered inside `require_session` on every signed-in
/// router. In marketplace mode it only passes the request on.
pub async fn require_store_seller(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let (mut parts, body) = request.into_parts();
    let matched_path = parts.extensions.get::<MatchedPath>().cloned();
    let scope = matched_path
        .as_ref()
        .and_then(|path| route_scope(parts.method.as_str(), path.as_str()));
    let actor = parts
        .extensions
        .get::<Actor>()
        .map(|actor| actor.0.clone())
        .unwrap_or_default();
    let active = match state.store.refresh(&state.pool, false).await {
        Ok(active) => active,
        Err(error) => return store_check_error(error),
    };
    match active {
        Active::Off => return next.run(Request::from_parts(parts, body)).await,
        Active::Unclaimed => {
            let own_session_admin = matched_path.as_ref().is_some_and(|path| {
                path.as_str().starts_with("/v1/auth/sessions/") && matches!(scope, Some(Open(_)))
            });
            if own_session_admin {
                return next.run(Request::from_parts(parts, body)).await;
            }
            return unclaimed_response();
        }
        Active::Claimed(_) => {}
    }
    let params = RawPathParams::from_request_parts(&mut parts, &state)
        .await
        .ok();
    let param = |name: &str| {
        params.as_ref().and_then(|params| {
            params
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        })
    };
    let admitted = match route_subjects(scope, &actor, param) {
        None => false,
        Some(subjects) => match serves(&state.pool, &state.store, &subjects).await {
            Ok(admitted) => admitted,
            Err(error) => return store_check_error(error),
        },
    };
    if admitted {
        return next.run(Request::from_parts(parts, body)).await;
    }
    route_refusal(&state, &actor)
}

fn store_check_error(error: sqlx::Error) -> Response {
    tracing::error!(error = %error, "store seller check failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "ok": false,
            "error": { "code": "INTERNAL", "message": "The request could not be processed." },
        })),
    )
        .into_response()
}

pub fn unclaimed_response() -> Response {
    failure_response(&CommandFailure::unaudited(
        ErrorCode::StoreUnclaimed,
        STORE_UNCLAIMED_MESSAGE,
    ))
}

pub fn failure_response(failure: &CommandFailure) -> Response {
    (
        StatusCode::from_u16(failure.http_status()).expect("error codes map to HTTP statuses"),
        Json(failure.body()),
    )
        .into_response()
}

/// The whole-router guard, layered once by [`crate::http::serve`] over every
/// route however it was mounted. In single-seller mode it refuses a route
/// with no entry in [`ROUTES`], so a route the table misses fails closed
/// even on a router without [`require_store_seller`]. It runs before any
/// session check, so the refusal-audit row, which tags an authenticated
/// actor, is written only when the request carries an active session.
pub async fn refuse_unlisted_routes(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let active = match state.store.refresh(&state.pool, false).await {
        Ok(active) => active,
        Err(error) => return store_check_error(error),
    };
    if matches!(active, Active::Off) {
        return next.run(request).await;
    }
    let listed = request
        .extensions()
        .get::<MatchedPath>()
        .and_then(|path| route_scope(request.method().as_str(), path.as_str()))
        .is_some();
    if listed {
        return next.run(request).await;
    }
    if matches!(active, Active::Unclaimed) {
        return unclaimed_response();
    }
    let actor = crate::auth::session_actor(&state, request.headers())
        .await
        .unwrap_or_default();
    route_refusal(&state, &actor)
}

fn route_refusal(state: &AppState, actor: &str) -> Response {
    if let Some(audit) = &state.refusal_audit {
        if let Ok(envelope) = audit.envelope(
            state.clock.now(),
            crate::refusal_audit::SurfaceKind::SellerRoute,
            crate::refusal_audit::CommandKind::SellerRoute,
            crate::refusal_audit::RefusalKind::SellerNotServed,
            actor,
            None,
        ) {
            audit.try_send(envelope);
        }
    }
    let failure = not_served();
    (
        StatusCode::from_u16(failure.http_status()).expect("error codes map to HTTP statuses"),
        Json(failure.body()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> String {
        marketplace_domain::pubky::encode_pubky(&[1; 32])
    }

    fn other() -> String {
        marketplace_domain::pubky::encode_pubky(&[2; 32])
    }

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        move |name| {
            vars.iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    fn modes(vars: &[(&str, &str)]) -> anyhow::Result<(StoreMode, ListingModeration)> {
        let get = lookup(vars);
        let store = store_mode_from_lookup(&get)?;
        let moderation = moderation_from_lookup(&get, &store)?;
        Ok((store, moderation))
    }

    const NEXUS: (&str, &str) = ("NEXUS_URL", "https://nexus.example");

    #[test]
    fn unset_is_a_marketplace_that_requires_the_index() {
        assert_eq!(
            modes(&[NEXUS]).expect("boots"),
            (StoreMode::Marketplace, ListingModeration::On)
        );
        assert_eq!(
            modes(&[
                NEXUS,
                ("STORE_SELLER_PUBKY", ""),
                ("LISTING_MODERATION", "")
            ])
            .expect("empty values are unset"),
            (StoreMode::Marketplace, ListingModeration::On)
        );
        let error = modes(&[]).expect_err("NEXUS_URL stays required");
        assert_eq!(error.to_string(), "NEXUS_URL must be set");
    }

    #[test]
    fn a_store_seller_binds_one_seller_and_keeps_moderation_on_by_default() {
        let store = store();
        assert_eq!(
            modes(&[NEXUS, ("STORE_SELLER_PUBKY", &store)]).expect("boots"),
            (
                StoreMode::SingleSeller {
                    pinned: Some(store.clone())
                },
                ListingModeration::On
            )
        );
        assert_eq!(
            modes(&[
                NEXUS,
                ("STORE_SELLER_PUBKY", &store),
                ("LISTING_MODERATION", "on")
            ])
            .expect("boots"),
            (
                StoreMode::SingleSeller {
                    pinned: Some(store.clone())
                },
                ListingModeration::On
            )
        );
        let error = modes(&[("STORE_SELLER_PUBKY", &store)])
            .expect_err("moderation on still needs the index");
        assert_eq!(error.to_string(), "NEXUS_URL must be set");
    }

    #[test]
    fn an_invalid_store_seller_refuses_to_boot() {
        let store = store();
        for value in [
            "not-a-pubky",
            &store[..51],
            &format!("{store}y"),
            &format!("pubky{store}"),
            &format!(" {store}"),
            &store.to_uppercase(),
            " ",
            "\t",
            " \n ",
        ] {
            let error = modes(&[NEXUS, ("STORE_SELLER_PUBKY", value)]).expect_err("refused");
            assert!(
                error.to_string().starts_with("STORE_SELLER_PUBKY must be"),
                "{value}: {error}"
            );
        }
    }

    #[test]
    fn moderation_off_needs_single_seller_mode_and_no_index() {
        let store = store();
        assert_eq!(
            modes(&[
                ("STORE_SELLER_PUBKY", &store),
                ("LISTING_MODERATION", "off")
            ])
            .expect("boots without NEXUS_URL"),
            (
                StoreMode::SingleSeller {
                    pinned: Some(store.clone())
                },
                ListingModeration::Off
            )
        );
        for vars in [
            vec![("LISTING_MODERATION", "off")],
            vec![NEXUS, ("LISTING_MODERATION", "off")],
        ] {
            let error = modes(&vars).expect_err("off without a store seller");
            assert!(
                error
                    .to_string()
                    .contains("allowed only in single-seller mode"),
                "{error}"
            );
        }
        let error = modes(&[
            NEXUS,
            ("STORE_SELLER_PUBKY", &store),
            ("LISTING_MODERATION", "off"),
        ])
        .expect_err("an index that is not read");
        assert_eq!(
            error.to_string(),
            "NEXUS_URL is not read when LISTING_MODERATION is `off`; unset it"
        );
    }

    #[test]
    fn any_other_moderation_value_refuses_to_boot() {
        let store = store();
        for value in ["OFF", "false", "0", "disabled", "yes"] {
            let error = modes(&[
                NEXUS,
                ("STORE_SELLER_PUBKY", &store),
                ("LISTING_MODERATION", value),
            ])
            .expect_err("refused");
            assert_eq!(
                error.to_string(),
                "LISTING_MODERATION must be `on` or `off`",
                "{value}"
            );
        }
    }

    #[test]
    fn aggregates_name_their_seller_or_record() {
        let other = other();
        let id = Uuid::from_u128(7);
        assert_eq!(
            Subject::of_aggregate(&format!("listing:{other}_l-1")),
            Some(Subject::Seller(other.clone()))
        );
        assert_eq!(
            Subject::of_aggregate(&format!("drop:{other}_d_2")),
            Some(Subject::Seller(other.clone()))
        );
        assert_eq!(
            Subject::of_aggregate(&format!("seller_settings:{other}")),
            Some(Subject::Seller(other.clone()))
        );
        assert_eq!(
            Subject::of_aggregate("listing:no-separator"),
            Some(Subject::Seller("no-separator".to_string()))
        );
        assert_eq!(
            Subject::of_aggregate(&format!("offer:{id}")),
            Some(Subject::Offer(id))
        );
        assert_eq!(
            Subject::of_aggregate(&format!("order:{id}")),
            Some(Subject::Order(id))
        );
        assert_eq!(
            Subject::of_aggregate(&format!("payment:{id}")),
            Some(Subject::Payment(id))
        );
        assert_eq!(Subject::of_aggregate(&format!("checkout:{id}")), None);
        assert_eq!(Subject::of_aggregate("order:not-a-uuid"), None);
        assert_eq!(Subject::of_aggregate("no-kind"), None);
    }

    #[test]
    fn routes_are_unique_and_parameters_exist_in_their_paths() {
        for (index, rule) in ROUTES.iter().enumerate() {
            assert!(
                ROUTES[..index]
                    .iter()
                    .all(|other| (other.method, other.path) != (rule.method, rule.path)),
                "{} {} is listed twice",
                rule.method,
                rule.path
            );
            let param = match rule.scope {
                Seller(name) | Aggregate(name) | Order(name) | Payment(name) | Receipt(name) => {
                    Some(name)
                }
                Commands | StoreSeller | Open(_) | Public(_) => None,
            };
            if let Some(name) = param {
                assert!(
                    rule.path.contains(&format!("{{{name}}}")),
                    "{} {} has no {{{name}}}",
                    rule.method,
                    rule.path
                );
            }
        }
    }

    #[test]
    fn head_reads_the_get_route_and_unknown_routes_have_no_scope() {
        let store = store();
        assert_eq!(route_scope("HEAD", "/v1/connectors"), Some(StoreSeller));
        assert_eq!(route_scope("POST", "/v1/unknown"), None);
        assert_eq!(route_subjects(None, &store, |_| None), None);
        assert_eq!(
            route_subjects(Some(Public("health")), &store, |_| None),
            None,
            "a public route never reaches the guard; listed so, it is refused"
        );
        assert_eq!(
            route_subjects(Some(Order("id")), &store, |_| None),
            None,
            "a missing parameter is refused"
        );
        assert_eq!(
            route_subjects(Some(Order("id")), &store, |_| Some("x".to_string())),
            Some(Vec::new()),
            "an unparsable id names nothing; the handler refuses it"
        );
    }
}
