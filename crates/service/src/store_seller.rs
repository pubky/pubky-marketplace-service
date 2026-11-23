//! Single-seller mode: `STORE_SELLER_PUBKY` binds the service to one seller.
//!
//! Every command and every signed-in route that acts for a seller names that
//! seller through what it touches: a listing or drop aggregate, a seller
//! pubky, an offer, an order, a payment or a receipt. [`serves`] resolves
//! each to its seller and admits the request only when every one is the
//! store seller. The executor runs it for each command before any other gate
//! ([`command_refusal`]); [`require_store_seller`] runs it for each signed-in
//! route from the route's entry in [`ROUTES`]. Both refuse with
//! `SELLER_NOT_SERVED` (HTTP 409) and write a `seller_not_served` refusal
//! audit row. Sessions are still issued to any identity: buyers need them.
//! At boot, [`refuse_other_sellers`] refuses a database that already holds
//! another seller's listings, orders or holds.
//!
//! With `STORE_SELLER_PUBKY` unset the service is a marketplace and nothing
//! here runs.

use axum::extract::{FromRequestParts, MatchedPath, RawPathParams, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use marketplace_domain::commands::{Command, CommandPayload};
use marketplace_domain::ErrorCode;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::Actor;
use crate::result::CommandFailure;
use crate::AppState;

/// The one seller this deployment serves. Unset (or empty), the service is a
/// marketplace for any seller.
pub const ENV_STORE_SELLER_PUBKY: &str = "STORE_SELLER_PUBKY";

/// `off` turns the index moderation gate off. Allowed only with
/// [`ENV_STORE_SELLER_PUBKY`]; unset (or `on`) keeps it on.
pub const ENV_LISTING_MODERATION: &str = "LISTING_MODERATION";

pub const SELLER_NOT_SERVED_MESSAGE: &str = "This service sells only its store seller's listings.";

/// Which sellers the deployment serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreMode {
    /// Any seller (the default).
    Marketplace,
    /// Only this seller.
    SingleSeller(String),
}

impl StoreMode {
    pub fn store_seller(&self) -> Option<&str> {
        match self {
            Self::Marketplace => None,
            Self::SingleSeller(seller) => Some(seller),
        }
    }

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Marketplace => "marketplace",
            Self::SingleSeller(_) => "single_seller",
        }
    }
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
/// marketplace mode; set, it must be a 52-character z-base-32 pubky.
pub fn store_mode_from_lookup(get: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<StoreMode> {
    match get(ENV_STORE_SELLER_PUBKY).filter(|value| !value.trim().is_empty()) {
        None => Ok(StoreMode::Marketplace),
        Some(seller) if marketplace_domain::pubky::is_valid_pubky(&seller) => {
            Ok(StoreMode::SingleSeller(seller))
        }
        Some(_) => anyhow::bail!(
            "{ENV_STORE_SELLER_PUBKY} must be a 52-character z-base-32 pubky, without a \
             `pubky` prefix or surrounding whitespace"
        ),
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

/// What the database holds for sellers other than the store seller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtherSellerRows {
    pub listings: i64,
    pub orders: i64,
    /// Stock holds (`reservations`) on those sellers' listings.
    pub holds: i64,
    /// Up to three of those sellers, by the first eight characters of their
    /// pubky.
    pub sellers: Vec<String>,
}

impl OtherSellerRows {
    pub fn is_empty(&self) -> bool {
        self.listings == 0 && self.orders == 0 && self.holds == 0
    }
}

/// Counts the listings, orders and holds that belong to a seller other than
/// `store`.
pub async fn other_seller_rows(pool: &PgPool, store: &str) -> Result<OtherSellerRows, sqlx::Error> {
    let (listings, orders, holds): (i64, i64, i64) = sqlx::query_as(
        "SELECT \
             (SELECT COUNT(*) FROM listings WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM orders WHERE seller_pubky <> $1), \
             (SELECT COUNT(*) FROM reservations r \
                  JOIN listings l ON l.aggregate_id = r.listing_aggregate_id \
              WHERE l.seller_pubky <> $1)",
    )
    .bind(store)
    .fetch_one(pool)
    .await?;
    let sellers: Vec<String> = sqlx::query_scalar(
        "SELECT seller_pubky FROM ( \
             SELECT seller_pubky FROM listings WHERE seller_pubky <> $1 \
             UNION SELECT seller_pubky FROM orders WHERE seller_pubky <> $1 \
         ) other ORDER BY seller_pubky LIMIT 3",
    )
    .bind(store)
    .fetch_all(pool)
    .await?;
    Ok(OtherSellerRows {
        listings,
        orders,
        holds,
        sellers: sellers
            .iter()
            .map(|seller| format!("{}…", crate::logging::actor_prefix(seller)))
            .collect(),
    })
}

/// Refuses to start single-seller mode on a database that holds a listing,
/// order or hold of another seller: the mode would refuse every route and
/// command for them, leaving their buyers' orders and holds unserved.
pub async fn refuse_other_sellers(pool: &PgPool, mode: &StoreMode) -> anyhow::Result<()> {
    let Some(store) = mode.store_seller() else {
        return Ok(());
    };
    let rows = other_seller_rows(pool, store).await?;
    if rows.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "{ENV_STORE_SELLER_PUBKY} is set, but the database holds {} listings, {} orders and \
         {} holds of other sellers ({}); single-seller mode needs a database that only its \
         store seller has used; refusing to start",
        rows.listings,
        rows.orders,
        rows.holds,
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
    mode: &StoreMode,
    subjects: &[Subject],
) -> Result<bool, sqlx::Error> {
    let Some(store) = mode.store_seller() else {
        return Ok(true);
    };
    for subject in subjects {
        if let Some(seller) = seller_of(pool, subject).await? {
            if seller != store {
                return Ok(false);
            }
        }
    }
    Ok(true)
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
    let mode = &state.config.store_mode;
    if *mode == StoreMode::Marketplace
        || serves(&state.pool, mode, &command_subjects(command)).await?
    {
        return Ok(None);
    }
    Ok(Some(not_served()))
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

/// Every route the router serves, and what it acts on. A signed-in route
/// missing here is refused in single-seller mode; `tests/store_seller_test.rs`
/// fails when this table and `http.rs` disagree.
pub const ROUTES: &[RouteRule] = &[
    rule("GET", "/health", Public("deployment health")),
    rule("GET", "/ready", Public("deployment readiness")),
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
    if state.config.store_mode.store_seller().is_none() {
        return next.run(request).await;
    }
    let (mut parts, body) = request.into_parts();
    let scope = parts
        .extensions
        .get::<MatchedPath>()
        .and_then(|path| route_scope(parts.method.as_str(), path.as_str()));
    let actor = parts
        .extensions
        .get::<Actor>()
        .map(|actor| actor.0.clone())
        .unwrap_or_default();
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
        Some(subjects) => match serves(&state.pool, &state.config.store_mode, &subjects).await {
            Ok(admitted) => admitted,
            Err(error) => {
                tracing::error!(error = %error, "store seller check failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "ok": false,
                        "error": { "code": "INTERNAL", "message": "The request could not be processed." },
                    })),
                )
                    .into_response();
            }
        },
    };
    if admitted {
        return next.run(Request::from_parts(parts, body)).await;
    }
    if let Some(audit) = &state.refusal_audit {
        if let Ok(envelope) = audit.envelope(
            state.clock.now(),
            crate::refusal_audit::SurfaceKind::SellerRoute,
            crate::refusal_audit::CommandKind::SellerRoute,
            crate::refusal_audit::RefusalKind::SellerNotServed,
            &actor,
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
                StoreMode::SingleSeller(store.clone()),
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
                StoreMode::SingleSeller(store.clone()),
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
                StoreMode::SingleSeller(store.clone()),
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
