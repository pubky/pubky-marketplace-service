//! Zone-priced shipping.
//!
//! A listing record prices shipping in one of three ways: the flat rate the
//! service has always charged (`shipping_minor`), a reference to the seller's
//! shipping profile record (`shippingProfileId`), or zones the record carries
//! itself (`shippingZones`). The zone arithmetic is the shared quote function
//! of `pubky-market-specs`, so the service and the Shop cannot drift.
//!
//! Inline zones are stored on the registered listing row and priced from it
//! under the listing's row lock, so the `expected_revision` check already
//! covers them. A profile is a separate record on the seller's homeserver.
//! The service reads its exact bytes before a command's transaction opens (a
//! slow homeserver never holds a row lock), hashes them, and refuses with
//! `SHIPPING_CHANGED` when the hash differs from the one the buyer chose a
//! rate under.
//!
//! The destination country is the buyer's claim: the service cannot open the
//! sealed address. The seller compares it with the address after payment.

use std::collections::HashMap;

use marketplace_domain::commands::{
    Command, CommandPayload, ShippingChoice, ShippingGroupKey, MAX_SHIPPING_CHOICES,
};
use marketplace_domain::ErrorCode;
use pubky_market_specs::traits::Validatable;
use pubky_market_specs::{
    quote_shipping, PubkyMarketShippingProfile, PubkyMarketShippingQuoteError,
    PubkyMarketShippingZone,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::homeserver::{
    award_terms_from_bytes, AwardShipping, HomeserverFetchOutcome, HomeserverRawFetchOutcome,
};
use crate::model::ListingRow;
use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;
use crate::AppState;

/// Where the Shop writes a seller's shipping profiles.
pub const PROFILE_PATH: &str = "/pub/pubky.app/marketplace/v1/shipping_profiles/";

pub const REASON_DESTINATION_REQUIRED: &str = "destination_country_required";
pub const REASON_DESTINATION_MISMATCH: &str = "destination_country_mismatch";
pub const REASON_CHOICE_REQUIRED: &str = "shipping_choice_required";
pub const REASON_CHOICE_UNEXPECTED: &str = "shipping_choice_unexpected";
pub const REASON_UNKNOWN_RATE: &str = "unknown_shipping_rate";
pub const REASON_SHIPPING_OVERFLOW: &str = "shipping_overflow";
pub const REASON_PROFILE_MISSING: &str = "shipping_profile_missing";
pub const REASON_PROFILE_UNREADABLE: &str = "shipping_profile_unreadable";
pub const REASON_ASSET_MISMATCH: &str = "shipping_asset_mismatch";
pub const REASON_ZONES_UNREADABLE: &str = "shipping_zones_unreadable";
pub const REASON_SHOP_UNREADABLE: &str = "shop_unreadable";
pub const REASON_LISTING_RECORD_UNAVAILABLE: &str = "listing_record_unavailable";

/// The profile record the service priced an order with, as read.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileTerms {
    /// SHA-256 (lowercase hex) of the record's exact bytes.
    pub sha256: String,
    pub currency: String,
    pub exponent: i64,
    pub excluded_countries: Vec<String>,
    pub zones: Vec<PubkyMarketShippingZone>,
}

/// Parses a profile record's bytes the way the format validates it: the path
/// id and the record's `profileId` agree, the owner is the seller whose
/// homeserver served it, and every zone and rate is valid.
pub fn parse_profile(seller_pubky: &str, profile_id: &str, bytes: &[u8]) -> Option<ProfileTerms> {
    let profile = <PubkyMarketShippingProfile as Validatable>::try_from(bytes, profile_id).ok()?;
    if profile.owner_pubky != seller_pubky {
        return None;
    }
    Some(ProfileTerms {
        sha256: hex::encode(Sha256::digest(bytes)),
        currency: profile.currency,
        exponent: profile.exponent,
        excluded_countries: profile.excluded_countries,
        zones: profile.zones,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProfileRead {
    Found(ProfileTerms),
    /// The homeserver has no profile at that id, or its record is not a valid
    /// profile of that seller.
    Missing,
    /// The homeserver could not be reached or answered unusably.
    Unreadable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShopRead {
    /// The shop publishes no ships-to list: the zones alone decide.
    Open,
    /// The shop ships only to these countries.
    ShipsTo(Vec<String>),
    /// A read was tried and failed, or the record cannot be interpreted.
    Unreadable,
    /// No read was tried for this seller: its listing was not yet registered
    /// when the command's reads were planned. Always refused, so a retry
    /// plans it.
    NotRead,
}

/// What the homeserver said about the profiles and shops one command prices
/// against, read before the command's transaction opens.
#[derive(Debug, Default)]
pub struct ShippingPlan {
    profiles: HashMap<(String, String), ProfileRead>,
    shops: HashMap<String, ShopRead>,
}

impl ShippingPlan {
    pub fn off() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub fn with_profile(mut self, seller: &str, profile_id: &str, read: ProfileRead) -> Self {
        self.profiles
            .insert((seller.to_string(), profile_id.to_string()), read);
        self
    }

    #[cfg(test)]
    pub fn with_shop(mut self, seller: &str, read: ShopRead) -> Self {
        self.shops.insert(seller.to_string(), read);
        self
    }

    fn profile(&self, seller: &str, profile_id: &str) -> &ProfileRead {
        self.profiles
            .get(&(seller.to_string(), profile_id.to_string()))
            .unwrap_or(&ProfileRead::Unreadable)
    }

    fn shop(&self, seller: &str) -> &ShopRead {
        self.shops.get(seller).unwrap_or(&ShopRead::NotRead)
    }

    /// Reads what `command` will price against. Every seller's shop record is
    /// read, so a shop that limits where it ships is known even when the buyer
    /// sent no destination. Profiles are read only for a command that carries
    /// a destination: without one a zone-priced line is refused before any
    /// profile is consulted.
    pub async fn read(state: &AppState, command: &Command) -> Result<Self, sqlx::Error> {
        let mut wanted = Wanted::default();
        match &command.payload {
            CommandPayload::CreateCheckout(payload) => {
                let aggregate_ids: Vec<&str> = payload
                    .lines
                    .iter()
                    .map(|line| line.listing_aggregate_id.as_str())
                    .collect();
                wanted
                    .listings(
                        &state.pool,
                        &aggregate_ids,
                        payload.destination_country.is_some(),
                        false,
                    )
                    .await?;
            }
            CommandPayload::CreateOffer(payload) => {
                wanted
                    .listings(
                        &state.pool,
                        &[command.aggregate_id.as_str()],
                        payload.destination_country.is_some(),
                        payload.destination_country.is_some(),
                    )
                    .await?;
            }
            CommandPayload::PlaceBid(payload) => {
                wanted
                    .listings(
                        &state.pool,
                        &[command.aggregate_id.as_str()],
                        payload.destination_country.is_some(),
                        false,
                    )
                    .await?;
            }
            CommandPayload::AcceptOffer(_) => {
                wanted
                    .accepted_offer(&state.pool, &command.aggregate_id)
                    .await?;
            }
            _ => {}
        }
        let mut plan = Self::off();
        let Some(homeserver) = state.homeserver.clone() else {
            // No homeserver is configured, so no shop record states a limit
            // and no profile can be read: shops are open, profiles refuse.
            for seller in wanted.shops {
                plan.shops.insert(seller, ShopRead::Open);
            }
            return Ok(plan);
        };
        // An offer is priced from the listing record the homeserver serves
        // when it can be read, so the profile that record names is read too,
        // even while the registered row still names another.
        for (seller, listing_id) in std::mem::take(&mut wanted.records) {
            if let HomeserverRawFetchOutcome::Found(raw) =
                homeserver.fetch_listing_raw(&seller, &listing_id).await
            {
                if let Ok(snapshot) = award_terms_from_bytes(&raw) {
                    if let AwardShipping::Profile(profile_id) = snapshot.shipping {
                        wanted.profiles.insert((seller, profile_id));
                    }
                }
            }
        }
        let mut reads = tokio::task::JoinSet::new();
        for (seller, profile_id) in wanted.profiles {
            let homeserver = homeserver.clone();
            reads.spawn(async move {
                let read = match homeserver
                    .fetch_shipping_profile_raw(&seller, &profile_id)
                    .await
                {
                    HomeserverRawFetchOutcome::Found(bytes) => {
                        match parse_profile(&seller, &profile_id, &bytes) {
                            Some(terms) => ProfileRead::Found(terms),
                            None => ProfileRead::Missing,
                        }
                    }
                    HomeserverRawFetchOutcome::NotFound | HomeserverRawFetchOutcome::TooLarge => {
                        ProfileRead::Missing
                    }
                    HomeserverRawFetchOutcome::Unavailable => ProfileRead::Unreadable,
                };
                Read::Profile(seller, profile_id, read)
            });
        }
        for seller in wanted.shops {
            let homeserver = homeserver.clone();
            reads.spawn(async move {
                let read = match homeserver.fetch_shop(&seller).await {
                    HomeserverFetchOutcome::Found(record) => shop_ships_to(&record),
                    HomeserverFetchOutcome::NotFound => ShopRead::Open,
                    HomeserverFetchOutcome::Unavailable => ShopRead::Unreadable,
                };
                Read::Shop(seller, read)
            });
        }
        while let Some(joined) = reads.join_next().await {
            match joined {
                Ok(Read::Profile(seller, profile_id, read)) => {
                    plan.profiles.insert((seller, profile_id), read);
                }
                Ok(Read::Shop(seller, read)) => {
                    plan.shops.insert(seller, read);
                }
                Err(error) => {
                    tracing::error!(%error, "shipping read task failed");
                }
            }
        }
        Ok(plan)
    }
}

enum Read {
    Profile(String, String, ProfileRead),
    Shop(String, ShopRead),
}

/// The ships-to list of a shop record. A malformed list reads as unreadable:
/// a restriction the service cannot interpret is never treated as none.
fn shop_ships_to(record: &Value) -> ShopRead {
    match record.get("shipsToCountries") {
        None | Some(Value::Null) => ShopRead::Open,
        Some(Value::Array(entries)) => {
            let mut countries = Vec::with_capacity(entries.len());
            for entry in entries {
                match entry.as_str() {
                    Some(code)
                        if code.len() == 2
                            && code.bytes().all(|byte| byte.is_ascii_uppercase()) =>
                    {
                        countries.push(code.to_string());
                    }
                    _ => return ShopRead::Unreadable,
                }
            }
            ShopRead::ShipsTo(countries)
        }
        Some(_) => ShopRead::Unreadable,
    }
}

#[derive(Default)]
struct Wanted {
    profiles: std::collections::BTreeSet<(String, String)>,
    shops: std::collections::BTreeSet<String>,
    records: Vec<(String, String)>,
}

impl Wanted {
    async fn listings(
        &mut self,
        pool: &sqlx::PgPool,
        aggregate_ids: &[&str],
        with_profiles: bool,
        with_record: bool,
    ) -> Result<(), sqlx::Error> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT seller_pubky, listing_id, shipping_profile_id FROM listings \
             WHERE aggregate_id = ANY($1) AND deleted_at IS NULL",
        )
        .bind(aggregate_ids)
        .fetch_all(pool)
        .await?;
        for (seller, listing_id, profile_id) in rows {
            if with_profiles {
                if let Some(profile_id) = profile_id {
                    self.profiles.insert((seller.clone(), profile_id));
                }
            }
            if with_record {
                self.records.push((seller.clone(), listing_id));
            }
            self.shops.insert(seller);
        }
        Ok(())
    }

    async fn accepted_offer(
        &mut self,
        pool: &sqlx::PgPool,
        offer_aggregate_id: &str,
    ) -> Result<(), sqlx::Error> {
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT seller_pubky, destination_country, shipping_choice ->> 'profile_id' \
             FROM offers WHERE aggregate_id = $1",
        )
        .bind(offer_aggregate_id)
        .fetch_optional(pool)
        .await?;
        if let Some((seller, destination, profile_id)) = row {
            if destination.is_some() {
                if let Some(profile_id) = profile_id {
                    self.profiles.insert((seller.clone(), profile_id));
                }
            }
            self.shops.insert(seller);
        }
        Ok(())
    }
}

/// How one listing line is priced for shipping.
#[derive(Debug, Clone, PartialEq)]
pub enum LineShipping {
    /// The flat rate, in the listing's minor units, charged once per line.
    Flat(i64),
    Profile(String),
    Inline(Vec<PubkyMarketShippingZone>),
}

impl LineShipping {
    /// The shipping the registered listing row prices with. A listing that
    /// does not publish shipping has none to charge; one with neither a
    /// profile nor zones keeps the flat rate.
    pub fn from_row(listing: &ListingRow) -> Result<Self, PricingError> {
        if !listing
            .fulfillment_methods
            .iter()
            .any(|method| method == "shipping")
        {
            return Ok(Self::Flat(0));
        }
        match (&listing.shipping_profile_id, &listing.shipping_zones) {
            (Some(profile_id), _) => Ok(Self::Profile(profile_id.clone())),
            (None, Some(zones)) => {
                let zones: Vec<PubkyMarketShippingZone> = serde_json::from_value(zones.clone())
                    .map_err(|_| PricingError::ZonesUnreadable)?;
                Ok(Self::Inline(zones))
            }
            (None, None) => Ok(Self::Flat(listing.shipping_minor)),
        }
    }
}

/// One line of a shipped seller order.
#[derive(Debug, Clone)]
pub struct PricedLine {
    pub listing_aggregate_id: String,
    pub quantity: i64,
    pub shipping: LineShipping,
}

/// The rate charged to one shipping group, as stored on the order.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupQuote {
    pub seller_pubky: String,
    pub key: ShippingGroupKey,
    pub profile_sha256: Option<String>,
    pub zone_id: String,
    pub rate_id: String,
    pub label: String,
    pub amount_minor: i64,
    pub units: i64,
    pub estimated_min_days: i64,
    pub estimated_max_days: i64,
}

impl GroupQuote {
    pub fn to_json(&self) -> Value {
        let mut quote = json!({
            "seller_pubky": self.seller_pubky,
            "zone_id": self.zone_id,
            "rate_id": self.rate_id,
            "label": self.label,
            "amount_minor": self.amount_minor,
            "units": self.units,
            "estimated_min_days": self.estimated_min_days,
            "estimated_max_days": self.estimated_max_days,
        });
        match &self.key {
            ShippingGroupKey::Profile(profile_id) => {
                quote["profile_id"] = json!(profile_id);
                quote["profile_sha256"] = json!(self.profile_sha256);
            }
            ShippingGroupKey::Inline(aggregate_id) => {
                quote["listing_aggregate_id"] = json!(aggregate_id);
            }
        }
        quote
    }
}

/// What one shipped seller order is charged for shipping.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderShipping {
    pub shipping_minor: i64,
    pub quotes: Vec<GroupQuote>,
}

impl OrderShipping {
    /// The quotes as the order stores them; `None` when every line is flat.
    pub fn quotes_json(&self) -> Option<Value> {
        (!self.quotes.is_empty())
            .then(|| Value::Array(self.quotes.iter().map(GroupQuote::to_json).collect()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PricingError {
    DestinationRequired,
    ChoiceRequired,
    ChoiceUnexpected,
    UnknownRate,
    Overflow,
    NotShipped,
    ProfileMissing,
    ProfileUnreadable,
    ProfileChanged,
    AssetMismatch,
    ZonesUnreadable,
    ShopUnreadable,
}

impl PricingError {
    pub fn failure(self) -> CommandFailure {
        match self {
            Self::DestinationRequired => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A destination country is required to price this shipping.",
                REASON_DESTINATION_REQUIRED,
            ),
            Self::ChoiceRequired => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A shipping rate is required for each shipping group.",
                REASON_CHOICE_REQUIRED,
            ),
            Self::ChoiceUnexpected => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "A shipping choice names a group this order does not have.",
                REASON_CHOICE_UNEXPECTED,
            ),
            Self::UnknownRate => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "The chosen shipping rate is not offered to this destination.",
                REASON_UNKNOWN_RATE,
            ),
            Self::Overflow => CommandFailure::refused_with_reason(
                RefusalKind::InvalidCommand,
                ErrorCode::InvalidCommand,
                "The shipping charge exceeds the largest supported amount.",
                REASON_SHIPPING_OVERFLOW,
            ),
            Self::NotShipped => CommandFailure::refused(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailableToCountry,
                "The seller does not ship to that country.",
            ),
            Self::ProfileMissing => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailable,
                "The seller's shipping profile is unavailable.",
                REASON_PROFILE_MISSING,
            ),
            Self::ProfileUnreadable => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailable,
                "The seller's shipping profile could not be read. Try again shortly.",
                REASON_PROFILE_UNREADABLE,
            ),
            Self::ProfileChanged => CommandFailure::refused(
                RefusalKind::InvalidState,
                ErrorCode::ShippingChanged,
                "The seller changed their shipping rates. Review the new rates.",
            ),
            Self::AssetMismatch => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailable,
                "The seller's shipping rates are priced in a different currency than the item.",
                REASON_ASSET_MISMATCH,
            ),
            Self::ZonesUnreadable => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailable,
                "The listing's shipping zones are unreadable.",
                REASON_ZONES_UNREADABLE,
            ),
            Self::ShopUnreadable => CommandFailure::refused_with_reason(
                RefusalKind::InvalidState,
                ErrorCode::ShippingUnavailable,
                "The seller's shop could not be read. Try again shortly.",
                REASON_SHOP_UNREADABLE,
            ),
        }
    }
}

impl From<PubkyMarketShippingQuoteError> for PricingError {
    fn from(error: PubkyMarketShippingQuoteError) -> Self {
        match error {
            PubkyMarketShippingQuoteError::NotShipped => Self::NotShipped,
            PubkyMarketShippingQuoteError::UnknownRate => Self::UnknownRate,
            PubkyMarketShippingQuoteError::Overflow => Self::Overflow,
            // The domain validates the country's shape and the units come from
            // quantities bounded at 1,000,000; a failure here is an
            // out-of-range request, never a shipped order.
            PubkyMarketShippingQuoteError::InvalidDestination
            | PubkyMarketShippingQuoteError::InvalidQuantity => Self::Overflow,
        }
    }
}

/// The buyer's rate choices of one command, each consumed by the group it
/// prices. A choice left over names a group the order does not have.
pub struct ChoiceBook<'a> {
    choices: Vec<(&'a ShippingChoice, bool)>,
}

impl<'a> ChoiceBook<'a> {
    pub fn new(choices: &'a [ShippingChoice]) -> Self {
        debug_assert!(choices.len() <= MAX_SHIPPING_CHOICES);
        Self {
            choices: choices.iter().map(|choice| (choice, false)).collect(),
        }
    }

    fn take(&mut self, seller: &str, key: &ShippingGroupKey) -> Option<&'a ShippingChoice> {
        let entry = self.choices.iter_mut().find(|(choice, _)| {
            choice.seller_pubky == seller && choice.group_key().as_ref() == Some(key)
        })?;
        entry.1 = true;
        Some(entry.0)
    }

    pub fn first_unused(&self) -> Option<&'a ShippingChoice> {
        self.choices
            .iter()
            .find(|(_, used)| !used)
            .map(|(choice, _)| *choice)
    }

    /// `Err` when a choice names a group no order of the command has.
    pub fn finish(&self) -> Result<(), PricingError> {
        match self.first_unused() {
            Some(_) => Err(PricingError::ChoiceUnexpected),
            None => Ok(()),
        }
    }
}

/// Prices one listing line (an offer or a bid): its group is the line alone,
/// so exactly the buyer's one choice, when the line is zone-priced, prices it.
pub fn price_single_line(
    plan: &ShippingPlan,
    seller: &str,
    line: PricedLine,
    asset: Asset<'_>,
    destination: Option<&str>,
    choice: Option<&ShippingChoice>,
) -> Result<OrderShipping, PricingError> {
    let choices: Vec<ShippingChoice> = choice.cloned().into_iter().collect();
    let mut book = ChoiceBook::new(&choices);
    let priced = price_shipped_order(plan, seller, &[line], asset, destination, &mut book)?;
    book.finish()?;
    Ok(priced)
}

/// What the listing line asset is, as the lines of a seller order share it.
#[derive(Debug, Clone, Copy)]
pub struct Asset<'a> {
    pub currency: &'a str,
    pub exponent: i32,
}

/// Prices one shipped seller order: flat lines are charged per line as they
/// always were; zone-priced lines are grouped by profile (or by listing, for
/// inline zones) and each group is quoted under the buyer's rate choice.
///
/// `destination` is the country the order ships to. It is checked against the
/// shop's ships-to list when the shop publishes one, for flat lines too.
pub fn price_shipped_order(
    plan: &ShippingPlan,
    seller: &str,
    lines: &[PricedLine],
    asset: Asset<'_>,
    destination: Option<&str>,
    book: &mut ChoiceBook<'_>,
) -> Result<OrderShipping, PricingError> {
    match (destination, plan.shop(seller)) {
        (_, ShopRead::NotRead) => return Err(PricingError::ShopUnreadable),
        (_, ShopRead::Open) => {}
        (Some(destination), ShopRead::ShipsTo(countries)) => {
            if !countries.iter().any(|country| country == destination) {
                return Err(PricingError::NotShipped);
            }
        }
        (None, ShopRead::ShipsTo(_)) => return Err(PricingError::DestinationRequired),
        (Some(_), ShopRead::Unreadable) => return Err(PricingError::ShopUnreadable),
        // Without a destination the buyer claims none, so nothing is checked
        // against the list. An unreadable shop record then does not stop an
        // order that needs no zone price: a homeserver outage must not stop
        // every flat-rate checkout on the platform.
        (None, ShopRead::Unreadable) => {}
    }

    let mut flat_total: i64 = 0;
    let mut groups: Vec<(ShippingGroupKey, i64, &PricedLine)> = Vec::new();
    for line in lines {
        match &line.shipping {
            LineShipping::Flat(amount) => {
                flat_total = flat_total
                    .checked_add(*amount)
                    .ok_or(PricingError::Overflow)?;
            }
            shipping => {
                let key = match shipping {
                    LineShipping::Profile(profile_id) => {
                        ShippingGroupKey::Profile(profile_id.clone())
                    }
                    _ => ShippingGroupKey::Inline(line.listing_aggregate_id.clone()),
                };
                match groups.iter_mut().find(|(existing, _, _)| *existing == key) {
                    Some((_, units, _)) => {
                        *units = units
                            .checked_add(line.quantity)
                            .ok_or(PricingError::Overflow)?;
                    }
                    None => groups.push((key, line.quantity, line)),
                }
            }
        }
    }

    let mut quotes = Vec::with_capacity(groups.len());
    let mut total = flat_total;
    for (key, units, line) in groups {
        let destination = destination.ok_or(PricingError::DestinationRequired)?;
        let choice = book
            .take(seller, &key)
            .ok_or(PricingError::ChoiceRequired)?;
        let quote = quote_group(
            plan,
            seller,
            &key,
            &line.shipping,
            asset,
            choice,
            destination,
            units,
        )?;
        total = total
            .checked_add(quote.amount_minor)
            .ok_or(PricingError::Overflow)?;
        quotes.push(quote);
    }
    if !(0..=marketplace_domain::money::MAX_SAFE_INTEGER).contains(&total) {
        return Err(PricingError::Overflow);
    }
    Ok(OrderShipping {
        shipping_minor: total,
        quotes,
    })
}

#[allow(clippy::too_many_arguments)]
fn quote_group(
    plan: &ShippingPlan,
    seller: &str,
    key: &ShippingGroupKey,
    shipping: &LineShipping,
    asset: Asset<'_>,
    choice: &ShippingChoice,
    destination: &str,
    units: i64,
) -> Result<GroupQuote, PricingError> {
    let (zones, excluded, profile_sha256): (&[PubkyMarketShippingZone], &[String], Option<String>) =
        match shipping {
            LineShipping::Inline(zones) => (zones, &[], None),
            LineShipping::Profile(profile_id) => match plan.profile(seller, profile_id) {
                ProfileRead::Found(terms) => {
                    if choice.profile_sha256.as_deref() != Some(terms.sha256.as_str()) {
                        return Err(PricingError::ProfileChanged);
                    }
                    if terms.currency != asset.currency
                        || terms.exponent != i64::from(asset.exponent)
                    {
                        return Err(PricingError::AssetMismatch);
                    }
                    (
                        &terms.zones,
                        &terms.excluded_countries,
                        Some(terms.sha256.clone()),
                    )
                }
                ProfileRead::Missing => return Err(PricingError::ProfileMissing),
                ProfileRead::Unreadable => return Err(PricingError::ProfileUnreadable),
            },
            LineShipping::Flat(_) => return Err(PricingError::ChoiceUnexpected),
        };
    let quote = quote_shipping(zones, excluded, destination, &choice.rate_id, units)?;
    Ok(GroupQuote {
        seller_pubky: seller.to_string(),
        key: key.clone(),
        profile_sha256,
        zone_id: quote.zone_id,
        rate_id: quote.rate_id,
        label: quote.label,
        amount_minor: quote.amount_minor,
        units,
        estimated_min_days: quote.estimated_min_days,
        estimated_max_days: quote.estimated_max_days,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky_market_specs::PubkyMarketShippingRate;

    const SELLER: &str = "operrr8wsbpr3ue9d4qj41ge1kcc6r7fdiy6o3ugjrrhi4y77rdo";
    const OTHER: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
    const ASSET: Asset<'static> = Asset {
        currency: "USD",
        exponent: 2,
    };

    fn zone(
        id: &str,
        countries: &[&str],
        rest_of_world: bool,
        rate: PubkyMarketShippingRate,
    ) -> PubkyMarketShippingZone {
        PubkyMarketShippingZone {
            zone_id: id.to_string(),
            name: id.to_string(),
            countries: countries.iter().map(|c| c.to_string()).collect(),
            rest_of_world,
            rates: vec![rate],
        }
    }

    fn per_item(first: i64, additional: i64) -> PubkyMarketShippingRate {
        PubkyMarketShippingRate::PerItem {
            rate_id: "std".into(),
            label: "Standard".into(),
            first_minor: first,
            additional_minor: additional,
            estimated_min_days: 3,
            estimated_max_days: 7,
        }
    }

    fn flat(id: &str, amount: i64) -> PubkyMarketShippingRate {
        PubkyMarketShippingRate::Flat {
            rate_id: id.into(),
            label: "Flat".into(),
            amount_minor: amount,
            estimated_min_days: 3,
            estimated_max_days: 7,
        }
    }

    fn inline_line(
        aggregate: &str,
        quantity: i64,
        zones: Vec<PubkyMarketShippingZone>,
    ) -> PricedLine {
        PricedLine {
            listing_aggregate_id: aggregate.to_string(),
            quantity,
            shipping: LineShipping::Inline(zones),
        }
    }

    fn inline_choice(aggregate: &str, rate: &str) -> ShippingChoice {
        ShippingChoice {
            seller_pubky: SELLER.to_string(),
            profile_id: None,
            listing_aggregate_id: Some(aggregate.to_string()),
            rate_id: rate.to_string(),
            profile_sha256: None,
        }
    }

    #[test]
    fn flat_lines_charge_per_line_as_before() {
        let lines = vec![
            PricedLine {
                listing_aggregate_id: "a".into(),
                quantity: 5,
                shipping: LineShipping::Flat(300),
            },
            PricedLine {
                listing_aggregate_id: "b".into(),
                quantity: 1,
                shipping: LineShipping::Flat(450),
            },
        ];
        let mut book = ChoiceBook::new(&[]);
        let priced = price_shipped_order(
            &ShippingPlan::off().with_shop(SELLER, ShopRead::Open),
            SELLER,
            &lines,
            ASSET,
            None,
            &mut book,
        )
        .expect("flat lines price without a destination");
        assert_eq!(priced.shipping_minor, 750);
        assert!(priced.quotes.is_empty());
        assert_eq!(priced.quotes_json(), None);
    }

    #[test]
    fn a_per_item_group_charges_first_plus_additional() {
        let zones = vec![zone("us", &["US"], false, per_item(500, 200))];
        let lines = vec![inline_line("l1", 3, zones)];
        let choices = [inline_choice("l1", "std")];
        let mut book = ChoiceBook::new(&choices);
        let priced = price_shipped_order(
            &ShippingPlan::off().with_shop(SELLER, ShopRead::Open),
            SELLER,
            &lines,
            ASSET,
            Some("US"),
            &mut book,
        )
        .expect("quote");
        book.finish().expect("choice consumed");
        assert_eq!(priced.shipping_minor, 900);
        assert_eq!(priced.quotes[0].units, 3);
        assert_eq!(priced.quotes[0].zone_id, "us");
    }

    #[test]
    fn lines_sharing_a_profile_are_one_group_and_inline_lines_are_not() {
        let profile = ProfileTerms {
            sha256: "a".repeat(64),
            currency: "USD".into(),
            exponent: 2,
            excluded_countries: vec![],
            zones: vec![zone("w", &[], true, per_item(400, 100))],
        };
        let plan = ShippingPlan::off()
            .with_shop(SELLER, ShopRead::Open)
            .with_profile(SELLER, "p1", ProfileRead::Found(profile));
        let profile_line = |aggregate: &str, quantity| PricedLine {
            listing_aggregate_id: aggregate.to_string(),
            quantity,
            shipping: LineShipping::Profile("p1".into()),
        };
        let lines = vec![
            profile_line("l1", 2),
            profile_line("l2", 3),
            inline_line("l3", 1, vec![zone("z", &["DE"], false, flat("fast", 999))]),
        ];
        let choices = [
            ShippingChoice {
                seller_pubky: SELLER.into(),
                profile_id: Some("p1".into()),
                listing_aggregate_id: None,
                rate_id: "std".into(),
                profile_sha256: Some("a".repeat(64)),
            },
            inline_choice("l3", "fast"),
        ];
        let mut book = ChoiceBook::new(&choices);
        let priced = price_shipped_order(&plan, SELLER, &lines, ASSET, Some("DE"), &mut book)
            .expect("quote");
        // 5 profile units: 400 + 4 * 100; the inline listing is its own group.
        assert_eq!(priced.shipping_minor, 800 + 999);
        assert_eq!(priced.quotes.len(), 2);
        assert_eq!(priced.quotes[0].units, 5);
    }

    #[test]
    fn a_changed_profile_is_shipping_changed_not_a_silent_requote() {
        let profile = ProfileTerms {
            sha256: "b".repeat(64),
            currency: "USD".into(),
            exponent: 2,
            excluded_countries: vec![],
            zones: vec![zone("w", &[], true, flat("std", 100))],
        };
        let plan = ShippingPlan::off()
            .with_shop(SELLER, ShopRead::Open)
            .with_profile(SELLER, "p1", ProfileRead::Found(profile));
        let lines = vec![PricedLine {
            listing_aggregate_id: "l1".into(),
            quantity: 1,
            shipping: LineShipping::Profile("p1".into()),
        }];
        let choices = [ShippingChoice {
            seller_pubky: SELLER.into(),
            profile_id: Some("p1".into()),
            listing_aggregate_id: None,
            rate_id: "std".into(),
            profile_sha256: Some("a".repeat(64)),
        }];
        let mut book = ChoiceBook::new(&choices);
        assert_eq!(
            price_shipped_order(&plan, SELLER, &lines, ASSET, Some("US"), &mut book),
            Err(PricingError::ProfileChanged)
        );
    }

    #[test]
    fn refusals_name_what_failed() {
        let zones = vec![zone("us", &["US"], false, flat("std", 100))];
        let lines = vec![inline_line("l1", 1, zones)];
        let plan = ShippingPlan::off().with_shop(SELLER, ShopRead::Open);
        let price = |destination: Option<&str>, choices: &[ShippingChoice]| {
            let mut book = ChoiceBook::new(choices);
            let priced = price_shipped_order(&plan, SELLER, &lines, ASSET, destination, &mut book);
            (priced.map(|p| p.shipping_minor), book.finish())
        };
        assert_eq!(
            price(None, &[inline_choice("l1", "std")]).0,
            Err(PricingError::DestinationRequired)
        );
        assert_eq!(price(Some("US"), &[]).0, Err(PricingError::ChoiceRequired));
        assert_eq!(
            price(Some("FR"), &[inline_choice("l1", "std")]).0,
            Err(PricingError::NotShipped)
        );
        assert_eq!(
            price(Some("US"), &[inline_choice("l1", "nope")]).0,
            Err(PricingError::UnknownRate)
        );
        let (priced, finished) = price(
            Some("US"),
            &[
                inline_choice("l1", "std"),
                inline_choice("elsewhere", "std"),
            ],
        );
        assert_eq!(priced, Ok(100));
        assert_eq!(finished, Err(PricingError::ChoiceUnexpected));
    }

    #[test]
    fn the_shop_ships_to_list_limits_flat_lines_too() {
        let lines = vec![PricedLine {
            listing_aggregate_id: "l1".into(),
            quantity: 1,
            shipping: LineShipping::Flat(100),
        }];
        let ships_to = |read| {
            let plan = ShippingPlan::off().with_shop(SELLER, read);
            let mut book = ChoiceBook::new(&[]);
            price_shipped_order(&plan, SELLER, &lines, ASSET, Some("DE"), &mut book)
                .map(|p| p.shipping_minor)
        };
        assert_eq!(ships_to(ShopRead::ShipsTo(vec!["DE".into()])), Ok(100));
        assert_eq!(
            ships_to(ShopRead::ShipsTo(vec!["US".into()])),
            Err(PricingError::NotShipped)
        );
        assert_eq!(ships_to(ShopRead::Open), Ok(100));
        assert_eq!(
            ships_to(ShopRead::Unreadable),
            Err(PricingError::ShopUnreadable)
        );
        // A different seller's list is not this seller's.
        let plan = ShippingPlan::off().with_shop(OTHER, ShopRead::Open);
        let mut book = ChoiceBook::new(&[]);
        assert_eq!(
            price_shipped_order(&plan, SELLER, &lines, ASSET, Some("DE"), &mut book)
                .map(|p| p.shipping_minor),
            Err(PricingError::ShopUnreadable)
        );
    }

    #[test]
    fn a_seller_the_plan_never_read_is_refused_with_or_without_a_destination() {
        let lines = vec![PricedLine {
            listing_aggregate_id: "l1".into(),
            quantity: 1,
            shipping: LineShipping::Flat(100),
        }];
        for destination in [None, Some("DE")] {
            let mut book = ChoiceBook::new(&[]);
            assert_eq!(
                price_shipped_order(
                    &ShippingPlan::off(),
                    SELLER,
                    &lines,
                    ASSET,
                    destination,
                    &mut book
                )
                .map(|p| p.shipping_minor),
                Err(PricingError::ShopUnreadable),
                "{destination:?}"
            );
        }
        // A read that was tried and failed lets a destination-less order
        // through: the buyer names no country to check.
        let plan = ShippingPlan::off().with_shop(SELLER, ShopRead::Unreadable);
        let mut book = ChoiceBook::new(&[]);
        assert_eq!(
            price_shipped_order(&plan, SELLER, &lines, ASSET, None, &mut book)
                .map(|p| p.shipping_minor),
            Ok(100)
        );
    }

    #[test]
    fn a_profile_in_another_asset_is_refused_never_converted() {
        let profile = ProfileTerms {
            sha256: "a".repeat(64),
            currency: "EUR".into(),
            exponent: 2,
            excluded_countries: vec![],
            zones: vec![zone("w", &[], true, flat("std", 100))],
        };
        let plan = ShippingPlan::off()
            .with_shop(SELLER, ShopRead::Open)
            .with_profile(SELLER, "p1", ProfileRead::Found(profile));
        let lines = vec![PricedLine {
            listing_aggregate_id: "l1".into(),
            quantity: 1,
            shipping: LineShipping::Profile("p1".into()),
        }];
        let choices = [ShippingChoice {
            seller_pubky: SELLER.into(),
            profile_id: Some("p1".into()),
            listing_aggregate_id: None,
            rate_id: "std".into(),
            profile_sha256: Some("a".repeat(64)),
        }];
        let mut book = ChoiceBook::new(&choices);
        assert_eq!(
            price_shipped_order(&plan, SELLER, &lines, ASSET, Some("US"), &mut book),
            Err(PricingError::AssetMismatch)
        );
    }

    #[test]
    fn shop_lists_with_malformed_entries_are_unreadable() {
        assert_eq!(shop_ships_to(&json!({})), ShopRead::Open);
        assert_eq!(
            shop_ships_to(&json!({"shipsToCountries": ["US", "DE"]})),
            ShopRead::ShipsTo(vec!["US".into(), "DE".into()])
        );
        assert_eq!(
            shop_ships_to(&json!({"shipsToCountries": ["us"]})),
            ShopRead::Unreadable
        );
        assert_eq!(
            shop_ships_to(&json!({"shipsToCountries": "US"})),
            ShopRead::Unreadable
        );
    }
}
