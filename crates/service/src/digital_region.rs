//! No digital sales to EU or UK buyers at launch (legal launch plan C3b),
//! behind `DIGITAL_REGION_BLOCK_ENABLED`.
//!
//! For electronically supplied services sold through a platform, EU VAT
//! treats the platform as the supplier when it authorises the charge or the
//! delivery (IR 282/2011 Art. 9a), and the UK has the same platform rule.
//! The service releases instant digital items and Locks items itself, so
//! until counsel says how to open them, a checkout or an offer checkout that
//! contains a digital line is refused when the buyer's country
//! (`buyer_country`, C2) is in the EU-27 or is GB:
//! `DIGITAL_UNAVAILABLE_IN_REGION` (409). A drop claim is a `checkout.create`
//! on a drop-bound listing, so it passes the same gate. Offers on digital
//! items are already refused (`offers_unavailable_for_digital`), so
//! `offer.create` needs no gate; `offer.checkout` is gated for completeness.
//!
//! A line is digital when its listing is a Locks listing, when the buyer
//! chose the `digital` fulfillment for it, or when `digital` is the only
//! method the listing publishes. A listing that publishes digital next to
//! shipping or pickup is physical when the buyer chose one of those. Every
//! digital delivery kind (file, link, text, email, message) publishes the
//! `digital` method. Auctions ship only, so a bid is never digital.
//!
//! The buyer's declared country is not trusted on its own: a buyer in the EU
//! can declare Switzerland. So the request's own country, read from a header
//! the trusted edge sets from the client's IP (`CLIENT_COUNTRY_HEADER`: the GCP
//! load balancer's `{client_region}`, Vercel's `x-vercel-ip-country`), is
//! checked too. A digital line is refused when EITHER the declared country OR
//! the IP country is in the EU-27 or is GB. With the flag on and no usable IP
//! country (header absent, malformed, or the unknown markers `XX` and `T1`),
//! digital lines fail closed with `reason: buyer_region_unknown` and an ALERT is
//! logged; physical lines never need it. The edge must set the header itself
//! and overwrite any value the client sent.
//!
//! The flag is independent of `EU_FENCE_ENABLED`. With it on, a command that
//! contains a digital line must carry `buyer_country` (the Shop asks for it
//! on digital listings); a command with no digital line does not need one on
//! this account. With it off nothing here reads the database or refuses.

use marketplace_domain::commands::{Command, CommandPayload, FulfillmentMethod};
use marketplace_domain::region::{is_eu_country, GB};
use marketplace_domain::{ErrorCode, ValidationIssue};
use sqlx::PgPool;

use crate::refusal_audit::RefusalKind;
use crate::result::CommandFailure;
use crate::AppState;

pub const DIGITAL_UNAVAILABLE_MESSAGE: &str =
    "Digital items aren't available to buyers in the EU or UK yet.";
pub const REASON_EU_BUYER: &str = "eu_buyer";
pub const REASON_GB_BUYER: &str = "gb_buyer";
/// The IP country is in the EU-27.
pub const REASON_EU_IP: &str = "eu_ip";
/// The IP country is GB.
pub const REASON_GB_IP: &str = "gb_ip";
/// No usable IP country: digital lines fail closed.
pub const REASON_REGION_UNKNOWN: &str = "buyer_region_unknown";

/// What the trusted edge told the service about the request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeContext {
    /// The client's country from the edge header, uppercase ISO alpha-2.
    /// `None` when the header is absent or holds no usable country.
    pub client_country: Option<String>,
}

impl EdgeContext {
    /// Reads the configured header. `XX` and `T1` are the unknown and Tor
    /// markers CDNs send; they are not countries.
    pub fn from_headers(headers: &axum::http::HeaderMap, header: &str) -> Self {
        let client_country = headers
            .get(header)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| {
                value.len() == 2
                    && value.bytes().all(|byte| byte.is_ascii_uppercase())
                    && value != "XX"
            });
        Self { client_country }
    }
}

/// Whether digital sales are held back for a buyer in `country_code`.
pub fn blocked_country(country_code: &str) -> Option<&'static str> {
    if is_eu_country(country_code) {
        Some(REASON_EU_BUYER)
    } else if country_code == GB {
        Some(REASON_GB_BUYER)
    } else {
        None
    }
}

/// The listing facts that decide whether a line is digital.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListingFlavor {
    pub locks: bool,
    pub methods: Vec<String>,
}

/// Whether a line is digital, given the fulfillment the buyer chose for it
/// (`None` when the command carries no choice).
pub fn line_is_digital(listing: &ListingFlavor, chosen: Option<FulfillmentMethod>) -> bool {
    if listing.locks {
        return true;
    }
    match chosen {
        Some(method) => method == FulfillmentMethod::Digital,
        None => listing.methods.len() == 1 && listing.methods[0] == "digital",
    }
}

fn blocked(reason: &'static str) -> CommandFailure {
    CommandFailure::refused_with_reason(
        RefusalKind::InvalidState,
        ErrorCode::DigitalUnavailableInRegion,
        DIGITAL_UNAVAILABLE_MESSAGE,
        reason,
    )
}

fn missing_country() -> CommandFailure {
    CommandFailure::refused_with_issues(
        RefusalKind::InvalidCommand,
        ErrorCode::InvalidCommand,
        "The marketplace command is invalid.",
        vec![ValidationIssue {
            path: "payload.buyer_country".to_string(),
            message: "Expected the buyer's ISO 3166-1 alpha-2 country code".to_string(),
            reason: None,
        }],
    )
}

fn unavailable() -> CommandFailure {
    CommandFailure::refused(
        RefusalKind::UpstreamUnavailable,
        ErrorCode::UpstreamUnavailable,
        "The listing could not be read. Try again shortly.",
    )
}

/// The listings a command would sell, each with the fulfillment the buyer
/// chose for it. Empty for a command that is not gated here.
fn gated_listings(command: &Command) -> Vec<(String, Option<FulfillmentMethod>)> {
    match &command.payload {
        CommandPayload::CreateCheckout(payload) => payload
            .lines
            .iter()
            .map(|line| (line.listing_aggregate_id.clone(), line.fulfillment))
            .collect(),
        CommandPayload::OfferCheckout(payload) => {
            vec![(payload.listing_aggregate_id.clone(), payload.fulfillment)]
        }
        _ => Vec::new(),
    }
}

async fn flavors(
    pool: &PgPool,
    aggregate_ids: &[&str],
) -> Result<Vec<(String, ListingFlavor)>, sqlx::Error> {
    let rows: Vec<(String, bool, Vec<String>)> = sqlx::query_as(
        "SELECT aggregate_id, digital_lock_policy_uri IS NOT NULL, fulfillment_methods \
         FROM listings WHERE aggregate_id = ANY($1) AND deleted_at IS NULL",
    )
    .bind(aggregate_ids)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(aggregate_id, locks, methods)| (aggregate_id, ListingFlavor { locks, methods }))
        .collect())
}

/// Runs before the command's transaction. `Ok` when the flag is off, the
/// command is not gated, no line is digital or the buyer is outside the
/// EU-27 and GB.
pub async fn gate(
    state: &AppState,
    command: &Command,
    edge: &EdgeContext,
) -> Result<(), CommandFailure> {
    if !state.config.digital_region_block_enabled {
        return Ok(());
    }
    let wanted = gated_listings(command);
    if wanted.is_empty() {
        return Ok(());
    }
    let ids: Vec<&str> = wanted.iter().map(|(id, _)| id.as_str()).collect();
    let found = flavors(&state.pool, &ids).await.map_err(|error| {
        tracing::error!(%error, "digital region gate could not read the listings");
        unavailable()
    })?;
    let any_digital = wanted.iter().any(|(id, chosen)| {
        found
            .iter()
            .find(|(found_id, _)| found_id == id)
            .is_some_and(|(_, flavor)| line_is_digital(flavor, *chosen))
    });
    if !any_digital {
        return Ok(());
    }
    let Some(country) = crate::region_fence::buyer_country_of(&command.payload).flatten() else {
        return Err(missing_country());
    };
    if let Some(reason) = blocked_country(country) {
        return Err(blocked(reason));
    }
    match edge.client_country.as_deref() {
        Some(ip_country) => match blocked_country(ip_country) {
            Some(REASON_GB_BUYER) => Err(blocked(REASON_GB_IP)),
            Some(_) => Err(blocked(REASON_EU_IP)),
            None => Ok(()),
        },
        None => {
            tracing::error!(
                header = %state.config.client_country_header,
                "ALERT digital_region_client_country_missing: no usable client country from the \
                 edge header; refusing a digital line (fail closed)"
            );
            Err(blocked(REASON_REGION_UNKNOWN))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flavor(locks: bool, methods: &[&str]) -> ListingFlavor {
        ListingFlavor {
            locks,
            methods: methods.iter().map(|method| method.to_string()).collect(),
        }
    }

    #[test]
    fn the_blocked_countries_are_the_eu_27_and_gb() {
        for code in marketplace_domain::region::EU_27 {
            assert_eq!(blocked_country(code), Some(REASON_EU_BUYER), "{code}");
        }
        assert_eq!(blocked_country("GB"), Some(REASON_GB_BUYER));
        for code in ["US", "CH", "NO", "IS", "LI", "BR", "JP", "UK", "gb", ""] {
            assert_eq!(blocked_country(code), None, "{code}");
        }
    }

    #[test]
    fn the_edge_header_yields_only_a_real_uppercase_country() {
        use axum::http::{HeaderMap, HeaderValue};
        let read = |value: Option<&str>| {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert("x-client-region", HeaderValue::from_str(value).unwrap());
            }
            EdgeContext::from_headers(&headers, "x-client-region").client_country
        };
        assert_eq!(read(Some("DE")).as_deref(), Some("DE"));
        assert_eq!(read(Some(" ch ")).as_deref(), Some("CH"));
        for bad in [
            None,
            Some(""),
            Some("XX"),
            Some("T1"),
            Some("DEU"),
            Some("D"),
            Some("1A"),
        ] {
            assert_eq!(read(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn a_locks_listing_is_digital_whatever_the_buyer_chose() {
        let locks = flavor(true, &["digital"]);
        assert!(line_is_digital(&locks, None));
        assert!(line_is_digital(&locks, Some(FulfillmentMethod::Digital)));
        assert!(line_is_digital(&locks, Some(FulfillmentMethod::Shipping)));
    }

    #[test]
    fn a_chosen_fulfillment_decides_for_a_mixed_listing() {
        let mixed = flavor(false, &["shipping", "digital"]);
        assert!(line_is_digital(&mixed, Some(FulfillmentMethod::Digital)));
        assert!(!line_is_digital(&mixed, Some(FulfillmentMethod::Shipping)));
        assert!(!line_is_digital(&mixed, Some(FulfillmentMethod::Pickup)));
        assert!(!line_is_digital(&mixed, None));
    }

    #[test]
    fn a_digital_only_listing_is_digital_without_a_choice() {
        let digital = flavor(false, &["digital"]);
        assert!(line_is_digital(&digital, None));
        assert!(!line_is_digital(&flavor(false, &["shipping"]), None));
        assert!(!line_is_digital(
            &flavor(false, &["pickup", "shipping"]),
            None
        ));
    }
}
