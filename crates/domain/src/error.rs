use serde::{Deserialize, Serialize};

/// Wire error codes shared with the TypeScript prototype engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidCommand,
    Unauthorized,
    NotFound,
    RevisionConflict,
    IdempotencyConflict,
    InsufficientInventory,
    InvariantViolation,
    OfferExpired,
    InvalidState,
    AuctionClosed,
    BidTooLow,
    SellerRegistrationRequired,
    /// An upstream dependency (the seller's homeserver) could not be
    /// reached or answered unusably. Retriable: the command may succeed
    /// once the upstream recovers, so it maps to 503 rather than a 4xx.
    UpstreamUnavailable,
    AwardExpired,
    AwardAlreadyConverted,
    AwardQuantityMismatch,
    AwardVariantMismatch,
    AwardListingChanged,
    AwardHoldMissing,
    /// The listing, or its seller, is moderated: it cannot be bought, bid on
    /// or offered on. A 409, not a 403: clients read a 403 on a command as a
    /// session problem and ask the user to sign in again.
    ListingModerated,
    /// The seller's current listing record is not `active` (paused, ended,
    /// removed): it cannot be bought, bid on or offered on. `error.reason`
    /// names the state.
    ListingNotActive,
    /// The seller does not sell to the buyer's region: a business or
    /// undeclared seller with a buyer in the EU-27, or a private seller who
    /// must re-confirm their status first. `error.reason` names which.
    /// Only returned while `EU_FENCE_ENABLED` is on.
    SellerUnavailableInRegion,
    /// The seller reached the annual sales limit for sellers resident in the
    /// EU (29 paid sales or EUR 2,000 of paid orders per calendar year). Only
    /// returned while `EU_FENCE_ENABLED` is on.
    SellerAnnualLimitReached,
    /// The cart has a digital line and the buyer's country is in the EU-27
    /// or is GB. Only returned while `DIGITAL_REGION_BLOCK_ENABLED` is on.
    DigitalUnavailableInRegion,
}

impl ErrorCode {
    /// HTTP status carried by each error code. Idempotency and concurrency
    /// conflicts are 409 per ADR-0019 §3.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorCode::InvalidCommand => 422,
            ErrorCode::Unauthorized => 403,
            ErrorCode::NotFound => 404,
            ErrorCode::RevisionConflict
            | ErrorCode::IdempotencyConflict
            | ErrorCode::InsufficientInventory
            | ErrorCode::InvariantViolation
            | ErrorCode::OfferExpired
            | ErrorCode::InvalidState
            | ErrorCode::AuctionClosed
            | ErrorCode::BidTooLow
            | ErrorCode::SellerRegistrationRequired => 409,
            ErrorCode::AwardExpired
            | ErrorCode::AwardAlreadyConverted
            | ErrorCode::AwardQuantityMismatch
            | ErrorCode::AwardVariantMismatch
            | ErrorCode::AwardListingChanged
            | ErrorCode::AwardHoldMissing
            | ErrorCode::ListingModerated
            | ErrorCode::ListingNotActive
            | ErrorCode::SellerUnavailableInRegion
            | ErrorCode::SellerAnnualLimitReached
            | ErrorCode::DigitalUnavailableInRegion => 409,
            ErrorCode::UpstreamUnavailable => 503,
        }
    }
}
