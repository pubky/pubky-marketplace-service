//! The multi-asset prepare and activate bodies captured from paykit-server
//! (`tests/fixtures/paykit-server-pk1/`, PR-A `f3697b7`) replayed through the
//! real client: the offer each body records, the refusals the client makes of
//! a body that disagrees with the request, and the error codes the service
//! maps. Nothing here is written by hand: the bodies are the capture.

mod common;

use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Router;
use common::TEST_PAYKIT_SIGNING_SEED;
use marketplace_service::payment_attempt::{
    AcceptedAsset, OfferedAssets, PaymentAsset, PaymentTerms,
};
use marketplace_service::payments::{
    ActivationTerms, MarketplaceRequestTerms, PaykitApi, PaykitClient, PaykitCommandError,
    PaykitLifecycleTarget, PaykitRequestError,
};

const SELLER: &str = "buuk6nr1caaumhms4tph5oa656z9f5k9q867h4u8ze441mpfuhyo";
const BUYER: &str = "7ahm9mquyezn4ny85baxqo43r8epijtwo6h8149yxsm7eiu155uy";
const INVOICE: &str = "4c6e66c7-8a8b-4d54-b97b-340cedb3e1d0";

fn fixture(name: &str) -> String {
    let path = format!(
        "{}/tests/fixtures/paykit-server-pk1/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"))
}

/// A server that answers every request with one captured body.
async fn replaying(status: u16, name: &str) -> PaykitClient {
    let body = fixture(name);
    let router = Router::new().fallback(move || {
        let body = body.clone();
        async move {
            (
                StatusCode::from_u16(status).expect("status"),
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("replay binds");
    let base_url = format!("http://{}", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("replay serves");
    });
    PaykitClient::new(&base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("client")
        .with_api(PaykitApi::Upstream)
}

fn offered(btc_sats: Option<u64>, usdt_units: Option<u64>) -> OfferedAssets {
    OfferedAssets {
        btc_sats,
        usdt_units,
    }
}

async fn prepare(
    name: &str,
    status: u16,
    denomination: PaymentAsset,
    amount_minor: u64,
    only: Option<Vec<AcceptedAsset>>,
) -> Result<OfferedAssets, PaykitRequestError> {
    let client = replaying(status, name).await;
    client
        .prepare_marketplace_request(
            SELLER,
            BUYER,
            uuid::Uuid::new_v4(),
            1,
            &MarketplaceRequestTerms {
                denomination,
                amount_minor,
                only,
            },
            1_800,
        )
        .await
        .map(|prepared| {
            assert_eq!(prepared.invoice_id.to_string(), INVOICE);
            assert_eq!(
                prepared.prepare_expires_at.to_rfc3339(),
                "2026-10-23T09:15:00+00:00"
            );
            prepared.offered
        })
}

use AcceptedAsset::{Btc, Usdt};

#[tokio::test]
async fn every_captured_usd_and_bitcoin_prepare_records_the_offer_it_made() {
    let cases: [(
        &str,
        PaymentAsset,
        u64,
        Option<Vec<AcceptedAsset>>,
        OfferedAssets,
    ); 10] = [
        (
            "prepare-usd-derived-btc-usdt",
            PaymentAsset::Usd,
            500,
            None,
            offered(Some(6_173), Some(5_000_000)),
        ),
        (
            "prepare-usd-derived-btc",
            PaymentAsset::Usd,
            500,
            None,
            offered(Some(6_173), None),
        ),
        (
            "prepare-usd-derived-usdt",
            PaymentAsset::Usd,
            500,
            None,
            offered(None, Some(5_000_000)),
        ),
        (
            "prepare-usd-derived-btc-without-usdt-config",
            PaymentAsset::Usd,
            500,
            None,
            offered(Some(6_173), None),
        ),
        (
            "prepare-usd-btc-usdt",
            PaymentAsset::Usd,
            500,
            Some(vec![Btc, Usdt]),
            offered(Some(6_173), Some(5_000_000)),
        ),
        (
            "prepare-usd-btc",
            PaymentAsset::Usd,
            500,
            Some(vec![Btc]),
            offered(Some(6_173), None),
        ),
        (
            "prepare-usd-usdt",
            PaymentAsset::Usd,
            500,
            Some(vec![Usdt]),
            offered(None, Some(5_000_000)),
        ),
        (
            "prepare-btc-btc-usdt",
            PaymentAsset::Btc,
            50_000,
            Some(vec![Btc, Usdt]),
            offered(Some(50_000), Some(40_500_000)),
        ),
        (
            "prepare-btc-btc",
            PaymentAsset::Btc,
            50_000,
            Some(vec![Btc]),
            offered(Some(50_000), None),
        ),
        (
            "prepare-btc-usdt",
            PaymentAsset::Btc,
            50_000,
            Some(vec![Usdt]),
            offered(None, Some(40_500_000)),
        ),
    ];
    for (name, denomination, amount, only, expected) in cases {
        assert_eq!(
            prepare(name, 200, denomination, amount, only).await,
            Ok(expected),
            "{name}"
        );
    }
}

#[tokio::test]
async fn the_captured_usdt_amount_of_a_usd_price_is_exactly_the_parity_amount() {
    let terms = PaymentTerms::usdt_at_parity("USD", 2, 500).expect("parity");
    assert_eq!(terms.amount_minor, 5_000_000);
    assert_eq!(terms.amount_minor, 500 * 10_000);
}

#[tokio::test]
async fn a_captured_body_that_disagrees_with_the_request_is_refused() {
    // Another denomination, another amount, or an asset outside the
    // narrowing: each is a body the request did not ask for.
    for (name, denomination, amount, only) in [
        (
            "prepare-usdt-usdt",
            PaymentAsset::Usd,
            500,
            Some(vec![Usdt]),
        ),
        ("prepare-btc-btc", PaymentAsset::Usd, 500, Some(vec![Btc])),
        ("prepare-usd-btc-usdt", PaymentAsset::Usd, 501, None),
        (
            "prepare-usd-btc-usdt",
            PaymentAsset::Usd,
            500,
            Some(vec![Btc]),
        ),
        ("prepare-usd-btc-usdt", PaymentAsset::Btc, 500, None),
    ] {
        assert_eq!(
            prepare(name, 200, denomination, amount, only).await,
            Err(PaykitRequestError::Rejected),
            "{name}"
        );
    }
}

#[tokio::test]
async fn the_captured_prepare_errors_map_to_the_service_refusals() {
    for (name, status, expected) in [
        (
            "seller-setup-pending-bitcoin",
            503,
            PaykitRequestError::SellerAccountUnavailable,
        ),
        (
            "seller-setup-pending-usdt",
            503,
            PaykitRequestError::SellerAccountUnavailable,
        ),
        (
            "seller-setup-pending-both",
            503,
            PaykitRequestError::SellerAccountUnavailable,
        ),
        ("asset-unsupported", 409, PaykitRequestError::Unavailable),
        (
            "payment-window-exceeds-cap",
            400,
            PaykitRequestError::Unavailable,
        ),
        ("operation-conflict", 409, PaykitRequestError::Rejected),
    ] {
        assert_eq!(
            prepare(name, status, PaymentAsset::Usd, 500, None).await,
            Err(expected),
            "{name}"
        );
    }
}

async fn activate(
    name: &str,
    status: u16,
    terms: &ActivationTerms,
) -> Result<(String, String), PaykitCommandError> {
    let client = replaying(status, name).await;
    client
        .activate_marketplace_request(
            &PaykitLifecycleTarget::Upstream {
                creator: SELLER.to_string(),
            },
            INVOICE.parse().expect("invoice"),
            terms,
        )
        .await
        .map(|activated| {
            (
                activated.activated_at.to_rfc3339(),
                activated.payment_deadline.to_rfc3339(),
            )
        })
}

#[tokio::test]
async fn the_captured_activation_must_echo_the_persisted_terms() {
    let persisted = ActivationTerms {
        denomination: PaymentAsset::Usd,
        amount_minor: 500,
        offered: offered(Some(6_173), Some(5_000_000)),
    };
    assert_eq!(
        activate("activate-usd-btc-usdt", 200, &persisted).await,
        Ok((
            "2026-10-23T09:05:00+00:00".to_string(),
            "2026-10-23T09:35:00+00:00".to_string()
        ))
    );
    for other in [
        ActivationTerms {
            amount_minor: 501,
            ..persisted.clone()
        },
        ActivationTerms {
            denomination: PaymentAsset::Btc,
            ..persisted.clone()
        },
        ActivationTerms {
            offered: offered(Some(6_174), Some(5_000_000)),
            ..persisted.clone()
        },
        ActivationTerms {
            offered: offered(Some(6_173), None),
            ..persisted.clone()
        },
    ] {
        assert_eq!(
            activate("activate-usd-btc-usdt", 200, &other).await,
            Err(PaykitCommandError::ActivationTotalMismatch),
            "{other:?}"
        );
    }
    assert_eq!(
        activate("total-mismatch", 409, &persisted).await,
        Err(PaykitCommandError::ActivationTotalMismatch)
    );
}
