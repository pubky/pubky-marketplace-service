//! Delivered orders for the review tests: listings registered, checked out
//! in one order, paid, shipped and confirmed delivered (revision 4).

use axum::http::StatusCode;
use serde_json::{json, Value};

use super::{execute, indexed_command_id, order_command, payment_command, TestActor, TestApp};

pub const LISTINGS: [&str; 3] = ["boots_01", "laces_02", "socks_03"];

/// Registers one unit of each listing, checks all of them out in one order,
/// pays, ships and confirms delivery. Returns the order id.
pub async fn delivered_order(
    app: &TestApp,
    seller: &TestActor,
    buyer: &TestActor,
    listing_ids: &[&str],
) -> String {
    for (index, listing_id) in listing_ids.iter().enumerate() {
        let (status, body) = execute(
            app,
            &seller.token,
            &json!({
                "version": 1,
                "command_id": indexed_command_id(0x7100, index as u64),
                "aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
                "expected_revision": 0,
                "issued_at": "2026-08-19T22:00:00.000Z",
                "kind": "listing.register",
                "payload": {
                    "seller_pubky": seller.pubky,
                    "listing_id": listing_id,
                    "listing_revision": 1,
                    "content_hash": "a".repeat(64),
                    "quantity": 1,
                    "unit_price": { "amount_minor": 12_500, "currency": "USD", "exponent": 2 },
                    "shipping_minor": 1_200,
                },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "register {listing_id}: {body}");
    }
    let checkout_id = indexed_command_id(0x7200, 1);
    let lines: Vec<Value> = listing_ids
        .iter()
        .map(|listing_id| {
            json!({
                "listing_aggregate_id": format!("listing:{}_{listing_id}", seller.pubky),
                "expected_revision": 1,
                "quantity": 1,
            })
        })
        .collect();
    let (status, body) = execute(
        app,
        &buyer.token,
        &json!({
            "version": 1,
            "command_id": checkout_id,
            "aggregate_id": format!("checkout:{checkout_id}"),
            "expected_revision": 0,
            "issued_at": "2026-08-19T22:00:00.000Z",
            "kind": "checkout.create",
            "payload": {
                "lines": lines,
                "delivery_address": {
                    "name": "Alice Buyer",
                    "line1": "1 Market Street",
                    "line2": "",
                    "city": "New York",
                    "region": "NY",
                    "postal_code": "10001",
                    "country_code": "US",
                },
                "guarantee_policy_version": 1,
            },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "checkout: {body}");
    let order_id = body["result"]["orders"][0]["id"]
        .as_str()
        .expect("order id")
        .to_string();
    let payment_id = body["result"]["payments"][0]["id"]
        .as_str()
        .expect("payment id")
        .to_string();
    let (status, body) = execute(
        app,
        &buyer.token,
        &payment_command(&payment_id, 1, "confirmed", 1, 7_301),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "payment: {body}");
    let (status, body) = execute(
        app,
        &seller.token,
        &order_command(
            "fulfillment.ship",
            &order_id,
            2,
            json!({ "carrier": "Sandbox Post", "tracking_number": "TRACK-BLIND" }),
            7_302,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ship: {body}");
    let (status, body) = execute(
        app,
        &buyer.token,
        &order_command(
            "fulfillment.confirm_delivery",
            &order_id,
            3,
            json!({}),
            7_303,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delivery: {body}");
    order_id
}
