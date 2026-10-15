//! A local stand-in for Shopify's Admin GraphQL API and OAuth token endpoint,
//! for the Shopify provider tests. Lives in the test crate only.
//!
//! There is no Shopify store to run against, so this server is deliberately
//! strict about what the provider sends:
//!
//! * every GraphQL document is validated against Shopify's published 2026-10
//!   schema (`fixtures/shopify/schema-2026-10.pruned.graphql`, see
//!   `scripts/shopify-schema-prune.py`);
//! * every request's variables are coerced against that operation's variable
//!   definitions (enum values, required input fields, types);
//! * every response is checked to carry each field its document selects.
//!
//! Anything that fails a check is recorded in [`Inner::problems`], and tests
//! call [`FakeShopify::assert_clean`].
//!
//! The static responses under `fixtures/shopify/responses/*.json` are
//! SPEC-DERIVED: written from the schema, not captured from a store. They are
//! listed in `fixtures/shopify/README.md` for replacement with real captures
//! after the first dogfood run against a development store. The stateful
//! behaviour (orders, tags, search-index lag, idempotency keys, throttling) is
//! this file's reading of Shopify's documentation, not a capture either.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use tokio::sync::Notify;

use apollo_compiler::executable::{Selection, SelectionSet};
use apollo_compiler::response::JsonMap;
use apollo_compiler::validation::Valid;
use apollo_compiler::{ExecutableDocument, Schema};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};

pub const ACCESS_TOKEN: &str = "shpat_fake_offline_access_token_0123456789";
pub const GOOD_CODE: &str = "good-code";

const SCHEMA: &str = include_str!("../fixtures/shopify/schema-2026-10.pruned.graphql");

#[derive(Debug, Clone)]
pub struct Recorded {
    pub operation: String,
    pub variables: Value,
    pub document: String,
}

#[derive(Debug, Clone)]
pub struct FakeOrder {
    pub id: String,
    pub input: Value,
    pub options: Value,
    pub cancelled: bool,
    pub cancel_variables: Option<Value>,
    pub refunds: Vec<Value>,
    pub created_at: DateTime<Utc>,
}

impl FakeOrder {
    /// The sale amount in minor units (two decimals, as every fake order is).
    pub fn sale_minor(&self) -> i64 {
        to_minor(
            self.input["transactions"][0]["amountSet"]["shopMoney"]["amount"]
                .as_str()
                .unwrap_or("0"),
        )
    }

    pub fn refunded_minor(&self) -> i64 {
        self.refunds
            .iter()
            .map(|input| to_minor(input["transactions"][0]["amount"].as_str().unwrap_or("0")))
            .sum()
    }

    pub fn tags(&self) -> Vec<String> {
        self.input["tags"]
            .as_array()
            .map(|tags| {
                tags.iter()
                    .filter_map(|tag| tag.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn to_minor(amount: &str) -> i64 {
    let (whole, fraction) = amount.split_once('.').unwrap_or((amount, ""));
    let fraction = format!("{fraction:0<2}");
    whole.parse::<i64>().unwrap_or(0) * 100 + fraction[..2].parse::<i64>().unwrap_or(0)
}

fn decimal(minor: i64) -> String {
    format!("{}.{:02}", minor / 100, minor % 100)
}

#[derive(Debug, Clone)]
pub enum Fault {
    /// Hold the request open until the test releases it: `entered` is
    /// notified when the request has arrived, and the operation runs only
    /// after `release` is notified.
    Hold {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    },
    /// Answer with this HTTP status and an empty body, without running the
    /// operation.
    Status(u16),
    /// Answer 429 with `Retry-After: 0`.
    TooManyRequests,
    /// GraphQL `THROTTLED`, without running the operation.
    Throttled,
    /// GraphQL `ACCESS_DENIED`, without running the operation.
    AccessDenied,
    /// Run the operation, then answer 500: the outcome the caller sees is
    /// unknown but the store applied it.
    AppliedThenServerError,
}

pub struct Inner {
    schema: Valid<Schema>,
    pub requests: Vec<Recorded>,
    pub problems: Vec<String>,
    pub token_requests: Vec<Value>,
    faults: HashMap<String, Vec<Fault>>,
    pub shop_currency: String,
    pub granted_scopes: Vec<String>,
    pub client_secret: Option<String>,
    pub orders: Vec<FakeOrder>,
    /// Shopify's order search lags a create; while true the tag search finds
    /// nothing.
    pub search_lags: bool,
    pub webhooks: Vec<(String, String, String)>,
    pub app_uninstalled: bool,
    refund_keys: HashMap<String, String>,
    /// How many webhook subscriptions one page returns (Shopify's maximum is
    /// 250; tests lower it to exercise paging).
    pub subscription_page_size: usize,
    /// `orderCancel` answers `userErrors` with this code; when the flag is
    /// true the cancel nevertheless takes effect (a retry racing the job
    /// that is already cancelling the order).
    pub cancel_refusal: Option<(String, bool)>,
    /// A store that forces tax onto every order whatever the order says
    /// (basis points of the order total).
    pub forced_tax_bps: i64,
    next_id: u64,
}

#[derive(Clone)]
pub struct FakeShopify {
    pub base_url: String,
    inner: Arc<Mutex<Inner>>,
}

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/shopify/responses")
        .join(format!("{name}.json"));
    let mut value: Value = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("fixture {name}: {error}")),
    )
    .unwrap_or_else(|error| panic!("fixture {name} is not JSON: {error}"));
    value
        .as_object_mut()
        .expect("fixture is an object")
        .remove("_provenance");
    value
}

impl FakeShopify {
    pub async fn start() -> Self {
        let schema = Schema::parse_and_validate(SCHEMA, "schema-2026-10.pruned.graphql")
            .unwrap_or_else(|errors| panic!("the pruned schema is invalid:\n{errors}"));
        let inner = Arc::new(Mutex::new(Inner {
            schema,
            requests: Vec::new(),
            problems: Vec::new(),
            token_requests: Vec::new(),
            faults: HashMap::new(),
            shop_currency: "USD".into(),
            granted_scopes: [
                "read_products",
                "read_inventory",
                "read_locations",
                "read_fulfillments",
                "write_orders",
            ]
            .iter()
            .map(|scope| scope.to_string())
            .collect(),
            client_secret: None,
            orders: Vec::new(),
            search_lags: false,
            webhooks: Vec::new(),
            app_uninstalled: false,
            refund_keys: HashMap::new(),
            subscription_page_size: 100,
            cancel_refusal: None,
            forced_tax_bps: 0,
            next_id: 7000,
        }));
        let router = Router::new()
            .route("/admin/oauth/access_token", post(token))
            .route("/admin/api/2026-10/graphql.json", post(graphql))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the fake Shopify");
        let base_url = format!("http://{}", listener.local_addr().expect("address"));
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("fake Shopify serves");
        });
        Self { base_url, inner }
    }

    pub fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("fake Shopify state")
    }

    pub fn expect_client_secret(&self, secret: &str) {
        self.lock().client_secret = Some(secret.to_string());
    }

    /// The next requests for `operation` fail this way, in order.
    pub fn fault(&self, operation: &str, faults: Vec<Fault>) {
        self.lock().faults.insert(operation.to_string(), faults);
    }

    pub fn calls(&self, operation: &str) -> Vec<Recorded> {
        self.lock()
            .requests
            .iter()
            .filter(|recorded| recorded.operation == operation)
            .cloned()
            .collect()
    }

    pub fn orders(&self) -> Vec<FakeOrder> {
        self.lock().orders.clone()
    }

    /// Makes every order the store holds `hours` older.
    pub fn age_orders(&self, hours: i64) {
        for order in &mut self.lock().orders {
            order.created_at -= ChronoDuration::hours(hours);
        }
    }

    /// Adds `count` other orders (not Pubky's) created now, newer than any
    /// order already there.
    pub fn add_other_orders(&self, count: usize) {
        let mut inner = self.lock();
        for _ in 0..count {
            inner.next_id += 1;
            let id = gid("Order", inner.next_id);
            inner.orders.push(FakeOrder {
                id,
                input: json!({ "tags": ["web"], "transactions": [{ "amountSet": { "shopMoney": { "amount": "1.00" } } }] }),
                options: Value::Null,
                cancelled: false,
                cancel_variables: None,
                refunds: Vec::new(),
                created_at: Utc::now(),
            });
        }
    }

    /// A refund the merchant makes in the store's admin: it reaches the order
    /// like any refund, with no Pubky mark.
    pub fn merchant_refund(&self, order_id: &str, minor: i64) {
        let mut inner = self.lock();
        let order = inner
            .orders
            .iter_mut()
            .find(|order| order.id == order_id)
            .expect("order");
        order.refunds.push(json!({
            "orderId": order_id,
            "note": "Refunded in the admin",
            "transactions": [{ "amount": decimal(minor), "kind": "REFUND" }],
        }));
    }

    /// No request failed schema validation, variable coercion or the response
    /// shape check.
    pub fn assert_clean(&self) {
        let problems = self.lock().problems.clone();
        assert!(problems.is_empty(), "the provider sent: {problems:#?}");
    }
}

async fn token(State(inner): State<Arc<Mutex<Inner>>>, body: Bytes) -> Response {
    let mut inner = inner.lock().expect("fake Shopify state");
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    inner.token_requests.push(request.clone());
    let secret_ok = inner
        .client_secret
        .as_deref()
        .is_none_or(|expected| request["client_secret"] == expected);
    if request["code"] == GOOD_CODE && secret_ok && request["client_id"].is_string() {
        let scope = inner.granted_scopes.join(",");
        axum::Json(json!({ "access_token": ACCESS_TOKEN, "scope": scope })).into_response()
    } else {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": "invalid_request" })),
        )
            .into_response()
    }
}

fn operation_name(document: &ExecutableDocument) -> Option<String> {
    document
        .operations
        .anonymous
        .as_ref()
        .map(|_| "(anonymous)".to_string())
        .or_else(|| {
            document
                .operations
                .named
                .keys()
                .next()
                .map(|name| name.to_string())
        })
}

fn json_map(value: &Value) -> JsonMap {
    serde_json::from_value(value.clone()).unwrap_or_default()
}

/// Every field `selection` asks for is present in `value` (null is fine for a
/// nullable field, and an absent object is checked no further).
fn check_shape(selection: &SelectionSet, value: &Value, path: &str, out: &mut Vec<String>) {
    let Some(object) = value.as_object() else {
        return;
    };
    for item in &selection.selections {
        match item {
            Selection::Field(field) => {
                let key = field.response_key().as_str();
                if key == "__typename" {
                    continue;
                }
                let Some(child) = object.get(key) else {
                    out.push(format!("response lacks {path}.{key}"));
                    continue;
                };
                if field.selection_set.selections.is_empty() {
                    continue;
                }
                let next = format!("{path}.{key}");
                match child {
                    Value::Array(items) => {
                        for element in items {
                            check_shape(&field.selection_set, element, &next, out);
                        }
                    }
                    other => check_shape(&field.selection_set, other, &next, out),
                }
            }
            Selection::InlineFragment(fragment) => {
                check_shape(&fragment.selection_set, value, path, out);
            }
            Selection::FragmentSpread(_) => {}
        }
    }
}

fn throttle_status() -> Value {
    json!({ "maximumAvailable": 2000.0, "currentlyAvailable": 1900, "restoreRate": 100.0 })
}

async fn graphql(
    State(state): State<Arc<Mutex<Inner>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match process(&state, &headers, &body, false) {
        Ok(response) => response,
        Err((entered, release)) => {
            entered.notify_one();
            release.notified().await;
            process(&state, &headers, &body, true)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
    }
}

type Held = (Arc<Notify>, Arc<Notify>);

/// One request, start to finish. A request held by [`Fault::Hold`] returns the
/// gate to wait on and is processed again, with `resumed`, once it opens: the
/// state lock is never held across the wait.
fn process(
    state: &Mutex<Inner>,
    headers: &HeaderMap,
    body: &Bytes,
    resumed: bool,
) -> Result<Response, Held> {
    let mut inner = state.lock().expect("fake Shopify state");
    if headers
        .get("x-shopify-access-token")
        .and_then(|value| value.to_str().ok())
        != Some(ACCESS_TOKEN)
    {
        return Ok(StatusCode::UNAUTHORIZED.into_response());
    }
    let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let text = request["query"].as_str().unwrap_or_default().to_string();
    let variables = request
        .get("variables")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let document = match ExecutableDocument::parse_and_validate(
        &inner.schema,
        text.clone(),
        "request.graphql",
    ) {
        Ok(document) => document,
        Err(errors) => {
            inner.problems.push(format!("invalid document:\n{errors}"));
            return Ok(axum::Json(json!({
                "errors": [{ "message": "invalid document", "extensions": { "code": "undefinedField" } }]
            }))
            .into_response());
        }
    };
    let name = operation_name(&document).unwrap_or_default();
    let operation = document
        .operations
        .iter()
        .next()
        .expect("a validated document has an operation");
    let mut apply_then_fail = false;
    if !resumed {
        if let Err(error) = apollo_compiler::request::coerce_variable_values(
            &inner.schema,
            operation,
            &json_map(&variables),
        ) {
            inner
                .problems
                .push(format!("{name}: variables do not coerce: {error:?}"));
        }
        inner.requests.push(Recorded {
            operation: name.clone(),
            variables: variables.clone(),
            document: text.clone(),
        });
        let fault = inner
            .faults
            .get_mut(&name)
            .and_then(|queue| (!queue.is_empty()).then(|| queue.remove(0)));
        if let Some(fault) = fault {
            match fault {
                Fault::Hold { entered, release } => return Err((entered, release)),
                Fault::Status(code) => {
                    return Ok(StatusCode::from_u16(code).expect("status").into_response());
                }
                Fault::TooManyRequests => {
                    return Ok(
                        (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "0")], "").into_response()
                    );
                }
                Fault::Throttled => {
                    return Ok(axum::Json(json!({
                        "errors": [{ "message": "Throttled", "extensions": { "code": "THROTTLED" } }],
                        "extensions": { "cost": {
                            "requestedQueryCost": 600,
                            "actualQueryCost": null,
                            "throttleStatus": { "maximumAvailable": 2000.0, "currentlyAvailable": 599, "restoreRate": 1000.0 }
                        } }
                    }))
                    .into_response());
                }
                Fault::AccessDenied => {
                    return Ok(axum::Json(json!({
                        "errors": [{ "message": "Access denied for orderCreate field.", "extensions": { "code": "ACCESS_DENIED" } }]
                    }))
                    .into_response());
                }
                Fault::AppliedThenServerError => apply_then_fail = true,
            }
        }
    }

    let data = run(&mut inner, &name, &text, &variables);
    let mut response = match data {
        Ok(response) => response,
        Err(message) => {
            inner.problems.push(format!("{name}: {message}"));
            json!({ "errors": [{ "message": message }] })
        }
    };
    if response.get("data").is_some() {
        let mut shape = Vec::new();
        check_shape(
            &operation.selection_set,
            &response["data"],
            "data",
            &mut shape,
        );
        for problem in shape {
            inner.problems.push(format!("{name}: {problem}"));
        }
    }
    if apply_then_fail {
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }
    response["extensions"] = json!({ "cost": {
        "requestedQueryCost": 10, "actualQueryCost": 10, "throttleStatus": throttle_status()
    }});
    Ok(axum::Json(response).into_response())
}

fn gid(kind: &str, number: u64) -> String {
    format!("gid://shopify/{kind}/{number}")
}

fn run(inner: &mut Inner, name: &str, document: &str, variables: &Value) -> Result<Value, String> {
    match name {
        "ShopOverview" => {
            let mut response = fixture("shop_overview");
            response["data"]["shop"]["currencyCode"] = json!(inner.shop_currency);
            response["data"]["currentAppInstallation"]["accessScopes"] = Value::Array(
                inner
                    .granted_scopes
                    .iter()
                    .map(|scope| json!({ "handle": scope }))
                    .collect(),
            );
            Ok(json!({ "data": response["data"] }))
        }
        "ShopCurrency" => {
            Ok(json!({ "data": { "shop": { "currencyCode": inner.shop_currency } } }))
        }
        "ProductsPage" => {
            let page = if variables["after"].is_null() {
                "products_page_1"
            } else {
                "products_page_2"
            };
            let mut response = fixture(page);
            response["data"]["shop"]["currencyCode"] = json!(inner.shop_currency);
            Ok(json!({ "data": response["data"] }))
        }
        "ProductVariantsPage" => Ok(json!({ "data": fixture("product_variants_page")["data"] })),
        "InventoryItemVariant" => {
            if variables["id"] == "gid://shopify/InventoryItem/4242" {
                Ok(json!({ "data": fixture("inventory_item")["data"] }))
            } else {
                Ok(json!({ "data": { "inventoryItem": null } }))
            }
        }
        "OrderCreate" => {
            inner.next_id += 1;
            let id = gid("Order", inner.next_id);
            inner.orders.push(FakeOrder {
                id: id.clone(),
                input: variables["order"].clone(),
                options: variables["options"].clone(),
                cancelled: false,
                cancel_variables: None,
                refunds: Vec::new(),
                created_at: Utc::now(),
            });
            Ok(json!({ "data": { "orderCreate": { "order": { "id": id }, "userErrors": [] } } }))
        }
        "OrdersByTag" => {
            let query = variables["query"].as_str().unwrap_or_default();
            let tag = query.strip_prefix("tag:").unwrap_or_default();
            let nodes: Vec<Value> = if inner.search_lags {
                Vec::new()
            } else {
                inner
                    .orders
                    .iter()
                    .filter(|order| order.tags().iter().any(|candidate| candidate == tag))
                    .map(order_node)
                    .collect()
            };
            Ok(json!({ "data": { "orders": { "nodes": nodes } } }))
        }
        "RecentOrders" => {
            let first = variables["first"].as_u64().unwrap_or(10) as usize;
            let start = variables["after"]
                .as_str()
                .and_then(|cursor| cursor.strip_prefix("recent-"))
                .and_then(|index| index.parse::<usize>().ok())
                .unwrap_or(0);
            let newest_first: Vec<&FakeOrder> = inner.orders.iter().rev().collect();
            let page: Vec<Value> = newest_first
                .iter()
                .skip(start)
                .take(first)
                .map(|order| order_node(order))
                .collect();
            let end = start + page.len();
            Ok(json!({ "data": { "orders": {
                "pageInfo": {
                    "hasNextPage": end < newest_first.len(),
                    "endCursor": format!("recent-{end}"),
                },
                "nodes": page,
            } } }))
        }
        "OrderTotals" => {
            let id = variables["id"].as_str().unwrap_or_default();
            let bps = inner.forced_tax_bps;
            let Some(order) = inner.orders.iter().find(|order| order.id == id) else {
                return Ok(json!({ "data": { "order": null } }));
            };
            let lines: i64 = order.input["lineItems"]
                .as_array()
                .map(|lines| {
                    lines
                        .iter()
                        .map(|line| {
                            to_minor(
                                line["priceSet"]["shopMoney"]["amount"]
                                    .as_str()
                                    .unwrap_or("0"),
                            ) * line["quantity"].as_i64().unwrap_or(1)
                        })
                        .sum()
                })
                .unwrap_or(0);
            let shipping: i64 = order.input["shippingLines"]
                .as_array()
                .map(|lines| {
                    lines
                        .iter()
                        .map(|line| {
                            to_minor(
                                line["priceSet"]["shopMoney"]["amount"]
                                    .as_str()
                                    .unwrap_or("0"),
                            )
                        })
                        .sum()
                })
                .unwrap_or(0);
            let tax = (lines + shipping) * bps / 10_000;
            let currency = order.input["currency"].clone();
            Ok(json!({ "data": { "order": {
                "id": order.id,
                "currencyCode": currency,
                "totalPriceSet": { "shopMoney": { "amount": decimal(lines + shipping + tax), "currencyCode": currency } },
                "totalTaxSet": { "shopMoney": { "amount": decimal(tax), "currencyCode": currency } },
            } } }))
        }
        "OrderTransactions" => {
            let id = variables["id"].as_str().unwrap_or_default();
            let Some(order) = inner.orders.iter().find(|order| order.id == id) else {
                return Ok(json!({ "data": { "order": null } }));
            };
            let sale = &order.input["transactions"][0];
            Ok(json!({ "data": { "order": {
                "id": order.id,
                "currencyCode": order.input["currency"],
                "cancelledAt": if order.cancelled { json!("2026-10-15T10:00:00Z") } else { Value::Null },
                "totalRefundedSet": { "shopMoney": {
                    "amount": decimal(order.refunded_minor()),
                    "currencyCode": order.input["currency"],
                } },
                "transactions": [{
                    "id": format!("{}-sale", order.id.replace("Order", "OrderTransaction")),
                    "kind": sale["kind"],
                    "status": sale["status"],
                    "gateway": sale["gateway"],
                    "amountSet": { "shopMoney": sale["amountSet"]["shopMoney"].clone() },
                }]
            } } }))
        }
        "OrderCancel" => {
            let refusal = inner.cancel_refusal.clone();
            let id = variables["orderId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let Some(order) = inner.orders.iter_mut().find(|order| order.id == id) else {
                return Ok(json!({ "data": { "orderCancel": {
                    "job": null,
                    "orderCancelUserErrors": [{ "field": ["orderId"], "message": "Order does not exist", "code": "INVALID" }]
                } } }));
            };
            if let Some((code, applies)) = refusal {
                order.cancelled = applies;
                return Ok(json!({ "data": { "orderCancel": {
                    "job": null,
                    "orderCancelUserErrors": [{ "field": ["orderId"], "message": "The order cannot be cancelled right now", "code": code }]
                } } }));
            }
            order.cancelled = true;
            order.cancel_variables = Some(variables.clone());
            Ok(json!({ "data": { "orderCancel": {
                "job": { "id": "gid://shopify/Job/1", "done": false },
                "orderCancelUserErrors": []
            } } }))
        }
        "RefundCreate" => {
            let key = idempotency_key(document).ok_or("refundCreate sent no @idempotent key")?;
            let input = &variables["input"];
            let id = input["orderId"].as_str().unwrap_or_default().to_string();
            if let Some(refund) = inner.refund_keys.get(&key) {
                return Ok(
                    json!({ "data": { "refundCreate": { "refund": { "id": refund }, "userErrors": [] } } }),
                );
            }
            inner.next_id += 1;
            let refund_id = gid("Refund", inner.next_id);
            let Some(order) = inner.orders.iter_mut().find(|order| order.id == id) else {
                return Ok(json!({ "data": { "refundCreate": {
                    "refund": null,
                    "userErrors": [{ "field": ["orderId"], "message": "Order does not exist" }]
                } } }));
            };
            let amount = to_minor(input["transactions"][0]["amount"].as_str().unwrap_or("0"));
            if order.refunded_minor() + amount > order.sale_minor() {
                return Ok(json!({ "data": { "refundCreate": {
                    "refund": null,
                    "userErrors": [{ "field": ["transactions"], "message": "Refund amount is greater than the refundable amount" }]
                } } }));
            }
            order.refunds.push(input.clone());
            inner.refund_keys.insert(key, refund_id.clone());
            Ok(
                json!({ "data": { "refundCreate": { "refund": { "id": refund_id }, "userErrors": [] } } }),
            )
        }
        "WebhookSubscriptions" => {
            let start = variables["after"]
                .as_str()
                .and_then(|cursor| cursor.strip_prefix("subs-"))
                .and_then(|index| index.parse::<usize>().ok())
                .unwrap_or(0);
            let size = inner.subscription_page_size;
            let nodes: Vec<Value> = inner
                .webhooks
                .iter()
                .skip(start)
                .take(size)
                .map(|(id, topic, uri)| json!({ "id": id, "topic": topic, "uri": uri }))
                .collect();
            let end = start + nodes.len();
            Ok(json!({ "data": { "webhookSubscriptions": {
                "pageInfo": {
                    "hasNextPage": end < inner.webhooks.len(),
                    "endCursor": format!("subs-{end}"),
                },
                "nodes": nodes,
            } } }))
        }
        "WebhookSubscriptionCreate" => {
            let topic = variables["topic"].as_str().unwrap_or_default().to_string();
            let uri = variables["webhookSubscription"]["uri"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if inner
                .webhooks
                .iter()
                .any(|(_, t, u)| *t == topic && *u == uri)
            {
                return Ok(json!({ "data": { "webhookSubscriptionCreate": {
                    "webhookSubscription": null,
                    "userErrors": [{ "field": ["webhookSubscription", "callbackUrl"], "message": "Address for this topic has already been taken" }]
                } } }));
            }
            inner.next_id += 1;
            let id = gid("WebhookSubscription", inner.next_id);
            inner.webhooks.push((id.clone(), topic, uri));
            Ok(json!({ "data": { "webhookSubscriptionCreate": {
                "webhookSubscription": { "id": id }, "userErrors": []
            } } }))
        }
        "WebhookSubscriptionDelete" => {
            let id = variables["id"].as_str().unwrap_or_default().to_string();
            inner.webhooks.retain(|(existing, _, _)| *existing != id);
            Ok(json!({ "data": { "webhookSubscriptionDelete": {
                "deletedWebhookSubscriptionId": id, "userErrors": []
            } } }))
        }
        "AppUninstall" => {
            inner.app_uninstalled = true;
            Ok(json!({ "data": { "appUninstall": {
                "app": { "id": "gid://shopify/App/1" }, "userErrors": []
            } } }))
        }
        other => Err(format!("the fake has no response for {other}")),
    }
}

fn order_node(order: &FakeOrder) -> Value {
    json!({
        "id": order.id,
        "tags": order.tags(),
        "cancelledAt": if order.cancelled { json!("2026-10-15T10:00:00Z") } else { Value::Null },
        "createdAt": order.created_at.to_rfc3339(),
    })
}

fn idempotency_key(document: &str) -> Option<String> {
    let rest = document.split("@idempotent(key: \"").nth(1)?;
    Some(rest.split('"').next()?.to_string())
}
