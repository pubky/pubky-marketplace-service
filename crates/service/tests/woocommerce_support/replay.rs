//! Replays the exchanges captured from a real WooCommerce (see
//! `tests/fixtures/woocommerce/README.md`).
//!
//! A fixture file is the list of requests the connector made to the store in
//! one scenario, with the store's answer to each. The server matches an
//! incoming request on method, path, query (without the credential) and JSON
//! body, and answers with what the real store answered. Requests that repeat
//! (a catalog read before and after an order) consume the recorded answers in
//! order; the last one is reused once they run out. It also checks how the
//! request authenticated (Basic header, query string, or nothing), so a
//! provider that sends credentials the wrong way fails here.
//!
//! Three things differ between the recording and a replay and are
//! normalised on both sides: the origin (`https://localhost:8443` becomes this
//! server), the webhook secret in a registration body (random per
//! connection), and ids and timestamps the test generates (UUIDs and RFC 3339
//! instants in request bodies and queries; a UUID in the answer is swapped for
//! the one the request carried).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value;

pub const RECORDED_ORIGIN: &str = "https://localhost:8443";
pub const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/woocommerce");

#[derive(Debug, Clone, Deserialize)]
pub struct Exchange {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub auth: String,
    pub request_body: Option<Value>,
    pub status: u16,
    #[serde(default)]
    pub response_headers: BTreeMap<String, String>,
    pub response_kind: Option<String>,
    pub response_body: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ExchangeFile {
    exchanges: Vec<Exchange>,
}

/// Large answers (WordPress's REST index is over a megabyte) are stored once
/// under `exchanges/_blobs/` and referenced as `{"$blob": "<id>"}`.
pub fn load_exchanges(name: &str) -> Vec<Exchange> {
    let path = format!("{FIXTURE_ROOT}/exchanges/{name}.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("fixture {path} is missing: {error}"));
    let mut exchanges = serde_json::from_str::<ExchangeFile>(&raw)
        .unwrap_or_else(|error| panic!("fixture {path} is not valid: {error}"))
        .exchanges;
    for exchange in &mut exchanges {
        let blob = exchange
            .response_body
            .as_ref()
            .and_then(|body| body.get("$blob"))
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(id) = blob {
            let blob_path = format!("{FIXTURE_ROOT}/exchanges/_blobs/{id}.json");
            let text = std::fs::read_to_string(&blob_path)
                .unwrap_or_else(|error| panic!("blob {blob_path} is missing: {error}"));
            exchange.response_body = Some(serde_json::from_str(&text).expect("blob is JSON"));
        }
    }
    exchanges
}

/// What the connector sent, as the replay saw it.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub auth: String,
    pub body: Option<Value>,
    pub matched: bool,
}

struct Hold {
    method: String,
    path_contains: String,
    body_contains: Option<String>,
    delay: std::time::Duration,
    used: bool,
}

struct Shared {
    holds: Vec<Hold>,
    exchanges: Vec<Exchange>,
    used: Vec<usize>,
    seen: Vec<Seen>,
    origin: String,
    /// Ids the test generated (a connection id in a webhook's delivery URL)
    /// against the ones recorded, learned from every request so far and applied
    /// to every answer, so a later list that mentions them reads as it should.
    uuid_map: BTreeMap<String, String>,
}

#[derive(Clone)]
pub struct Replay {
    pub addr: SocketAddr,
    shared: Arc<Mutex<Shared>>,
}

/// UUIDs and RFC 3339 instants (`2026-08-19T22:00:00Z`) become placeholders.
/// Returns the text and the UUIDs in order of appearance.
pub fn normalise(text: &str) -> (String, Vec<String>) {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut uuids = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(uuid) = uuid_at(bytes, i) {
            out.push_str("<uuid>");
            uuids.push(uuid);
            i += 36;
        } else if instant_at(bytes, i) {
            out.push_str("<time>");
            i += 20;
        } else {
            let ch = text[i..].chars().next().expect("char boundary");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    (out, uuids)
}

fn uuid_at(bytes: &[u8], at: usize) -> Option<String> {
    let slice = bytes.get(at..at + 36)?;
    let shape = [8, 13, 18, 23];
    let ok = slice.iter().enumerate().all(|(index, byte)| {
        if shape.contains(&index) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    });
    let boundary_before = at == 0 || !bytes[at - 1].is_ascii_alphanumeric();
    let boundary_after = bytes
        .get(at + 36)
        .is_none_or(|byte| !byte.is_ascii_alphanumeric());
    (ok && boundary_before && boundary_after)
        .then(|| String::from_utf8(slice.to_vec()).expect("ascii"))
}

fn instant_at(bytes: &[u8], at: usize) -> bool {
    let Some(slice) = bytes.get(at..at + 20) else {
        return false;
    };
    slice.iter().enumerate().all(|(index, byte)| match index {
        4 | 7 => *byte == b'-',
        10 => *byte == b'T',
        13 | 16 => *byte == b':',
        19 => *byte == b'Z',
        _ => byte.is_ascii_digit(),
    })
}

fn mask_secret(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map.iter_mut() {
                if key == "secret" && inner.is_string() {
                    *inner = Value::String("<secret>".into());
                } else {
                    mask_secret(inner);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask_secret),
        _ => {}
    }
}

fn canonical_body(body: &Option<Value>) -> (String, Vec<String>) {
    match body {
        None => (String::new(), Vec::new()),
        Some(value) => {
            let mut value = value.clone();
            mask_secret(&mut value);
            normalise(&serde_json::to_string(&value).expect("json"))
        }
    }
}

fn canonical_query(query: &[(String, String)]) -> (String, Vec<String>) {
    let mut pairs = query.to_vec();
    pairs.sort();
    normalise(
        &pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&"),
    )
}

impl Replay {
    /// Starts a server answering with the exchanges of the named fixtures,
    /// concatenated in the order given.
    pub async fn start(fixtures: &[&str]) -> Self {
        Self::start_with(fixtures, true).await
    }

    /// `rewrite_origin: false` leaves the recorded origin in the answers, for
    /// the scenario where the store reports an address other than the one
    /// that was asked.
    pub async fn start_with(fixtures: &[&str], rewrite_origin: bool) -> Self {
        let exchanges: Vec<Exchange> = fixtures
            .iter()
            .flat_map(|name| load_exchanges(name))
            .collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind replay");
        let addr = listener.local_addr().expect("replay address");
        let shared = Arc::new(Mutex::new(Shared {
            holds: Vec::new(),
            used: vec![0; exchanges.len()],
            exchanges,
            seen: Vec::new(),
            uuid_map: BTreeMap::new(),
            origin: if rewrite_origin {
                format!("http://{addr}")
            } else {
                RECORDED_ORIGIN.to_string()
            },
        }));
        let app = Router::new().fallback(handle).with_state(shared.clone());
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("replay serves");
        });
        Self { addr, shared }
    }

    /// Holds the answer to the next matching request for `delay` before it is
    /// sent, so a client with a shorter timeout gives up first. The recording
    /// holds the real store the same way (`pubky-test-delay.php`).
    pub fn hold_next(&self, method: &str, path_contains: &str, delay: std::time::Duration) {
        self.hold_next_with(method, path_contains, None, delay);
    }

    /// Like [`Self::hold_next`], for the next request whose body also contains
    /// `body_contains` (a webhook create with a given topic).
    pub fn hold_next_with(
        &self,
        method: &str,
        path_contains: &str,
        body_contains: Option<&str>,
        delay: std::time::Duration,
    ) {
        self.shared.lock().expect("replay lock").holds.push(Hold {
            method: method.to_string(),
            path_contains: path_contains.to_string(),
            body_contains: body_contains.map(str::to_string),
            delay,
            used: false,
        });
    }

    /// The store address to connect: `127.0.0.1:port`.
    pub fn site(&self) -> String {
        self.addr.to_string()
    }

    pub fn seen(&self) -> Vec<Seen> {
        self.shared.lock().expect("replay lock").seen.clone()
    }

    /// Everything the connector asked that matched a recorded exchange.
    pub fn matched(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| seen.matched)
            .collect()
    }

    pub fn unmatched(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(|seen| !seen.matched)
            .collect()
    }

    pub fn requests(&self, method: &str, path: &str) -> Vec<Seen> {
        self.matched()
            .into_iter()
            .filter(|seen| seen.method == method && seen.path == path)
            .collect()
    }

    pub fn assert_no_misses(&self) {
        let misses = self.unmatched();
        assert!(
            misses.is_empty(),
            "requests with no recorded answer: {misses:#?}"
        );
    }
}

fn auth_of(headers: &HeaderMap, query: &[(String, String)]) -> &'static str {
    if query.iter().any(|(name, _)| name == "consumer_key") {
        "query"
    } else if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("basic "))
    {
        "basic"
    } else {
        "none"
    }
}

async fn handle(
    State(shared): State<Arc<Mutex<Shared>>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (response, delay) = answer(&shared, method, uri, headers, body);
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await;
    }
    response
}

fn answer(
    shared: &Arc<Mutex<Shared>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> (Response, Option<std::time::Duration>) {
    let raw_query: Vec<(String, String)> =
        url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
    let auth = auth_of(&headers, &raw_query);
    let query: Vec<(String, String)> = raw_query
        .into_iter()
        .filter(|(name, _)| name != "consumer_key" && name != "consumer_secret")
        .collect();
    let body_json: Option<Value> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice(&body).ok()
    };
    let (query_key, query_uuids) = canonical_query(&query);
    let (body_key, body_uuids) = canonical_body(&body_json);

    let mut guard = shared.lock().expect("replay lock");
    let shared = &mut *guard;
    let candidates: Vec<usize> = shared
        .exchanges
        .iter()
        .enumerate()
        .filter(|(_, exchange)| {
            exchange.method == method.as_str()
                && exchange.path == uri.path()
                && canonical_query(&exchange.query).0 == query_key
                && canonical_body(&exchange.request_body).0 == body_key
        })
        .map(|(index, _)| index)
        .collect();
    let chosen = candidates
        .iter()
        .copied()
        .find(|index| shared.used[*index] == 0)
        .or_else(|| candidates.last().copied());
    let seen = |matched: bool| Seen {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: query.clone(),
        auth: auth.to_string(),
        body: body_json.clone(),
        matched,
    };
    let Some(index) = chosen else {
        shared.seen.push(seen(false));
        return (
            (
                StatusCode::from_u16(599).expect("status"),
                format!("replay: no recorded answer for {method} {uri} (body {body_key})"),
            )
                .into_response(),
            None,
        );
    };
    shared.used[index] += 1;
    let exchange = shared.exchanges[index].clone();
    if exchange.auth != auth {
        shared.seen.push(seen(false));
        return (
            (
                StatusCode::from_u16(599).expect("status"),
                format!(
                    "replay: {method} {} was recorded with auth {:?} but arrived with {auth:?}",
                    exchange.path, exchange.auth
                ),
            )
                .into_response(),
            None,
        );
    }
    shared.seen.push(seen(true));
    let delay = shared
        .holds
        .iter_mut()
        .find(|hold| {
            !hold.used
                && hold.method == method.as_str()
                && uri.path().contains(&hold.path_contains)
                && hold
                    .body_contains
                    .as_ref()
                    .is_none_or(|needle| String::from_utf8_lossy(&body).contains(needle.as_str()))
        })
        .map(|hold| {
            hold.used = true;
            hold.delay
        });

    let mut swaps: Vec<(String, String)> = Vec::new();
    let recorded_uuids: Vec<String> = canonical_query(&exchange.query)
        .1
        .into_iter()
        .chain(canonical_body(&exchange.request_body).1)
        .collect();
    for (recorded, actual) in recorded_uuids
        .iter()
        .zip(query_uuids.iter().chain(body_uuids.iter()))
    {
        if recorded != actual {
            swaps.push((recorded.clone(), actual.clone()));
        }
    }
    let mut payload = match (&exchange.response_kind, &exchange.response_body) {
        (Some(kind), Some(Value::String(text))) if kind == "text" => text.clone(),
        (_, Some(value)) => serde_json::to_string(value).expect("json"),
        _ => String::new(),
    };
    payload = payload.replace(
        &RECORDED_ORIGIN.replace('/', "\\/"),
        &shared.origin.replace('/', "\\/"),
    );
    payload = payload.replace(RECORDED_ORIGIN, &shared.origin);
    for (recorded, actual) in swaps {
        shared.uuid_map.insert(recorded, actual);
    }
    for (recorded, actual) in &shared.uuid_map {
        payload = payload.replace(recorded, actual);
    }
    let mut response = Response::new(axum::body::Body::from(payload));
    *response.status_mut() = StatusCode::from_u16(exchange.status).expect("recorded status");
    for (name, value) in &exchange.response_headers {
        let value = value.replace(RECORDED_ORIGIN, &shared.origin);
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::from_str(&value),
        ) {
            response.headers_mut().insert(name, value);
        }
    }
    (response, delay)
}

/// A captured webhook delivery or the wc-auth callback, as the receiver wrote
/// it: headers and the raw body, byte for byte.
#[derive(Debug, Clone, Deserialize)]
pub struct Delivery {
    pub headers: BTreeMap<String, String>,
    pub body_base64: String,
    #[serde(default)]
    pub webhook_secret: Option<String>,
}

impl Delivery {
    pub fn load(name: &str) -> Self {
        let path = format!("{FIXTURE_ROOT}/deliveries/{name}.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("fixture {path} is missing: {error}"));
        serde_json::from_str(&raw)
            .unwrap_or_else(|error| panic!("fixture {path} is not valid: {error}"))
    }

    pub fn body(&self) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.body_base64)
            .expect("fixture body is base64")
    }
}
