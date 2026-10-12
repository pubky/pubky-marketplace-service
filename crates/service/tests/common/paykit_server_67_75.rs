//! HTTP exchanges captured from `pubky/paykit-server` #75 at
//! `80352cebb011d2f57318170068ae4ec24af6a417`, which contains #67 head
//! `ca0a88e8f3f34874ec0ee0b41dc672b238043397`. #67 was squash-merged as
//! `c7fe8efd`; see the capture README for the merged-producer provenance check.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{Response, StatusCode},
    Router,
};
use serde_json::Value;

pub const SERVER_REVISION: &str = "80352cebb011d2f57318170068ae4ec24af6a417";
pub const SERVER_TREE: &str = "20e584cb520acea32280cc71f9b49d661bbd1519";
pub const PR67_REVIEWED_HEAD: &str = "ca0a88e8f3f34874ec0ee0b41dc672b238043397";
pub const PR75_HEAD: &str = "80352cebb011d2f57318170068ae4ec24af6a417";

pub const FIXTURE_NAMES: [&str; 7] = [
    "prepare_new",
    "prepare_replay",
    "activate",
    "status_active",
    "resolve_paid_manually",
    "prepare_for_void",
    "void",
];

#[derive(Clone, Debug)]
pub struct Fixture {
    pub name: String,
    pub server_revision: String,
    pub method: String,
    pub path: String,
    pub content_type: Option<String>,
    pub signature: String,
    pub request_body: String,
    pub status: u16,
    pub response_content_type: Option<String>,
    pub response_body: String,
}

fn directory() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/paykit-server-67-75")
}

pub fn load(name: &str) -> Fixture {
    let path = directory().join(format!("{name}.json"));
    let document: Value = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|error| panic!("fixture {path:?}: {error}")),
    )
    .expect("fixture is JSON");
    let text = |value: &Value| value.as_str().expect("fixture string").to_string();
    Fixture {
        name: text(&document["name"]),
        server_revision: text(&document["server_revision"]),
        method: text(&document["request"]["method"]),
        path: text(&document["request"]["path"]),
        content_type: document["request"]["content_type"]
            .as_str()
            .map(str::to_string),
        signature: text(&document["request"]["signature"]),
        request_body: text(&document["request"]["body"]),
        status: u16::try_from(document["response"]["status"].as_u64().expect("status"))
            .expect("status fits"),
        response_content_type: document["response"]["content_type"]
            .as_str()
            .map(str::to_string),
        response_body: text(&document["response"]["body"]),
    }
}

pub fn load_all() -> Vec<Fixture> {
    FIXTURE_NAMES.iter().map(|name| load(name)).collect()
}

#[derive(Clone)]
struct ReplayState(Arc<Mutex<VecDeque<Fixture>>>);

pub struct ReplayServer {
    pub base_url: String,
    remaining: Arc<Mutex<VecDeque<Fixture>>>,
}

impl ReplayServer {
    pub fn assert_drained(&self) {
        let remaining = self.remaining.lock().expect("replay fixture lock");
        assert!(
            remaining.is_empty(),
            "unconsumed captured exchanges: {remaining:?}"
        );
    }
}

async fn replay(State(state): State<ReplayState>, request: Request) -> Response<Body> {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let signature = request
        .headers()
        .get("x-paykit-signature")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("request body reads");
    let fixture = state
        .0
        .lock()
        .expect("replay fixture lock")
        .pop_front()
        .expect("captured exchange remains");

    assert_eq!(method, fixture.method, "{} method", fixture.name);
    assert_eq!(path, fixture.path, "{} path", fixture.name);
    assert_eq!(
        signature.as_deref(),
        Some(fixture.signature.as_str()),
        "{} signature",
        fixture.name
    );
    assert_eq!(
        content_type, fixture.content_type,
        "{} content-type",
        fixture.name
    );
    assert_eq!(
        body.as_ref(),
        fixture.request_body.as_bytes(),
        "{} request bytes",
        fixture.name
    );

    let mut response = Response::builder()
        .status(StatusCode::from_u16(fixture.status).expect("captured status is valid"));
    if let Some(content_type) = fixture.response_content_type {
        response = response.header("content-type", content_type);
    }
    response
        .body(Body::from(fixture.response_body))
        .expect("captured response builds")
}

pub async fn spawn_replay(fixtures: Vec<Fixture>) -> ReplayServer {
    let remaining = Arc::new(Mutex::new(VecDeque::from(fixtures)));
    let router = Router::new()
        .fallback(replay)
        .with_state(ReplayState(remaining.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capture replay listener binds");
    let address = listener.local_addr().expect("capture replay address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("capture replay server runs");
    });
    ReplayServer {
        base_url: format!("http://{address}"),
        remaining,
    }
}
