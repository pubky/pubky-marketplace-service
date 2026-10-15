//! A local double for the bounded sole FX source (Blocktank's `BTC<CCY>`
//! tickers): serves one scripted body, counts fetches, and lets a test force
//! transport failures — the feed-side counterpart of [`super::FakePaykit`].
//! The marketplace's own FX wrapper is never mocked; the double speaks the
//! pinned wire contract from `tests/fixtures/blocktank_fx_rates_btc.json`.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::Router;

#[derive(Clone)]
struct FakeFxState {
    inner: Arc<Mutex<FakeFxInner>>,
}

struct FakeFxInner {
    status: u16,
    body: String,
    fetches: usize,
    gate: Option<FetchGate>,
}

/// One held fetch: the response body is taken when the request arrives, the
/// response is withheld until the test releases the gate.
#[derive(Clone)]
struct FetchGate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

/// The test side of a held fetch.
pub struct HeldFetch {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl HeldFetch {
    /// Resolves once the held request has reached the feed (and taken its
    /// body); the request is still unanswered.
    pub async fn request_arrived(&self) {
        self.entered.notified().await;
    }

    /// Lets the held request return the body it took on arrival.
    pub fn release(&self) {
        self.release.notify_one();
    }
}

/// A local FX feed double. `fetches` is the fetch counter the idempotency
/// tests assert against.
pub struct FakeFxFeed {
    state: FakeFxState,
    pub base_url: String,
}

impl FakeFxFeed {
    /// Serves the given body at 200 for every later fetch.
    pub fn set_body(&self, body: String) {
        let mut guard = self.state.inner.lock().expect("fake fx lock");
        guard.status = 200;
        guard.body = body;
    }

    /// Forces a transport-level failure (a 500 the client maps to its
    /// malformed/unavailable class) for every later fetch.
    pub fn set_status(&self, status: u16) {
        self.state.inner.lock().expect("fake fx lock").status = status;
    }

    /// Holds the next fetch only: its body is whatever is set when it
    /// arrives, and it answers after [`HeldFetch::release`]. Later fetches
    /// answer immediately with the then-current body.
    pub fn hold_next_fetch(&self) -> HeldFetch {
        let gate = FetchGate {
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        self.state.inner.lock().expect("fake fx lock").gate = Some(gate.clone());
        HeldFetch {
            entered: gate.entered,
            release: gate.release,
        }
    }

    pub fn fetch_count(&self) -> usize {
        self.state.inner.lock().expect("fake fx lock").fetches
    }
}

/// The pinned Blocktank wire shape with one BTCUSD ticker.
pub fn fx_body(price: &str, last_updated_millis: i64) -> String {
    format!(
        r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"{price}","base":"BTC","quote":"USD","lastUpdatedAt":{last_updated_millis}}}]}}"#
    )
}

/// The pinned wire shape with one ticker per `(currency, price)`, all
/// updated at `last_updated_millis`.
pub fn fx_body_multi(prices: &[(&str, &str)], last_updated_millis: i64) -> String {
    let tickers = prices
        .iter()
        .map(|(currency, price)| {
            format!(
                r#"{{"symbol":"BTC{currency}","lastPrice":"{price}","base":"BTC","quote":"{currency}","lastUpdatedAt":{last_updated_millis}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(r#"{{"tickers":[{tickers}]}}"#)
}

async fn serve_fx_rates(State(state): State<FakeFxState>) -> (StatusCode, String) {
    let (response, gate) = {
        let mut guard = state.inner.lock().expect("fake fx lock");
        guard.fetches += 1;
        (
            (
                StatusCode::from_u16(guard.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                guard.body.clone(),
            ),
            guard.gate.take(),
        )
    };
    if let Some(gate) = gate {
        gate.entered.notify_one();
        gate.release.notified().await;
    }
    response
}

pub async fn spawn_fake_fx() -> FakeFxFeed {
    let state = FakeFxState {
        inner: Arc::new(Mutex::new(FakeFxInner {
            status: 200,
            body: String::new(),
            fetches: 0,
            gate: None,
        })),
    };
    let router = Router::new()
        .route("/api/fx/rates/btc", axum::routing::get(serve_fx_rates))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake fx binds");
    let base_url = format!("http://{}/api/fx/rates/btc", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("fake fx serves");
    });
    FakeFxFeed { state, base_url }
}
