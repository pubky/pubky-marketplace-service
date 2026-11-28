//! Dedicated single-test integration binary verifying that the FX HTTP client
//! ignores process-wide system proxy environment variables (`HTTP_PROXY`,
//! `HTTPS_PROXY`, `ALL_PROXY`, and lowercase equivalents) and keeps its
//! non-public IP / DNS-pinning guard intact.
//!
//! Isolated in its own integration test binary so mutating proxy environment
//! variables can never race with parallel unit tests in `crates/service`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use marketplace_domain::currency::Currency;
use marketplace_service::fx::{fetch_snapshot_for, FxError, FxFeedProvider};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

const PROXY_VARS: [&str; 8] = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
];

struct ProxyEnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl Drop for ProxyEnvGuard {
    fn drop(&mut self) {
        for (name, previous) in &self.saved {
            match previous {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

async fn spawn_raw_http_server(raw_response: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("raw http server binds");
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_bg = hits.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            hits_bg.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let _ = socket.write_all(&raw_response).await;
            let _ = socket.shutdown().await;
        }
    });
    (format!("http://{addr}"), hits)
}

#[tokio::test]
async fn fx_client_ignores_system_proxy_env_and_preserves_non_public_guard() {
    let _restore = ProxyEnvGuard {
        saved: PROXY_VARS
            .iter()
            .map(|&name| (name, std::env::var(name).ok()))
            .collect(),
    };

    let now: DateTime<Utc> = "2026-09-13T16:21:00Z".parse().unwrap();
    let current = now.timestamp();

    // Spy proxy: answers 502 immediately and counts every connection.
    let proxy_raw =
        b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
    let (proxy_url, proxy_hits) = spawn_raw_http_server(proxy_raw).await;

    let mempool_body = format!(r#"{{"time":{current},"USD":77197}}"#);
    let mempool_raw = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{mempool_body}",
        mempool_body.len()
    );
    let (target_url, target_hits) = spawn_raw_http_server(mempool_raw.into_bytes()).await;

    let kraken_body = format!(
        r#"{{"error":[],"result":{{"XXBTZUSD":[["77197","0.01",{current},"b","l","",1]],"last":"1"}}}}"#
    );
    let kraken_raw = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{kraken_body}",
        kraken_body.len()
    );
    let (kraken_url, kraken_hits) = spawn_raw_http_server(kraken_raw.into_bytes()).await;

    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        std::env::set_var(name, &proxy_url);
    }
    std::env::remove_var("NO_PROXY");
    std::env::remove_var("no_proxy");

    // Control check: a default reqwest client (auto_sys_proxy = true) routes
    // through the env proxy, proving proxy env vars are active in this process.
    let control = reqwest::Client::builder()
        .build()
        .expect("control client")
        .get(format!("{target_url}/control"))
        .send()
        .await
        .expect("control request reaches spy proxy");
    assert_eq!(control.status(), reqwest::StatusCode::BAD_GATEWAY);
    assert_eq!(
        proxy_hits.load(Ordering::SeqCst),
        1,
        "control client without no_proxy must hit the env proxy"
    );
    proxy_hits.store(0, Ordering::SeqCst);

    // Hardened FX client ignores system proxy env vars on IP literals,
    // domain hosts (`localhost`, exercising lookup_host + resolve_to_addrs),
    // and Kraken's Trades client path.
    let direct = fetch_snapshot_for(
        FxFeedProvider::Mempool,
        &format!("{target_url}/api/v1/prices"),
        now,
    )
    .await
    .expect("direct loopback fetch succeeds despite proxy env vars");
    assert_eq!(
        direct.sample(Currency::Usd).unwrap().rate.to_string_value(),
        "77197"
    );

    let parsed_target = Url::parse(&target_url).unwrap();
    let localhost_url = format!(
        "http://localhost:{}/api/v1/prices",
        parsed_target.port().unwrap()
    );
    let via_domain = fetch_snapshot_for(FxFeedProvider::Mempool, &localhost_url, now)
        .await
        .expect("localhost domain fetch succeeds despite proxy env vars");
    assert_eq!(
        via_domain
            .sample(Currency::Usd)
            .unwrap()
            .rate
            .to_string_value(),
        "77197"
    );

    let kraken_direct = fetch_snapshot_for(
        FxFeedProvider::Kraken,
        &format!("{kraken_url}/0/public/Trades?pair=XXBTZUSD&count=1"),
        now,
    )
    .await
    .expect("kraken trades client ignores proxy env vars");
    assert_eq!(
        kraken_direct
            .sample(Currency::Usd)
            .unwrap()
            .rate
            .to_string_value(),
        "77197"
    );

    assert_eq!(target_hits.load(Ordering::SeqCst), 2);
    assert_eq!(kraken_hits.load(Ordering::SeqCst), 1);
    assert_eq!(
        proxy_hits.load(Ordering::SeqCst),
        0,
        "FX client must never connect to system proxy"
    );

    // Non-public addresses remain refused even when HTTPS_PROXY is set.
    for blocked in [
        "https://169.254.169.254/latest/meta-data/",
        "https://10.0.0.1/fx",
        "https://[fe80::1]/fx",
    ] {
        assert_eq!(
            fetch_snapshot_for(FxFeedProvider::Mempool, blocked, now).await,
            Err(FxError::Malformed),
            "{blocked} must be refused without contacting proxy"
        );
    }
    assert_eq!(
        proxy_hits.load(Ordering::SeqCst),
        0,
        "refused non-public destinations must never contact system proxy"
    );
}
