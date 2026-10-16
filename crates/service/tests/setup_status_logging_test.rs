//! Every way the seller-readiness lookups can fail to say `ready` or
//! `setup_required` leaves one warn line with a `reason`, the seller's
//! 8-character prefix and the elapsed time, and nothing secret. Before this,
//! a timeout, a transport error, a non-JSON body and paykit-server's own
//! `unavailable` were silent.
//!
//! One test in its own binary: the log capture is process-global, so no
//! other test here may produce these lines.

mod common;

use std::time::Duration;

use common::paykit_server_u4::Fixture;
use common::*;
use marketplace_service::payments::{
    PaykitApi, PaykitClient, PaykitRequestError, SetupAsset, SetupStatus,
};

const SETUP_STATUS_LINE: &str = "paykit setup status unavailable";
const ACCOUNT_LINE: &str = "paykit account lookup unavailable";

fn client_for(base_url: &str, api: PaykitApi) -> PaykitClient {
    PaykitClient::new(base_url, TEST_PAYKIT_SIGNING_SEED)
        .expect("paykit client")
        .with_api(api)
}

fn prefix(seller: &str) -> String {
    seller.chars().take(8).collect()
}

/// The single warn line naming this seller, asserted to carry the reason.
fn the_line(message: &str, seller: &str, reason: &str) -> String {
    let marker = format!("seller={}", prefix(seller));
    let logs = captured_logs();
    let lines: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains(message) && line.contains(&marker))
        .collect();
    assert_eq!(lines.len(), 1, "one line for {reason}: {lines:#?}");
    let line = lines[0].to_string();
    assert!(line.contains(" WARN "), "warn level: {line}");
    assert!(
        line.contains(&format!("reason=\"{reason}\"")),
        "reason {reason}: {line}"
    );
    assert!(line.contains("elapsed_ms="), "elapsed time: {line}");
    assert!(
        !line.contains(seller),
        "never the full seller pubky: {line}"
    );
    line
}

fn non_json_fixture() -> Fixture {
    Fixture {
        name: "non-json".into(),
        server: "test".into(),
        server_revision: "test".into(),
        method: "POST".into(),
        path: "/setup/status".into(),
        signature: String::new(),
        request_body: String::new(),
        status: 200,
        content_type: Some("text/plain".into()),
        response_body: "<html>gateway</html>".into(),
    }
}

#[tokio::test]
async fn every_unavailable_path_logs_a_warn_with_its_reason_and_no_secret() {
    install_log_capture();
    let paykit = spawn_fake_paykit().await;
    paykit.use_upstream_api();
    let upstream = client_for(&paykit.base_url, PaykitApi::Upstream);

    // Timeout.
    let (_, seller) = random_keypair();
    paykit.delay_setup_status(Duration::from_secs(2));
    let fast = client_for(&paykit.base_url, PaykitApi::Upstream)
        .with_setup_status_timeout(Duration::from_millis(200));
    assert_eq!(
        fast.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "timeout");
    assert!(line.contains("question=\"BTC\""), "{line}");
    paykit.delay_setup_status(Duration::ZERO);

    // Transport: nothing listens on port 1.
    let (_, seller) = random_keypair();
    let unreachable = client_for("http://127.0.0.1:1", PaykitApi::Upstream);
    assert_eq!(
        unreachable.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "transport");
    assert!(line.contains("detail=\"connect\""), "{line}");
    assert!(!line.contains("127.0.0.1"), "no URL: {line}");

    // Signing: the base URL passes construction but not URL parsing, which
    // the upstream preimage needs.
    let (_, seller) = random_keypair();
    let unsignable = client_for("http://[::1", PaykitApi::Upstream);
    assert_eq!(
        unsignable.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    the_line(SETUP_STATUS_LINE, &seller, "signing");

    // Non-2xx.
    let (_, seller) = random_keypair();
    paykit.fail_setup_status_with(503);
    assert_eq!(
        upstream.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "non_2xx");
    assert!(line.contains("status=503"), "{line}");
    paykit.clear_setup_status_failure();

    // A body that is not JSON.
    let (_, seller) = random_keypair();
    paykit.replay_setup_status(&seller, Some("asset:BTC"), &non_json_fixture());
    assert_eq!(
        upstream.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    the_line(SETUP_STATUS_LINE, &seller, "body_not_json");

    // JSON outside the closed contract.
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "degraded");
    assert_eq!(
        upstream.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    the_line(SETUP_STATUS_LINE, &seller, "out_of_contract");

    // paykit-server's own `unavailable`, for each question asked.
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "unavailable");
    assert_eq!(
        upstream.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "paykit_unavailable");
    assert!(line.contains("question=\"BTC\""), "{line}");
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "unavailable");
    assert_eq!(
        upstream.upstream_setup_status(&seller, None).await,
        Ok(SetupStatus::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "paykit_unavailable");
    assert!(line.contains("question=\"authority\""), "{line}");
    let (_, seller) = random_keypair();
    paykit.set_setup_status(&seller, "unavailable");
    assert_eq!(
        upstream
            .upstream_setup_status(&seller, Some(SetupAsset::Usdt))
            .await,
        Ok(SetupStatus::Unavailable)
    );
    let line = the_line(SETUP_STATUS_LINE, &seller, "paykit_unavailable");
    assert!(line.contains("question=\"USDT\""), "{line}");

    // `ready` and `setup_required` are answers, not failures: no line.
    let (_, ready) = random_keypair();
    paykit.set_setup_status(&ready, "ready");
    assert_eq!(upstream.seller_ready(&ready).await, Ok(true));
    let (_, not_set_up) = random_keypair();
    assert_eq!(upstream.seller_ready(&not_set_up).await, Ok(false));

    // The fork's account lookup: non-2xx, bad body, transport.
    let fork_paykit = spawn_fake_paykit().await;
    let fork = client_for(&fork_paykit.base_url, PaykitApi::Fork);
    let (_, seller) = random_keypair();
    fork_paykit.fail_account_exists();
    assert_eq!(
        fork.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    let line = the_line(ACCOUNT_LINE, &seller, "non_2xx");
    assert!(line.contains("status=503"), "{line}");
    let (_, seller) = random_keypair();
    let fork_unreachable = client_for("http://127.0.0.1:1", PaykitApi::Fork);
    assert_eq!(
        fork_unreachable.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    the_line(ACCOUNT_LINE, &seller, "transport");
    let (_, seller) = random_keypair();
    let fork_wrong_body = client_for(&spawn_non_json_server().await, PaykitApi::Fork);
    assert_eq!(
        fork_wrong_body.seller_ready(&seller).await,
        Err(PaykitRequestError::Unavailable)
    );
    the_line(ACCOUNT_LINE, &seller, "bad_body");

    let logs = captured_logs();
    for name in [&ready, &not_set_up] {
        assert!(
            !logs.contains(&format!("seller={}", prefix(name))),
            "no warn for an answer"
        );
    }
    assert!(
        !logs.contains(TEST_PAYKIT_SIGNING_SEED),
        "the signing seed never reaches the logs"
    );
    assert!(!logs.contains("x-paykit-signature"), "no signature header");
    for line in logs
        .lines()
        .filter(|line| line.contains(SETUP_STATUS_LINE) || line.contains(ACCOUNT_LINE))
    {
        assert!(!line.contains("pubky"), "no creator app key: {line}");
    }
}

/// Answers every request 200 with an HTML body, as a misrouted proxy does.
async fn spawn_non_json_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let router = axum::Router::new().fallback(|| async { "<html>gateway</html>" });
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    format!("http://{address}")
}
