//! Content-lock fetch failures must not disclose the lock document path or id.

use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HttpHomeserverClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const CREATOR: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
const LOCK_ID: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ0123456789ABCDEFGHJK";

#[derive(Clone, Default)]
struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = LogBuffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl LogBuffer {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = Self::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (buffer, guard)
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log buffer").clone()).expect("utf-8 log")
    }
}

async fn client(status: &'static str) -> (HttpHomeserverClient, JoinHandle<()>) {
    client_with_body(status, "").await
}

async fn client_with_body(
    status: &'static str,
    body: &'static str,
) -> (HttpHomeserverClient, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).await.expect("read request");
            assert_ne!(read, 0, "request headers complete");
            request.extend_from_slice(&buffer[..read]);
        }
        let request = String::from_utf8_lossy(&request);
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("pubky-host: {CREATOR}")),
            "the read is addressed to the creator: {request}"
        );
        stream
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("write response");
        stream.shutdown().await.ok();
    });
    (
        HttpHomeserverClient::new(&format!("http://{address}")).expect("client"),
        server,
    )
}

fn content_lock_path() -> String {
    format!("/pub/locks.app/{LOCK_ID}.json")
}

fn assert_content_lock_log_is_redacted(logs: &str, message: &str, status: Option<&str>) {
    assert!(logs.contains(message), "warning is captured: {logs}");
    if let Some(status) = status {
        assert!(logs.contains(status), "status is retained: {logs}");
    }
    assert!(
        logs.contains("pubky_host_prefix=8pinxxgq"),
        "creator prefix is retained: {logs}"
    );
    assert!(!logs.contains(LOCK_ID), "lock id is not logged: {logs}");
    assert!(
        !logs.contains(&LOCK_ID[..8]),
        "lock id prefix is not logged: {logs}"
    );
    assert!(
        !logs.contains(&content_lock_path()),
        "lock document path is not logged: {logs}"
    );
    assert!(
        logs.contains("record=\"content_lock\""),
        "record kind remains diagnosable: {logs}"
    );
}

#[tokio::test]
async fn content_lock_not_found_logs_omit_lock_id_and_path() {
    let (logs, _guard) = LogBuffer::capture();
    let (client, server) = client("404 Not Found").await;
    let outcome = client
        .fetch_content_lock(CREATOR, &content_lock_path())
        .await;
    server.await.expect("server");
    assert_eq!(outcome, HomeserverFetchOutcome::NotFound);
    assert_content_lock_log_is_redacted(
        &logs.text(),
        "homeserver record fetch not found",
        Some("status=404 Not Found"),
    );
}

#[tokio::test]
async fn content_lock_rejection_logs_omit_lock_id_and_path() {
    let (logs, _guard) = LogBuffer::capture();
    let (client, server) = client("500 Internal Server Error").await;
    let outcome = client
        .fetch_content_lock(CREATOR, &content_lock_path())
        .await;
    server.await.expect("server");
    assert_eq!(outcome, HomeserverFetchOutcome::Unavailable);
    assert_content_lock_log_is_redacted(
        &logs.text(),
        "homeserver record fetch rejected",
        Some("status=500 Internal Server Error"),
    );
}

#[tokio::test]
async fn content_lock_transport_failure_logs_omit_lock_id_and_path() {
    let (logs, _guard) = LogBuffer::capture();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("address").port();
    drop(listener);
    let client = HttpHomeserverClient::new(&format!("http://127.0.0.1:{port}")).expect("client");
    let outcome = client
        .fetch_content_lock(CREATOR, &content_lock_path())
        .await;
    assert_eq!(outcome, HomeserverFetchOutcome::Unavailable);
    assert_content_lock_log_is_redacted(
        &logs.text(),
        "homeserver record fetch transport failure",
        None,
    );
}

#[tokio::test]
async fn content_lock_non_json_logs_omit_lock_id_and_path() {
    let (logs, _guard) = LogBuffer::capture();
    let (client, server) = client_with_body("200 OK", "not json").await;
    let outcome = client
        .fetch_content_lock(CREATOR, &content_lock_path())
        .await;
    server.await.expect("server");
    assert_eq!(outcome, HomeserverFetchOutcome::Unavailable);
    assert_content_lock_log_is_redacted(
        &logs.text(),
        "homeserver record fetch returned a non-JSON body",
        Some("status=200 OK"),
    );
}

#[tokio::test]
async fn drop_not_found_still_logs_its_public_path() {
    let (logs, _guard) = LogBuffer::capture();
    let (client, server) = client("404 Not Found").await;
    let outcome = client.fetch_drop(CREATOR, "public-drop").await;
    server.await.expect("server");
    assert_eq!(outcome, HomeserverFetchOutcome::NotFound);
    let logs = logs.text();
    assert!(
        logs.contains("path=/pub/pubky.app/marketplace/v1/drops/public-drop"),
        "public record path is retained: {logs}"
    );
}
