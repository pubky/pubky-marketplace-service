//! The production homeserver client's record reads (listing, drop, content
//! lock) against a raw HTTP/1.1 server, so the response framing is exact.
//! A body is read through a running byte cap: an over-cap body, declared or
//! streamed without a length, is refused without being buffered whole, and a
//! body of exactly the cap is read.

use std::time::Duration;

use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HttpHomeserverClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PUBKY: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
const CAP: usize = 1024 * 1024;

/// A JSON document of exactly `len` bytes.
fn json_of_len(len: usize) -> Vec<u8> {
    let frame = br#"{"pad":""}"#.len();
    let body = format!(r#"{{"pad":"{}"}}"#, "x".repeat(len - frame));
    assert_eq!(body.len(), len);
    body.into_bytes()
}

fn chunked(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in body.chunks(16 * 1024) {
        out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
}

fn head(framing: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{framing}connection: close\r\n\r\n"
    )
}

async fn respond(mut stream: TcpStream) {
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await.expect("read request");
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let request = String::from_utf8_lossy(&request);
    let case = request
        .split_whitespace()
        .nth(1)
        .and_then(|path| path.rsplit('/').next())
        .unwrap_or_default()
        .to_string();
    match case.as_str() {
        "exact-cap" => {
            let body = json_of_len(CAP);
            let response = [
                head("transfer-encoding: chunked\r\n").into_bytes(),
                chunked(&body),
            ]
            .concat();
            stream.write_all(&response).await.expect("write response");
            stream.shutdown().await.ok();
        }
        "over-cap-chunked" => {
            let response = [
                head("transfer-encoding: chunked\r\n").into_bytes(),
                chunked(&json_of_len(CAP + 1)),
            ]
            .concat();
            stream.write_all(&response).await.expect("write response");
            stream.shutdown().await.ok();
        }
        "over-cap-length" => {
            stream
                .write_all(head(&format!("content-length: {}\r\n", CAP + 1)).as_bytes())
                .await
                .expect("write head");
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        "endless" => {
            // A body that never ends and declares no length: only the client's
            // own cap stops it.
            if stream
                .write_all(head("transfer-encoding: chunked\r\n").as_bytes())
                .await
                .is_err()
            {
                return;
            }
            let chunk = [b"4000\r\n".as_slice(), &[b'x'; 0x4000], b"\r\n"].concat();
            while stream.write_all(&chunk).await.is_ok() {}
        }
        other => panic!("unexpected case {other}"),
    }
}

async fn client() -> HttpHomeserverClient {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(respond(stream));
        }
    });
    HttpHomeserverClient::new(&format!("http://{address}")).expect("client")
}

/// Every record read the capped body applies to, for one case.
async fn reads(
    client: &HttpHomeserverClient,
    case: &str,
) -> Vec<(&'static str, HomeserverFetchOutcome)> {
    let path = format!("/pub/pubky.app/marketplace/v1/{case}");
    let within = |read: &'static str, outcome| async move { (read, outcome) };
    let timeout = Duration::from_secs(5);
    let refused = move |read: &'static str| format!("{read} must not wait on a body it refuses");
    vec![
        within(
            "listing",
            tokio::time::timeout(timeout, client.fetch_listing(PUBKY, case))
                .await
                .unwrap_or_else(|_| panic!("{}", refused("listing"))),
        )
        .await,
        within(
            "listing_from",
            tokio::time::timeout(timeout, client.fetch_listing_from(PUBKY, case))
                .await
                .unwrap_or_else(|_| panic!("{}", refused("listing_from")))
                .0,
        )
        .await,
        within(
            "listing_bytes",
            tokio::time::timeout(timeout, client.fetch_listing_bytes(PUBKY, case))
                .await
                .unwrap_or_else(|_| panic!("{}", refused("listing_bytes"))),
        )
        .await,
        within(
            "drop",
            tokio::time::timeout(timeout, client.fetch_drop(PUBKY, case))
                .await
                .unwrap_or_else(|_| panic!("{}", refused("drop"))),
        )
        .await,
        within(
            "content_lock",
            tokio::time::timeout(timeout, client.fetch_content_lock(PUBKY, &path))
                .await
                .unwrap_or_else(|_| panic!("{}", refused("content_lock"))),
        )
        .await,
    ]
}

#[tokio::test]
async fn a_body_of_exactly_the_cap_is_read() {
    let client = client().await;
    for (read, outcome) in reads(&client, "exact-cap").await {
        assert!(
            matches!(outcome, HomeserverFetchOutcome::Found(_)),
            "{read}: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn a_chunked_body_over_the_cap_without_a_length_is_refused() {
    let client = client().await;
    for (read, outcome) in reads(&client, "over-cap-chunked").await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
}

#[tokio::test]
async fn an_oversized_content_length_is_refused_without_reading_the_body() {
    let client = client().await;
    for (read, outcome) in reads(&client, "over-cap-length").await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
}

#[tokio::test]
async fn a_body_that_never_ends_is_cut_off_at_the_cap() {
    let client = client().await;
    for (read, outcome) in reads(&client, "endless").await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
}
