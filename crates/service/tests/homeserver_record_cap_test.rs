//! The production homeserver client's record reads (listing, drop, content
//! lock) against a raw HTTP/1.1 server, so the response framing is exact.
//! A body is read through a running byte cap: an over-cap body, declared or
//! streamed without a length, is refused without being buffered whole, and a
//! body of exactly the cap is read. The JSON reads stop at 4 MiB; the
//! listing-bytes read stops at 1 MiB.

use std::time::Duration;

use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HttpHomeserverClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PUBKY: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
const JSON_CAP: usize = 4 * 1024 * 1024;
const BYTES_CAP: usize = 1024 * 1024;

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
    let (kind, len) = case
        .split_once(':')
        .map(|(kind, len)| (kind, len.parse::<usize>().expect("length")))
        .unwrap_or((case.as_str(), 0));
    match kind {
        "exact" => {
            let body = json_of_len(len);
            let response = [
                head("transfer-encoding: chunked\r\n").into_bytes(),
                chunked(&body),
            ]
            .concat();
            stream.write_all(&response).await.expect("write response");
            stream.shutdown().await.ok();
        }
        "over-chunked" => {
            let response = [
                head("transfer-encoding: chunked\r\n").into_bytes(),
                chunked(&json_of_len(len)),
            ]
            .concat();
            stream.write_all(&response).await.expect("write response");
            stream.shutdown().await.ok();
        }
        "over-length" => {
            stream
                .write_all(head(&format!("content-length: {len}\r\n")).as_bytes())
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

const TIMEOUT: Duration = Duration::from_secs(5);

async fn within<T>(read: &str, call: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(TIMEOUT, call)
        .await
        .unwrap_or_else(|_| panic!("{read} must not wait on a body it refuses"))
}

/// The reads that stop at [`JSON_CAP`], for one case.
async fn json_reads(
    client: &HttpHomeserverClient,
    case: &str,
) -> Vec<(&'static str, HomeserverFetchOutcome)> {
    let path = format!("/pub/pubky.app/marketplace/v1/{case}");
    vec![
        (
            "listing",
            within("listing", client.fetch_listing(PUBKY, case)).await,
        ),
        (
            "listing_from",
            within("listing_from", client.fetch_listing_from(PUBKY, case))
                .await
                .0,
        ),
        ("drop", within("drop", client.fetch_drop(PUBKY, case)).await),
        (
            "content_lock",
            within("content_lock", client.fetch_content_lock(PUBKY, &path)).await,
        ),
    ]
}

/// The read that stops at [`BYTES_CAP`], for one case.
async fn bytes_read(client: &HttpHomeserverClient, case: &str) -> HomeserverFetchOutcome {
    within("listing_bytes", client.fetch_listing_bytes(PUBKY, case)).await
}

fn found(outcome: &HomeserverFetchOutcome) -> bool {
    matches!(outcome, HomeserverFetchOutcome::Found(_))
}

#[tokio::test]
async fn a_body_of_exactly_the_cap_is_read() {
    let client = client().await;
    for (read, outcome) in json_reads(&client, &format!("exact:{JSON_CAP}")).await {
        assert!(found(&outcome), "{read}: {outcome:?}");
    }
    let outcome = bytes_read(&client, &format!("exact:{BYTES_CAP}")).await;
    assert!(found(&outcome), "listing_bytes: {outcome:?}");
}

#[tokio::test]
async fn a_chunked_body_over_the_cap_without_a_length_is_refused() {
    let client = client().await;
    for (read, outcome) in json_reads(&client, &format!("over-chunked:{}", JSON_CAP + 1)).await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
    assert_eq!(
        bytes_read(&client, &format!("over-chunked:{}", BYTES_CAP + 1)).await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn an_oversized_content_length_is_refused_without_reading_the_body() {
    let client = client().await;
    for (read, outcome) in json_reads(&client, &format!("over-length:{}", JSON_CAP + 1)).await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
    assert_eq!(
        bytes_read(&client, &format!("over-length:{}", BYTES_CAP + 1)).await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn a_body_that_never_ends_is_cut_off_at_the_cap() {
    let client = client().await;
    for (read, outcome) in json_reads(&client, "endless").await {
        assert_eq!(outcome, HomeserverFetchOutcome::Unavailable, "{read}");
    }
    assert_eq!(
        bytes_read(&client, "endless").await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn a_json_record_just_over_a_megabyte_still_reads_where_the_bytes_read_refuses_it() {
    let client = client().await;
    let case = format!("exact:{}", BYTES_CAP + 1);
    for (read, outcome) in json_reads(&client, &case).await {
        assert!(found(&outcome), "{read}: {outcome:?}");
    }
    assert_eq!(
        bytes_read(&client, &case).await,
        HomeserverFetchOutcome::Unavailable
    );
}
