//! The production homeserver client's grant-session proof read
//! (`fetch_small_json`, 8 KB cap) against a raw HTTP/1.1 server, so the
//! response framing (Content-Length, chunked, a reset mid-body) is exact.

use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HttpHomeserverClient,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PUBKY: &str = "8pinxxgqs41n4aididenw5apqp1urfmzdztr8jt4abrkdn435ewo";
const CAP: usize = 8 * 1024;

/// A JSON document of exactly `len` bytes.
fn json_of_len(len: usize) -> Vec<u8> {
    let frame = br#"{"pad":""}"#.len();
    let body = format!(r#"{{"pad":"{}"}}"#, "x".repeat(len - frame));
    assert_eq!(body.len(), len);
    body.into_bytes()
}

fn chunked(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for chunk in body.chunks(1024) {
        out.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"0\r\n\r\n");
    out
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
    assert!(
        request
            .to_ascii_lowercase()
            .contains(&format!("pubky-host: {PUBKY}")),
        "the read is addressed to the issuer: {request}"
    );
    let case = request
        .split_whitespace()
        .nth(1)
        .and_then(|path| path.rsplit('/').next())
        .unwrap_or_default()
        .to_string();
    let head = |status: &str, framing: &str| {
        format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{framing}connection: close\r\n\r\n")
    };
    let response: Vec<u8> = match case.as_str() {
        "small" => {
            let body = serde_json::to_vec(&json!({"ok": true})).expect("json");
            [
                head("200 OK", &format!("content-length: {}\r\n", body.len())).into_bytes(),
                body,
            ]
            .concat()
        }
        "exact-cap" => {
            let body = json_of_len(CAP);
            [
                head("200 OK", "transfer-encoding: chunked\r\n").into_bytes(),
                chunked(&body),
            ]
            .concat()
        }
        "over-cap-chunked" => [
            head("200 OK", "transfer-encoding: chunked\r\n").into_bytes(),
            chunked(&json_of_len(CAP + 1)),
        ]
        .concat(),
        "over-cap-length" => {
            // Declares an oversized body and sends none of it: the client must
            // refuse on the header alone, not wait for or read the body.
            stream
                .write_all(head("200 OK", &format!("content-length: {}\r\n", CAP + 1)).as_bytes())
                .await
                .expect("write head");
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            return;
        }
        "reset-mid-body" => {
            let mut partial = head("200 OK", "transfer-encoding: chunked\r\n").into_bytes();
            partial.extend_from_slice(b"400\r\n{\"pad\":\"");
            stream.write_all(&partial).await.expect("write partial");
            // Dropping the stream mid-chunk ends the body with an error.
            return;
        }
        "missing" => head("404 Not Found", "content-length: 0\r\n").into_bytes(),
        "server-error" => head("500 Internal Server Error", "content-length: 0\r\n").into_bytes(),
        "not-json" => [
            head("200 OK", "content-length: 8\r\n").into_bytes(),
            b"not json".to_vec(),
        ]
        .concat(),
        other => panic!("unexpected case {other}"),
    };
    stream.write_all(&response).await.expect("write response");
    stream.shutdown().await.ok();
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

async fn fetch(client: &HttpHomeserverClient, case: &str) -> HomeserverFetchOutcome {
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.fetch_grant_session_proof(PUBKY, case),
    )
    .await
    .unwrap_or_else(|_| panic!("{case} must not wait for a body it refuses"))
}

#[tokio::test]
async fn a_small_document_is_read() {
    let client = client().await;
    assert_eq!(
        fetch(&client, "small").await,
        HomeserverFetchOutcome::Found(json!({"ok": true}))
    );
}

#[tokio::test]
async fn a_chunked_document_of_exactly_the_cap_is_read() {
    let client = client().await;
    assert!(matches!(
        fetch(&client, "exact-cap").await,
        HomeserverFetchOutcome::Found(_)
    ));
}

#[tokio::test]
async fn a_chunked_body_over_the_cap_without_a_length_is_refused() {
    let client = client().await;
    assert_eq!(
        fetch(&client, "over-cap-chunked").await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn an_oversized_content_length_is_refused_without_reading_the_body() {
    let client = client().await;
    assert_eq!(
        fetch(&client, "over-cap-length").await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn a_connection_reset_mid_body_is_unavailable() {
    let client = client().await;
    assert_eq!(
        fetch(&client, "reset-mid-body").await,
        HomeserverFetchOutcome::Unavailable
    );
}

#[tokio::test]
async fn not_found_other_statuses_and_invalid_json_map_fail_closed() {
    let client = client().await;
    assert_eq!(
        fetch(&client, "missing").await,
        HomeserverFetchOutcome::NotFound
    );
    assert_eq!(
        fetch(&client, "server-error").await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        fetch(&client, "not-json").await,
        HomeserverFetchOutcome::Unavailable
    );
}
