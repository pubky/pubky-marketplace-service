//! The grant exchange's outbound requests, against a real local Pubky
//! network: a mainline DHT testnet, an HTTP relay and `pubky-homeserver`
//! instances from `pubky-testnet`, with users who approve flows through the
//! SDK's signer. Each test drives whole grant flows: created over HTTP,
//! approved into the relay inbox, processed by the worker.
//!
//! Covered: a grant exchanged at the configured homeserver completes; one
//! whose issuer names another homeserver is refused with nothing sent to it;
//! one whose issuer is not the asserted pubky is not exchanged; an answer
//! for another pubky, homeserver, grant or client id than the exchanged
//! grant's is refused; a homeserver's redirect is not followed; records
//! naming an internal
//! address, in every form, are refused with no connection made; a record
//! mixing refused and allowed addresses is connected to only at the allowed
//! ones; and an endpoint that speaks only plain HTTP is never sent a
//! plain-HTTP request.
//!
//! A homeserver hands its database to `pubky_testnet::drop_test_databases`
//! only once its last task is gone, so each test runs on a runtime of its own
//! and drops the databases after that runtime shut down, panicked or not.

mod common;

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::panic::AssertUnwindSafe;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use ed25519_dalek::SigningKey;
use marketplace_domain::pubky::encode_pubky;
use marketplace_service::clock::Clock;
use marketplace_service::config::Config;
use marketplace_service::grant;
use marketplace_service::grant::test_support::GrantTestAuthority;
use marketplace_service::homeserver_transport::EndpointPolicy;
use pubky_common::auth::grant_session_responses::{GrantSessionInfo, GrantSessionResponse};
use pubky_common::auth::jws::{ClientId, GrantId};
use pubky_testnet::pubky::pkarr::dns::rdata::SVCB;
use pubky_testnet::pubky::pkarr::{Keypair as PkarrKeypair, SignedPacket};
use pubky_testnet::pubky::{GrantClaims, Keypair, PublicKey};
use pubky_testnet::pubky_homeserver::ConnectionString;
use pubky_testnet::Testnet;
use serde_json::json;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use url::Url;
use uuid::Uuid;

use common::{send, test_app_with_grant_authority, TestApp};

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL names the Postgres server the testnet homeservers use")
}

/// A local DHT. Homeservers it starts create their databases on the
/// `DATABASE_URL` server.
struct Network {
    testnet: Testnet,
    /// The inbox of the network's HTTP relay.
    relay: Url,
}

async fn network() -> Network {
    let postgres = ConnectionString::new(&database_url()).expect("DATABASE_URL is a Postgres URL");
    let mut testnet = Testnet::new_with_custom_postgres(postgres)
        .await
        .expect("testnet starts");
    let relay = testnet
        .create_http_relay()
        .await
        .expect("http relay starts")
        .local_url()
        .join("inbox")
        .expect("relay inbox URL");
    Network { testnet, relay }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

/// Runs `test` on a migrated service database of its own on the
/// `DATABASE_URL` server, then shuts its runtime down, drops the testnet's
/// and the service's databases on a fresh runtime, and re-raises a panic
/// from `test`.
fn on_testnet<F, T>(test: F)
where
    F: FnOnce(PgPool) -> T,
    T: Future<Output = ()>,
{
    let name = format!("market_grant_exchange_test_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url()).expect("DATABASE_URL");
    let admin = options.clone();
    let database = options.database(&name);
    let test_runtime = runtime();
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        test_runtime.block_on(async {
            sqlx::query(&format!("CREATE DATABASE {name}"))
                .execute(&mut admin.connect().await.expect("Postgres"))
                .await
                .expect("create the service database");
            let pool = PgPool::connect_with(database)
                .await
                .expect("service database");
            marketplace_service::TEST_MIGRATOR
                .run(&pool)
                .await
                .expect("migrate the service database");
            test(pool).await;
        })
    }));
    test_runtime.shutdown_timeout(Duration::from_secs(10));
    runtime().block_on(async {
        pubky_testnet::drop_test_databases().await;
        sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .execute(&mut admin.connect().await.expect("Postgres"))
            .await
            .expect("drop the service database");
    });
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// The service, relaying through `network`'s HTTP relay and resolving on
/// its DHT, exchanging grants only at `homeserver`.
async fn service(
    pool: PgPool,
    network: &Network,
    homeserver: PublicKey,
    policy: EndpointPolicy,
) -> (TestApp, GrantTestAuthority) {
    let authority = GrantTestAuthority::generate_with_relay(
        Config::for_tests().session_ttl_seconds,
        network.relay.clone(),
        30,
    )
    .with_network(
        network.testnet.client().expect("testnet client"),
        homeserver,
        policy,
    );
    test_app_with_grant_authority(pool, authority).await
}

/// Runs one grant flow for `user` to its end: created, approved through the
/// SDK's signer, processed by the worker. Returns its status and terminal
/// code.
async fn run_flow(
    app: &TestApp,
    authority: &GrantTestAuthority,
    network: &Network,
    user: &Keypair,
) -> (String, Option<String>) {
    run_flow_approved_by(app, authority, network, &user.public_key(), user).await
}

/// [`run_flow`] for a flow the Shop asserted for `asserted`, approved by
/// `approver`.
async fn run_flow_approved_by(
    app: &TestApp,
    authority: &GrantTestAuthority,
    network: &Network,
    asserted: &PublicKey,
    approver: &Keypair,
) -> (String, Option<String>) {
    // A fresh minute of the per-client creation limit for every flow.
    app.clock.advance_seconds(61);
    let result_key = SigningKey::from_bytes(&[19u8; 32]);
    let assertion = authority.sign_bootstrap_assertion(
        &asserted.z32(),
        &[23u8; 32],
        &encode_pubky(&result_key.verifying_key().to_bytes()),
        app.clock.now(),
        Uuid::new_v4(),
    );
    let (status, body) = send(
        app.router.clone(),
        "POST",
        "/v1/auth/grant-flows",
        None,
        &json!({"assertion": assertion}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let flow_id = Uuid::parse_str(body["flow_id"].as_str().expect("flow_id")).expect("uuid");
    network
        .testnet
        .sdk()
        .expect("testnet sdk")
        .signer(approver.clone())
        .approve_auth(body["authorization_url"].as_str().expect("url"))
        .await
        .expect("grant posted to the relay inbox");
    let scanned = tokio::time::timeout(Duration::from_secs(60), grant::poll_once(&app.state))
        .await
        .expect("the worker finishes the flow")
        .expect("worker pass");
    assert_eq!(scanned, 1);
    sqlx::query_as("SELECT status, terminal_code FROM grant_flows WHERE flow_id = $1")
        .bind(flow_id)
        .fetch_one(&app.pool)
        .await
        .expect("flow row")
}

/// Publishes `packets` on the testnet DHT, all at once.
async fn publish(network: &Network, packets: Vec<SignedPacket>) {
    let pkarr = network
        .testnet
        .pkarr_client_builder()
        .build()
        .expect("pkarr client");
    let mut publishing = tokio::task::JoinSet::new();
    for packet in packets {
        let pkarr = pkarr.clone();
        publishing.spawn(async move { pkarr.publish(&packet).await });
    }
    while let Some(published) = publishing.join_next().await {
        published
            .expect("publish task")
            .expect("publish on the testnet DHT");
    }
}

/// Where a homeserver key's own record says it can be reached.
#[derive(Default)]
struct Endpoints {
    /// Addresses and a port for the key itself (an HTTPS record targeting `.`).
    direct: Option<(Vec<IpAddr>, u16)>,
    /// An ICANN domain and port.
    icann: Option<(&'static str, u16)>,
}

/// `homeserver`'s endpoint record.
fn endpoint_record(homeserver: &PkarrKeypair, endpoints: Endpoints) -> SignedPacket {
    let mut packet = SignedPacket::builder();
    if let Some((addresses, port)) = endpoints.direct {
        let mut svcb = SVCB::new(1, ".".try_into().expect("root name"));
        svcb.set_port(port);
        packet = packet.https(".".try_into().expect("root name"), svcb, 60);
        for address in addresses {
            packet = packet.address(".".try_into().expect("root name"), address, 60);
        }
    }
    if let Some((domain, port)) = endpoints.icann {
        let mut svcb = SVCB::new(10, domain.try_into().expect("domain name"));
        svcb.set_port(port);
        packet = packet.https(".".try_into().expect("root name"), svcb, 60);
    }
    packet.sign(homeserver).expect("endpoint record")
}

/// A new user and their Pkarr record naming `homeserver`.
fn user_record(homeserver: &PublicKey) -> (Keypair, SignedPacket) {
    let user = Keypair::random();
    let target = homeserver.z32();
    let packet = SignedPacket::builder()
        .https(
            "_pubky".try_into().expect("_pubky name"),
            SVCB::new(0, target.as_str().try_into().expect("homeserver name")),
            60,
        )
        .sign(&user)
        .expect("user record");
    (user, packet)
}

/// Publishes `homeserver`'s endpoint record and a new user's record naming
/// `homeserver`, and returns the user.
async fn user_on(network: &Network, homeserver: &PkarrKeypair, endpoints: Endpoints) -> Keypair {
    let (user, record) = user_record(&homeserver.public_key().into());
    publish(
        network,
        vec![endpoint_record(homeserver, endpoints), record],
    )
    .await;
    user
}

/// A loopback listener that counts connections and the plain-HTTP requests
/// among them, and answers those `200`.
struct Listener {
    port: u16,
    connections: Arc<AtomicUsize>,
    plain_http_requests: Arc<AtomicUsize>,
}

async fn listener() -> Listener {
    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = socket.local_addr().expect("address").port();
    let connections = Arc::new(AtomicUsize::new(0));
    let plain_http_requests = Arc::new(AtomicUsize::new(0));
    let (counted, plain) = (connections.clone(), plain_http_requests.clone());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = socket.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            let plain = plain.clone();
            tokio::spawn(async move {
                let mut start = [0u8; 4];
                if stream.read_exact(&mut start).await.is_err()
                    || !(&start == b"POST" || &start == b"GET ")
                {
                    return;
                }
                plain.fetch_add(1, Ordering::SeqCst);
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    Listener {
        port,
        connections,
        plain_http_requests,
    }
}

impl Listener {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn plain_http_requests(&self) -> usize {
        self.plain_http_requests.load(Ordering::SeqCst)
    }
}

/// A listener on every local IPv4 address that records the local address
/// each connection reached.
struct AddressListener {
    port: u16,
    reached: Arc<Mutex<Vec<IpAddr>>>,
}

async fn address_listener() -> AddressListener {
    let socket = TcpListener::bind("0.0.0.0:0").await.expect("bind");
    let port = socket.local_addr().expect("address").port();
    let reached = Arc::new(Mutex::new(Vec::new()));
    let recorded = reached.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            if let Ok(local) = stream.local_addr() {
                recorded.lock().expect("reached").push(local.ip());
            }
        }
    });
    AddressListener { port, reached }
}

impl AddressListener {
    fn reached(&self) -> Vec<IpAddr> {
        self.reached.lock().expect("reached").clone()
    }
}

/// This host's address on its default route, if it has one: a local address
/// that is not loopback. Connecting a UDP socket sends nothing.
fn host_address() -> Option<IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("192.0.2.1:9").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

/// A homeserver on loopback holding `keypair`, over TLS pinned to that key
/// as a real one is, that answers every request with `302` to `location`.
/// Returns its port and the number of requests it answered.
async fn redirecting_homeserver(
    keypair: &PkarrKeypair,
    location: String,
) -> (u16, Arc<AtomicUsize>) {
    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = socket.local_addr().expect("address").port();
    let acceptor = TlsAcceptor::from(Arc::new(keypair.to_rpk_rustls_server_config()));
    let answered = Arc::new(AtomicUsize::new(0));
    let counted = answered.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            let (acceptor, counted, location) =
                (acceptor.clone(), counted.clone(), location.clone());
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match tls.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                counted.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {location}\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                );
                let _ = tls.write_all(response.as_bytes()).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    (port, answered)
}

/// What [`answering_homeserver`] answers a grant exchange with: the session
/// the grant is for, or that session with one field changed.
#[derive(Clone, Copy, Debug)]
enum Answer {
    Exact,
    OtherPubky,
    OtherHomeserver,
    OtherGrantId,
    OtherClientId,
}

/// A homeserver on loopback holding `keypair`, over TLS pinned to that key,
/// that answers every request with a grant session for the posted grant,
/// changed as `answer` currently says.
async fn answering_homeserver(keypair: &PkarrKeypair, answer: Arc<Mutex<Answer>>) -> u16 {
    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = socket.local_addr().expect("address").port();
    let acceptor = TlsAcceptor::from(Arc::new(keypair.to_rpk_rustls_server_config()));
    let homeserver: PublicKey = keypair.public_key().into();
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            let (acceptor, answer, homeserver) =
                (acceptor.clone(), answer.clone(), homeserver.clone());
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let Some(body) = read_request_body(&mut tls).await else {
                    return;
                };
                let request: serde_json::Value =
                    serde_json::from_slice(&body).expect("exchange request is JSON");
                let claims = GrantClaims::decode(request["grant"].as_str().expect("grant"))
                    .expect("the posted grant decodes");
                let mut session = GrantSessionInfo {
                    homeserver,
                    pubky: claims.iss.clone(),
                    client_id: claims.client_id.clone(),
                    capabilities: claims.caps.clone(),
                    grant_id: claims.jti.clone(),
                    token_expires_at: claims.exp,
                    grant_expires_at: claims.exp,
                    created_at: claims.iat,
                };
                let answer = *answer.lock().expect("answer");
                match answer {
                    Answer::Exact => {}
                    Answer::OtherPubky => session.pubky = Keypair::random().public_key(),
                    Answer::OtherHomeserver => {
                        session.homeserver = Keypair::random().public_key();
                    }
                    Answer::OtherGrantId => session.grant_id = GrantId::generate(),
                    Answer::OtherClientId => {
                        session.client_id = ClientId::new("other.example").expect("client id");
                    }
                }
                let body = serde_json::to_vec(&GrantSessionResponse {
                    token: "test-bearer".to_string(),
                    session,
                })
                .expect("session JSON");
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = tls.write_all(head.as_bytes()).await;
                let _ = tls.write_all(&body).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    port
}

/// The body of one HTTP/1.1 request with a `Content-Length`.
async fn read_request_body<S: AsyncReadExt + Unpin>(stream: &mut S) -> Option<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    let head_end = loop {
        if let Some(at) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    };
    let head = String::from_utf8_lossy(&request[..head_end]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))?
        .trim()
        .parse()
        .ok()?;
    while request.len() < head_end + length {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    Some(request[head_end..head_end + length].to_vec())
}

#[test]
fn a_grant_exchanged_at_the_configured_homeserver_completes() {
    on_testnet(|pool| async move {
        let mut network = network().await;
        let homeserver = network
            .testnet
            .create_random_homeserver()
            .await
            .expect("homeserver")
            .public_key();
        let user = Keypair::random();
        network
            .testnet
            .sdk()
            .expect("testnet sdk")
            .signer(user.clone())
            .signup(&homeserver, None)
            .await
            .expect("signup on the testnet homeserver");
        // The testnet homeserver publishes only loopback endpoints.
        let (app, authority) =
            service(pool, &network, homeserver, EndpointPolicy::PublicOrLoopback).await;

        assert_eq!(
            run_flow(&app, &authority, &network, &user).await,
            ("complete".to_string(), None)
        );
        let pubky: Option<String> = sqlx::query_scalar(
            "SELECT s.pubky FROM grant_flows f \
             JOIN auth_sessions s ON s.session_id = f.result_auth_session_id",
        )
        .fetch_optional(&app.pool)
        .await
        .expect("session row");
        assert_eq!(pubky, Some(user.public_key().z32()));
    });
}

#[test]
fn a_grant_from_another_homeserver_is_refused_before_anything_is_sent_to_it() {
    on_testnet(|pool| async move {
        let mut network = network().await;
        let configured = network
            .testnet
            .create_random_homeserver()
            .await
            .expect("configured homeserver")
            .public_key();
        // The other homeserver's only endpoint is a listener the test seam
        // lets the service reach.
        let other = PkarrKeypair::random();
        let other_listener = listener().await;
        let user = user_on(
            &network,
            &other,
            Endpoints {
                direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], other_listener.port)),
                ..Endpoints::default()
            },
        )
        .await;

        let (app, authority) = service(
            pool.clone(),
            &network,
            configured,
            EndpointPolicy::PublicOrLoopback,
        )
        .await;
        assert_eq!(
            run_flow(&app, &authority, &network, &user).await,
            (
                "invalid".to_string(),
                Some("homeserver_not_allowed".to_string())
            )
        );
        assert_eq!(
            other_listener.connections(),
            0,
            "nothing was sent to the other homeserver"
        );

        // The same user and endpoint, with that homeserver configured: the
        // exchange is sent there.
        drop(app);
        let (app, authority) = service(
            pool,
            &network,
            other.public_key().into(),
            EndpointPolicy::PublicOrLoopback,
        )
        .await;
        assert_eq!(
            run_flow(&app, &authority, &network, &user).await,
            ("failed".to_string(), Some("grant_exchange".to_string()))
        );
        assert!(
            other_listener.connections() > 0,
            "the configured homeserver's endpoint is reached"
        );
    });
}

#[test]
fn a_grant_from_another_pubky_than_asserted_is_not_exchanged() {
    on_testnet(|pool| async move {
        let network = network().await;
        let homeserver = PkarrKeypair::random();
        let endpoint = listener().await;
        let approver = user_on(
            &network,
            &homeserver,
            Endpoints {
                direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], endpoint.port)),
                ..Endpoints::default()
            },
        )
        .await;
        let (app, authority) = service(
            pool,
            &network,
            homeserver.public_key().into(),
            EndpointPolicy::PublicOrLoopback,
        )
        .await;

        assert_eq!(
            run_flow_approved_by(
                &app,
                &authority,
                &network,
                &Keypair::random().public_key(),
                &approver,
            )
            .await,
            (
                "mismatch".to_string(),
                Some("identity_mismatch".to_string())
            )
        );
        assert_eq!(
            endpoint.connections(),
            0,
            "nothing was sent to the homeserver"
        );

        // The approver's own flow is exchanged at that endpoint.
        assert_eq!(
            run_flow(&app, &authority, &network, &approver).await,
            ("failed".to_string(), Some("grant_exchange".to_string()))
        );
        assert!(
            endpoint.connections() > 0,
            "the homeserver's endpoint is reached"
        );
    });
}

#[test]
fn a_redirect_from_the_homeserver_is_not_followed() {
    on_testnet(|pool| async move {
        let network = network().await;
        let internal = listener().await;
        for scheme in ["http", "https"] {
            let homeserver = PkarrKeypair::random();
            let (port, answered) = redirecting_homeserver(
                &homeserver,
                format!("{scheme}://127.0.0.1:{}/auth/grant/session", internal.port),
            )
            .await;
            let user = user_on(
                &network,
                &homeserver,
                Endpoints {
                    direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], port)),
                    ..Endpoints::default()
                },
            )
            .await;
            let (app, authority) = service(
                pool.clone(),
                &network,
                homeserver.public_key().into(),
                EndpointPolicy::PublicOrLoopback,
            )
            .await;

            assert_eq!(
                run_flow(&app, &authority, &network, &user).await,
                ("failed".to_string(), Some("grant_exchange".to_string()))
            );
            assert!(
                answered.load(Ordering::SeqCst) > 0,
                "the key-pinned homeserver was reached and answered 302"
            );
        }
        assert_eq!(
            internal.connections(),
            0,
            "no redirect target was contacted"
        );
    });
}

#[test]
fn an_answer_for_another_session_than_the_exchanged_grant_is_refused() {
    on_testnet(|pool| async move {
        let network = network().await;
        let homeserver = PkarrKeypair::random();
        let answer = Arc::new(Mutex::new(Answer::Exact));
        let port = answering_homeserver(&homeserver, answer.clone()).await;
        let user = user_on(
            &network,
            &homeserver,
            Endpoints {
                direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], port)),
                ..Endpoints::default()
            },
        )
        .await;
        let (app, authority) = service(
            pool,
            &network,
            homeserver.public_key().into(),
            EndpointPolicy::PublicOrLoopback,
        )
        .await;

        let refused = ("failed".to_string(), Some("grant_exchange".to_string()));
        for (case, expected) in [
            (Answer::Exact, ("complete".to_string(), None)),
            (Answer::OtherPubky, refused.clone()),
            (Answer::OtherHomeserver, refused.clone()),
            (Answer::OtherGrantId, refused.clone()),
            (Answer::OtherClientId, refused.clone()),
        ] {
            *answer.lock().expect("answer") = case;
            assert_eq!(
                run_flow(&app, &authority, &network, &user).await,
                expected,
                "{case:?}"
            );
        }
        let sessions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM auth_sessions")
            .fetch_one(&app.pool)
            .await
            .expect("session count");
        assert_eq!(sessions, 1, "only the exact answer minted a session");
    });
}

#[test]
fn a_record_naming_an_internal_address_is_refused_in_every_form() {
    on_testnet(|pool| async move {
        let network = network().await;
        let internal = listener().await;
        // ICANN IP literals in every form URL parsing reads as an address
        // (and the HTTP client then connects to without resolving), and an
        // ICANN name that resolves to loopback.
        let icann = [
            "127.0.0.1",
            "127.0.0.1.",
            "127.1",
            "2130706433",
            "0x7f.0.0.1",
            "0177.0.0.1",
            "10.1",
            "169.254.43518",
            "localhost",
        ]
        .map(|target| Endpoints {
            icann: Some((target, internal.port)),
            ..Endpoints::default()
        });
        // Direct addresses, which the SDK would probe over TCP: loopback,
        // link-local, private, and loopback and link-local inside IPv6
        // (IPv4-mapped, NAT64, 6to4).
        let direct = [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
            IpAddr::V6(Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0x7f00, 1)),
            IpAddr::V6(Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0xa9fe, 0xa9fe)),
            IpAddr::V6(Ipv6Addr::new(0x2002, 0x7f00, 1, 0, 0, 0, 0, 0)),
            IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)),
        ]
        .map(|address| Endpoints {
            direct: Some((vec![address], internal.port)),
            ..Endpoints::default()
        });

        let mut records = Vec::new();
        let mut cases: Vec<(PublicKey, Keypair)> = Vec::new();
        for endpoints in icann.into_iter().chain(direct) {
            let homeserver = PkarrKeypair::random();
            let (user, user_record) = user_record(&homeserver.public_key().into());
            records.push(endpoint_record(&homeserver, endpoints));
            records.push(user_record);
            cases.push((homeserver.public_key().into(), user));
        }
        publish(&network, records).await;

        for (homeserver, user) in cases {
            let (app, authority) = service(
                pool.clone(),
                &network,
                homeserver,
                EndpointPolicy::PublicOnly,
            )
            .await;
            assert_eq!(
                run_flow(&app, &authority, &network, &user).await,
                ("failed".to_string(), Some("grant_exchange".to_string()))
            );
        }
        assert_eq!(
            internal.connections(),
            0,
            "no internal address was contacted"
        );
    });
}

#[test]
fn a_record_mixing_refused_and_allowed_addresses_is_connected_only_at_the_allowed_one() {
    on_testnet(|pool| async move {
        let network = network().await;
        let endpoint = address_listener().await;
        // Refused addresses that would otherwise reach the listener, listed
        // before the allowed one so an unfiltered connector tries them first.
        let refused: Vec<IpAddr> = host_address()
            .into_iter()
            .chain([IpAddr::V6(Ipv4Addr::new(127, 0, 0, 2).to_ipv6_mapped())])
            .collect();
        let allowed = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let homeserver = PkarrKeypair::random();
        let user = user_on(
            &network,
            &homeserver,
            Endpoints {
                direct: Some((
                    refused.iter().copied().chain([allowed]).collect(),
                    endpoint.port,
                )),
                ..Endpoints::default()
            },
        )
        .await;
        let (app, authority) = service(
            pool,
            &network,
            homeserver.public_key().into(),
            EndpointPolicy::PublicOrLoopback,
        )
        .await;

        assert_eq!(
            run_flow(&app, &authority, &network, &user).await,
            ("failed".to_string(), Some("grant_exchange".to_string()))
        );
        let reached = endpoint.reached();
        assert!(
            reached.contains(&allowed),
            "the allowed address was connected to: {reached:?} (refused: {refused:?})"
        );
        assert!(
            reached.iter().all(|ip| *ip == allowed),
            "no refused address was connected to: {reached:?} (refused: {refused:?})"
        );
    });
}

#[test]
fn a_homeserver_reachable_only_over_plain_http_is_never_sent_a_plain_http_request() {
    on_testnet(|pool| async move {
        let network = network().await;
        let plain = listener().await;
        let homeserver = PkarrKeypair::random();
        let user = user_on(
            &network,
            &homeserver,
            Endpoints {
                icann: Some(("localhost", plain.port)),
                ..Endpoints::default()
            },
        )
        .await;
        let (app, authority) = service(
            pool,
            &network,
            homeserver.public_key().into(),
            EndpointPolicy::PublicOrLoopback,
        )
        .await;

        assert_eq!(
            run_flow(&app, &authority, &network, &user).await,
            ("failed".to_string(), Some("grant_exchange".to_string()))
        );
        assert!(plain.connections() > 0, "the endpoint was tried, over TLS");
        assert_eq!(
            plain.plain_http_requests(),
            0,
            "no plain-HTTP request was sent"
        );
    });
}
