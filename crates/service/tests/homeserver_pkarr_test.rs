//! `HOMESERVER_RESOLUTION=pkarr` against a real local Pubky network: a
//! mainline DHT testnet and `pubky-homeserver` instances from `pubky-testnet`,
//! each with its own key and Pkarr record, holding accounts the SDK signed up
//! and wrote to. The homeservers keep their state in databases they create on
//! the `DATABASE_URL` server.
//!
//! Covered: every read path for an identity on each of two homeservers (and
//! the fixed-URL client missing the one hosted elsewhere); a pubky with no
//! Pkarr record and a homeserver that went down reading `Unavailable`, never
//! `NotFound`; a pubky that moved homeservers read from the new one once its
//! cached record expires; and `listing.sync` plus the deletion follower
//! tombstoning a real delete while leaving an unreachable seller's listings
//! alone.
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
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use pubky_testnet::pubky::pkarr::dns::rdata::SVCB;
use pubky_testnet::pubky::pkarr::{Keypair as PkarrKeypair, SignedPacket};
use pubky_testnet::pubky::{ClientId, Keypair, PubkySession, PublicKey};
use pubky_testnet::pubky_homeserver::storage_config::StorageConfigToml;
use pubky_testnet::pubky_homeserver::{ConfigToml, ConnectionString, HomeserverApp, MockDataDir};
use pubky_testnet::Testnet;
use serde_json::{json, Value};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, PgPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

use common::{execute, new_actor, register_command, sync_command, test_app_with_homeserver_client};
use marketplace_service::clock::Clock;
use marketplace_service::homeserver::{
    DeliverableFetchOutcome, HomeserverEventKind, HomeserverEventsOutcome, HomeserverFetchOutcome,
    HomeserverListingClient, HomeserverRawFetchOutcome, HttpHomeserverClient, PkarrResolutionCache,
    DELIVERABLES_PATH, LISTINGS_PATH, SHOP_PATH,
};
use marketplace_service::listing_deletion::{
    confirm_deleted, confirm_missing_record_deleted, DeletionCheck,
};

const CLIENT_ID: &str = "market.pubky.service-test";
const LISTING_ID: &str = "boots_01";
const PROFILE_ID: &str = "standard";
const DROP_ID: &str = "winter_drop";
const CHALLENGE_ID: &str = "challenge_01";
const DELIVERABLE_ID: &str = "ebook_01";
const LOCK_PATH: &str = "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL names the Postgres server the testnet homeservers use")
}

/// A local DHT. Homeservers it starts create their databases on the
/// `DATABASE_URL` server.
async fn testnet() -> Testnet {
    let postgres = ConnectionString::new(&database_url()).expect("DATABASE_URL is a Postgres URL");
    Testnet::new_with_custom_postgres(postgres)
        .await
        .expect("testnet starts")
}

/// A homeserver on `testnet`'s DHT that the test owns, so it can stop it.
async fn own_homeserver(testnet: &Testnet) -> HomeserverApp {
    let mut config = ConfigToml::minimal_test_config();
    config.general.database_url =
        Some(ConnectionString::new(&database_url()).expect("DATABASE_URL is a Postgres URL"));
    config.pkdns.dht_bootstrap_nodes = Some(testnet.dht_bootstrap_nodes());
    config.storage.backend = StorageConfigToml::InMemory;
    let data_dir = MockDataDir::new(config, Some(Keypair::random())).expect("homeserver data dir");
    HomeserverApp::start_with_mock_data_dir(data_dir)
        .await
        .expect("homeserver starts")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

/// Runs `test` to completion and its runtime to shutdown, then runs
/// `cleanup` on a fresh runtime, and re-raises a panic from `test`.
fn run_then_clean_up<T, C>(test: T, cleanup: C)
where
    T: Future<Output = ()>,
    C: Future<Output = ()>,
{
    let test_runtime = runtime();
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| test_runtime.block_on(test)));
    test_runtime.shutdown_timeout(Duration::from_secs(10));
    runtime().block_on(cleanup);
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

fn on_testnet(test: impl Future<Output = ()>) {
    run_then_clean_up(test, pubky_testnet::drop_test_databases());
}

/// Like [`on_testnet`], with a migrated service database of the test's own
/// on the `DATABASE_URL` server, dropped afterwards.
fn on_testnet_with_service_database<F, T>(test: F)
where
    F: FnOnce(PgPool) -> T,
    T: Future<Output = ()>,
{
    let name = format!("market_pkarr_test_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url()).expect("DATABASE_URL");
    let admin = options.clone();
    let database = options.database(&name);
    run_then_clean_up(
        async {
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
        },
        async {
            pubky_testnet::drop_test_databases().await;
            sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                .execute(&mut admin.connect().await.expect("Postgres"))
                .await
                .expect("drop the service database");
        },
    );
}

/// A `pkarr` client that also connects to loopback, where the testnet's
/// homeservers listen.
fn pkarr_client(testnet: &Testnet, ttl: Duration) -> HttpHomeserverClient {
    HttpHomeserverClient::pkarr_with_loopback_homeservers(
        testnet.client_builder(),
        PkarrResolutionCache { capacity: 64, ttl },
    )
    .expect("pkarr homeserver client builds")
}

/// The production `pkarr` client, on the testnet's DHT: public addresses
/// only.
fn public_only_client(testnet: &Testnet) -> HttpHomeserverClient {
    HttpHomeserverClient::pkarr(testnet.client_builder(), PkarrResolutionCache::default())
        .expect("pkarr homeserver client builds")
}

/// Where a homeserver key's own record says it can be reached.
#[derive(Default)]
struct Endpoints {
    /// Addresses and a port for the key itself (an HTTPS record targeting `.`).
    direct: Option<(Vec<IpAddr>, u16)>,
    /// An ICANN domain and port.
    icann: Option<(&'static str, u16)>,
}

async fn publish(testnet: &Testnet, packet: &SignedPacket) {
    testnet
        .pkarr_client_builder()
        .build()
        .expect("pkarr client")
        .publish(packet)
        .await
        .expect("publish on the testnet DHT");
}

/// Publishes `homeserver`'s endpoint record.
async fn publish_endpoints(testnet: &Testnet, homeserver: &PkarrKeypair, endpoints: Endpoints) {
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
    publish(testnet, &packet.sign(homeserver).expect("endpoint record")).await;
}

/// Publishes a record for a new user naming `homeserver`, and returns the
/// user's pubky.
async fn publish_user_on(testnet: &Testnet, homeserver: &PkarrKeypair) -> String {
    let user = PkarrKeypair::random();
    let target = homeserver.public_key().to_z32();
    let packet = SignedPacket::builder()
        .https(
            "_pubky".try_into().expect("_pubky name"),
            SVCB::new(0, target.as_str().try_into().expect("homeserver name")),
            60,
        )
        .sign(&user)
        .expect("user record");
    publish(testnet, &packet).await;
    user.public_key().to_z32()
}

/// A loopback listener that answers a plain-HTTP `GET` with `200` and a
/// listing record, and counts connections and the plain-HTTP requests among
/// them.
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
                if stream.read_exact(&mut start).await.is_err() || &start != b"GET " {
                    return;
                }
                plain.fetch_add(1, Ordering::SeqCst);
                let body = serde_json::to_vec(&listing_record("x", LISTING_ID, "internal"))
                    .expect("record");
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
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

/// The reads of `pubky` a homeserver at an address the client refuses (or
/// one that answers only with a redirect) must all fail as `Unavailable`.
async fn assert_refused(client: &HttpHomeserverClient, pubky: &str) {
    assert_eq!(
        client.fetch_listing(pubky, LISTING_ID).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_listing_raw(pubky, LISTING_ID).await,
        HomeserverRawFetchOutcome::Unavailable
    );
    assert_eq!(
        client
            .fetch_deliverable_digest(pubky, DELIVERABLE_ID, 1, 1024 * 1024)
            .await,
        DeliverableFetchOutcome::Unavailable
    );
    assert!(
        matches!(
            client
                .listing_events(pubky, LISTINGS_PATH, None, false, 10)
                .await,
            HomeserverEventsOutcome::Unavailable | HomeserverEventsOutcome::Rejected { .. }
        ),
        "never events, never UnknownUser"
    );
    assert_eq!(
        confirm_deleted(client, pubky, LISTING_ID).await,
        DeletionCheck::Unavailable { retry_after: None }
    );
}

/// Signs `keypair` up on `homeserver`, which publishes its Pkarr record
/// naming that homeserver, and returns a session to write with.
async fn account(testnet: &Testnet, keypair: &Keypair, homeserver: &PublicKey) -> PubkySession {
    let signer = testnet.sdk().expect("testnet sdk").signer(keypair.clone());
    signer
        .signup(homeserver, None)
        .await
        .expect("signup on the testnet homeserver");
    signer
        .signin(ClientId::new(CLIENT_ID).expect("client id"))
        .await
        .expect("signin on the testnet homeserver")
}

async fn put(session: &PubkySession, path: &str, body: &[u8]) {
    session
        .storage()
        .put(path, body.to_vec())
        .await
        .unwrap_or_else(|error| panic!("writing {path}: {error}"));
}

fn listing_record(seller: &str, listing_id: &str, title: &str) -> Value {
    json!({
        "recordType": "listing",
        "schemaVersion": 1,
        "ownerPubky": seller,
        "listingId": listing_id,
        "title": title,
        "revision": 1,
        "media": [{ "id": "media_01", "contentHash": "a".repeat(64) }],
        "variants": [{
            "id": "variant_1",
            "enabled": true,
            "quantity": 2,
            "sku": null,
            "options": [],
        }],
        "shippingOptions": [{ "pricing": "free" }],
        "fulfillmentMethods": ["shipping"],
        "sale": {
            "acceptsOffers": true,
            "format": "fixed_price",
            "unitPrice": { "amountMinor": 12_500, "currency": "USD", "exponent": 2 },
        },
    })
}

fn listing_path(listing_id: &str) -> String {
    format!("{LISTINGS_PATH}{listing_id}")
}

/// Everything the service reads from one identity's homeserver.
struct Published {
    pubky: String,
    listing: Vec<u8>,
    profile: Vec<u8>,
    drop: Value,
    shop: Value,
    proof: Value,
    lock: Value,
    deliverable: Vec<u8>,
}

async fn publish_everything(session: &PubkySession, pubky: &str, tag: &str) -> Published {
    let published = Published {
        pubky: pubky.to_string(),
        listing: serde_json::to_vec(&listing_record(pubky, LISTING_ID, tag)).expect("listing"),
        profile: format!(r#"{{"id":"{PROFILE_ID}","name":"{tag}"}}"#).into_bytes(),
        drop: json!({ "dropId": DROP_ID, "ownerPubky": pubky, "title": tag }),
        shop: json!({ "ownerPubky": pubky, "name": tag }),
        proof: json!({ "challengeId": CHALLENGE_ID, "pubky": pubky }),
        lock: json!({ "creator": pubky, "title": tag }),
        deliverable: format!("ciphertext of {tag}").repeat(64).into_bytes(),
    };
    put(session, &listing_path(LISTING_ID), &published.listing).await;
    put(
        session,
        &format!(
            "{}{PROFILE_ID}",
            marketplace_service::shipping::PROFILE_PATH
        ),
        &published.profile,
    )
    .await;
    put(
        session,
        &format!("/pub/pubky.app/marketplace/v1/drops/{DROP_ID}"),
        &serde_json::to_vec(&published.drop).expect("drop"),
    )
    .await;
    put(
        session,
        SHOP_PATH,
        &serde_json::to_vec(&published.shop).expect("shop"),
    )
    .await;
    put(
        session,
        &format!(
            "{}{CHALLENGE_ID}",
            marketplace_service::grant_session::PROOF_DOCUMENTS_PATH
        ),
        &serde_json::to_vec(&published.proof).expect("proof"),
    )
    .await;
    put(
        session,
        LOCK_PATH,
        &serde_json::to_vec(&published.lock).expect("lock"),
    )
    .await;
    put(
        session,
        &format!("{DELIVERABLES_PATH}{DELIVERABLE_ID}/1"),
        &published.deliverable,
    )
    .await;
    published
}

async fn assert_reads_everything(client: &HttpHomeserverClient, published: &Published) {
    let pubky = published.pubky.as_str();
    let listing: Value = serde_json::from_slice(&published.listing).expect("listing JSON");
    assert_eq!(
        client.fetch_listing(pubky, LISTING_ID).await,
        HomeserverFetchOutcome::Found(listing.clone())
    );
    assert_eq!(
        client.fetch_listing_bytes(pubky, LISTING_ID).await,
        HomeserverFetchOutcome::Found(listing)
    );
    assert_eq!(
        client.fetch_listing_raw(pubky, LISTING_ID).await,
        HomeserverRawFetchOutcome::Found(published.listing.clone())
    );
    assert_eq!(
        client.fetch_shipping_profile_raw(pubky, PROFILE_ID).await,
        HomeserverRawFetchOutcome::Found(published.profile.clone())
    );
    assert_eq!(
        client.fetch_drop(pubky, DROP_ID).await,
        HomeserverFetchOutcome::Found(published.drop.clone())
    );
    assert_eq!(
        client.fetch_shop(pubky).await,
        HomeserverFetchOutcome::Found(published.shop.clone())
    );
    assert_eq!(
        client.fetch_grant_session_proof(pubky, CHALLENGE_ID).await,
        HomeserverFetchOutcome::Found(published.proof.clone())
    );
    assert_eq!(
        client.fetch_content_lock(pubky, LOCK_PATH).await,
        HomeserverFetchOutcome::Found(published.lock.clone())
    );
    assert_eq!(
        client
            .fetch_deliverable_digest(pubky, DELIVERABLE_ID, 1, 1024 * 1024)
            .await,
        DeliverableFetchOutcome::Found {
            blake3: blake3::hash(&published.deliverable).to_hex().to_string(),
            len: published.deliverable.len() as u64,
        }
    );
    assert_eq!(
        client
            .fetch_deliverable_digest(pubky, DELIVERABLE_ID, 1, 16)
            .await,
        DeliverableFetchOutcome::TooLarge,
        "the size cap holds on the resolved homeserver"
    );
    assert_eq!(
        client.fetch_listing(pubky, "never_published").await,
        HomeserverFetchOutcome::NotFound,
        "a missing record on the pubky's own homeserver is NotFound"
    );
    let HomeserverEventsOutcome::Events { events, .. } = client
        .listing_events(pubky, LISTINGS_PATH, None, false, 10)
        .await
    else {
        panic!("the event stream of {pubky}'s homeserver answers");
    };
    let uri = format!("pubky://{pubky}{}", listing_path(LISTING_ID));
    assert!(
        events
            .iter()
            .any(|event| event.kind == HomeserverEventKind::Put && event.uri == uri),
        "{events:?}"
    );
}

/// Every read a `pkarr` client makes for a pubky it cannot reach.
async fn assert_unavailable_everywhere(client: &HttpHomeserverClient, pubky: &str) {
    assert_eq!(
        client.fetch_listing(pubky, LISTING_ID).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_listing_bytes(pubky, LISTING_ID).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_listing_raw(pubky, LISTING_ID).await,
        HomeserverRawFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_shipping_profile_raw(pubky, PROFILE_ID).await,
        HomeserverRawFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_drop(pubky, DROP_ID).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_shop(pubky).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_grant_session_proof(pubky, CHALLENGE_ID).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client.fetch_content_lock(pubky, LOCK_PATH).await,
        HomeserverFetchOutcome::Unavailable
    );
    assert_eq!(
        client
            .fetch_deliverable_digest(pubky, DELIVERABLE_ID, 1, 1024 * 1024)
            .await,
        DeliverableFetchOutcome::Unavailable
    );
    assert_eq!(
        client
            .listing_events(pubky, LISTINGS_PATH, None, false, 10)
            .await,
        HomeserverEventsOutcome::Unavailable,
        "never UnknownUser, which the follower reads as an unhosted seller"
    );
    assert_eq!(
        confirm_deleted(client, pubky, LISTING_ID).await,
        DeletionCheck::Unavailable { retry_after: None }
    );
}

/// Waits for a stopped homeserver's connections to close. Until then a read
/// may still be answered, but it must never be `NotFound`.
async fn wait_until_unreachable(client: &HttpHomeserverClient, pubky: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.fetch_listing(pubky, LISTING_ID).await {
            HomeserverFetchOutcome::Unavailable => return,
            HomeserverFetchOutcome::Found(_) => {}
            HomeserverFetchOutcome::NotFound => {
                panic!("a stopped homeserver must not read as a deleted record")
            }
        }
        assert!(
            Instant::now() < deadline,
            "the stopped homeserver kept answering"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn reads_each_pubky_from_the_homeserver_its_pkarr_record_names() {
    on_testnet(async {
        let mut testnet = testnet().await;
        let first = testnet
            .create_random_homeserver()
            .await
            .expect("first homeserver")
            .public_key();
        let (second, second_url) = {
            let app = testnet
                .create_random_homeserver()
                .await
                .expect("second homeserver");
            (app.public_key(), app.icann_http_url().to_string())
        };
        assert_ne!(first, second);

        let seller = Keypair::random();
        let buyer = Keypair::random();
        let seller_pubky = seller.public_key().z32();
        let buyer_pubky = buyer.public_key().z32();
        let seller_session = account(&testnet, &seller, &first).await;
        let buyer_session = account(&testnet, &buyer, &second).await;
        let on_first = publish_everything(&seller_session, &seller_pubky, "first").await;
        let on_second = publish_everything(&buyer_session, &buyer_pubky, "second").await;

        let client = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);
        assert_reads_everything(&client, &on_first).await;
        assert_reads_everything(&client, &on_second).await;

        // The fixed client reads only what its one homeserver hosts.
        let fixed = HttpHomeserverClient::new(&second_url).expect("fixed client");
        assert_reads_everything(&fixed, &on_second).await;
        assert_eq!(
            fixed.fetch_listing(&seller_pubky, LISTING_ID).await,
            HomeserverFetchOutcome::NotFound
        );

        // A delete on either homeserver is proven from that homeserver.
        for (session, pubky) in [
            (&seller_session, &seller_pubky),
            (&buyer_session, &buyer_pubky),
        ] {
            session
                .storage()
                .delete(listing_path(LISTING_ID))
                .await
                .expect("delete the listing record");
            assert_eq!(
                client.fetch_listing(pubky, LISTING_ID).await,
                HomeserverFetchOutcome::NotFound
            );
            assert!(
                matches!(
                    confirm_deleted(&client, pubky, LISTING_ID).await,
                    DeletionCheck::Deleted { .. }
                ),
                "the DEL is read from {pubky}'s homeserver"
            );
        }
    });
}

#[test]
fn a_pubky_with_no_pkarr_record_is_unavailable_never_not_found() {
    on_testnet(async {
        let mut testnet = testnet().await;
        testnet
            .create_random_homeserver()
            .await
            .expect("homeserver");
        let client = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);

        let unpublished = Keypair::random().public_key().z32();
        assert_unavailable_everywhere(&client, &unpublished).await;
        assert_unavailable_everywhere(&client, "not-a-pubky").await;
    });
}

#[test]
fn a_homeserver_that_goes_down_is_unavailable_never_not_found() {
    on_testnet(async {
        let testnet = testnet().await;
        let homeserver = own_homeserver(&testnet).await;
        let seller = Keypair::random();
        let seller_pubky = seller.public_key().z32();
        let session = account(&testnet, &seller, &homeserver.public_key()).await;
        let published = publish_everything(&session, &seller_pubky, "down").await;
        let warm = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);
        assert_reads_everything(&warm, &published).await;

        drop(session);
        drop(homeserver);
        // A client that resolved the homeserver before it stopped, and one that
        // resolves it now from the record still on the DHT.
        wait_until_unreachable(&warm, &seller_pubky).await;
        assert_unavailable_everywhere(&warm, &seller_pubky).await;
        let cold = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);
        assert_unavailable_everywhere(&cold, &seller_pubky).await;
    });
}

#[test]
fn a_pubky_that_moved_is_read_from_its_new_homeserver_once_its_record_expires() {
    on_testnet(async {
        let mut testnet = testnet().await;
        let old = testnet
            .create_random_homeserver()
            .await
            .expect("old homeserver")
            .public_key();
        let new = testnet
            .create_random_homeserver()
            .await
            .expect("new homeserver")
            .public_key();
        let seller = Keypair::random();
        let seller_pubky = seller.public_key().z32();
        let old_session = account(&testnet, &seller, &old).await;
        let old_record = listing_record(&seller_pubky, LISTING_ID, "on the old homeserver");
        put(
            &old_session,
            &listing_path(LISTING_ID),
            &serde_json::to_vec(&old_record).expect("record"),
        )
        .await;

        let ttl = Duration::from_secs(10);
        let client = pkarr_client(&testnet, ttl);
        let resolved_at = Instant::now();
        assert_eq!(
            client.fetch_listing(&seller_pubky, LISTING_ID).await,
            HomeserverFetchOutcome::Found(old_record.clone())
        );

        let new_session = account(&testnet, &seller, &new).await;
        let new_record = listing_record(&seller_pubky, LISTING_ID, "on the new homeserver");
        put(
            &new_session,
            &listing_path(LISTING_ID),
            &serde_json::to_vec(&new_record).expect("record"),
        )
        .await;
        assert!(
            resolved_at.elapsed() < ttl,
            "the move finished inside the cache TTL"
        );
        assert_eq!(
            client.fetch_listing(&seller_pubky, LISTING_ID).await,
            HomeserverFetchOutcome::Found(old_record),
            "the cached record still names the old homeserver"
        );

        tokio::time::sleep_until((resolved_at + ttl + Duration::from_secs(2)).into()).await;
        assert_eq!(
            client.fetch_listing(&seller_pubky, LISTING_ID).await,
            HomeserverFetchOutcome::Found(new_record),
            "the expired record is resolved again"
        );
    });
}

async fn deletion_failures(pool: &PgPool, seller: &str) -> Option<(i32, bool, bool)> {
    sqlx::query_as(
        "SELECT failures, polled_at IS NOT NULL, retry_not_before IS NOT NULL \
         FROM listing_deletion_cursors WHERE seller_pubky = $1",
    )
    .bind(seller)
    .fetch_optional(pool)
    .await
    .expect("deletion cursor")
}

async fn deleted_at_is_set(pool: &PgPool, seller: &str, listing_id: &str) -> bool {
    sqlx::query_scalar("SELECT deleted_at IS NOT NULL FROM listings WHERE aggregate_id = $1")
        .bind(format!("listing:{seller}_{listing_id}"))
        .fetch_one(pool)
        .await
        .expect("listing row")
}

#[test]
fn sync_and_the_follower_tombstone_deletes_but_not_unreachable_sellers() {
    on_testnet_with_service_database(|pool| async move {
        let mut testnet = testnet().await;
        let kept = testnet
            .create_random_homeserver()
            .await
            .expect("homeserver")
            .public_key();
        let stopping = own_homeserver(&testnet).await;
        let client = Arc::new(pkarr_client(&testnet, PkarrResolutionCache::default().ttl));
        let app = test_app_with_homeserver_client(pool, client.clone()).await;
        let mut command_numbers = 1..;
        let mut next_command = || command_numbers.next().expect("command numbers");

        // Three sellers: one whose homeserver stays up and who deletes a listing,
        // one whose homeserver goes down, one with no Pkarr record at all.
        let deleting = new_actor(&app).await;
        let stranded = new_actor(&app).await;
        let unpublished = new_actor(&app).await;
        let deleting_session = account(&testnet, &deleting.keypair, &kept).await;
        let stranded_session = account(&testnet, &stranded.keypair, &stopping.public_key()).await;
        for (session, seller) in [
            (&deleting_session, &deleting),
            (&stranded_session, &stranded),
        ] {
            for listing_id in [LISTING_ID, "boots_02"] {
                let record = listing_record(&seller.pubky, listing_id, "Winter boots");
                put(
                    session,
                    &listing_path(listing_id),
                    &serde_json::to_vec(&record).expect("record"),
                )
                .await;
                let (status, body) = execute(
                    &app,
                    &seller.token,
                    &sync_command(&seller.pubky, listing_id, next_command()),
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "sync registers {listing_id}: {body}"
                );
                assert_eq!(body["result"]["listing"]["title"], json!("Winter boots"));
            }
        }
        let (status, body) = execute(
            &app,
            &unpublished.token,
            &register_command(&unpublished.pubky, 2),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "register: {body}");

        // The deleting seller removes both listings on their homeserver; the
        // stranded seller's homeserver stops.
        for listing_id in [LISTING_ID, "boots_02"] {
            deleting_session
                .storage()
                .delete(listing_path(listing_id))
                .await
                .expect("delete the listing record");
        }
        drop(stranded_session);
        drop(stopping);
        wait_until_unreachable(&client, &stranded.pubky).await;

        // `listing.sync` tombstones the proven delete and refuses the others.
        let (status, body) = execute(
            &app,
            &deleting.token,
            &sync_command(&deleting.pubky, LISTING_ID, next_command()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["result"]["kind"], json!("listing_deleted"), "{body}");
        for seller in [&stranded, &unpublished] {
            let (status, body) = execute(
                &app,
                &seller.token,
                &sync_command(&seller.pubky, LISTING_ID, next_command()),
            )
            .await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
            assert_eq!(body["error"]["code"], json!("UPSTREAM_UNAVAILABLE"));
        }

        // The follower tombstones the other delete it reads on the seller's
        // homeserver, and backs off from the sellers it cannot reach.
        let summary =
            marketplace_service::workers::run_once(&app.state, Uuid::new_v4(), app.clock.now())
                .await
                .expect("worker pass");
        assert_eq!(summary.listings_tombstoned, 1);
        assert!(deleted_at_is_set(&app.pool, &deleting.pubky, "boots_02").await);
        assert_eq!(
            deletion_failures(&app.pool, &deleting.pubky).await,
            Some((0, true, false))
        );
        for seller in [&stranded, &unpublished] {
            assert!(!deleted_at_is_set(&app.pool, &seller.pubky, LISTING_ID).await);
            assert_eq!(
                deletion_failures(&app.pool, &seller.pubky).await,
                Some((1, false, true)),
                "an unreachable seller is a failed poll, not an unhosted one"
            );
        }
        assert!(!deleted_at_is_set(&app.pool, &stranded.pubky, "boots_02").await);
    });
}

/// The follower's saved position for `seller`: its cursor and the
/// homeserver it counts on.
async fn follower_position(pool: &PgPool, seller: &str) -> (Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT event_cursor, homeserver FROM listing_deletion_cursors WHERE seller_pubky = $1",
    )
    .bind(seller)
    .fetch_one(pool)
    .await
    .expect("deletion cursor")
}

fn cursor_number(cursor: &str) -> u64 {
    cursor
        .parse()
        .expect("a homeserver event cursor is a number")
}

#[test]
fn the_follower_restarts_a_seller_who_moved_and_tombstones_a_delete_below_the_old_cursor() {
    on_testnet_with_service_database(|pool| async move {
        let mut testnet = testnet().await;
        let old = testnet
            .create_random_homeserver()
            .await
            .expect("old homeserver")
            .public_key();
        let new = testnet
            .create_random_homeserver()
            .await
            .expect("new homeserver")
            .public_key();
        let ttl = Duration::from_secs(20);
        let client = Arc::new(pkarr_client(&testnet, ttl));
        let app = test_app_with_homeserver_client(pool, client.clone()).await;
        let seller = new_actor(&app).await;
        let kept_id = "boots_02";
        // The service's client caches the seller's record no earlier than this.
        let first_resolution = Instant::now();

        // Two listings registered from the old homeserver, whose stream then
        // runs well past anything the new homeserver will hold.
        let old_session = account(&testnet, &seller.keypair, &old).await;
        for (number, listing_id) in [LISTING_ID, kept_id].into_iter().enumerate() {
            let record = listing_record(&seller.pubky, listing_id, "Winter boots");
            put(
                &old_session,
                &listing_path(listing_id),
                &serde_json::to_vec(&record).expect("record"),
            )
            .await;
            let (status, body) = execute(
                &app,
                &seller.token,
                &sync_command(&seller.pubky, listing_id, number as u64 + 1),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::OK,
                "sync registers {listing_id}: {body}"
            );
        }
        let kept = serde_json::to_vec(&listing_record(&seller.pubky, kept_id, "Winter boots"))
            .expect("record");
        for _ in 0..20 {
            put(&old_session, &listing_path(kept_id), &kept).await;
        }

        let holder = Uuid::new_v4();
        let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
            .await
            .expect("worker pass on the old homeserver");
        assert_eq!(summary.listings_tombstoned, 0);
        let (old_cursor, old_homeserver) = follower_position(&app.pool, &seller.pubky).await;
        let old_cursor = old_cursor.expect("the follower read the old homeserver");
        assert_eq!(old_homeserver, Some(old.z32()));

        // The seller moves, re-publishes both listings on the new homeserver
        // and deletes one there. `boots_02` is deleted and re-created, so its
        // DEL is not the latest event for it.
        let moved_at = Instant::now();
        let new_session = account(&testnet, &seller.keypair, &new).await;
        put(
            &new_session,
            &listing_path(LISTING_ID),
            &serde_json::to_vec(&listing_record(&seller.pubky, LISTING_ID, "Winter boots"))
                .expect("record"),
        )
        .await;
        put(&new_session, &listing_path(kept_id), &kept).await;
        new_session
            .storage()
            .delete(listing_path(LISTING_ID))
            .await
            .expect("delete the listing record on the new homeserver");
        new_session
            .storage()
            .delete(listing_path(kept_id))
            .await
            .expect("delete the kept record");
        put(&new_session, &listing_path(kept_id), &kept).await;

        // Read through a client with no cached record: the new homeserver's
        // DEL is numbered below the position saved on the old one.
        let HomeserverEventsOutcome::Events {
            homeserver: answered,
            events,
        } = pkarr_client(&testnet, ttl)
            .listing_events(&seller.pubky, LISTINGS_PATH, None, false, 100)
            .await
        else {
            panic!("the new homeserver's event stream answers");
        };
        assert_eq!(answered, Some(new.z32()));
        let deleted_uri = format!("pubky://{}{}", seller.pubky, listing_path(LISTING_ID));
        let delete = events
            .iter()
            .find(|event| event.kind == HomeserverEventKind::Del && event.uri == deleted_uri)
            .expect("the DEL on the new homeserver");
        assert!(
            cursor_number(&delete.cursor) < cursor_number(&old_cursor),
            "the DEL at {} must sit below the old position {old_cursor}",
            delete.cursor
        );
        let last_new_cursor = events.last().expect("events").cursor.clone();

        // A pass while the cached record still names the old homeserver
        // reads nothing new there and keeps the old position.
        app.clock.advance_seconds(61);
        let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
            .await
            .expect("worker pass with the cached record");
        assert!(
            first_resolution.elapsed() < ttl,
            "the pass ran inside the cache TTL"
        );
        assert_eq!(summary.listings_tombstoned, 0);
        assert_eq!(
            follower_position(&app.pool, &seller.pubky).await,
            (Some(old_cursor.clone()), Some(old.z32()))
        );

        // Once the record expires, the follower reads the new homeserver,
        // restarts there and tombstones the delete it would have skipped.
        tokio::time::sleep_until((moved_at + ttl + Duration::from_secs(2)).into()).await;
        app.clock.advance_seconds(61);
        let summary = marketplace_service::workers::run_once(&app.state, holder, app.clock.now())
            .await
            .expect("worker pass on the new homeserver");
        assert_eq!(summary.listings_tombstoned, 1);
        let tombstone: (bool, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT deleted_at IS NOT NULL, deleted_event_cursor, deleted_event_homeserver \
             FROM listings WHERE aggregate_id = $1",
        )
        .bind(format!("listing:{}_{LISTING_ID}", seller.pubky))
        .fetch_one(&app.pool)
        .await
        .expect("listing row");
        assert_eq!(
            tombstone,
            (true, Some(delete.cursor.clone()), Some(new.z32()))
        );
        assert!(
            !deleted_at_is_set(&app.pool, &seller.pubky, kept_id).await,
            "a listing re-created on the new homeserver stays live"
        );
        assert_eq!(
            follower_position(&app.pool, &seller.pubky).await,
            (Some(last_new_cursor), Some(new.z32())),
            "the position now counts on the new homeserver"
        );
    });
}

#[test]
fn a_redirect_from_a_homeserver_is_not_followed() {
    on_testnet(async {
        let testnet = testnet().await;
        let internal = listener().await;
        let client = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);
        for scheme in ["http", "https"] {
            let homeserver = PkarrKeypair::random();
            let (port, answered) = redirecting_homeserver(
                &homeserver,
                format!(
                    "{scheme}://127.0.0.1:{}{}",
                    internal.port,
                    listing_path(LISTING_ID)
                ),
            )
            .await;
            publish_endpoints(
                &testnet,
                &homeserver,
                Endpoints {
                    direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], port)),
                    ..Endpoints::default()
                },
            )
            .await;
            let pubky = publish_user_on(&testnet, &homeserver).await;

            assert_refused(&client, &pubky).await;
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
fn a_record_naming_an_internal_address_is_refused() {
    on_testnet(async {
        let mut testnet = testnet().await;
        let real = testnet
            .create_random_homeserver()
            .await
            .expect("homeserver")
            .public_key();
        let seller = Keypair::random();
        let seller_pubky = seller.public_key().z32();
        let session = account(&testnet, &seller, &real).await;
        let record = listing_record(&seller_pubky, LISTING_ID, "on loopback");
        put(
            &session,
            &listing_path(LISTING_ID),
            &serde_json::to_vec(&record).expect("record"),
        )
        .await;
        let production = public_only_client(&testnet);

        // The testnet homeserver publishes only loopback endpoints: readable
        // through the test seam, refused by the production client.
        assert_eq!(
            pkarr_client(&testnet, PkarrResolutionCache::default().ttl)
                .fetch_listing(&seller_pubky, LISTING_ID)
                .await,
            HomeserverFetchOutcome::Found(record)
        );
        assert_refused(&production, &seller_pubky).await;

        // Direct addresses (IPv4, IPv4-mapped IPv6), ICANN IP literals in every
        // form URL parsing reads as an address (and the HTTP client then
        // connects to without resolving), and an ICANN name that resolves to
        // loopback: no connection is made to any.
        let internal = listener().await;
        let literals = [
            "127.0.0.1",
            "127.0.0.1.",
            "127.1",
            "2130706433",
            "0x7f.0.0.1",
            "0177.0.0.1",
            "10.1",
            "169.254.43518",
        ]
        .map(|literal| Endpoints {
            icann: Some((literal, internal.port)),
            ..Endpoints::default()
        });
        for endpoints in literals.into_iter().chain([
            Endpoints {
                direct: Some((vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], internal.port)),
                ..Endpoints::default()
            },
            Endpoints {
                direct: Some((
                    vec![IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped())],
                    internal.port,
                )),
                ..Endpoints::default()
            },
            Endpoints {
                icann: Some(("localhost", internal.port)),
                ..Endpoints::default()
            },
        ]) {
            let homeserver = PkarrKeypair::random();
            publish_endpoints(&testnet, &homeserver, endpoints).await;
            let pubky = publish_user_on(&testnet, &homeserver).await;
            assert_refused(&production, &pubky).await;
        }
        assert_eq!(
            internal.connections(),
            0,
            "no internal address was contacted"
        );

        // Private, link-local and shared addresses stay refused even where the
        // test seam allows loopback; so does loopback in its IPv4-mapped form.
        let seam = pkarr_client(&testnet, PkarrResolutionCache::default().ttl);
        for address in [
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ] {
            let homeserver = PkarrKeypair::random();
            publish_endpoints(
                &testnet,
                &homeserver,
                Endpoints {
                    direct: Some((vec![address], internal.port)),
                    ..Endpoints::default()
                },
            )
            .await;
            let pubky = publish_user_on(&testnet, &homeserver).await;
            assert_refused(&seam, &pubky).await;
        }
        assert_eq!(
            internal.connections(),
            0,
            "no internal address was contacted"
        );
    });
}

#[test]
fn a_homeserver_reachable_only_over_plain_http_is_refused() {
    on_testnet(async {
        let testnet = testnet().await;
        // An ICANN endpoint whose port serves the record over plain HTTP, as the
        // testnet homeservers' `localhost` endpoints do.
        let plain = listener().await;
        let homeserver = PkarrKeypair::random();
        publish_endpoints(
            &testnet,
            &homeserver,
            Endpoints {
                icann: Some(("localhost", plain.port)),
                ..Endpoints::default()
            },
        )
        .await;
        let pubky = publish_user_on(&testnet, &homeserver).await;

        assert_refused(
            &pkarr_client(&testnet, PkarrResolutionCache::default().ttl),
            &pubky,
        )
        .await;
        assert!(plain.connections() > 0, "the endpoint was tried, over TLS");
        assert_eq!(
            plain.plain_http_requests(),
            0,
            "no plain-HTTP request was sent"
        );
    });
}

#[test]
fn a_stale_cache_naming_the_old_homeserver_cannot_prove_a_delete_there() {
    on_testnet_with_service_database(|pool| async move {
        let mut testnet = testnet().await;
        let old = testnet
            .create_random_homeserver()
            .await
            .expect("old homeserver")
            .public_key();
        let new = testnet
            .create_random_homeserver()
            .await
            .expect("new homeserver")
            .public_key();
        let ttl = Duration::from_secs(30);
        let client = Arc::new(pkarr_client(&testnet, ttl));
        let app = test_app_with_homeserver_client(pool, client.clone()).await;
        let seller = new_actor(&app).await;
        // Two more clients that resolve the seller while the record names the
        // old homeserver, one per proof function.
        let for_confirm = pkarr_client(&testnet, ttl);
        let for_missing = pkarr_client(&testnet, ttl);

        let old_session = account(&testnet, &seller.keypair, &old).await;
        let record = serde_json::to_vec(&listing_record(&seller.pubky, LISTING_ID, "Winter boots"))
            .expect("record");
        put(&old_session, &listing_path(LISTING_ID), &record).await;
        let cached_at = Instant::now();
        let (status, body) = execute(
            &app,
            &seller.token,
            &sync_command(&seller.pubky, LISTING_ID, 1),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "sync registers the listing: {body}");
        for warm in [&for_confirm, &for_missing] {
            assert!(matches!(
                warm.fetch_listing(&seller.pubky, LISTING_ID).await,
                HomeserverFetchOutcome::Found(_)
            ));
        }

        // The listing is deleted on the old homeserver, and the seller moves to
        // the new one, where the listing lives on.
        old_session
            .storage()
            .delete(listing_path(LISTING_ID))
            .await
            .expect("delete on the old homeserver");
        let new_session = account(&testnet, &seller.keypair, &new).await;
        put(&new_session, &listing_path(LISTING_ID), &record).await;

        // The follower reads the old homeserver's DEL through its stale cache,
        // and its proof, read from where the seller's record names now, holds
        // no delete.
        let summary =
            marketplace_service::workers::run_once(&app.state, Uuid::new_v4(), app.clock.now())
                .await
                .expect("worker pass");
        assert_eq!(summary.listings_tombstoned, 0);
        assert!(!deleted_at_is_set(&app.pool, &seller.pubky, LISTING_ID).await);

        // Each proof function, from a client whose cache still names the old
        // homeserver.
        assert_eq!(
            for_missing.fetch_listing(&seller.pubky, LISTING_ID).await,
            HomeserverFetchOutcome::NotFound,
            "the cached record still names the old homeserver"
        );
        assert_eq!(
            confirm_missing_record_deleted(&for_missing, &seller.pubky, LISTING_ID).await,
            DeletionCheck::NotDeleted
        );
        assert_eq!(
            confirm_deleted(&for_confirm, &seller.pubky, LISTING_ID).await,
            DeletionCheck::NotDeleted
        );
        assert!(
            cached_at.elapsed() < ttl,
            "every read ran while the old homeserver was cached"
        );
    });
}
