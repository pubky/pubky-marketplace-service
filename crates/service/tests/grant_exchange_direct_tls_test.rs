//! The grant exchange's TLS handshake with the configured homeserver's
//! direct endpoint reads no record: the key the request is addressed to is
//! the only key accepted. Every record lookup goes to a relay on loopback
//! that counts it, through a Pkarr client that caches nothing, so a lookup
//! made during a handshake cannot be answered from memory unseen.
//!
//! Covered: the homeserver presenting its own key completes the exchange
//! while the relay is silent; another key is refused at once, with no
//! lookup during the handshake; and a key the homeserver's record delegates
//! to is refused with no lookup during the handshake.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use marketplace_service::grant_exchange::{ExchangeRefusal, GrantExchange};
use marketplace_service::homeserver_transport::EndpointPolicy;
use pubky::pkarr::dns::rdata::SVCB;
use pubky::pkarr::{self, SignedPacket};
use pubky::{GrantClaims, Keypair, PublicKey};
use pubky_common::auth::grant_session_responses::{GrantSessionInfo, GrantSessionResponse};
use pubky_common::auth::jws::{ClientId, GrantId, GRANT_JWS_TYP};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;

/// How long the Pkarr client waits for a relay answer. A handshake that made
/// a lookup at a relay gone silent takes at least this.
const RELAY_TIMEOUT: Duration = Duration::from_secs(2);

/// A Pkarr relay on loopback serving the records it was given and counting
/// the lookups it receives. Once silenced it answers none.
#[derive(Clone)]
struct Relay {
    url: String,
    records: Arc<Mutex<HashMap<String, SignedPacket>>>,
    lookups: Arc<AtomicUsize>,
    silent: Arc<AtomicBool>,
}

impl Relay {
    async fn start() -> Self {
        let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let relay = Self {
            url: format!("http://{}", socket.local_addr().expect("address")),
            records: Arc::default(),
            lookups: Arc::default(),
            silent: Arc::default(),
        };
        let serving = relay.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = socket.accept().await {
                let relay = serving.clone();
                tokio::spawn(async move {
                    let Some(head) = request_head(&mut stream).await else {
                        return;
                    };
                    relay.lookups.fetch_add(1, Ordering::SeqCst);
                    if relay.silent.load(Ordering::SeqCst) {
                        let _ = stream.read(&mut [0u8; 1]).await;
                        return;
                    }
                    let _ = stream.write_all(&relay.answer(&head)).await;
                });
            }
        });
        relay
    }

    fn serve(&self, packet: SignedPacket) {
        self.records
            .lock()
            .expect("records")
            .insert(packet.public_key().to_z32(), packet);
    }

    fn answer(&self, head: &str) -> Vec<u8> {
        let path = head.split_whitespace().nth(1).unwrap_or_default();
        let key = path.split('?').next().unwrap_or_default();
        let key = key.rsplit('/').next().unwrap_or_default();
        match self.records.lock().expect("records").get(key) {
            Some(packet) => {
                let body = packet.to_relay_payload();
                let mut answer = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                answer.extend_from_slice(&body);
                answer
            }
            None => {
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
            }
        }
    }

    fn lookups(&self) -> usize {
        self.lookups.load(Ordering::SeqCst)
    }

    /// A Pkarr client that reads records only from this relay and caches
    /// none.
    fn client(&self) -> pkarr::Client {
        pkarr::Client::builder()
            .no_default_network()
            .relays(&[self.url.as_str()])
            .expect("relay URL")
            .cache_size(0)
            .relay_request_timeout(RELAY_TIMEOUT)
            .build()
            .expect("pkarr client")
    }
}

/// The head of one HTTP/1.1 request.
async fn request_head<S: AsyncRead + Unpin>(stream: &mut S) -> Option<String> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => head.extend_from_slice(&buffer[..read]),
        }
    }
    Some(String::from_utf8_lossy(&head).into_owned())
}

/// Reads the rest of a request whose head is `head`, by its
/// `Content-Length`, so the answer does not race the body.
async fn drain_body<S: AsyncRead + Unpin>(stream: &mut S, head: &str) -> Option<()> {
    let (headers, read) = head.split_once("\r\n\r\n")?;
    let length: usize = headers
        .to_ascii_lowercase()
        .lines()
        .find_map(|line| line.strip_prefix("content-length:").map(str::to_owned))?
        .trim()
        .parse()
        .ok()?;
    let mut remaining = length.checked_sub(read.len())?;
    let mut buffer = [0u8; 4096];
    while remaining > 0 {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => remaining = remaining.saturating_sub(read),
        }
    }
    Some(())
}

/// One TLS handshake the homeserver ran: whether it completed, the relay
/// lookups made while it ran, and how long it took.
#[derive(Debug, Clone, Copy)]
struct Handshake {
    completed: bool,
    lookups: usize,
    took: Duration,
}

/// A grant `user` approved for a new client key.
struct Grant {
    jws: String,
    claims: GrantClaims,
    client: Keypair,
}

fn grant(user: &Keypair) -> Grant {
    let client = Keypair::random();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let claims = GrantClaims {
        iss: user.public_key(),
        client_id: ClientId::new("marketplace.example").expect("client id"),
        caps: Vec::new(),
        cnf: client.public_key(),
        jti: GrantId::generate(),
        iat: now,
        exp: now + 3600,
    };
    Grant {
        jws: claims.sign(user, GRANT_JWS_TYP),
        claims,
        client,
    }
}

/// A homeserver on loopback presenting `presented`'s raw public key, that
/// answers an exchange with the session of `claims` at `homeserver`. When
/// `silence` is set, a connection silences `relay` before the handshake
/// starts.
async fn homeserver_presenting(
    presented: &pkarr::Keypair,
    homeserver: PublicKey,
    claims: &GrantClaims,
    relay: &Relay,
    silence: bool,
) -> (u16, Arc<Mutex<Vec<Handshake>>>) {
    let socket = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = socket.local_addr().expect("address").port();
    let acceptor = TlsAcceptor::from(Arc::new(presented.to_rpk_rustls_server_config()));
    let body = serde_json::to_vec(&GrantSessionResponse {
        token: "test-bearer".to_string(),
        session: GrantSessionInfo {
            homeserver,
            pubky: claims.iss.clone(),
            client_id: claims.client_id.clone(),
            capabilities: claims.caps.clone(),
            grant_id: claims.jti.clone(),
            token_expires_at: claims.exp,
            grant_expires_at: claims.exp,
            created_at: claims.iat,
        },
    })
    .expect("session JSON");
    let handshakes = Arc::new(Mutex::new(Vec::new()));
    let (relay, recorded) = (relay.clone(), handshakes.clone());
    tokio::spawn(async move {
        while let Ok((stream, _)) = socket.accept().await {
            if silence {
                relay.silent.store(true, Ordering::SeqCst);
            }
            let (started, before) = (Instant::now(), relay.lookups());
            let accepted = acceptor.accept(stream).await;
            recorded.lock().expect("handshakes").push(Handshake {
                completed: accepted.is_ok(),
                lookups: relay.lookups() - before,
                took: started.elapsed(),
            });
            let Ok(mut tls) = accepted else {
                continue;
            };
            let body = body.clone();
            tokio::spawn(async move {
                let Some(head) = request_head(&mut tls).await else {
                    return;
                };
                if drain_body(&mut tls, &head).await.is_none() {
                    return;
                }
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
    (port, handshakes)
}

/// `homeserver`'s record listing 127.0.0.1 at `port` for itself and, if
/// given, a delegation to `delegate`.
fn direct_record(
    homeserver: &pkarr::Keypair,
    port: u16,
    delegate: Option<&pkarr::PublicKey>,
) -> SignedPacket {
    let root = || ".".try_into().expect("root name");
    let mut direct = SVCB::new(1, root());
    direct.set_port(port);
    let mut packet = SignedPacket::builder().https(root(), direct, 60).address(
        root(),
        std::net::Ipv4Addr::LOCALHOST.into(),
        60,
    );
    if let Some(delegate) = delegate {
        let target = delegate.to_z32();
        let mut delegated = SVCB::new(2, target.as_str().try_into().expect("key name"));
        delegated.set_port(port);
        packet = packet.https(root(), delegated, 60);
    }
    packet.sign(homeserver).expect("endpoint record")
}

/// `user`'s record naming `homeserver`.
fn user_record(user: &Keypair, homeserver: &PublicKey) -> SignedPacket {
    let target = homeserver.z32();
    SignedPacket::builder()
        .https(
            "_pubky".try_into().expect("_pubky name"),
            SVCB::new(0, target.as_str().try_into().expect("homeserver name")),
            60,
        )
        .sign(user)
        .expect("user record")
}

/// Exchanges `grant` through a Pkarr client reading only `relay`, at
/// `homeserver` alone.
async fn exchange(
    relay: &Relay,
    homeserver: PublicKey,
    grant: &Grant,
) -> Result<GrantSessionInfo, ExchangeRefusal> {
    let exchange = GrantExchange::new(relay.client(), homeserver, EndpointPolicy::PublicOrLoopback)
        .expect("grant exchange");
    let deadline = Instant::now() + Duration::from_secs(30);
    exchange
        .exchange(&grant.jws, &grant.claims, &grant.client, deadline)
        .await
}

/// The handshakes the homeserver recorded. A refused client can return
/// before the homeserver reads its alert, so this waits for the first.
async fn handshakes_of(recorded: &Mutex<Vec<Handshake>>) -> Vec<Handshake> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let handshakes = recorded.lock().expect("handshakes").clone();
        if !handshakes.is_empty() {
            return handshakes;
        }
        assert!(Instant::now() < deadline, "the homeserver was reached");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_homeservers_own_key_completes_the_exchange_with_no_lookup_while_the_relay_is_silent() {
    let relay = Relay::start().await;
    let homeserver = pkarr::Keypair::random();
    let homeserver_key: PublicKey = homeserver.public_key().into();
    let user = Keypair::random();
    let grant = grant(&user);
    let (port, recorded) = homeserver_presenting(
        &homeserver,
        homeserver_key.clone(),
        &grant.claims,
        &relay,
        true,
    )
    .await;
    relay.serve(direct_record(&homeserver, port, None));
    relay.serve(user_record(&user, &homeserver_key));

    let session = exchange(&relay, homeserver_key.clone(), &grant)
        .await
        .expect("the exchange completes");
    assert_eq!(session.pubky, user.public_key());
    assert_eq!(session.homeserver, homeserver_key);
    for handshake in handshakes_of(&recorded).await {
        assert!(handshake.completed, "{handshake:?}");
        assert_eq!(handshake.lookups, 0, "{handshake:?}");
        assert!(handshake.took < RELAY_TIMEOUT, "{handshake:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_key_at_the_direct_address_is_refused_at_once_with_no_lookup() {
    let relay = Relay::start().await;
    let homeserver = pkarr::Keypair::random();
    let homeserver_key: PublicKey = homeserver.public_key().into();
    let impostor = pkarr::Keypair::random();
    let user = Keypair::random();
    let grant = grant(&user);
    let (port, recorded) = homeserver_presenting(
        &impostor,
        homeserver_key.clone(),
        &grant.claims,
        &relay,
        true,
    )
    .await;
    relay.serve(direct_record(&homeserver, port, None));
    relay.serve(user_record(&user, &homeserver_key));

    let refused = exchange(&relay, homeserver_key, &grant)
        .await
        .expect_err("another key is refused");
    assert_eq!(refused, ExchangeRefusal::Failed);
    for handshake in handshakes_of(&recorded).await {
        assert!(!handshake.completed, "{handshake:?}");
        assert_eq!(handshake.lookups, 0, "{handshake:?}");
        assert!(handshake.took < RELAY_TIMEOUT, "{handshake:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_the_homeservers_record_delegates_to_is_refused_with_no_lookup() {
    let relay = Relay::start().await;
    let homeserver = pkarr::Keypair::random();
    let homeserver_key: PublicKey = homeserver.public_key().into();
    let delegate = pkarr::Keypair::random();
    let user = Keypair::random();
    let grant = grant(&user);
    let (port, recorded) = homeserver_presenting(
        &delegate,
        homeserver_key.clone(),
        &grant.claims,
        &relay,
        false,
    )
    .await;
    relay.serve(direct_record(
        &homeserver,
        port,
        Some(&delegate.public_key()),
    ));
    relay.serve(direct_record(&delegate, port, None));
    relay.serve(user_record(&user, &homeserver_key));

    let refused = exchange(&relay, homeserver_key, &grant)
        .await
        .expect_err("a delegated key is refused");
    assert_eq!(refused, ExchangeRefusal::Failed);
    for handshake in handshakes_of(&recorded).await {
        assert!(!handshake.completed, "{handshake:?}");
        assert_eq!(handshake.lookups, 0, "{handshake:?}");
    }
}
