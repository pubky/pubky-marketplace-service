//! The JSON record parse bounds on both homeserver resolutions, against a
//! real `pubky-homeserver` on a local Pkarr testnet: a listing, drop, shop or
//! content-lock record well under the byte cap whose JSON would build an
//! outsized value tree reads as `Unavailable` through the `pkarr` client and
//! through a `fixed` client pointed at the same homeserver, while an ordinary
//! record next to it still reads.
//!
//! As in `homeserver_pkarr_test.rs`, the testnet runs on a runtime of its own
//! and its databases are dropped after that runtime shut down.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use pubky_testnet::pubky::{ClientId, Keypair, PubkySession, PublicKey};
use pubky_testnet::pubky_homeserver::ConnectionString;
use pubky_testnet::Testnet;
use serde_json::{json, Value};

use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HttpHomeserverClient, PkarrResolutionCache,
    LISTINGS_PATH, SHOP_PATH,
};

const CLIENT_ID: &str = "market.pubky.service-test";
const LOCK_PATH: &str = "/pub/locks.app/000G40R40M30E209185GR38E1W8124GK2GAHC5RR34D1P70X3RFG.json";
const DROPS_PATH: &str = "/pub/pubky.app/marketplace/v1/drops/";

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .expect("DATABASE_URL names the Postgres server the testnet homeservers use")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
}

fn on_testnet(test: impl Future<Output = ()>) {
    let test_runtime = runtime();
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| test_runtime.block_on(test)));
    test_runtime.shutdown_timeout(Duration::from_secs(10));
    runtime().block_on(pubky_testnet::drop_test_databases());
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

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

async fn put(session: &PubkySession, path: &str, body: Vec<u8>) {
    session
        .storage()
        .put(path, body)
        .await
        .unwrap_or_else(|error| panic!("writing {path}: {error}"));
}

/// `n` zeros in one array: about `2n` bytes on the wire.
fn zeros(n: usize) -> Vec<u8> {
    serde_json::to_vec(&vec![0; n]).expect("zeros serialize")
}

fn ordinary(pubky: &str) -> Value {
    json!({ "ownerPubky": pubky, "title": "Winter boots", "tags": ["a", "b"] })
}

/// Every JSON record read, for one record name.
async fn json_reads(
    client: &HttpHomeserverClient,
    pubky: &str,
    name: &str,
) -> Vec<(&'static str, HomeserverFetchOutcome)> {
    let lock = if name == "amp" {
        LOCK_PATH.to_string()
    } else {
        format!("/pub/locks.app/{name}.json")
    };
    vec![
        ("listing", client.fetch_listing(pubky, name).await),
        (
            "listing_from",
            client.fetch_listing_from(pubky, name).await.0,
        ),
        (
            "listing_bytes",
            client.fetch_listing_bytes(pubky, name).await,
        ),
        ("drop", client.fetch_drop(pubky, name).await),
        (
            "content_lock",
            client.fetch_content_lock(pubky, &lock).await,
        ),
    ]
}

#[test]
fn an_amplifying_record_is_unavailable_on_both_resolutions() {
    on_testnet(async {
        let postgres =
            ConnectionString::new(&database_url()).expect("DATABASE_URL is a Postgres URL");
        let mut testnet = Testnet::new_with_custom_postgres(postgres)
            .await
            .expect("testnet starts");
        let (homeserver, homeserver_url) = {
            let app = testnet
                .create_random_homeserver()
                .await
                .expect("homeserver");
            (app.public_key(), app.icann_http_url().to_string())
        };
        let seller = Keypair::random();
        let pubky = seller.public_key().z32();
        let session = account(&testnet, &seller, &homeserver).await;

        // 100,001 values in about 200 KiB, far under the 4 MiB body cap.
        let amplifying = zeros(100_001);
        put(&session, &format!("{LISTINGS_PATH}amp"), amplifying.clone()).await;
        put(&session, &format!("{DROPS_PATH}amp"), amplifying.clone()).await;
        put(&session, LOCK_PATH, amplifying.clone()).await;
        put(&session, SHOP_PATH, amplifying).await;

        let record = ordinary(&pubky);
        let bytes = serde_json::to_vec(&record).expect("record serializes");
        put(&session, &format!("{LISTINGS_PATH}ok"), bytes.clone()).await;
        put(&session, &format!("{DROPS_PATH}ok"), bytes.clone()).await;
        put(&session, "/pub/locks.app/ok.json", bytes).await;

        let pkarr = HttpHomeserverClient::pkarr_with_loopback_homeservers(
            testnet.client_builder(),
            PkarrResolutionCache {
                capacity: 64,
                ttl: PkarrResolutionCache::default().ttl,
            },
        )
        .expect("pkarr homeserver client builds");
        let fixed = HttpHomeserverClient::new(&homeserver_url).expect("fixed client");

        for (resolution, client) in [("pkarr", &pkarr), ("fixed", &fixed)] {
            for (read, outcome) in json_reads(client, &pubky, "amp").await {
                assert_eq!(
                    outcome,
                    HomeserverFetchOutcome::Unavailable,
                    "{resolution} {read}"
                );
            }
            assert_eq!(
                client.fetch_shop(&pubky).await,
                HomeserverFetchOutcome::Unavailable,
                "{resolution} shop"
            );
            for (read, outcome) in json_reads(client, &pubky, "ok").await {
                assert_eq!(
                    outcome,
                    HomeserverFetchOutcome::Found(record.clone()),
                    "{resolution} {read}"
                );
            }
        }
    });
}
