//! Every command reads the seller's homeserver before its database
//! transaction, and only for an actor its transaction would not refuse
//! first: offers, `drop.sync`, `payment.sandbox_advance` and
//! `payment.prepare_locks`. The payment-method bind is covered in
//! `checkout_holds_test`.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use marketplace_service::homeserver::{
    HomeserverFetchOutcome, HomeserverListingClient, HomeserverRawFetchOutcome,
};
use marketplace_service::http::build_router;
use serde_json::json;
use sqlx::PgPool;

use common::{
    counter_offer_command, create_offer_command, create_pending_locks_order, create_pending_order,
    execute, new_actor, offer_action, payment_command, prepare_locks_command, register_command,
    sync_drop_command, test_app_with_homeserver_client, test_app_with_locks, test_locks_homeserver,
    CommandMirrorHomeserver, TestApp,
};

/// Each read this homeserver served: what was read, for whom, and how many
/// database transactions other than the counting query's own were open
/// while it ran.
struct WatchingHomeserver {
    pool: PgPool,
    inner: Arc<dyn HomeserverListingClient>,
    reads: Mutex<Vec<(&'static str, String, i64)>>,
}

impl WatchingHomeserver {
    fn new(pool: PgPool, inner: Arc<dyn HomeserverListingClient>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            inner,
            reads: Mutex::new(Vec::new()),
        })
    }

    async fn watch(&self, what: &'static str, pubky: &str) {
        let open: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND backend_type = 'client backend' \
             AND pid <> pg_backend_pid() AND xact_start IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .expect("activity");
        self.reads
            .lock()
            .expect("lock")
            .push((what, pubky.to_string(), open));
    }

    fn take(&self) -> Vec<(&'static str, String, i64)> {
        std::mem::take(&mut *self.reads.lock().expect("lock"))
    }
}

impl HomeserverListingClient for WatchingHomeserver {
    fn fetch_listing<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.watch("listing", seller_pubky).await;
            self.inner.fetch_listing(seller_pubky, listing_id).await
        })
    }

    fn fetch_listing_raw<'a>(
        &'a self,
        seller_pubky: &'a str,
        listing_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverRawFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.watch("listing_raw", seller_pubky).await;
            self.inner.fetch_listing_raw(seller_pubky, listing_id).await
        })
    }

    fn fetch_drop<'a>(
        &'a self,
        seller_pubky: &'a str,
        drop_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.watch("drop", seller_pubky).await;
            self.inner.fetch_drop(seller_pubky, drop_id).await
        })
    }

    fn fetch_shop<'a>(
        &'a self,
        seller_pubky: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.watch("shop", seller_pubky).await;
            self.inner.fetch_shop(seller_pubky).await
        })
    }

    fn fetch_content_lock<'a>(
        &'a self,
        creator_pubky: &'a str,
        content_path: &'a str,
    ) -> Pin<Box<dyn Future<Output = HomeserverFetchOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.watch("content_lock", creator_pubky).await;
            self.inner
                .fetch_content_lock(creator_pubky, content_path)
                .await
        })
    }
}

fn with_homeserver(app: TestApp, homeserver: Arc<WatchingHomeserver>) -> TestApp {
    let state = app.state.clone().with_homeserver(Some(homeserver));
    TestApp {
        router: build_router(state.clone()),
        pool: app.pool,
        clock: app.clock,
        state,
    }
}

/// The reads made, each with no transaction open.
fn assert_read_outside_transactions(reads: &[(&'static str, String, i64)]) {
    for (what, pubky, open) in reads {
        assert_eq!(
            *open, 0,
            "the {what} read for {pubky} ran with a transaction open"
        );
    }
}

fn kinds(reads: &[(&'static str, String, i64)]) -> Vec<&'static str> {
    reads.iter().map(|(what, _, _)| *what).collect()
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn offers_read_the_listing_record_before_their_transaction(pool: PgPool) {
    let homeserver = WatchingHomeserver::new(pool.clone(), Arc::new(CommandMirrorHomeserver));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let stranger = new_actor(&app).await;
    let (status, body) = execute(&app, &seller.token, &register_command(&seller.pubky, 2)).await;
    assert_eq!(status, StatusCode::OK, "register: {body}");
    homeserver.take();

    // The seller cannot offer on their own listing: its record is not read.
    let (status, body) =
        execute(&app, &seller.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(!kinds(&homeserver.take()).contains(&"listing_raw"));

    let (status, body) = execute(&app, &buyer.token, &create_offer_command(&seller.pubky, 1)).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    let reads = homeserver.take();
    assert!(kinds(&reads).contains(&"listing_raw"), "{reads:?}");
    assert_read_outside_transactions(&reads);

    // Someone outside the offer, or its author, has the record read for
    // nothing.
    for token in [&stranger.token, &buyer.token] {
        let (status, body) = execute(&app, token, &counter_offer_command(1)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert!(!kinds(&homeserver.take()).contains(&"listing_raw"));
    }

    let (status, body) = execute(&app, &seller.token, &counter_offer_command(1)).await;
    assert_eq!(status, StatusCode::OK, "counter: {body}");
    let reads = homeserver.take();
    assert!(kinds(&reads).contains(&"listing_raw"), "{reads:?}");
    assert_read_outside_transactions(&reads);

    for (token, number) in [(&stranger.token, 0x0503), (&seller.token, 0x0504)] {
        let (status, body) = execute(
            &app,
            token,
            &offer_action(
                "offer.accept",
                2,
                &format!("00000000-0000-4000-8000-00000000{number:04x}"),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert!(!kinds(&homeserver.take()).contains(&"listing_raw"));
    }

    let (status, body) = execute(
        &app,
        &buyer.token,
        &offer_action("offer.accept", 2, "00000000-0000-4000-8000-000000000502"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "accept: {body}");
    assert_eq!(body["result"]["offer"]["state"], json!("accepted"));
    let reads = homeserver.take();
    assert!(kinds(&reads).contains(&"listing_raw"), "{reads:?}");
    assert_read_outside_transactions(&reads);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_drop_sync_reads_its_record_before_its_transaction(pool: PgPool) {
    let homeserver = WatchingHomeserver::new(pool.clone(), Arc::new(CommandMirrorHomeserver));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let anyone = new_actor(&app).await;

    // A sync whose aggregate names another drop reads nothing.
    let mut mismatched = sync_drop_command(&seller.pubky, "winter_drop", 1);
    mismatched["aggregate_id"] = json!(format!("drop:{}_other_drop", seller.pubky));
    let (_, body) = execute(&app, &anyone.token, &mismatched).await;
    assert_eq!(body["error"]["code"], json!("INVALID_COMMAND"), "{body}");
    assert!(kinds(&homeserver.take()).is_empty());

    let (status, body) = execute(
        &app,
        &anyone.token,
        &sync_drop_command(&seller.pubky, "winter_drop", 2),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let reads = homeserver.take();
    assert_eq!(kinds(&reads), vec!["drop"]);
    assert_read_outside_transactions(&reads);
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_sandbox_payment_start_reads_the_terms_before_its_transaction(pool: PgPool) {
    let homeserver = WatchingHomeserver::new(pool.clone(), Arc::new(CommandMirrorHomeserver));
    let app = test_app_with_homeserver_client(pool, homeserver.clone()).await;
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_order(&app, &seller, &buyer).await;
    homeserver.take();

    // Only the buyer makes the service read the seller's records.
    let (status, body) = execute(
        &app,
        &seller.token,
        &payment_command(&order.payment_id, 1, "detected", 0, 1),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(kinds(&homeserver.take()).is_empty());

    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 1, "detected", 0, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "advance: {body}");
    let reads = homeserver.take();
    assert_eq!(kinds(&reads), vec!["shop", "listing"]);
    assert!(reads.iter().all(|(_, pubky, _)| *pubky == seller.pubky));
    assert_read_outside_transactions(&reads);
    let captured: bool = sqlx::query_scalar(
        "SELECT delivery_terms_captured_at IS NOT NULL FROM orders WHERE id = $1::uuid",
    )
    .bind(&order.order_id)
    .fetch_one(&app.pool)
    .await
    .expect("order row");
    assert!(captured);

    // Once the terms are captured, a later advance reads nothing.
    let (status, body) = execute(
        &app,
        &buyer.token,
        &payment_command(&order.payment_id, 2, "confirmed", 1, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "confirm: {body}");
    assert!(kinds(&homeserver.take()).is_empty());
}

#[sqlx::test(migrator = "marketplace_service::TEST_MIGRATOR")]
async fn a_locks_preparation_reads_before_its_transaction(pool: PgPool) {
    let (app, _locks) = test_app_with_locks(pool.clone()).await;
    let homeserver = WatchingHomeserver::new(pool, test_locks_homeserver());
    let app = with_homeserver(app, homeserver.clone());
    let seller = new_actor(&app).await;
    let buyer = new_actor(&app).await;
    let order = create_pending_locks_order(&app, &seller, &buyer).await;
    homeserver.take();

    let (status, body) = execute(
        &app,
        &seller.token,
        &prepare_locks_command(&order.payment_id, 1),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(kinds(&homeserver.take()).is_empty());

    let (status, body) = execute(
        &app,
        &buyer.token,
        &prepare_locks_command(&order.payment_id, 2),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "prepare: {body}");
    assert!(body["result"]["client_reference"].is_string(), "{body}");
    let reads = homeserver.take();
    // The listing lifecycle check, then the lock document and the terms.
    assert_eq!(
        kinds(&reads),
        vec!["listing", "content_lock", "shop", "listing"]
    );
    assert!(reads.iter().all(|(_, pubky, _)| *pubky == seller.pubky));
    assert_read_outside_transactions(&reads);

    // A second prepare recovers the same reference with no read.
    let (status, again) = execute(
        &app,
        &buyer.token,
        &prepare_locks_command(&order.payment_id, 3),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(
        again["result"]["client_reference"], body["result"]["client_reference"],
        "{again}"
    );
    assert!(kinds(&homeserver.take()).is_empty());
}
