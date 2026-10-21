use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tracing_subscriber::EnvFilter;

use marketplace_service::clock::SystemClock;
use marketplace_service::config::Config;
use marketplace_service::{http, workers, AppState};

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,marketplace_service=info"))
        .add_directive("sqlx=warn".parse().expect("valid sqlx log directive"))
        .add_directive("hyper=warn".parse().expect("valid hyper log directive"))
        .add_directive(
            "tower_http=warn"
                .parse()
                .expect("valid tower_http log directive"),
        )
        .add_directive("h2=warn".parse().expect("valid h2 log directive"))
        .add_directive("rustls=warn".parse().expect("valid rustls log directive"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(env_filter())
        .init();

    let config = Config::from_env()?;
    let grant = marketplace_service::grant::GrantRuntime::from_env(config.session_ttl_seconds)?;
    let grant_session = marketplace_service::grant_session::GrantSessionConfig::from_env(
        config.public_service_origin.as_deref(),
    )?;
    tracing::info!(
        grant_sessions = if grant_session.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "grant session mode resolved"
    );
    marketplace_service::inventory::validate_rate_config_from_env()?;
    // Fail closed before serving: a partial Locks configuration (URL without
    // keys, or keys without URL) refuses to start rather than running with
    // verification silently disabled or bearer material unprotected.
    let locks = marketplace_service::locks::runtime_from_env()?;
    tracing::info!(
        locks_verification = if locks.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "locks verification mode resolved"
    );
    // Fail closed likewise for the attestor: a partial configuration (key
    // without salt, or salt without key) refuses to start rather than
    // issuing attestations with unlinkable order refs.
    let attestor = marketplace_service::attestor::Attestor::from_env()?;
    // Fail closed for payment methods: STRIPE_KEY_ENCRYPTION_KEY (the
    // seller-secret key, a legacy name) enables the surface, and the Paykit
    // pair (server URL + signing key) is all-or-nothing within it.
    let payments = marketplace_service::payments::payments_runtime_from_env()?;
    match &payments {
        Some(payments) => tracing::info!(
            paykit = if payments.paykit.is_some() {
                "enabled"
            } else {
                "disabled"
            },
            "payment methods enabled"
        ),
        None => tracing::info!("payment methods disabled (no seller-secret encryption key)"),
    }
    // All-or-none for local pickup (§A8): PICKUP_DETAILS_ENCRYPTION_KEY
    // enables the sealed pickup-details store; absent, pickup is OFF and
    // `pickup_details.set` is refused. The key must be distinct from the
    // Locks key material.
    let pickup = marketplace_service::pickup::pickup_keys_from_env(locks.as_deref())?;
    tracing::info!(
        pickup = if pickup.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "pickup details sealing mode resolved"
    );
    let digital =
        marketplace_service::digital::digital_keys_from_env(locks.as_deref(), pickup.as_deref())?;
    tracing::info!(
        digital_delivery = if digital.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "digital delivery sealing mode resolved"
    );
    let priv_keys = marketplace_service::priv_keys::priv_keys_from_env(
        locks.as_deref(),
        pickup.as_deref(),
        digital.as_deref(),
    )?;
    tracing::info!(
        priv_keys = if priv_keys.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "priv data key sealing mode resolved"
    );
    // Connectors are off unless CONNECTORS_ENABLED is true. Enabled, the
    // credential key is required and must differ from every other sealing key.
    let connectors = marketplace_service::connectors::runtime_from_env(
        marketplace_service::connectors::providers_from_env()?,
        || {
            marketplace_service::connectors::keys::connector_keys_from_env(
                locks.as_deref(),
                pickup.as_deref(),
                digital.as_deref(),
                priv_keys.as_deref(),
            )
        },
    )?;
    tracing::info!(
        connectors = if connectors.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "store connectors mode resolved"
    );
    // HOMESERVER_URL is required: `listing.sync` fetches canonical
    // seller-signed records from it, and running without the sync path would
    // silently re-open the unregistered-listing dead-end it exists to fix.
    let homeserver = Arc::new(marketplace_service::homeserver::client_from_env()?);
    tracing::info!("homeserver listing sync enabled");
    // NEXUS_URL is required: checkout, bids and offers on a moderated listing
    // are refused from the index's moderation state, and running without it
    // would sell moderated listings by omission.
    let listing_moderation = Arc::new(
        marketplace_service::listing_moderation::ListingModerationGate::new(Arc::new(
            marketplace_service::listing_moderation::client_from_env()?,
        )),
    );
    tracing::info!("listing moderation gate enabled");
    match &attestor {
        Some(attestor) => tracing::info!(
            attestor_pubky = attestor.pubky(),
            "attestation issuance enabled"
        ),
        None => tracing::info!("attestation issuance disabled (no attestor key configured)"),
    }
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await?;
    marketplace_service::migrations::run(&pool).await?;
    tracing::info!("database migrations applied");
    let audit_writer_options: PgConnectOptions = config.refusal_audit_database_url.parse()?;
    let audit_writer_pool = PgPoolOptions::new()
        .max_connections(marketplace_service::refusal_audit::WRITER_POOL_MAX_CONNECTIONS)
        .min_connections(0)
        .acquire_timeout(Duration::from_millis(100))
        .idle_timeout(Some(Duration::from_secs(60)))
        .connect_lazy_with(audit_writer_options);
    let audit_retention_options: PgConnectOptions =
        config.refusal_audit_retention_database_url.parse()?;
    let audit_retention_pool = PgPoolOptions::new()
        .max_connections(marketplace_service::refusal_audit::RETENTION_POOL_MAX_CONNECTIONS)
        .min_connections(0)
        .acquire_timeout(Duration::from_millis(100))
        .idle_timeout(Some(Duration::from_secs(60)))
        .connect_lazy_with(audit_retention_options);
    let refusal_audit = Arc::new(
        marketplace_service::refusal_audit::RefusalAuditRuntime::spawn(
            audit_writer_pool,
            config.refusal_audit_keys.clone(),
        ),
    );

    // The pickup sealing boot probe runs AFTER migrations (the schema must
    // exist first) and attempts one real open — current key, then previous —
    // across both sealed families, so a wrong or half-rotated key fails the
    // boot rather than the first buyer's reveal (§A8).
    marketplace_service::pickup::assert_pickup_sealing_coherent(&pool, pickup.as_deref()).await?;
    tracing::info!("pickup sealing coherence probe passed");
    marketplace_service::digital::assert_digital_sealing_coherent(&pool, digital.as_deref())
        .await?;
    tracing::info!("digital delivery sealing coherence probe passed");
    marketplace_service::priv_keys::assert_priv_key_sealing_coherent(&pool, priv_keys.as_deref())
        .await?;
    tracing::info!("priv data key sealing coherence probe passed");
    if let Some(connectors) = &connectors {
        marketplace_service::connectors::assert_credentials_coherent(
            &pool,
            Some(connectors.keys.as_ref()),
        )
        .await?;
        tracing::info!("connector credential sealing coherence probe passed");
    }

    let address_search = marketplace_service::address_search::runtime_from_env()?;
    tracing::info!(
        address_search = if address_search.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "address search proxy resolved"
    );

    let listing_availability =
        Arc::new(marketplace_service::listing_availability::AvailabilityLimiter::from_env()?);

    let refund_chain = marketplace_service::refund_chain::client_from_env()?
        .map(|client| Arc::new(client) as Arc<dyn marketplace_service::refund_chain::RefundChain>);
    tracing::info!(
        refund_chain = if refund_chain.is_some() {
            "enabled"
        } else {
            "disabled"
        },
        "usdt refund check on arbitrum resolved"
    );

    let bind_addr = config.bind_addr;
    let state = AppState::new(pool, Arc::new(SystemClock), config)
        .with_locks(locks)
        .with_attestor(attestor)
        .with_homeserver(Some(homeserver))
        .with_refund_chain(refund_chain)
        .with_listing_moderation(Some(listing_moderation))
        .with_payments(payments)
        .with_pickup(pickup)
        .with_digital(digital)
        .with_priv_keys(priv_keys)
        .with_connectors(connectors)
        .with_refusal_audit(refusal_audit)
        .with_refusal_audit_retention_pool(audit_retention_pool)
        .with_grant(grant)
        .with_grant_session(grant_session)
        .with_address_search(address_search)
        .with_listing_availability(listing_availability);
    workers::spawn(state.clone());
    marketplace_service::automation::spawn_webhook_worker(state.clone());
    marketplace_service::grant::spawn(state.clone());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = %bind_addr, "marketplace transaction service listening");
    axum::serve(
        listener,
        http::build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::env_filter;

    #[test]
    fn dependency_directives_remain_at_warn() {
        let filter = env_filter().to_string();
        for directive in [
            "sqlx=warn",
            "hyper=warn",
            "tower_http=warn",
            "h2=warn",
            "rustls=warn",
        ] {
            assert!(
                filter.contains(directive),
                "{directive} missing from {filter}"
            );
        }
    }
}
