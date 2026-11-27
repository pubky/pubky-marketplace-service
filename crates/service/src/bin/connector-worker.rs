//! The store connector worker: consumes the service's event feed, pushes
//! paid orders to connected stores, mirrors cancels and refunds, applies
//! inbound store webhooks and polls stock. Shares the service's database and
//! domain; owns the `connector_*` tables.
//!
//! Needs the same environment as `marketplace-service` plus
//! `CONNECTORS_ENABLED=true`. It refuses to start while connectors are off.

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use marketplace_service::clock::SystemClock;
use marketplace_service::config::Config;
use marketplace_service::connectors;
use marketplace_service::AppState;

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,marketplace_service=info"))
        .add_directive("sqlx=warn".parse().expect("valid sqlx log directive"))
        .add_directive("hyper=warn".parse().expect("valid hyper log directive"))
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
    let grant_session = marketplace_service::grant_session::GrantSessionConfig::from_env(
        config.public_service_origin.as_deref(),
    )?;
    if config.store_mode.is_claim_mode()
        && grant_session
            .as_ref()
            .is_none_or(|grant| grant.client_ids.is_empty())
    {
        anyhow::bail!(
            "single-seller claim mode requires MARKETPLACE_GRANT_SESSION_ENABLED=true \
             and at least one MARKETPLACE_GRANT_SESSION_CLIENT_IDS entry"
        );
    }
    marketplace_service::inventory::validate_rate_config_from_env()?;
    let locks = marketplace_service::locks::runtime_from_env()?;
    let pickup = marketplace_service::pickup::pickup_keys_from_env(locks.as_deref())?;
    let digital =
        marketplace_service::digital::digital_keys_from_env(locks.as_deref(), pickup.as_deref())?;
    let priv_keys = marketplace_service::priv_keys::priv_keys_from_env(
        locks.as_deref(),
        pickup.as_deref(),
        digital.as_deref(),
    )?;
    let Some(runtime) = connectors::runtime_from_env(connectors::providers_from_env()?, || {
        connectors::keys::connector_keys_from_env(
            locks.as_deref(),
            pickup.as_deref(),
            digital.as_deref(),
            priv_keys.as_deref(),
        )
    })?
    else {
        anyhow::bail!(
            "{} is not true; the connector worker has nothing to do",
            connectors::config::ENV_CONNECTORS_ENABLED
        );
    };

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await?;
    marketplace_service::migrations::run(&pool).await?;
    let store = loop {
        let store = marketplace_service::store_seller::resolve_store_without_code(
            &pool,
            &config.store_mode,
            config.store_claim_code.clone(),
        )
        .await?;
        match store.active() {
            marketplace_service::store_seller::Active::Off
            | marketplace_service::store_seller::Active::Claimed(_) => break store,
            marketplace_service::store_seller::Active::Unclaimed => {
                tracing::info!("connector worker waiting for the store claim");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    };
    connectors::assert_credentials_coherent(&pool, Some(runtime.keys.as_ref())).await?;
    tracing::info!(
        providers = ?runtime.providers.kinds(),
        "connector worker starting"
    );

    let state = AppState::new(pool, Arc::new(SystemClock), config)
        .with_store(store)
        .with_connectors(Some(runtime));
    connectors::worker::run_forever(state).await
}
