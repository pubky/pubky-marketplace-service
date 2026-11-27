use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let _binary = args.next();
    if args.next().as_deref() != Some("release") || args.next().as_deref() != Some("--confirm") {
        anyhow::bail!("usage: store-binding-admin release --confirm <pubky-or-first-8-characters>");
    }
    let confirm = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("--confirm requires a value"))?;
    if args.next().is_some() {
        anyhow::bail!("usage: store-binding-admin release --confirm <pubky-or-first-8-characters>");
    }
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| anyhow::anyhow!("DATABASE_URL must be set"))?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect(&database_url)
        .await?;
    marketplace_service::store_binding_admin::release(&pool, &confirm).await?;
    pool.close().await;
    Ok(())
}
