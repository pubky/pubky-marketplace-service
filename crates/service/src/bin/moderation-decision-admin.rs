//! Operator commands for moderation decisions (runbook:
//! `docs/moderation-decisions.md`).
//!
//! ```text
//! moderation-decision-admin record <decision.json>
//! moderation-decision-admin show <decision_id>
//! ```
//!
//! `DATABASE_URL` names the database; `MODERATION_ADMIN_OPERATOR` names the
//! operator recorded with a decision.

use std::io::Read;

use marketplace_service::moderation_decisions::{find, record, Decision, DecisionInput};
use uuid::Uuid;

/// Far above any valid decision (every text field is bounded), so a larger
/// file is a mistake, not a statement.
const MAX_DECISION_FILE_BYTES: u64 = 64 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| anyhow::anyhow!("DATABASE_URL is required"))?;
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["record", path] => {
            let operator = std::env::var("MODERATION_ADMIN_OPERATOR")
                .map_err(|_| anyhow::anyhow!("MODERATION_ADMIN_OPERATOR is required"))?;
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(MAX_DECISION_FILE_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_DECISION_FILE_BYTES {
                anyhow::bail!("the decision file is larger than {MAX_DECISION_FILE_BYTES} bytes");
            }
            let input: DecisionInput = serde_json::from_slice(&bytes)?;
            let now = chrono::Utc::now();
            let decision = Decision::validate(input, now)?;
            let pool = connect(&database_url).await?;
            let recorded = record(&pool, &decision, &operator, now).await?;
            if recorded.created {
                println!("{}", recorded.id);
            } else {
                println!("{} (already recorded; no second notice sent)", recorded.id);
            }
            Ok(())
        }
        ["show", id] => {
            let pool = connect(&database_url).await?;
            let row = find(&pool, id.parse::<Uuid>()?)
                .await?
                .ok_or_else(|| anyhow::anyhow!("no moderation decision with that id"))?;
            println!("{}", serde_json::to_string_pretty(&row.view())?);
            Ok(())
        }
        _ => anyhow::bail!(
            "usage: moderation-decision-admin record <decision.json> | show <decision_id>"
        ),
    }
}

async fn connect(database_url: &str) -> anyhow::Result<sqlx::PgPool> {
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?)
}
