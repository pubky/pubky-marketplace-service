//! Checks a recorded USDT refund on Arbitrum One.
//!
//! The seller sends a USDT refund from their own wallet and records its
//! Arbitrum transaction hash (`refund.record_external`). With
//! `ARBITRUM_RPC_URL` set, the service reads that transaction's receipt and
//! sums the USDT0 transfers it made to the buyer's confirmed refund address.
//! A hash that Arbitrum does not show, a reverted transaction, or one that
//! sent the buyer no USDT is refused; otherwise the record keeps what the
//! buyer actually received, so a short refund is shown as short. Without the
//! variable, or when the RPC cannot be reached, the refund is recorded
//! unchecked, as before.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{json, Value};

/// The variable naming the Arbitrum One JSON-RPC endpoint.
pub const ENV_ARBITRUM_RPC_URL: &str = "ARBITRUM_RPC_URL";
/// USDT0 on Arbitrum One (chain 42161).
pub const USDT0_ARBITRUM_ONE: &str = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9";
/// USDT0's decimals: amounts are in millionths.
pub const USDT0_DECIMALS: u32 = 6;
/// `keccak256("Transfer(address,address,uint256)")`.
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// What Arbitrum shows for a refund transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefundChainOutcome {
    /// The transaction succeeded and sent this many USDT0 millionths to the
    /// recipient (zero when it sent them none).
    Transferred { units: i64 },
    /// No receipt: the hash is wrong, or the transaction is not mined yet.
    NotFound,
    /// The transaction was mined and reverted.
    Reverted,
    /// The RPC could not be reached or answered something unreadable.
    Unavailable,
}

pub trait RefundChain: Send + Sync + 'static {
    fn check<'a>(
        &'a self,
        transaction_hash: &'a str,
        recipient: &'a str,
    ) -> Pin<Box<dyn Future<Output = RefundChainOutcome> + Send + 'a>>;
}

/// The production client: `eth_getTransactionReceipt` on an Arbitrum One RPC.
pub struct ArbitrumRpc {
    url: String,
    http: reqwest::Client,
}

impl ArbitrumRpc {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        if !url.starts_with("http://") && !url.starts_with("https://") {
            anyhow::bail!("{ENV_ARBITRUM_RPC_URL} must be an http(s) URL");
        }
        let http = reqwest::Client::builder().timeout(RPC_TIMEOUT).build()?;
        Ok(Self {
            url: url.to_string(),
            http,
        })
    }

    async fn receipt(&self, transaction_hash: &str) -> Option<Value> {
        let response = self
            .http
            .post(&self.url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getTransactionReceipt",
                "params": [transaction_hash],
            }))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        if body.get("error").is_some_and(|error| !error.is_null()) {
            return None;
        }
        body.get("result").cloned()
    }
}

impl RefundChain for ArbitrumRpc {
    fn check<'a>(
        &'a self,
        transaction_hash: &'a str,
        recipient: &'a str,
    ) -> Pin<Box<dyn Future<Output = RefundChainOutcome> + Send + 'a>> {
        Box::pin(async move {
            match self.receipt(transaction_hash).await {
                None => {
                    tracing::warn!("arbitrum refund check: rpc unavailable");
                    RefundChainOutcome::Unavailable
                }
                Some(receipt) => outcome_from_receipt(&receipt, recipient),
            }
        })
    }
}

/// The client from `ARBITRUM_RPC_URL`; `None` when it is unset or empty.
pub fn client_from_env() -> anyhow::Result<Option<ArbitrumRpc>> {
    match std::env::var(ENV_ARBITRUM_RPC_URL) {
        Ok(url) if !url.trim().is_empty() => Ok(Some(ArbitrumRpc::new(url.trim())?)),
        _ => Ok(None),
    }
}

/// Reads an `eth_getTransactionReceipt` result: `null` is not found, status
/// `0x0` reverted, otherwise the USDT0 sent to `recipient`.
pub fn outcome_from_receipt(receipt: &Value, recipient: &str) -> RefundChainOutcome {
    if receipt.is_null() {
        return RefundChainOutcome::NotFound;
    }
    match receipt.get("status").and_then(Value::as_str) {
        Some("0x1") => {}
        Some("0x0") => return RefundChainOutcome::Reverted,
        _ => return RefundChainOutcome::Unavailable,
    }
    let recipient_topic = format!(
        "0x{:0>64}",
        recipient.trim_start_matches("0x").to_ascii_lowercase()
    );
    let mut units: u128 = 0;
    for log in receipt
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let address = log.get("address").and_then(Value::as_str).unwrap_or("");
        let topics: Vec<String> = log
            .get("topics")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|topic| topic.as_str().map(str::to_ascii_lowercase))
            .collect();
        if !address.eq_ignore_ascii_case(USDT0_ARBITRUM_ONE)
            || topics.len() != 3
            || topics[0] != TRANSFER_TOPIC
            || topics[2] != recipient_topic
        {
            continue;
        }
        let data = log.get("data").and_then(Value::as_str).unwrap_or("0x");
        let digits = data.trim_start_matches("0x").trim_start_matches('0');
        let value = if digits.is_empty() {
            Ok(0)
        } else {
            u128::from_str_radix(digits, 16)
        };
        let Ok(value) = value else {
            return RefundChainOutcome::Unavailable;
        };
        units = units.saturating_add(value);
    }
    RefundChainOutcome::Transferred {
        units: i64::try_from(units).unwrap_or(i64::MAX),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUYER: &str = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed";
    const OTHER: &str = "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359";

    fn transfer(token: &str, to: &str, units: u64) -> Value {
        json!({
            "address": token,
            "topics": [
                TRANSFER_TOPIC,
                format!("0x{:0>64}", "1234"),
                format!("0x{:0>64}", to.trim_start_matches("0x").to_ascii_lowercase()),
            ],
            "data": format!("0x{units:064x}"),
        })
    }

    #[test]
    fn sums_usdt0_sent_to_the_recipient() {
        let receipt = json!({
            "status": "0x1",
            "logs": [
                transfer("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", BUYER, 970_435),
                transfer(USDT0_ARBITRUM_ONE, OTHER, 29_565),
                transfer("0x0000000000000000000000000000000000000001", BUYER, 5_000_000),
            ],
        });
        assert_eq!(
            outcome_from_receipt(&receipt, BUYER),
            RefundChainOutcome::Transferred { units: 970_435 }
        );
        assert_eq!(
            outcome_from_receipt(&receipt, "0x8617E340B3D01FA5F11F306F4090FD50E238070D"),
            RefundChainOutcome::Transferred { units: 0 }
        );
    }

    #[test]
    fn reads_missing_and_reverted_receipts() {
        assert_eq!(
            outcome_from_receipt(&Value::Null, BUYER),
            RefundChainOutcome::NotFound
        );
        assert_eq!(
            outcome_from_receipt(&json!({ "status": "0x0", "logs": [] }), BUYER),
            RefundChainOutcome::Reverted
        );
        assert_eq!(
            outcome_from_receipt(&json!({ "logs": [] }), BUYER),
            RefundChainOutcome::Unavailable
        );
    }

    #[test]
    fn rejects_a_non_http_url() {
        assert!(ArbitrumRpc::new("ws://arbitrum.example").is_err());
        assert!(ArbitrumRpc::new("https://arbitrum.example").is_ok());
    }
}
