use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::time::{self, Duration};

/// Type of risk event gathered by the monitoring task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RiskEventType {
    ProofOfReserves,
    Incident,
    RegulatoryNews,
}

/// Generic risk event reported by the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvent {
    pub kind: RiskEventType,
    pub source: String,
    pub details: String,
    pub timestamp: DateTime<Utc>,
}

/// Event describing a smart contract audit score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractRisk {
    pub contract_address: String,
    pub score: u8,
    pub timestamp: DateTime<Utc>,
}

/// Event describing a stablecoin issuer update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StablecoinRisk {
    pub symbol: String,
    pub issuer: String,
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

/// Spawn a task that periodically emits placeholder [`RiskEvent`] records.
pub fn spawn_risk_monitor(interval: Duration) -> broadcast::Receiver<RiskEvent> {
    let (tx, rx) = broadcast::channel(16);
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        loop {
            ticker.tick().await;
            let event = RiskEvent {
                kind: RiskEventType::RegulatoryNews,
                source: "placeholder".into(),
                details: "no news".into(),
                timestamp: Utc::now(),
            };
            let _ = tx.send(event);
        }
    });
    rx
}

/// Return a mock blacklist dataset.
pub fn sync_blacklists() -> HashSet<String> {
    HashSet::from(["0xDEADBEEF".to_string()])
}

/// Flag any addresses that appear in the blacklist.
pub fn flag_blacklisted(addrs: &[String], blacklist: &HashSet<String>) -> Vec<String> {
    addrs
        .iter()
        .filter(|a| blacklist.contains(*a))
        .cloned()
        .collect()
}

/// Convert audit scores into [`ContractRisk`] events.
pub fn integrate_audit_scores(scores: Vec<(String, u8)>) -> Vec<ContractRisk> {
    scores
        .into_iter()
        .map(|(addr, score)| ContractRisk {
            contract_address: addr,
            score,
            timestamp: Utc::now(),
        })
        .collect()
}

/// Convert issuer updates into [`StablecoinRisk`] events.
pub fn integrate_stablecoin_updates(
    updates: Vec<(String, String, String)>,
) -> Vec<StablecoinRisk> {
    updates
        .into_iter()
        .map(|(symbol, issuer, status)| StablecoinRisk {
            symbol,
            issuer,
            status,
            timestamp: Utc::now(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emits_placeholder_risk_events() {
        let mut rx = spawn_risk_monitor(Duration::from_millis(10));
        let evt = rx.recv().await.unwrap();
        assert_eq!(evt.source, "placeholder");
    }

    #[test]
    fn flags_blacklisted_addresses() {
        let blacklist = sync_blacklists();
        let addrs = vec!["0xDEADBEEF".to_string(), "0x123".to_string()];
        let flagged = flag_blacklisted(&addrs, &blacklist);
        assert_eq!(flagged, vec!["0xDEADBEEF"]);
    }

    #[test]
    fn integrates_contract_and_stablecoin_risk() {
        let contracts = integrate_audit_scores(vec![("0x1".into(), 90)]);
        assert_eq!(contracts[0].score, 90);
        let stable = integrate_stablecoin_updates(vec![("USDC".into(), "Circle".into(), "ok".into())]);
        assert_eq!(stable[0].issuer, "Circle");
    }
}

