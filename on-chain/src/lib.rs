use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents the state of a liquidity pool at a given block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolState {
    /// Identifier for the pool (e.g. address on chain)
    pub pool: String,
    /// Reserve of token0
    pub reserve_0: u128,
    /// Reserve of token1
    pub reserve_1: u128,
    /// Current tick for Uniswap V3 style pools
    pub tick: i32,
    /// Timestamp for the snapshot
    pub timestamp: DateTime<Utc>,
}

/// Canonical swap event emitted by DEX pools.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DexSwap {
    /// Pool identifier
    pub pool: String,
    /// Amount of token0 that changed (positive for input, negative for output)
    pub amount_0: i128,
    /// Amount of token1 that changed
    pub amount_1: i128,
    /// Address of the trader initiating the swap
    pub sender: String,
    /// Transaction hash for reference
    pub tx_hash: String,
    /// Timestamp for the swap
    pub timestamp: DateTime<Utc>,
}

/// Price information from an external oracle.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OraclePrice {
    /// Asset symbol being priced
    pub asset: String,
    /// Price denominated in quote currency (e.g. USD)
    pub price: f64,
    /// Source oracle
    pub source: OracleSource,
    /// Timestamp when the price was observed
    pub timestamp: DateTime<Utc>,
}

/// Enumeration of supported oracle sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OracleSource {
    Chainlink,
    Pyth,
}

/// Normalize raw reserve/tick updates into a [`PoolState`].
pub fn normalize_pool_state(
    pool: &str,
    reserve_0: u128,
    reserve_1: u128,
    tick: i32,
    timestamp: DateTime<Utc>,
) -> PoolState {
    PoolState {
        pool: pool.to_string(),
        reserve_0,
        reserve_1,
        tick,
        timestamp,
    }
}

/// Normalize swap information into a [`DexSwap`].
pub fn normalize_swap(
    pool: &str,
    amount_0: i128,
    amount_1: i128,
    sender: &str,
    tx_hash: &str,
    timestamp: DateTime<Utc>,
) -> DexSwap {
    DexSwap {
        pool: pool.to_string(),
        amount_0,
        amount_1,
        sender: sender.to_string(),
        tx_hash: tx_hash.to_string(),
        timestamp,
    }
}

/// Common error type for oracle operations.
#[derive(Debug, thiserror::Error)]
pub enum OracleError {
    #[error("request failed: {0}")]
    Request(String),
    #[error("parse error: {0}")]
    Parse(String),
}

/// Trait implemented by price oracles.
#[async_trait::async_trait]
pub trait Oracle {
    async fn get_price(&self, asset: &str) -> Result<f64, OracleError>;
}

/// Chainlink price oracle using a REST endpoint.
pub struct ChainlinkOracle {
    pub endpoint: String,
}

#[async_trait::async_trait]
impl Oracle for ChainlinkOracle {
    async fn get_price(&self, asset: &str) -> Result<f64, OracleError> {
        let url = format!("{}/{}", self.endpoint, asset);
        let resp = reqwest::get(&url)
            .await
            .map_err(|e| OracleError::Request(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| OracleError::Parse(e.to_string()))?;
        resp["price"].as_f64().ok_or_else(|| OracleError::Parse("missing price".into()))
    }
}

/// Pyth price oracle using a REST endpoint.
pub struct PythOracle {
    pub endpoint: String,
}

#[async_trait::async_trait]
impl Oracle for PythOracle {
    async fn get_price(&self, asset: &str) -> Result<f64, OracleError> {
        let url = format!("{}/{}", self.endpoint, asset);
        let resp = reqwest::get(&url)
            .await
            .map_err(|e| OracleError::Request(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| OracleError::Parse(e.to_string()))?;
        resp["price"].as_f64().ok_or_else(|| OracleError::Parse("missing price".into()))
    }
}

/// Fetch prices from both Chainlink and Pyth and emit [`OraclePrice`] events.
///
/// This helper cross checks both sources and returns their individual readings.
/// Consumers can compare these prices against DEX observations to detect
/// discrepancies.
pub async fn cross_check_oracles<O1, O2>(
    asset: &str,
    chainlink: &O1,
    pyth: &O2,
) -> Result<Vec<OraclePrice>, OracleError>
where
    O1: Oracle + Sync,
    O2: Oracle + Sync,
{
    let cl_price = chainlink.get_price(asset).await?;
    let pyth_price = pyth.get_price(asset).await?;
    let now = Utc::now();
    Ok(vec![
        OraclePrice {
            asset: asset.to_string(),
            price: cl_price,
            source: OracleSource::Chainlink,
            timestamp: now,
        },
        OraclePrice {
            asset: asset.to_string(),
            price: pyth_price,
            source: OracleSource::Pyth,
            timestamp: now,
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyOracle(f64);

    #[async_trait::async_trait]
    impl Oracle for DummyOracle {
        async fn get_price(&self, _asset: &str) -> Result<f64, OracleError> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn cross_check_produces_events() {
        let chainlink = DummyOracle(100.0);
        let pyth = DummyOracle(101.0);
        let events = cross_check_oracles("ETH/USD", &chainlink, &pyth)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|e| e.source == OracleSource::Chainlink && e.price == 100.0));
        assert!(events.iter().any(|e| e.source == OracleSource::Pyth && e.price == 101.0));
    }

    #[test]
    fn swap_normalization_works() {
        let ts = Utc::now();
        let swap = normalize_swap("pool", 1, -2, "0xabc", "0xhash", ts);
        assert_eq!(swap.amount_0, 1);
        assert_eq!(swap.amount_1, -2);
        assert_eq!(swap.sender, "0xabc");
        assert_eq!(swap.tx_hash, "0xhash");
    }

    #[test]
    fn pool_state_normalization_works() {
        let ts = Utc::now();
        let state = normalize_pool_state("pool", 10, 20, 3, ts);
        assert_eq!(state.reserve_0, 10);
        assert_eq!(state.reserve_1, 20);
        assert_eq!(state.tick, 3);
    }
}

