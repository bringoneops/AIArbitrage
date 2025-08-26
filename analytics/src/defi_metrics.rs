use std::collections::HashMap;

/// State of a liquidity pool for a single token.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolState {
    /// Protocol name hosting the pool.
    pub protocol: String,
    /// Token symbol of the pool.
    pub token: String,
    /// Liquidity value denominated in USD.
    pub liquidity: f64,
    /// Annual percentage rate expressed as a fraction (e.g. 0.05 for 5%).
    pub apr: f64,
}

/// Aggregated metrics for a protocol.
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolMetrics {
    /// Total value locked across all pools of the protocol.
    pub tvl: f64,
    /// Liquidity weighted average APR across pools.
    pub average_yield: f64,
}

/// Aggregate a slice of [`PoolState`] records by protocol.
///
/// Returns a mapping from protocol name to [`ProtocolMetrics`].
pub fn aggregate_pool_states(pools: &[PoolState]) -> HashMap<String, ProtocolMetrics> {
    let mut out: HashMap<String, ProtocolMetrics> = HashMap::new();
    for pool in pools {
        let entry = out
            .entry(pool.protocol.clone())
            .or_insert(ProtocolMetrics { tvl: 0.0, average_yield: 0.0 });
        entry.tvl += pool.liquidity;
        entry.average_yield += pool.liquidity * pool.apr;
    }
    for metrics in out.values_mut() {
        if metrics.tvl > 0.0 {
            metrics.average_yield /= metrics.tvl;
        }
    }
    out
}

/// Point on a liquidity curve representing cumulative depth at a price.
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidityPoint {
    /// Cumulative quantity available up to this price level.
    pub depth: f64,
    /// Price at the level.
    pub price: f64,
}

/// Build a cumulative liquidity curve from price levels.
///
/// `levels` is expected to be sorted best price first. Each element is a
/// `(price, size)` pair describing liquidity at that level.
pub fn liquidity_curve(levels: &[(f64, f64)]) -> Vec<LiquidityPoint> {
    let mut curve = Vec::with_capacity(levels.len());
    let mut depth = 0.0;
    for &(price, size) in levels {
        depth += size;
        curve.push(LiquidityPoint { depth, price });
    }
    curve
}

/// Estimate the impact cost of executing a market order of `trade_size`.
///
/// The function consumes liquidity from `levels` until the desired size is
/// filled and returns the difference between the volume weighted average price
/// and the best price. Returns `None` if the order book is empty or does not
/// provide enough size.
pub fn impact_cost(levels: &[(f64, f64)], trade_size: f64) -> Option<f64> {
    if levels.is_empty() || trade_size <= 0.0 {
        return None;
    }
    let best_price = levels[0].0;
    let mut remaining = trade_size;
    let mut total_cost = 0.0;
    for &(price, size) in levels {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(size);
        total_cost += take * price;
        remaining -= take;
    }
    if remaining > 0.0 {
        return None;
    }
    let avg_price = total_cost / trade_size;
    Some(avg_price - best_price)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_protocol_metrics() {
        let pools = vec![
            PoolState { protocol: "A".into(), token: "ETH".into(), liquidity: 100.0, apr: 0.10 },
            PoolState { protocol: "A".into(), token: "DAI".into(), liquidity: 50.0, apr: 0.20 },
            PoolState { protocol: "B".into(), token: "USDC".into(), liquidity: 200.0, apr: 0.15 },
        ];
        let metrics = aggregate_pool_states(&pools);
        let a = metrics.get("A").unwrap();
        assert!((a.tvl - 150.0).abs() < 1e-9);
        assert!((a.average_yield - (20.0/150.0)).abs() < 1e-9);
        let b = metrics.get("B").unwrap();
        assert!((b.tvl - 200.0).abs() < 1e-9);
        assert!((b.average_yield - 0.15).abs() < 1e-9);
    }

    #[test]
    fn builds_liquidity_curve_and_impact_cost() {
        let levels = vec![(100.0, 1.0), (101.0, 2.0)];
        let curve = liquidity_curve(&levels);
        assert_eq!(curve,
            vec![
                LiquidityPoint { depth: 1.0, price: 100.0 },
                LiquidityPoint { depth: 3.0, price: 101.0 }
            ]);

        let cost = impact_cost(&levels, 2.0).unwrap();
        assert!((cost - 0.5).abs() < 1e-9);
        assert!(impact_cost(&levels, 5.0).is_none());
    }
}
