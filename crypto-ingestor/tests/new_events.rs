use canonicalizer::CanonicalService;
use serde_json::json;
use std::sync::Once;

async fn canonicalize(exchange: &str, symbol: &str) -> String {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        std::env::set_var("BINANCE_QUOTES", "usdt,usdc,busd,usd,btc,eth,bnb");
    });
    CanonicalService::init().await;
    CanonicalService::canonical_pair(exchange, symbol).unwrap()
}

#[tokio::test]
async fn options_chain_event_is_canonicalized() {
    let canon = canonicalize("binance", "btcusdt").await;
    let event = json!({
        "agent": "binance",
        "type": "options_chain",
        "s": canon,
        "strike": "30000",
        "expiry": "2024-12-31T00:00:00Z",
        "option_type": "call",
        "p": "100.00",
        "q": "0.01",
        "ts": 0
    });
    assert_eq!(event["s"], "BTC-USDT");
}

#[tokio::test]
async fn mempool_event_is_canonicalized() {
    let canon = canonicalize("binance", "ethbtc").await;
    let event = json!({
        "agent": "binance",
        "type": "mempool",
        "s": canon,
        "hash": "0x",
        "value": "1.0",
        "ts": 0
    });
    assert_eq!(event["s"], "ETH-BTC");
}

#[tokio::test]
async fn bridge_flow_event_is_canonicalized() {
    let canon = canonicalize("binance", "bnbeth").await;
    let event = json!({
        "agent": "binance",
        "type": "bridge_flow",
        "s": canon,
        "amount": "10",
        "from_chain": "bsc",
        "to_chain": "eth",
        "ts": 0
    });
    assert_eq!(event["s"], "BNB-ETH");
}

#[tokio::test]
async fn mev_signal_event_is_canonicalized() {
    let canon = canonicalize("binance", "adausdt").await;
    let event = json!({
        "agent": "binance",
        "type": "mev_signal",
        "s": canon,
        "strategy": "arbitrage",
        "profit": "5.0",
        "ts": 0
    });
    assert_eq!(event["s"], "ADA-USDT");
}
