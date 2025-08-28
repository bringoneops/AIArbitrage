use canonicalizer::CanonicalService;
use ingestor::agents::binance::ohlcv::parse_bar as parse_binance_bar;
use ingestor::agents::coinbase::ohlcv::parse_bar as parse_coinbase_bar;
use serde_json::json;
use std::sync::Once;

async fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        std::env::set_var("BINANCE_QUOTES", "usdt,usdc,busd,usd,btc,eth,bnb");
    });
    CanonicalService::init().await;
}

#[tokio::test]
async fn binance_bar_canonicalized() {
    setup().await;
    let data = json!([[0, "1.0", "2.0", "0.5", "1.5", "100", 0, "0", 0, "0", "0", "0"]]);
    let bar = parse_binance_bar("btcusdt", 60, &data).expect("parse");
    assert_eq!(bar.symbol, "BTC-USDT");
    assert_eq!(bar.open, "1.0");
}

#[tokio::test]
async fn coinbase_bar_canonicalized() {
    setup().await;
    let data = json!([[0, 0.5, 2.0, 1.0, 1.5, 100.0]]);
    let bar = parse_coinbase_bar("BTC-USD", 60, &data).expect("parse");
    assert_eq!(bar.symbol, "BTC-USD");
    assert_eq!(bar.close, "1.5");
}
