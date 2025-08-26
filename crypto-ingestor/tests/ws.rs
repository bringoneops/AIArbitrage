use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use ingestor::agent::Agent;
use ingestor::agents::{binance::BinanceAgent, coinbase::CoinbaseAgent};
use ingestor::config::Settings;

#[tokio::test]
async fn coinbase_trade_messages_are_canonicalized_with_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        // read subscription
        let _ = ws.next().await;
        let msg = json!({
            "type": "match",
            "product_id": "eth-usd",
            "trade_id": 42,
            "price": "100.00",
            "size": "0.5",
            "time": "2024-01-01T00:00:00Z"
        })
        .to_string();
        ws.send(Message::Text(msg)).await.unwrap();
        // wait for client close
        let _ = ws.next().await;
    });

    let cfg = Settings {
        binance_ws_url: "ws://localhost".into(),
        binance_refresh_interval_mins: 60,
        binance_max_reconnect_delay_secs: 1,
        coinbase_ws_url: format!("ws://{}", addr),
        coinbase_refresh_interval_mins: 60,
        coinbase_max_reconnect_delay_secs: 1,
    };

    let mut agent = CoinbaseAgent::new(vec!["ETH-USD".into()], &cfg);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (tx, mut rx) = mpsc::channel::<String>(1);

    let handle = tokio::spawn(async move {
        agent.run(shutdown_rx, tx).await.unwrap();
    });

    let line = rx.recv().await.expect("no message");
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["s"], "ETH-USD");
    assert_eq!(v["t"], 42);
    assert_eq!(v["p"], "100");
    assert_eq!(v["q"], "0.5");

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn binance_trade_messages_are_canonicalized_with_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        // read subscription
        let _ = ws.next().await;
        // send ack
        let ack = json!({"id":1}).to_string();
        ws.send(Message::Text(ack)).await.unwrap();
        let msg = json!({
            "s": "btcusdt",
            "t": 7,
            "p": "50.00",
            "q": "0.1",
            "T": 1
        })
        .to_string();
        ws.send(Message::Text(msg)).await.unwrap();
        let _ = ws.next().await;
    });

    let cfg = Settings {
        binance_ws_url: format!("ws://{}", addr),
        binance_refresh_interval_mins: 60,
        binance_max_reconnect_delay_secs: 1,
        coinbase_ws_url: "ws://localhost".into(),
        coinbase_refresh_interval_mins: 60,
        coinbase_max_reconnect_delay_secs: 1,
    };

    let mut agent = BinanceAgent::new(Some(vec!["btcusdt".into()]), &cfg)
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (tx, mut rx) = mpsc::channel::<String>(1);

    let handle = tokio::spawn(async move {
        agent.run(shutdown_rx, tx).await.unwrap();
    });

    let line = rx.recv().await.expect("no message");
    let v: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["s"], "BTC-USDT");
    assert_eq!(v["t"], 7);
    assert_eq!(v["p"], "50");
    assert_eq!(v["q"], "0.1");

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap();
    server.await.unwrap();
}
