use crate::agents::{binance, coinbase};
use crate::sink::DynSink;

/// Spawn metadata agents for supported exchanges and wait for completion.
pub async fn run(shutdown: tokio::sync::watch::Receiver<bool>, sink: DynSink) {
    let b = binance::metadata::run(shutdown.clone(), sink.clone());
    let c = coinbase::metadata::run(shutdown, sink);
    let _ = tokio::join!(b, c);
}
