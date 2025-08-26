mod sink;

use canonicalizer::onchain::{format_log, format_tx};
use clap::Parser;
use ethers::providers::{Middleware, Provider, StreamExt, Ws};
use ethers::types::Filter;
use sink::{DynSink, KafkaSink, StdoutSink};
use std::sync::Arc;

#[derive(Parser)]
struct Cli {
    /// Websocket URL of the Ethereum node
    #[arg(long, default_value = "ws://localhost:8546")]
    ws_url: String,

    /// Output sink type (stdout or kafka)
    #[arg(long, default_value = "stdout")]
    sink: String,

    /// Kafka broker list
    #[arg(long)]
    kafka_brokers: Option<String>,

    /// Kafka topic
    #[arg(long)]
    kafka_topic: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let provider = Provider::<Ws>::connect(cli.ws_url).await?;

    let sink: DynSink = match cli.sink.as_str() {
        "kafka" => {
            let brokers = cli
                .kafka_brokers
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("kafka_brokers not set"))?;
            let topic = cli
                .kafka_topic
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("kafka_topic not set"))?;
            Arc::new(KafkaSink::new(brokers, topic)?)
        }
        _ => Arc::new(StdoutSink::new()),
    };

    let mut block_stream = provider.subscribe_blocks().await?;
    let mut log_stream = provider.subscribe_logs(&Filter::new()).await?;

    loop {
        tokio::select! {
            maybe_block = block_stream.next() => {
                if let Some(block) = maybe_block {
                    if let Some(hash) = block.hash {
                        if let Some(full) = provider.get_block_with_txs(hash).await? {
                            for tx in full.transactions {
                                let ev = format_tx(&tx);
                                let line = serde_json::to_string(&ev)?;
                                sink.send(&line).await?;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            maybe_log = log_stream.next() => {
                if let Some(log) = maybe_log {
                    let ev = format_log(&log);
                    let line = serde_json::to_string(&ev)?;
                    sink.send(&line).await?;
                } else {
                    break;
                }
            }
        }
    }

    Ok(())
}
