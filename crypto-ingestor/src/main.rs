mod agent;
mod agents;
mod config;
mod error;
mod http_client;
mod parse;
mod sink;

use agents::{available_agents, make_agent};
use canonicalizer::CanonicalService;
use clap::Parser;
use config::{partition_specs, Cli, Settings};
use error::IngestorError;
use metrics_exporter_prometheus::PrometheusBuilder;
use sink::{DynSink, FileSink, StdoutSink};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), IngestorError> {
    // parse CLI and configuration
    let cli = Cli::parse();

    // logger
    let filter = if let Some(level) = cli.log_level.as_deref() {
        EnvFilter::new(level)
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
    };
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9000))
        .install()
        .expect("failed to install Prometheus recorder");

    let mut specs = cli.specs.clone();
    if specs.is_empty() {
        eprintln!("Usage: ingestor <agent_spec> [<agent_spec> ...]");
        eprintln!("Examples:");
        eprintln!("  ingestor binance:btcusdt");
        eprintln!("  ingestor binance:btcusdt,ethusdt binance:solusdt");
        eprintln!("Available:");
        for a in available_agents() {
            eprintln!("  - {a}");
        }
        std::process::exit(2);
    }

    if cli.instance_index >= cli.instance_count {
        eprintln!(
            "instance-index ({}) must be < instance-count ({})",
            cli.instance_index, cli.instance_count
        );
        std::process::exit(2);
    }
    specs = partition_specs(specs, cli.instance_count, cli.instance_index);

    let settings = Settings::load(&cli)?;

    // initialise output sink
    let sink: DynSink = if let Some(path) = &cli.output {
        Arc::new(FileSink::new(path).await?)
    } else {
        Arc::new(StdoutSink::new())
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // channel forwarding agent output directly to sink
    let (tx, mut rx) = mpsc::channel::<String>(100);
    let sink_clone = sink.clone();
    let writer = tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if let Err(e) = sink_clone.send(&line).await {
                tracing::error!(error=%e, "sink error");
            }
        }
    });

    // initialise canonical service before creating agents
    CanonicalService::init().await;

    let mut handles = Vec::new();
    for spec in specs.drain(..) {
        match make_agent(&spec, &settings).await {
            Some(mut agent) => {
                let rx = shutdown_rx.clone();
                let name = agent.name();
                let tx_clone = tx.clone();
                tracing::info!(%spec, agent=%name, "spawning agent");
                handles.push(tokio::spawn(async move {
                    if let Err(e) = agent.run(rx, tx_clone).await {
                        tracing::error!(agent=%name, error=%e, "agent exited with error");
                    } else {
                        tracing::info!(agent=%name, "agent exited");
                    }
                }));
            }
            None => {
                eprintln!("Unknown agent spec: {spec}");
                for a in available_agents() {
                    eprintln!("  - {a}");
                }
                std::process::exit(2);
            }
        }
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received; shutting down…");
            let _ = shutdown_tx.send(true);
        }
        _ = async { for h in handles { let _ = h.await; } } => {
            tracing::info!("all agents finished");
        }
    }

    drop(tx);
    let _ = writer.await;

    Ok(())
}
