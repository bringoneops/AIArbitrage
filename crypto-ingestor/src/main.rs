mod agent;
mod agents;
mod config;
mod error;
mod http_client;
mod metrics;
mod parse;
mod sink;

use agents::{available_agents, make_agent};
use canonicalizer::CanonicalService;
use clap::Parser;
use config::{Cli, Settings};
use error::IngestorError;
use sink::{DynSink, FileSink, KafkaSink, StdoutSink};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing_subscriber::FmtSubscriber;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), IngestorError> {
    // logger
    let subscriber = FmtSubscriber::builder().with_target(false).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    // parse CLI and configuration
    let cli = Cli::parse();
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
    let settings = Settings::load(&cli)?;

    // metrics server
    tokio::spawn(metrics::serve(([0, 0, 0, 0], 9898).into()));

    // initialise output sink
    let sink: DynSink = match settings.sink.as_str() {
        "stdout" => Arc::new(StdoutSink::new()),
        "file" => {
            let path = settings
                .file_path
                .as_ref()
                .ok_or_else(|| IngestorError::Other("file_path not set".into()))?;
            Arc::new(FileSink::new(path).await.map_err(IngestorError::Io)?)
        }
        "kafka" => {
            let brokers = settings
                .kafka_brokers
                .as_ref()
                .ok_or_else(|| IngestorError::Other("kafka_brokers not set".into()))?;
            let topic = settings
                .kafka_topic
                .as_ref()
                .ok_or_else(|| IngestorError::Other("kafka_topic not set".into()))?;
            Arc::new(KafkaSink::new(brokers, topic)?)
        }
        other => {
            return Err(IngestorError::Other(format!(
                "unknown sink type: {}",
                other
            )));
        }
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // spawn canonicalizer process
    let exe = std::env::current_exe()?;
    let canon_path = exe.with_file_name("canonicalizer");
    if !canon_path.exists() {
        let mut build = Command::new("cargo");
        build
            .arg("build")
            .arg("-p")
            .arg("canonicalizer")
            .arg("--bin")
            .arg("canonicalizer");
        if !cfg!(debug_assertions) {
            build.arg("--release");
        }
        let status = build.status().await?;
        if !status.success() {
            return Err(IngestorError::Other("failed to build canonicalizer".into()));
        }
    }
    let (tx, rx) = mpsc::channel::<String>(100);

    // spawn watchdog for canonicalizer process
    let canon_path_clone = canon_path.clone();
    let sink_clone = sink.clone();
    let canon_watchdog = tokio::spawn(async move {
        let mut rx = rx;
        loop {
            let mut canon_child = match Command::new(&canon_path_clone)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    tracing::error!(error=%e, "failed to spawn canonicalizer");
                    return;
                }
            };

            let mut canon_stdin = canon_child.stdin.take().expect("canonicalizer stdin");
            let canon_stdout = canon_child.stdout.take().expect("canonicalizer stdout");
            let mut reader = tokio::io::BufReader::new(canon_stdout).lines();
            let sink = sink_clone.clone();

            loop {
                tokio::select! {
                    line = rx.recv() => {
                        match line {
                            Some(line) => {
                                if canon_stdin.write_all(line.as_bytes()).await.is_err() {
                                    break;
                                }
                                if canon_stdin.write_all(b"\n").await.is_err() {
                                    break;
                                }
                            }
                            None => {
                                let _ = canon_child.kill().await;
                                return;
                            }
                        }
                    }
                    res = reader.next_line() => {
                        match res {
                            Ok(Some(line)) => {
                                if let Err(e) = sink.send(&line).await {
                                    tracing::error!(error=%e, "sink error");
                                }
                            }
                            _ => break,
                        }
                    }
                    status = canon_child.wait() => {
                        tracing::warn!(?status, "canonicalizer exited; restarting");
                        metrics::CANONICALIZER_RESTARTS.inc();
                        break;
                    }
                }
            }

            let _ = canon_child.kill().await;
        }
    });

    // Initialise the canonical service before any agents are created so that
    // the required quote asset list is available for symbol comparisons.
    CanonicalService::init().await;

    let mut handles = Vec::new();
    for spec in specs.drain(..) {
        match make_agent(&spec, &settings).await {
            Some(mut agent) => {
                let rx = shutdown_rx.clone(); // no need for `mut`
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
        _ = async {
            for h in handles { let _ = h.await; }
        } => {
            tracing::info!("all agents finished");
        }
    }

    drop(tx);
    let _ = canon_watchdog.await;

    Ok(())
}
