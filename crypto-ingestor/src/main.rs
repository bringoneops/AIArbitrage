mod agent;
mod agents;
mod bar;
mod config;
mod error;
mod http_client;
mod parse;
mod sink;

use agents::{available_agents, make_agent};
use bar::BarAggregator;
use canonicalizer::CanonicalService;
use clap::Parser;
use config::{Cli, Settings};
use error::IngestorError;
use sink::{DynSink, FileSink, StdoutSink};
use std::{sync::Arc, thread};
use std::sync::Arc;
use std::thread;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
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

    // initialise output sink
    let sink: DynSink = if let Some(path) = &cli.output {
        Arc::new(FileSink::new(path).await?)
    } else {
        Arc::new(StdoutSink::new())
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
    // create a channel per canonicalizer worker
    // NOTE: switch to `mpsc::unbounded_channel` with backpressure metrics if lossless delivery is required
    let worker_count = thread::available_parallelism().map_or(1, |n| n.get());
    let mut txs = Vec::with_capacity(worker_count);
    let mut rxs = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, rx) = mpsc::channel::<String>(100);
        txs.push(tx);
        rxs.push(rx);
    }

    // spawn watchdogs for canonicalizer processes
    let mut canon_handles = Vec::new();
    let bar_interval = cli.bars; // captured once for reuse
    for rx in rxs.into_iter() {
        let canon_path_clone = canon_path.clone();
        let sink_clone = sink.clone();
        canon_handles.push(tokio::spawn(async move {
            let mut rx = rx;
    // spawn watchdog pool for canonicalizer processes
    let worker_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mut worker_senders = Vec::new();
    let mut canon_handles = Vec::new();
    for _ in 0..worker_count {
        let (wtx, wrx) = mpsc::channel::<String>(100);
        worker_senders.push(wtx);
        let canon_path_clone = canon_path.clone();
        let sink_clone = sink.clone();
        let bar_interval = cli.bars;
        canon_handles.push(tokio::spawn(async move {
            let mut rx = wrx;
            let mut bar_agg = bar_interval.map(BarAggregator::new);
            loop {
                let mut cmd = Command::new(&canon_path_clone);
                if bar_agg.is_some() {
                    cmd.arg("--json");
                }
                let mut canon_child = match cmd
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
                                    if canon_stdin.write_all(line.as_bytes()).await.is_err() { break; }
                                    if canon_stdin.write_all(b"\n").await.is_err() { break; }
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
                                    if let Some(agg) = bar_agg.as_mut() {
                                        if let Some(bar) = agg.process_line(&line) {
                                            let out = serde_json::to_string(&bar).unwrap_or_default();
                                            if let Err(e) = sink.send(&out).await {
                                                tracing::error!(error=%e, "sink error");
                                            }
                                        }
                                    } else if let Err(e) = sink.send(&line).await {
                                        tracing::error!(error=%e, "sink error");
                                    }
                                }
                                _ => break,
                            }
                        }
                        status = canon_child.wait() => {
                            tracing::warn!(?status, "canonicalizer exited; restarting");
                            break;
                        }
                    }
                }

                if let Some(agg) = bar_agg.as_mut() {
                    for bar in agg.drain() {
                        let out = serde_json::to_string(&bar).unwrap_or_default();
                        if let Err(e) = sink.send(&out).await {
                            tracing::error!(error=%e, "sink error");
                        }
                    }
                }

                let _ = canon_child.kill().await;
            }
        }));
    }

                let _ = canon_child.kill().await;
            }
        }));
    }

    // dispatcher to partition input across workers (round-robin)
    let dispatcher = {
        let mut rx = rx;
        let worker_senders = worker_senders.clone();
        tokio::spawn(async move {
            let mut idx = 0usize;
            while let Some(line) = rx.recv().await {
                let tx = &worker_senders[idx % worker_senders.len()];
                let _ = tx.send(line).await;
                idx += 1;
            }
            // dropping worker_senders closes worker channels
        })
    };
    // Initialise the canonical service before any agents are created so that
    // the required quote asset list is available for symbol comparisons.
    CanonicalService::init().await;

    let mut handles = Vec::new();
    let mut next_worker = 0usize;
    for spec in specs.drain(..) {
        match make_agent(&spec, &settings).await {
            Some(mut agent) => {
                let rx = shutdown_rx.clone(); // no need for `mut`
                let name = agent.name();
                let tx_clone = txs[next_worker].clone();
                next_worker = (next_worker + 1) % txs.len();
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
    drop(txs);
    for handle in canon_handles {
        let _ = handle.await;
    drop(tx);
    let _ = dispatcher.await;
    for h in canon_handles {
        let _ = h.await;
    }

    Ok(())
}
