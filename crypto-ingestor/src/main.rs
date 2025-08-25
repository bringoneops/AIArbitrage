mod agent;
mod agents;

use agents::{available_agents, make_agent};
use canonical::CanonicalService;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tracing_subscriber::FmtSubscriber;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // logger
    let subscriber = FmtSubscriber::builder().with_target(false).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    // CLI: ingestor binance:btcusdt,ethusdt binance:solusdt
    let mut specs = std::env::args().skip(1).collect::<Vec<_>>();
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

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // spawn canonicalizer process
    let exe = std::env::current_exe()?;
    let canon_path = exe.with_file_name("canonicalizer");
    if !canon_path.exists() {
        let mut build = Command::new("cargo");
        build.arg("build").arg("--bin").arg("canonicalizer");
        if !cfg!(debug_assertions) {
            build.arg("--release");
        }
        let status = build.status().await?;
        if !status.success() {
            return Err("failed to build canonicalizer".into());
        }
    }
    let mut canon_child = Command::new(&canon_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let canon_stdin = canon_child.stdin.take().expect("canonicalizer stdin");
    let canon_stdout = canon_child.stdout.take().expect("canonicalizer stdout");

    let (tx, mut rx) = mpsc::channel::<String>(100);

    // writer task
    tokio::spawn(async move {
        let mut stdin = canon_stdin;
        while let Some(line) = rx.recv().await {
            if stdin.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if stdin.write_all(b"\n").await.is_err() {
                break;
            }
        }
    });

    // reader task
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(canon_stdout).lines();
        let mut out = tokio::io::stdout();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = out.write_all(line.as_bytes()).await;
            let _ = out.write_all(b"\n").await;
        }
    });

    // Initialise the canonical service before any agents are created so that
    // the required quote asset list is available for symbol comparisons.
    CanonicalService::init().await;

    let mut handles = Vec::new();
    for spec in specs.drain(..) {
        match make_agent(&spec).await {
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
    let _ = canon_child.wait().await;

    Ok(())
}
