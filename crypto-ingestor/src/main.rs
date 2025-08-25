mod agent;
mod agents;

use agents::{available_agents, make_agent};
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
        for a in available_agents() { eprintln!("  - {a}"); }
        std::process::exit(2);
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let mut handles = Vec::new();
    for spec in specs.drain(..) {
        match make_agent(&spec) {
            Some(mut agent) => {
                let rx = shutdown_rx.clone(); // no need for `mut`
                let name = agent.name();
                tracing::info!(%spec, agent=%name, "spawning agent");
                handles.push(tokio::spawn(async move {
                    if let Err(e) = agent.run(rx).await {
                        tracing::error!(agent=%name, error=%e, "agent exited with error");
                    } else {
                        tracing::info!(agent=%name, "agent exited");
                    }
                }));
            }
            None => {
                eprintln!("Unknown agent spec: {spec}");
                for a in available_agents() { eprintln!("  - {a}"); }
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

    Ok(())
}
