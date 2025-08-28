use macro_data::spawn;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let shutdown = CancellationToken::new();
    let (mut macro_rx, mut crypto_rx) = spawn(shutdown.clone());
    loop {
        tokio::select! {
            Ok(metric) = macro_rx.recv() => println!("macro: {:?}", metric),
            Ok(index) = crypto_rx.recv() => println!("crypto: {:?}", index),
            _ = signal::ctrl_c() => {
                shutdown.cancel();
                break;
            }
        }
    }
}
