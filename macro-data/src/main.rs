use macro_data::spawn;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (mut macro_rx, mut crypto_rx) = spawn();
    loop {
        tokio::select! {
            Ok(metric) = macro_rx.recv() => println!("macro: {:?}", metric),
            Ok(index) = crypto_rx.recv() => println!("crypto: {:?}", index),
            _ = signal::ctrl_c() => break,
        }
    }
}
