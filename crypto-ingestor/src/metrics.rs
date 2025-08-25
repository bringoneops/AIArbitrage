use once_cell::sync::Lazy;
use prometheus::{gather, register_int_counter_vec, Encoder, IntCounterVec, TextEncoder};
use std::net::SocketAddr;

pub static TRADES_RECEIVED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "trades_received_total",
        "Total number of trades received",
        &["agent"]
    )
    .unwrap()
});

pub static RECONNECTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "reconnections_total",
        "Total number of reconnection attempts",
        &["agent"]
    )
    .unwrap()
});

pub static ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("errors_total", "Total number of errors", &["agent"]).unwrap()
});

async fn metrics_handler(
    _req: hyper::Request<hyper::Body>,
) -> Result<hyper::Response<hyper::Body>, hyper::Error> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Ok(hyper::Response::builder()
        .status(200)
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(hyper::Body::from(buffer))
        .unwrap())
}

pub async fn serve(addr: SocketAddr) {
    use hyper::service::{make_service_fn, service_fn};

    let make_svc =
        make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(metrics_handler)) });
    if let Err(e) = hyper::Server::bind(&addr).serve(make_svc).await {
        eprintln!("metrics server error: {e}");
    }
}
