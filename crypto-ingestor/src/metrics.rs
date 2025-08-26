use std::net::SocketAddr;

use axum::{routing::get, Router};
use once_cell::sync::Lazy;
use prometheus::{
    gather, register_int_counter, register_int_counter_vec, register_int_gauge_vec, Encoder,
    IntCounter, IntCounterVec, IntGaugeVec, TextEncoder,
};

pub static MESSAGES_INGESTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "messages_ingested_total",
        "Total number of messages ingested",
        &["agent"]
    )
    .unwrap()
});

pub static ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("errors_total", "Total number of errors", &["agent"]).unwrap()
});

pub static ACTIVE_CONNECTIONS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "active_connections",
        "Number of active websocket connections",
        &["agent"]
    )
    .unwrap()
});

pub static LAST_TRADE_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_trade_timestamp",
        "Unix timestamp of last trade received",
        &["agent"]
    )
    .unwrap()
});

pub static ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("errors_total", "Total number of errors", &["agent"]).unwrap()
});

pub static CANONICALIZER_RESTARTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "canonicalizer_restarts_total",
        "Number of canonicalizer restarts"
    )
    .unwrap()
});

async fn metrics_handler() -> impl axum::response::IntoResponse {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )],
        buffer,
    )
}

async fn health_handler() -> &'static str {
    "ok"
}

pub async fn serve(addr: SocketAddr) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler));

    if let Err(e) = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
    {
        eprintln!("metrics server error: {e}");
    }
}
