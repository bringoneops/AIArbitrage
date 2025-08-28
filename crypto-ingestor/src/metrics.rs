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

pub static LAST_MARK_PRICE_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_mark_price_timestamp",
        "Unix timestamp of last mark price received",
        &["agent"]
    )
    .unwrap()
});

pub static LAST_FUNDING_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_funding_timestamp",
        "Unix timestamp of last funding event received",
        &["agent"]
    )
    .unwrap()
});

pub static LAST_OPEN_INTEREST_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_open_interest_timestamp",
        "Unix timestamp of last open interest event received",
        &["agent"]
    )
    .unwrap()
});

pub static LAST_TERM_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_term_structure_timestamp",
        "Unix timestamp of last term structure event received",
        &["agent"]
    )
    .unwrap()
});

pub static LAST_LIQUIDATION_TIMESTAMP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "last_liquidation_timestamp",
        "Unix timestamp of last liquidation event received",
        &["agent"]
    )
    .unwrap()
});

pub static CANONICALIZER_RESTARTS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "canonicalizer_restarts_total",
        "Number of canonicalizer restarts"
    )
    .unwrap()
});

pub static STREAM_LATENCY_MS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "stream_latency_ms",
        "Latency between event timestamp and ingest in ms",
        &["agent", "stream"]
    )
    .unwrap()
});


pub static STREAM_DROPS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "stream_dropped_total",
        "Messages dropped per stream",
        &["agent", "stream"]
    )
    .unwrap()
});

pub static STREAM_SEQ_GAPS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "stream_sequence_gaps_total",
        "Detected sequence gaps per stream",
        &["agent", "stream"]
    )
    .unwrap()
});

pub static STREAM_THROUGHPUT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "stream_throughput_total",
        "Number of events emitted per stream",
        &["agent", "stream"]
    )
    .unwrap()
});

pub static BACKPRESSURE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "stream_backpressure",
        "Queued messages waiting to be sent",
        &["agent", "stream"]
    )
    .unwrap()
});

pub static CLOCK_SKEW: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "clock_skew_ms",
        "Clock skew compared to NTP/PTP in ms",
        &["source"]
    )
    .unwrap()
});

pub static RECONNECTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "reconnects_total",
        "Reconnect attempts per agent",
        &["agent"]
    )
    .unwrap()
});

pub static BACKOFF_SECS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "backoff_seconds_total",
        "Total seconds spent backing off per agent",
        &["agent"]
    )
    .unwrap()
});

pub static VALIDATION_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "validation_errors_total",
        "Validation errors encountered",
        &["agent"]
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
