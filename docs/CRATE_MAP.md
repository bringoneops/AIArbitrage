# Crate Map

## Workspace Members
- `crypto-ingestor` – binary crate providing exchange ingestion agents.
- `canonicalizer` – library and binary for symbol/event normalization.

## Crate Details

### crypto-ingestor
*Targets*: bin `ingestor` (`src/main.rs`)

*Dependencies*: tokio 1, tokio-tungstenite 0.21, futures-util 0.3, serde 1, serde_json 1, async-trait 0.1,
reqwest 0.11, tracing 0.1, tracing-subscriber 0.3, chrono 0.4, canonicalizer (path), ntp 0.4,
time 0.1, hmac 0.12, sha2 0.10, hex 0.4, prometheus 0.13, hyper 0.14, once_cell 1, axum 0.6,
clap 4, config 0.13, rust_decimal 1, thiserror 1, metrics 0.21.

*Modules*:
- `agent` – defines `Agent` trait for ingestion workers.
- `agents` – factories for exchange adapters.
    - `binance`, `coinbase` – websocket agents emitting raw frames (use `CanonicalService`).
- `sink` – `OutputSink` trait with `StdoutSink`, `FileSink`.
- `config` – CLI & settings controlling which feeds run.
- `metrics`, `clock`, `http_client`, `metadata`, `parse`, `error` – helpers.

*Ingest implementations*: `agent` and `agents/*`.

*Normalization calls*: agents use `canonicalizer::CanonicalService`.

*Validator usage*: none present.

### canonicalizer
*Targets*: lib + bin

*Dependencies*: tokio 1, reqwest 0.11, serde 1, serde_json 1, tabwriter 1, tracing 0.1.

*Modules*:
- `lib` – `CanonicalService` and event types (`L2Diff`, etc.).
- `events` – additional canonical structs (`Bar`, `Order`, ...).
- `http_client` – helper to build TLS HTTP client.

*Normalization implementations*: `CanonicalService::canonical_pair`.

*Direct callers*: `crypto-ingestor` agents.
