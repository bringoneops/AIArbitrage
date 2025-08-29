# Crate Map

## Workspace Members
- `crypto-ingestor` – binary crate providing exchange ingestion agents.
- `canonicalizer` – library and binary for symbol/event normalization.
- `onchain-ingestor` – binary for Ethereum event ingestion.
- `macro-data` – library and binary for macroeconomic data fetchers.

## Crate Details

### crypto-ingestor
*Targets*: bin `ingestor` (`src/main.rs`)

*Dependencies*: tokio 1, tokio-tungstenite 0.21, futures-util 0.3, serde 1, serde_json 1, async-trait 0.1,
reqwest 0.11, tracing 0.1, tracing-subscriber 0.3, chrono 0.4, canonicalizer (path), ntp 0.4,
time 0.1, hmac 0.12, sha2 0.10, hex 0.4, prometheus 0.13, hyper 0.14, once_cell 1, axum 0.6,
clap 4, config 0.13, rust_decimal 1, thiserror 1, metrics 0.21, rdkafka 0.36, ethers 2.

*Modules*:
- `agent` – defines `Agent` trait for ingestion workers.
- `agents` – factories for exchange adapters.
    - `binance`, `coinbase` – websocket agents emitting raw frames (use `CanonicalService`).
    - `deribit`, `onchain` – additional agents.
- `sink` – `OutputSink` trait with `StdoutSink`, `FileSink`, `KafkaSink`.
- `config` – CLI & settings controlling which feeds run.
- `metrics`, `clock`, `http_client`, `labels`, `metadata`, `parse`, `error` – helpers.

*Ingest implementations*: `agent` and `agents/*`.

*Normalization calls*: agents use `canonicalizer::CanonicalService`.

*Validator usage*: none present.

### canonicalizer
*Targets*: lib + bin

*Dependencies*: tokio 1, reqwest 0.11, serde 1, serde_json 1, tabwriter 1, tracing 0.1, ethers-core 2.

*Modules*:
- `lib` – `CanonicalService` and event types (`L2Diff`, etc.).
- `events` – additional canonical structs (`Bar`, `Order`, ...).
- `http_client` – helper to build TLS HTTP client.
 - `onchain` – formatting for onchain transactions/logs.

*Normalization implementations*: `CanonicalService::canonical_pair`, `onchain::format_*`.

*Direct callers*: `crypto-ingestor` agents, `onchain-ingestor`.

### onchain-ingestor
*Target*: bin

*Dependencies*: tokio 1, ethers 2 (ws), serde 1, serde_json 1, clap 4, async-trait 0.1,
rdkafka 0.36, canonicalizer (path), anyhow 1.

*Modules*: `main` – subscribes to Ethereum blocks/logs and outputs
canonical JSON via sinks; `sink` – defines `DynSink`, `KafkaSink`, `StdoutSink`.

 *Ingest implementations*: `main` uses websocket provider to ingest onchain data.

### macro-data
*Targets*: lib + bin

*Dependencies*: tokio 1, reqwest 0.11, serde 1, serde_json 1, chrono 0.4,
tracing 0.1, tracing-subscriber 0.3, tokio-util 0.7.

*Modules*: `lib` spawning periodic fetchers for macro metrics
and crypto indices.

No ingest/normalize/validate traits.

