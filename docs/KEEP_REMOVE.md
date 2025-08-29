# Keep/Remove Plan

| Item | Decision | Reason |
| --- | --- | --- |
| `crypto-ingestor` crate | KEEP | Core exchange ingestion pipeline. |
| `canonicalizer` crate | KEEP | Provides symbol and event normalization. |
| `signals` crate | REMOVE | News and sentiment data outside core scope. |
| `onchain-ingestor` crate | REMOVE | Separate onchain ingestion; non-core. |
| `macro-data` crate | REMOVE | Macroeconomic data fetchers; out of scope. |
| `crypto-ingestor/agents/binance` | KEEP | Needed exchange adapter. |
| `crypto-ingestor/agents/coinbase` | KEEP | Needed exchange adapter. |
| `crypto-ingestor/agents/deribit` | REMOVE | Options ingestion; non-core. |
| `crypto-ingestor/agents/onchain` | REMOVE | Onchain transfers; non-core. |
| `crypto-ingestor/sink::StdoutSink` | KEEP | Minimal emit path. |
| `crypto-ingestor/sink::FileSink` | REMOVE | File output not required. |
| `crypto-ingestor/sink::KafkaSink` | REMOVE | External bus/outbox. |
| `crypto-ingestor/metrics` | REMOVE | Prometheus metrics server. |
| `crypto-ingestor/config` extra flags | STUB | Many feature toggles to be pruned later. |
| `canonicalizer/onchain` | STUB | Used only by onchain ingestion; revisit after removing onchain-ingestor. |
| Validator trait | STUB | Not yet defined; will be introduced in later step. |
