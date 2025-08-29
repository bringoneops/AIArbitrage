# Keep/Remove Plan

| Item | Decision | Reason |
| --- | --- | --- |
| `crypto-ingestor` crate | KEEP | Core exchange ingestion pipeline. |
| `canonicalizer` crate | KEEP | Provides symbol and event normalization. |
| `crypto-ingestor/agents/binance` | KEEP | Needed exchange adapter. |
| `crypto-ingestor/agents/coinbase` | KEEP | Needed exchange adapter. |
| `crypto-ingestor/sink::StdoutSink` | KEEP | Minimal emit path. |
| `crypto-ingestor/sink::FileSink` | REMOVE | File output not required. |
| `crypto-ingestor/sink::KafkaSink` | REMOVE | External bus/outbox. |
| `crypto-ingestor/metrics` | REMOVE | Prometheus metrics server. |
| `crypto-ingestor/config` extra flags | STUB | Many feature toggles to be pruned later. |
| Validator trait | STUB | Not yet defined; will be introduced in later step. |
