# AIArbitrage

Simple cryptocurrency data ingestor demonstrating async Rust agents. Both
Binance and Coinbase agents stream market data via WebSockets.

## Available agents

- `binance` – streams trade data for selected symbols via WebSocket.
- `coinbase` – streams trade data for selected pairs via WebSocket.

## Canonicalizer

The project includes a `canonicalizer` binary that normalizes symbols across
exchanges. It reads trade messages as JSON lines on `STDIN`, converts the `s`
field to the canonical `BASE-QUOTE` form using `canonical::CanonicalService`,
and emits the modified JSON on `STDOUT`.

The ingestor spawns this canonicalizer automatically so all output is already
canonicalized:

```bash
cargo run --release -- binance:btcusdt coinbase:BTC-USD
```

Example pipeline sending canonicalized trades to another process:

```bash
cargo run --release -- binance:btcusdt coinbase:BTC-USD | jq '.'
```

