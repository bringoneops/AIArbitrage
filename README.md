# AIArbitrage

Simple cryptocurrency data ingestor demonstrating async Rust agents. Both
Binance and Coinbase agents stream market data via WebSockets.

This repository is organised as a Cargo workspace containing two crates:

- `crypto-ingestor` – the main executable that spawns exchange agents.
- `canonicalizer` – a standalone service crate providing a library and binary
  for converting exchange-specific symbols into a canonical `BASE-QUOTE` form.
- `analytics` – consumes canonicalized trades, tracks latest prices per
  exchange and emits spread events.

## Available agents

- `binance` – streams trade data for selected symbols via WebSocket.
- `coinbase` – streams trade data for selected pairs via WebSocket.

## Phase 1 feeds

`crypto-ingestor` can toggle a variety of market and auxiliary data streams at
runtime. Each feed is enabled via a dedicated command-line flag:

- `--trades` – raw trade data
- `--l2-diffs` – incremental order book updates
- `--l2-snapshots` – full order book snapshots
- `--book-ticker` – best bid/ask updates
- `--ticker-24h` – rolling 24‑hour ticker
- `--ohlcv` – candlestick data
- `--index-price` – index prices
- `--mark-price` – futures mark prices
- `--funding-rates` – funding rate changes
- `--open-interest` – open interest statistics
- `--onchain-transfers` – on-chain transfer activity
- `--onchain-balances` – on-chain balance changes
- `--top-dex-pools` – top DEX pool prices
- `--news-headlines` – crypto news headlines
- `--telemetry` – system telemetry events

Example enabling trades and the 24h ticker:

```bash
cargo run --release -- --trades --ticker-24h binance:btcusdt
```

## Metrics

`crypto-ingestor` exposes Prometheus metrics and a health check on port `9898`.
Visit `http://localhost:9898/metrics` for metrics and `http://localhost:9898/health`
for a simple readiness probe.

## Canonicalizer

The `canonicalizer` crate provides both the `CanonicalService` library and a
`canonicalizer` binary that normalizes symbols across exchanges. The binary
reads trade messages as JSON lines on `STDIN`, converts the `s` field to the
canonical `BASE-QUOTE` form using `canonicalizer::CanonicalService`, and by
default prints the `agent`, `s`, `p`, and `q` fields in aligned tab-separated
columns for easy reading. Use the `--json` flag to emit the modified JSON
records, preserving the previous behaviour.

The ingestor spawns this canonicalizer automatically so all output is already
canonicalized:

```bash
cargo run --release -- binance:btcusdt coinbase:BTC-USD
```

Example pipeline sending canonicalized trades to another process:

```bash
cargo run --release -- binance:btcusdt coinbase:BTC-USD | jq '.'
```

To run the canonicalizer service on its own:

```bash
cargo run -p canonicalizer
```

Example using the default column output:

```bash
echo '{"agent":"binance","s":"btcusdt","p":"30000.00","q":"0.01"}' | cargo run -p canonicalizer
```

```
binance  BTC-USDT  30000.00  0.01
```

For raw JSON suitable for scripting, pass `--json`:

```bash
echo '{"agent":"binance","s":"btcusdt","p":"30000.00","q":"0.01"}' | cargo run -p canonicalizer -- --json
```

```
{"agent":"binance","s":"BTC-USDT","p":"30000.00","q":"0.01"}
```

## Trade format

Each line emitted by an agent is a JSON object:

```
{"agent":"binance","type":"trade","s":"BTC-USD","t":12345,"p":"30000.00","q":"0.01","ts":1680000000000}
```

Fields:

- `agent` – source exchange
- `type` – currently always `trade`
- `s` – canonical `BASE-QUOTE` symbol
- `t` – trade identifier if available, otherwise `null`
- `p` – price as a string
- `q` – quantity as a string
- `ts` – trade timestamp in milliseconds since Unix epoch

When either `binance:all` or `coinbase:all` agents are used, both exchanges
subscribe only to USD-quoted pairs common to both platforms so their symbol
sets align.

## Analytics

The `analytics` crate listens for canonicalized trade records, maintains the
latest price per exchange for each symbol, and computes inter-exchange spreads.
When a spread exceeds a configurable threshold it emits a JSON event and logs
the potential arbitrage opportunity.

Run it by piping canonicalized trades from the ingestor:

```bash
cargo run --release -- binance:btcusdt coinbase:BTC-USD | \
    cargo run -p analytics -- 10
```

The numeric argument specifies the minimum spread before an event is produced.
Consumers can also use the library directly via the channel returned from
`analytics::spawn`.

