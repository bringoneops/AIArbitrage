# Feature Flags and Event Schemas

Feature flags allow iterative development of new data sources. The following phases are planned:

1. Options Chain (`INGESTOR_ENABLE_OPTIONS`)
2. Mempool (`INGESTOR_ENABLE_MEMPOOL`)
3. Bridge Flows (`INGESTOR_ENABLE_BRIDGE`)
4. MEV Signals (`INGESTOR_ENABLE_MEV`)


## Options Chain (`INGESTOR_ENABLE_OPTIONS`)

Enables ingestion of options chain data.

Expected event schema:

```json
{
  "agent": "exchange name",
  "type": "options_chain",
  "s": "BTC-USDT",
  "strike": "30000",
  "expiry": "2024-12-31T00:00:00Z",
  "option_type": "call",
  "p": "100.00",
  "q": "0.01",
  "ts": 0
}
```

## Mempool (`INGESTOR_ENABLE_MEMPOOL`)

Enables ingestion of mempool transaction events.

Expected event schema:

```json
{
  "agent": "network",
  "type": "mempool",
  "s": "ETH-BTC",
  "hash": "0x...",
  "value": "1.0",
  "ts": 0
}
```

## Bridge Flows (`INGESTOR_ENABLE_BRIDGE`)

Monitors token transfers across chains.

Expected event schema:

```json
{
  "agent": "bridge",
  "type": "bridge_flow",
  "s": "BNB-ETH",
  "amount": "10",
  "from_chain": "bsc",
  "to_chain": "eth",
  "ts": 0
}
```

## MEV Signals (`INGESTOR_ENABLE_MEV`)

Surfacing miner-extractable value opportunities.

Expected event schema:

```json
{
  "agent": "searcher",
  "type": "mev_signal",
  "s": "ADA-USDT",
  "strategy": "arbitrage",
  "profit": "5.0",
  "ts": 0
}
```

Each event uses the `s` field for a canonical `BASE-QUOTE` symbol which will be normalized by the `canonicalizer` crate. These flags are placeholders intended to guide iterative development: enabling a flag will allow contributors to implement its corresponding data pipeline without affecting existing functionality.
