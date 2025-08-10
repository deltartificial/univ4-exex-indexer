### univ4-exex-indexer

High‑performance Reth ExEx indexer that streams Uniswap v4 `Initialize`, `Swap`, `ModifyLiquidity`, and `Donate` events into ClickHouse.

### Quick start

Set ClickHouse connection and run in release mode:
```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_DATABASE="default"
cargo run --release
```

### Build

```bash
cargo build --release
```