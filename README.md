## Run

```bash
# Install dependencies
yarn install

# Run Clickhouse
docker compose up -d ch 

# Run swaps indexing
yarn ts-node solana_dexes/swaps/cli.ts
```