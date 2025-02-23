import path from "node:path"
import { createLogger, formatNumber } from "../../examples/utils"
import { SolanaMintStream } from "../../streams/solana_mint/solana_mint"
import { createPostgresClient, ensureTables, toUnixTime } from "../postgres"
import { PostgresState } from "core/states/postgres_state"
import { DataSource } from "typeorm"

async function main() {
  const pgClient = createPostgresClient()
  const logger = createLogger("solana_tokens")

  const db = await new DataSource({
    type: "postgres",
    host: "localhost",
    port: 6432,
    username: "postgres",
    password: "postgres",
    database: "postgres",
  }).initialize()

  await ensureTables(pgClient, path.join(__dirname, "mints.sql"))

  const datasource = new SolanaMintStream({
    portal: "https://portal.sqd.dev/datasets/solana-mainnet",
    args: {
      fromBlock: 240_000_000,
    },
    logger,
    state: new PostgresState(db, {
      table: "solana_sync_status",
      id: "mint",
    }),
    onProgress: ({ state, interval }) => {
      logger.info({
        message: `${formatNumber(state.current)} / ${formatNumber(state.last)} (${formatNumber(
          state.percent
        )}%)`,
        speed: `${interval.processedPerSecond} blocks/second`,
      })
    },
  })

  for await (const mints of await datasource.stream()) {
    // TODO: refactor below to use PG interface
    await pgClient.insert({
      table: "solana_tokens",
      values: mints.map((m) => ({
        account: m.account,
        decimals: m.decimals,
        mint_authority: m.mintAuthority,
        block_number: m.block.number,
        transaction_hash: m.transaction.hash,
        timestamp: toUnixTime(m.timestamp),
      })),
      format: "JSONEachRow",
    })

    await datasource.ack(mints)
  }
}

void main()
