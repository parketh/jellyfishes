import path from "node:path"
import * as process from "node:process"
import { PostgresState } from "../../core/states/postgres_state"
import { createLogger, formatNumber } from "../../examples/utils"
import { SolanaSwapsStream } from "../../streams/solana_swaps/solana_swaps"
import { cleanAllBeforeOffset, createPostgresClient, ensureTables, toUnixTime } from "../postgres"
import { getSortFunction } from "./util"
import { DataSource } from "typeorm"

const DECIMALS = {
  So11111111111111111111111111111111111111112: 9,
}

function denominate(amount: bigint, mint: string) {
  const decimals = DECIMALS[mint] || 6

  return Number(amount) / 10 ** decimals
}

const TRACKED_TOKENS = [
  "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
  "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", // USDT
  "So11111111111111111111111111111111111111112", // SOL
]

const sortTokens = getSortFunction(TRACKED_TOKENS)

async function main() {
  // Create a logger
  const logger = createLogger("solana_swaps")
  logger.info("Creating database connection...")

  // Create a new Postgres client and test connection
  const pgClient = createPostgresClient()
  try {
    await pgClient.connect()
    logger.info("Postgres client connected successfully.")
  } catch (error) {
    logger.error("Failed to connect to Postgres:", error)
    process.exit(1)
  }

  // Create a new TypeORM db and initialize it
  const db = await new DataSource({
    type: "postgres",
    host: "localhost",
    port: 6432,
    username: "postgres",
    password: "postgres",
    database: "postgres",
  }).initialize()
  logger.info("Database connection initialized.")

  // define stream
  const ds = new SolanaSwapsStream({
    portal: "https://portal.sqd.dev/datasets/solana-mainnet",
    args: {
      // fromBlock: 240_000_000,
      fromBlock: 300_000_000,
      // toBlock: 242_936_378,
      tokens: TRACKED_TOKENS,
      type: ["raydium_cpmm"],
    },
    logger,
    state: new PostgresState(db, {
      table: "solana_sync_status",
      id: "dex_swaps",
    }),
    onStart: async ({ current, initial }) => {
      /**
       * Clean all data before the current offset.
       * There is a small chance if the stream is interrupted, the data will be duplicated.
       * We just clean it up at the start to avoid duplicates.
       */
      await cleanAllBeforeOffset(
        { pgClient, logger },
        { table: "solana_swaps_raw", column: "block_number", offset: current.number }
      )

      if (initial.number === current.number) {
        logger.info(`Syncing from ${formatNumber(current.number)}`)
        return
      }

      logger.info(`Resuming from ${formatNumber(current.number)}`)
    },
    onProgress: ({ state, interval }) => {
      logger.info({
        message: `${formatNumber(state.current)} / ${formatNumber(state.last)} (${formatNumber(
          state.percent
        )}%)`,
        speed: `${interval.processedPerSecond} blocks/second`,
      })
    },
  })

  logger.info("Ensuring tables...")
  await ensureTables(pgClient, path.join(__dirname, "swaps.sql"))
  logger.info("Tables ensured.")

  logger.info("Streaming swaps...")
  for await (const swaps of await ds.stream()) {
    logger.info(`Inserting ${swaps.length} swaps into Postgres`)

    const values = swaps
      .filter((s) => s.input.amount > 0 && s.output.amount > 0)
      .slice(0, 10)
      .map((s) => {
        /**
         * Sort tokens naturally to preserve the same pair order, i.e., ORCA/SOL and never SOL/ORCA.
         */
        const needTokenSwap = sortTokens(s.input.mint, s.output.mint)

        const tokenA = !needTokenSwap ? s.input : s.output
        const tokenB = !needTokenSwap ? s.output : s.input

        return {
          dex: s.type,
          block_number: s.block.number,
          transaction_hash: s.transaction.hash,
          transaction_index: s.transaction.index,
          instruction_address: s.instruction.address,
          account: s.account,
          token_a: tokenA.mint,
          token_b: tokenB.mint,
          a_to_b: !needTokenSwap,
          amount_a: denominate(tokenA.amount, tokenA.mint).toString(),
          amount_b: denominate(tokenB.amount, tokenB.mint).toString(),
          timestamp: toUnixTime(s.timestamp),
          sign: 1,
        }
      })
      .map(
        (v) =>
          `('${v.dex}', ${v.block_number}, '${v.transaction_hash}', ${
            v.transaction_index
          }, '{${v.instruction_address.join(",")}}', '${v.account}', '${v.token_a}', '${
            v.token_b
          }', ${v.a_to_b}, ${v.amount_a}, ${v.amount_b}, to_timestamp(${v.timestamp}), ${v.sign})`
      )
      .join(",")

    const query = `
      INSERT INTO solana_swaps_raw 
      (dex, block_number, transaction_hash, transaction_index, instruction_address, account, 
       token_a, token_b, a_to_b, amount_a, amount_b, timestamp, sign) 
      VALUES ${values}
      `
    await pgClient.query(query)

    await ds.ack(swaps)
  }
}

void main()
