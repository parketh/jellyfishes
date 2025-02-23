import fs from "node:fs/promises"
import path from "node:path"
import * as process from "node:process"
import { Client } from "pg"
import { Logger } from "../core/abstract_stream"

export async function loadSqlFiles(directoryOrFile: string): Promise<string[]> {
  let sqlFiles: string[] = []

  if (directoryOrFile.endsWith(".sql")) {
    sqlFiles = [directoryOrFile]
  } else {
    const files = await fs.readdir(directoryOrFile)
    sqlFiles = files
      .filter((file) => path.extname(file) === ".sql")
      .map((file) => path.join(directoryOrFile, file))
  }

  const tables = await Promise.all(sqlFiles.map((file) => fs.readFile(file, "utf-8")))

  return tables.flatMap((table) => table.split(";").filter((t) => t.trim().length > 0))
}

export async function ensureTables(pgClient: Client, dir: string) {
  const tables = await loadSqlFiles(dir)

  for (const table of tables) {
    try {
      await pgClient.query(table)
      console.info(`Successfully executed SQL statement`)
    } catch (e: any) {
      console.error(`======================`)
      console.error(table.trim())
      console.error(`======================`)
      console.error(`Failed to create table: ${e.message}`)
      if (!e.message) console.error(e)

      process.exit(1)
    }
  }
}

export function createPostgresClient() {
  const options = {
    host: process.env.PG_HOST || "localhost",
    port: parseInt(process.env.PG_PORT || "6432", 10),
    user: process.env.PG_USER || "postgres",
    password: process.env.PG_PASSWORD || "postgres",
    database: process.env.PG_DATABASE || "postgres",
  }
  console.info(`Creating Postgres client with options: ${JSON.stringify(options)}`)

  return new Client(options)
}

export function toUnixTime(time: Date | string | number): number {
  if (typeof time === "string") {
    time = new Date(time).getTime()
  } else if (typeof time !== "number") {
    time = time.getTime()
  }

  return Math.floor(time / 1000)
}

export async function cleanAllBeforeOffset(
  { pgClient, logger }: { pgClient: Client; logger: Logger },
  { table, offset, column }: { table: string | string[]; offset: number; column: string }
) {
  const tables = typeof table === "string" ? [table] : table

  await Promise.all(
    tables.map(async (table) => {
      const res = await pgClient.query({
        text: `SELECT *
               FROM ${table}
               WHERE ${column} >= $1`,
        values: [offset],
      })

      const rows = res.rows
      if (rows.length === 0) {
        return
      }

      logger.info(`Rolling back ${rows.length} rows from ${table}`)

      await Promise.all(
        rows.map((row: any) => {
          const rollbackQuery = `INSERT INTO ${table} (${Object.keys(row).join(", ")})
                                 VALUES (${Object.values(row)
                                   .map((_, i) => `$${i + 1}`)
                                   .join(", ")})
                                 ON CONFLICT DO NOTHING`
          return pgClient.query(rollbackQuery, Object.values(row))
        })
      )
    })
  )
}
