import { Client } from "pg"
import { Offset } from "../abstract_stream"
import { AbstractState, State } from "../state"

const table = (table: string) => `
    CREATE TABLE IF NOT EXISTS ${table} (
        id TEXT PRIMARY KEY,
        initial TEXT,
        offset TEXT
    )
`

type Options = { database?: string; table: string; id?: string }

export class PostgresState extends AbstractState implements State {
  options: Required<Options>
  initial?: string

  private readonly fullTableName: string

  constructor(private client: Client, options: { database?: string; table: string; id?: string }) {
    super()

    this.options = {
      database: "postgres",
      id: "stream",
      ...options,
    }

    this.fullTableName = `${this.options.table}`
  }

  async saveOffset(offset: Offset) {
    await this.client.query(
      `INSERT INTO ${this.fullTableName} (id, initial, "offset")
       VALUES ($1, $2, $3)
       ON CONFLICT (id) DO UPDATE SET initial = $2, "offset" = $3`,
      [this.options.id, this.initial, offset]
    )
  }

  async getOffset(defaultValue: Offset) {
    try {
      const query = `SELECT initial, "offset"
               FROM ${this.fullTableName}
               WHERE id = $1
               LIMIT 1`
      const res = await this.client.query(query, [this.options.id])

      const row = res.rows && res.rows.length > 0 ? res.rows[0] : null

      if (row) {
        this.initial = row.initial

        return { current: row.offset, initial: row.initial }
      } else {
        this.initial = defaultValue
        await this.saveOffset(defaultValue)

        return
      }
    } catch (e: unknown) {
      if (e instanceof Error && "code" in e && e.code === "42P01") {
        // PostgreSQL error code for undefined table
        await this.client.query(table(this.fullTableName))

        this.initial = defaultValue
        await this.saveOffset(defaultValue)

        return
      }

      throw e
    }
  }
}
