import { HashAndNumber } from '@abernatskiy/portal-client/src/query';
import { ClickHouseError } from '@clickhouse/client';
import { NodeClickHouseClient } from '@clickhouse/client/dist/client';
import { State } from '../state';

export class ClickhouseState implements State {
  private lastState?: HashAndNumber;

  constructor(
    private client: NodeClickHouseClient,
    private readonly options: { database?: string; table: string; id?: string },
  ) {
    this.options = {
      database: 'default',
      id: 'stream',
      ...options,
    };
  }

  async set(state: HashAndNumber) {
    await this.client.insert({
      table: this.options.table,
      values: [
        // cancel state
        this.lastState
          ? {
            id: this.options.id,
            block_number: state.number,
            block_hash: state.hash,
            sign: -1,
          }
          : null,
        // new state
        {
          id: this.options.id,
          block_number: state.number,
          block_hash: state.hash,
          sign: 1,
        },
      ].filter(Boolean),
      format: 'JSONEachRow',
    });

    this.lastState = state;
  }

  async get() {
    try {
      const res = await this.client.query({
        query: `SELECT * FROM "${this.options.database}"."${this.options.table}" FINAL WHERE id = {id:String} LIMIT 1`,
        format: 'JSONEachRow',
        query_params: {id: this.options.id},
      });

      const [block] = await res.json<{ block_number: string; block_hash: string }>();

      if (block) {
        this.lastState = {
          number: parseInt(block.block_number, 10),
          hash: block.block_hash,
        };

        return this.lastState;
      }
    } catch (e: unknown) {
      if (e instanceof ClickHouseError && e.type === 'UNKNOWN_TABLE') {
        await this.client.command({
          query: `
          CREATE TABLE IF NOT EXISTS "${this.options.database}"."${this.options.table}"
          (id String, block_number Int64, block_hash String, sign Int8)
          ENGINE = CollapsingMergeTree(sign)
          ORDER BY (id)
        `,
        });

        return;
      }

      throw e;
    }

    return;
  }
}
