import { HashAndNumber } from '@abernatskiy/portal-client/src/query';
import { DataSource, EntityManager } from 'typeorm';
import { State } from '../state';

export class TypeormState implements State {
  constructor(
    private db: DataSource,
    private options: { table: string; namespace?: string; id?: string },
  ) {
    this.options = {
      namespace: 'public',
      id: 'stream',
      ...options,
    };
  }

  async set(manager: EntityManager, state: HashAndNumber) {
    await manager.query(
      `UPDATE "${this.options.namespace}"."${this.options.table}" SET block_number = $1, block_hash = $2 WHERE id = $3`,
      [state.number, state.hash, this.options.id],
    );
  }

  async get() {
    try {
      const state = await this.db.query(
        `SELECT * FROM "${this.options.namespace}"."${this.options.table}" WHERE id = $1 LIMIT 1`,
        [this.options.id],
      );
      if (state.length > 0 && state[0].state > 0) {
        return state[0];
      }
    } catch (e: any) {
      if (e.code === '42P01') {
        await this.db.transaction(async (manager) => {
          await manager.query(
            `CREATE TABLE IF NOT EXISTS "${this.options.namespace}"."${this.options.table}" (id TEXT, block_number INT, block_hash TEXT)`,
          );
          await manager.query(
            `INSERT INTO "${this.options.namespace}"."${this.options.table}" (id, block_number, block_hash) VALUES ($1, $2, $3)`,
            [this.options.id, 0, ''],
          );
        });

        return;
      }

      throw e;
    }

    return;
  }
}
