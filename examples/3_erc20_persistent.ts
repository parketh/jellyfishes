import { PortalClient } from '@subsquid/portal-client';
import { DataSource as TypeormDatabase } from 'typeorm';
import { TypeormState } from '../core/states/typeorm_state';
import { Erc20Stream } from '../streams/erc20/erc20_stream';
import { last } from './utils';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  /**
   * Initialize and connect a Typeorm database
   */
  const db = await new TypeormDatabase({
    type: 'postgres',
    username: 'postgres',
    password: 'postgres',
    port: 6432,
  }).initialize();

  /**
   *  Current streaming progress will be stored in the 'status_erc20' postgres table.
   *  The table will be created if it does not exist.
   *  If the table already exists, the last processed block will be read from it,
   *  thus allowing the process to continue from the last block.
   */
  const state = new TypeormState(db, {
    table: 'status_erc20',
  });

  const ds = new Erc20Stream({
    portal,
    args: {
      fromBlock: 4634748,
      toBlock: 15844479,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },

    // 'sqd/streams'
    // forkManager
    state,
  });

  for await (const erc20 of await ds.stream()) {
    await db.transaction(async (manager) => {
      console.info(`processed ${erc20.length} erc20 transfers`);

      /**
       * Save the last block to the state
       */
      await state.set(manager, last(erc20).block);
    });
  }
}

void main();
