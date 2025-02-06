import { PortalClient } from '@abernatskiy/portal-client';
import { DataSource as TypeormDatabase } from 'typeorm';

import { mergeConcurrently } from '../core/merge_datasources';

import { Erc20Datasource } from '../erc20/erc20';
import { UniswapDatasource } from '../uniswap/uniswap';

import { TypeormState } from '../core/states/typeorm_state';
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
   *  Current streaming progress will be stored in the 'status' postgres table.
   *  The table will be created if it does not exist.
   *  If the table already exists, the last processed block will be read from it,
   *  thus allowing the process to continue from the last block.
   *
   *  The 'id' parameter is used to distinguish between different data sources.
   *  The table will have two rows: 'erc20' and 'uniswap'.
   */
  const erc20state = new TypeormState(db, {table: 'status', id: 'erc20'});
  const uniswapState = new TypeormState(db, {table: 'status', id: 'uniswap'});

  /**
   * Merge two datasources: Erc20 and Uniswap into one.
   */
  const ds = mergeConcurrently(
    new Erc20Datasource({
      portal,
      args: {
        fromBlock: 4634748,
        contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
      },
      state: erc20state,
    }),
    new UniswapDatasource({
      portal,
      args: {
        fromBlock: 4634748,
      },
      state: uniswapState,
    }),
  );

  for await (const batch of await ds.stream()) {
    /**
     * The batch will contain two arrays: the first one will contain erc20 transfers,
     * the second one will contain Uniswap swaps.
     */
    const [erc20, swaps] = batch;

    await db.transaction(async (manager) => {
      if (erc20.length) {
        console.log(`processed ${erc20.length} erc20 transfers`);

        /**
         * Save the last block to the erc20 state row
         */
        await erc20state.set(manager, last(erc20).block);
      }
      if (swaps.length) {
        console.log(`processed ${swaps.length} swaps`);

        /**
         * Save the last block to the Uniswap state row
         */
        await uniswapState.set(manager, last(swaps).block);
      }
    });
  }
}

void main();
