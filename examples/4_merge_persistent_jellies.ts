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

  const db = await new TypeormDatabase({
    type: 'postgres',
    username: 'postgres',
    password: 'postgres',
    port: 6432,
  }).initialize();

  const erc20state = new TypeormState(db, {table: 'status', id: 'erc20'});
  const uniswapState = new TypeormState(db, {table: 'status', id: 'uniswap'});

  const ds = mergeConcurrently(
    new Erc20Datasource({
      portal,
      args: {
        from: 4634748,
        contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
      },
      state: erc20state,
    }),
    new UniswapDatasource({
      portal,
      args: {
        from: 4634748,
      },
      state: uniswapState,
    }),
  );

  for await (const batch of await ds.stream()) {
    const [erc20, swaps] = batch;

    await db.transaction(async (manager) => {
      if (erc20.length) {
        console.log(`processed ${erc20.length} erc20 transfers`);

        await erc20state.set(manager, last(erc20).block);
      }
      if (swaps.length) {
        console.log(`processed ${swaps.length} swaps`);

        await uniswapState.set(manager, last(swaps).block);
      }
    });
  }
}

void main();
