import { PortalClient } from '@abernatskiy/portal-client';
import { DataSource as TypeormDatabase } from 'typeorm';
import { TypeormState } from '../core/states/typeorm_state';
import { Erc20Datasource } from '../erc20/erc20';
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

  const state = new TypeormState(db, {
    table: 'status_erc20',
  });

  const ds = new Erc20Datasource({
    portal,
    args: {
      from: 4634748,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },
    state,
  });

  for await (const erc20 of await ds.stream()) {
    await db.transaction(async (manager) => {
      console.info(`processed ${erc20.length} erc20 transfers`);

      await state.set(manager, last(erc20).block);
    });
  }
}

void main();
