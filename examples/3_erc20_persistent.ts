import { PortalClient } from '@abernatskiy/portal-client';

import { DataSource } from 'typeorm';
import { TypeormStateManager } from '../core/state_manager';
import { Erc20Datasource } from '../erc20/erc20';
import { last } from './utils';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  const db = await new DataSource({
    type: 'postgres',
    username: 'postgres',
    password: 'postgres',
    port: 6432,
  }).initialize();

  const stateManager = new TypeormStateManager(db, {
    table: 'status_erc20',
  });

  const ds = new Erc20Datasource({
    portal,
    args: {
      from: 4634748,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },
    stateManager,
  });

  for await (const erc20 of await ds.stream()) {
    await db.transaction(async (manager) => {
      console.log(`processed ${erc20.length} erc20 transfers`);

      await stateManager.setState(manager, last(erc20).block);
    });
  }
}

void main();
