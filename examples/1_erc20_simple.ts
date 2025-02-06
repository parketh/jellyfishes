import { PortalClient } from '@abernatskiy/portal-client';
import { Erc20Datasource } from '../erc20/erc20';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  const ds = new Erc20Datasource({
    portal,
    args: {
      fromBlock: 4634748,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },
  });

  for await (const erc20 of await ds.stream()) {
    console.log(`processed ${erc20.length} erc20 transfers`);
  }
}

void main();
