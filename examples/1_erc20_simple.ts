import { PortalClient } from '@subsquid/portal-client';
import { Erc20Stream } from '../streams/erc20/erc20_stream';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  const ds = new Erc20Stream({
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
