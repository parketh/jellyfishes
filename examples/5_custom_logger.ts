import { PortalClient } from '@abernatskiy/portal-client';
import { Erc20Datasource } from '../erc20/erc20';
import { createLogger } from './utils';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  const logger = createLogger('erc20');

  const ds = new Erc20Datasource({
    portal,
    args: {
      fromBlock: 4634748,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },
    logger,
  });

  for await (const erc20 of await ds.stream()) {
    logger.info(`processed ${erc20.length} erc20 transfers`);
  }
}

void main();
