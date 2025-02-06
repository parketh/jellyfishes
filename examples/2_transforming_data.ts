import { PortalClient } from '@abernatskiy/portal-client';
import { Erc20Datasource, Erc20Event } from '../erc20/erc20';

const addToken = new TransformStream<Erc20Event[], (Erc20Event & { token: string })[]>({
  transform: (events, controller) => {
    controller.enqueue(
      events.map((e) => ({
        ...e,
        token: 'USDT',
      })),
    );
  },
});

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
  });

  const ds = new Erc20Datasource({
    portal,
    args: {
      from: 4634748,
      contracts: ['0xdac17f958d2ee523a2206206994597c13d831ec7'],
    },
  });

  for await (const erc20 of (await ds.stream()).pipeThrough(addToken)) {
    for (const event of erc20) {
      console.log(event);
    }
    console.log(`processed ${erc20.length} erc20 transfers`);
  }
}

void main();
