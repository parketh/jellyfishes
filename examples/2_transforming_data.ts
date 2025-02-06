import { PortalClient } from '@abernatskiy/portal-client';
import { Erc20Datasource, Erc20Event } from '../erc20/erc20';
import { last } from './utils';

const Contracts = {
  USDT: '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
  SQD: '0x1337420ded5adb9980cfc35f8f2b054ea86f8ab1',
};

const mapToken = (address: string) => {
  switch (address) {
    case Contracts.USDT:
      return 'USDT';
    case Contracts.SQD:
      return 'SQD';
    default:
      return 'UNKNOWN';
  }
};

const addToken = new TransformStream<Erc20Event[], (Erc20Event & { token: string })[]>({
  transform: (events, controller) => {
    controller.enqueue(
      events.map((e) => ({
        ...e,
        token: mapToken(e.token_address),
      })),
    );
  },
});

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/arbitrum-one',
  });

  const ds = new Erc20Datasource({
    portal,
    args: {
      fromBlock: 200_120_655,
      contracts: [Contracts.USDT, Contracts.SQD],
    },
  });

  for await (const erc20 of (await ds.stream()).pipeThrough(addToken)) {
    for (const event of erc20) {
      console.log(event.token, event.from, event.to, event.amount);
    }
    console.log(`processed ${erc20.length} erc20 transfers, ${last(erc20).block.number} block`);
  }
}

void main();

/**
 USDT 0x75010b38696e3045072ea8bebabee8e4b8a3706c 0x641c00a822e8b671738d32a431a4fb6074e5c79d 1707884375n
 USDT 0xce9e64e9cfcd6f5d17d27e56f423d31f83ec3597 0x641c00a822e8b671738d32a431a4fb6074e5c79d 8600266624n
 USDT 0x641c00a822e8b671738d32a431a4fb6074e5c79d 0x00000000ede6d8d217c60f93191c060747324bca 167186548n
 USDT 0x00000000ede6d8d217c60f93191c060747324bca 0x145304a5cfec1b616cf035c43f084ce1233d9ea7 26247n
 processed 42989 erc20 transfers, 201724176 block
 */
