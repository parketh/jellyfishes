import { PortalClient } from '@subsquid/portal-client';
import { SolanaSwapsStream } from '../streams/solana_swaps/solana_swaps';

async function main() {
  const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/solana-mainnet',
  });

  const ds = new SolanaSwapsStream({
    portal,
    args: {
      fromBlock: 240_000_000,
    },
  });

  for await (const swaps of await ds.stream()) {
    console.log(swaps[0]);
  }
}

void main();
