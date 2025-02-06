import { HashAndNumber } from '@abernatskiy/portal-client/src/query';
import { AbstractDatasource, Datasource } from '../core/abstract_datasource';
import { events as abi_events } from './abi';

export type UniswapSwap = {
  sender: string;
  recipient: string;
  liquidity: bigint;
  tick: number;
  amount0: bigint;
  amount1: bigint;
  sqrtPriceX96: bigint;
  contract_address: string;
  block: HashAndNumber;
};

export class UniswapDatasource
  extends AbstractDatasource<{ args: { from: number; pairs?: string[] } }>
  implements Datasource {
  async stream(): Promise<ReadableStream<UniswapSwap[]>> {
    const {args, stateManager} = this.options;

    const fromState = stateManager ? await stateManager.getState() : null;

    console.log(`staring from block ${fromState || args.from}`);

    const source = this.options.portal.getFinalizedStream({
      type: 'evm',
      fromBlock: fromState || args.from,
      fields: {
        block: {
          number: true,
          hash: true,
        },
        transaction: {
          from: true,
          to: true,
          hash: true,
        },
        log: {
          address: true,
          topics: true,
          data: true,
          transactionHash: true,
          logIndex: true,
          transactionIndex: true,
        },
        stateDiff: {
          kind: true,
          next: true,
          prev: true,
        },
      },
      logs: [
        {
          topic0: [abi_events.Swap.topic],
        },
      ],
    });

    return source.pipeThrough(
      new TransformStream({
        transform: ({blocks}, controller) => {
          const events = blocks.flatMap((block) => {
            if (!block.logs) return [];

            return block.logs
              .filter((l) => abi_events.Swap.is(l))
              .map((l): UniswapSwap => {
                const data = abi_events.Swap.decode(l);

                return {
                  sender: data.sender,
                  recipient: data.recipient,
                  liquidity: data.liquidity,
                  tick: data.tick,
                  amount0: data.amount0,
                  amount1: data.amount1,
                  sqrtPriceX96: data.sqrtPriceX96,
                  contract_address: l.address,
                  block: block.header,
                };
              });
          });

          if (!events.length) return;

          controller.enqueue(events);
        },
      }),
    );
  }
}
