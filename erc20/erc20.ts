import { HashAndNumber } from '@abernatskiy/portal-client/src/query';
import { AbstractDatasource, Datasource } from '../core/abstract_datasource';
import { events as abi_events } from './abi';

export type Erc20Event = {
  from: string;
  to: string;
  amount: bigint;
  token_address: string;
  block: HashAndNumber;
};

export class Erc20Datasource
  extends AbstractDatasource<{ args: { from: number; contracts: string[] } }>
  implements Datasource {
  async stream(): Promise<ReadableStream<Erc20Event[]>> {
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
          address: this.options.args.contracts,
          topic0: ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'],
        },
      ],
    });

    return source.pipeThrough(
      new TransformStream({
        transform: ({blocks}, controller) => {
          const events = blocks.flatMap((block) => {
            if (!block.logs) return [];

            return block.logs
              .filter((l) => abi_events.Transfer.is(l))
              .map((l): Erc20Event => {
                const data = abi_events.Transfer.decode(l);

                return {
                  from: data.from,
                  to: data.to,
                  amount: data.value,
                  token_address: l.address,
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
