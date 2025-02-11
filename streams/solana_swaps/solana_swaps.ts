import assert from 'assert';
import { getInstructionDescriptor } from '@subsquid/solana-stream';
import { AbstractStream, Stream } from '../../core/abstract_stream';
import * as tokenProgram from './abi/tokenProgram/index';
import * as whirlpool from './abi/whirlpool/index';

export type SolanaSwap = {
  inputAmount: bigint;
  inputMint: string;
  inputVault: string;
  outputAmount: bigint;
  outputMint: string;
  outputVault: string;
  transactionIndex: number;
  instructionAddress: number[];
};

function extractInnerInstructions(instruction: any, instructions: any[]) {
  return instructions.filter(
    (i) =>
      i.transactionIndex === instruction.transactionIndex &&
      i.instructionAddress.length === instruction.instructionAddress.length + 1 &&
      instruction.instructionAddress.every((a, j) => a === i.instructionAddress[j]),
  );
}

export class SolanaSwapsStream
  extends AbstractStream<{
    args: {
      fromBlock: number;
      toBlock?: number;
    };
  }>
  implements Stream {
  async stream(): Promise<ReadableStream<SolanaSwap[]>> {
    const {args, state} = this.options;

    const fromState = state ? await state.get() : null;
    const fromBlock = fromState ? fromState.number : args.fromBlock;

    this.logger.debug(`starting from block ${fromBlock} ${fromState ? 'from state' : 'from args'}`);

    const source = this.options.portal.getFinalizedStream({
      type: 'solana',
      fromBlock,
      toBlock: args.toBlock,
      fields: {
        block: {
          timestamp: true,
        },
        transaction: {
          signatures: true,
        },
        instruction: {
          transactionIndex: true,
          data: true,
          instructionAddress: true,
          programId: true,
          accounts: true,
        },
        tokenBalance: {
          transactionIndex: true,
          account: true,
          preMint: true,
          postMint: true,
        },
      },
      instructions: [
        {
          programId: [whirlpool.programId], // where executed by Whirlpool program
          d8: [whirlpool.instructions.swap.d8], // have first 8 bytes of .data equal to swap descriptor
          isCommitted: true, // where successfully committed
          innerInstructions: true, // inner instructions
          transaction: true, // transaction, that executed the given instruction
          transactionTokenBalances: true, // all token balance records of executed transaction
        },
      ],
    });

    return source.pipeThrough(
      new TransformStream({
        transform: ({blocks}, controller) => {
          // FIXME
          const res = blocks.flatMap((block: any) => {
            if (!block.instructions) return [];

            const swaps: SolanaSwap[] = [];

            for (const ins of block.instructions) {
              if (ins.programId !== whirlpool.programId) {
                continue;
              } else if (getInstructionDescriptor(ins) !== whirlpool.instructions.swap.d8) {
                continue;
              }

              const tokenBalances =
                block.tokenBalances?.filter((t) => t.transactionIndex === ins.transactionIndex) ||
                [];

              const swap = whirlpool.instructions.swap.decode(ins);

              const [src, dest] = extractInnerInstructions(ins, block.instructions);
              const srcTransfer = tokenProgram.instructions.transfer.decode(src);
              const destTransfer = tokenProgram.instructions.transfer.decode(dest);

              const inputMint = tokenBalances.find(
                (balance) => balance.account === srcTransfer.accounts.destination,
              )?.preMint;
              assert(inputMint != null, 'inputMint != null');
              const inputAmount = srcTransfer.data.amount;

              const outputMint = tokenBalances.find(
                (balance) => balance.account === destTransfer.accounts.source,
              )?.preMint;
              assert(outputMint != null, 'outputMint != null');
              const outputAmount = destTransfer.data.amount;

              const [inputVault, outputVault] = swap.data.aToB
                ? [swap.accounts.tokenVaultA, swap.accounts.tokenVaultB]
                : [swap.accounts.tokenVaultB, swap.accounts.tokenVaultA];

              swaps.push({
                inputAmount,
                inputMint,
                inputVault,
                outputAmount: outputAmount,
                outputMint,
                outputVault,
                transactionIndex: ins.transactionIndex,
                instructionAddress: ins.instructionAddress,
              });
            }

            return swaps;
          });

          if (!res.length) return;

          controller.enqueue(res);
        },
      }),
    );
  }
}
