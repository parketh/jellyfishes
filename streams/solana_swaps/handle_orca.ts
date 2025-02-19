import assert from 'assert';
import * as tokenProgram from './abi/tokenProgram';
// import * as whirlpool from './abi/whirlpool';
import { SolanaSwapTransfer } from './solana_swaps';
import { Block, Instruction, getInnerTransfersByLevel, getInstructionBalances } from './utils';

export function handleWhirlpool(ins: Instruction, block: Block): SolanaSwapTransfer {
  // const swap = whirlpool.instructions.swap.decode(ins);
  const [src, dest] = getInnerTransfersByLevel(ins, block.instructions, 1).map((t) =>
    tokenProgram.instructions.transfer.decode(t),
  );

  const tokenBalances = getInstructionBalances(ins, block);
  const inputMint = tokenBalances.find(
    (balance) => balance.account === src.accounts.destination,
  )?.preMint;
  assert(inputMint != null, 'inputMint is null');
  const inputAmount = src.data.amount;

  const outputMint = tokenBalances.find(
    (balance) => balance.account === dest.accounts.source,
  )?.preMint;
  assert(outputMint != null, 'outputMint is null');
  const outputAmount = dest.data.amount;

  // const [inputVault, outputVault] = swap.data.aToB
  //   ? [swap.accounts.tokenVaultA, swap.accounts.tokenVaultB]
  //   : [swap.accounts.tokenVaultB, swap.accounts.tokenVaultA];

  return {
    type: 'orca_whirlpool',
    account: src.accounts.authority,
    input: {
      amount: inputAmount,
      mint: inputMint,
      // vault: inputVault,
    },
    output: {
      amount: outputAmount,
      mint: outputMint,
      // vault: outputVault,
    },
  };
}
