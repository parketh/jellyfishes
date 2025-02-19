import assert from 'assert';
import { Logger } from '../../core/abstract_stream';
import * as damm from './abi/damm';
import * as dlmm from './abi/dlmm';
import * as tokenProgram from './abi/tokenProgram';
import { SolanaSwapTransfer } from './solana_swaps';
import {
  Block,
  Instruction,
  getInnerTransfersByLevel,
  getInstructionBalances,
  getInstructionD1,
  getTransactionHash,
} from './utils';

export function handleMeteoraDamm(
  logger: Logger,
  ins: Instruction,
  block: Block,
): SolanaSwapTransfer | null {
  const swap = damm.instructions.swap.decode(ins);

  // We skip such zero transfers, this doesn't make sense
  if (swap.data.inAmount === 0n) {
    return null;
  }

  /**
   * Meteora DAMM has two transfers on the second level and also other tokenProgram instructions
   */
  const [src, dest] = block.instructions
    .filter((inner) => {
      if (inner.transactionIndex !== ins.transactionIndex) return false;
      if (inner.instructionAddress.length <= ins.instructionAddress.length) return false;
      if (inner.programId !== tokenProgram.programId) return false;
      if (getInstructionD1(inner) !== tokenProgram.instructions.transfer.d1) {
        return false;
      }

      return ins.instructionAddress.every((v, i) => v === inner.instructionAddress[i]);
    })
    .map((t) => {
      return tokenProgram.instructions.transfer.decode(t);
    });

  if (!src || !dest) {
    logger.warn({
      message: 'Meteora DAMM: src or dest not found',
      tx: getTransactionHash(ins, block),
      src,
      dest,
    });

    return null;
  }

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

  return {
    type: 'meteora_damm',
    account: src.accounts.authority,
    input: {
      amount: inputAmount,
      mint: inputMint,
      // vault: swap.accounts.aVault,
    },
    output: {
      amount: outputAmount,
      mint: outputMint,
      // vault: swap.accounts.bVault,
    },
  };
}

export function handleMeteoraDlmm(ins: Instruction, block: Block): SolanaSwapTransfer {
  // const swap = dlmm.instructions.swap.decode(ins);

  const [src, dest] = getInnerTransfersByLevel(ins, block.instructions, 1).map((t) => {
    return tokenProgram.instructions.transferChecked.decode(t);
  });

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

  return {
    type: 'meteora_dlmm',
    account: src.accounts.owner,
    input: {
      amount: inputAmount,
      mint: inputMint,
    },
    output: {
      amount: outputAmount,
      mint: outputMint,
    },
  };
}
