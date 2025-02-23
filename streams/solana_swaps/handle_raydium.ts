import assert from "assert"
import * as tokenProgram from "./abi/tokenProgram"
// import * as clmm from './abi/clmm/index';
import { SolanaSwapTransfer } from "./solana_swaps"
import { Block, Instruction, getInnerTransfersByLevel, getInstructionBalances } from "./utils"

export function handleRaydiumClmm(ins: Instruction, block: Block): SolanaSwapTransfer {
  // const swap = whirlpool.instructions.swap.decode(ins);
  const [src, dest] = getInnerTransfersByLevel(ins, block.instructions, 1).map((t) =>
    tokenProgram.instructions.transfer.decode(t)
  )

  const tokenBalances = getInstructionBalances(ins, block)
  const inputMint = tokenBalances.find(
    (balance) => balance.account === src.accounts.destination
  )?.preMint
  assert(inputMint != null, "inputMint is null")
  const inputAmount = src.data.amount

  const outputMint = tokenBalances.find(
    (balance) => balance.account === dest.accounts.source
  )?.preMint
  assert(outputMint != null, "outputMint is null")
  const outputAmount = dest.data.amount

  // const [inputVault, outputVault] = swap.data.aToB
  //   ? [swap.accounts.tokenVaultA, swap.accounts.tokenVaultB]
  //   : [swap.accounts.tokenVaultB, swap.accounts.tokenVaultA];

  return {
    type: "raydium_clmm",
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
  }
}

export function handleRaydiumCpmm(ins: Instruction, block: Block): SolanaSwapTransfer {
  const transferInstructions = getInnerTransfersByLevel(ins, block.instructions, 1)
  const [src, dest] = transferInstructions.map((t) =>
    tokenProgram.instructions.transferChecked.decode(t)
  )
  const tokenBalances = getInstructionBalances(ins, block)
  // TODO: the 'dest' instruction is not fetched because it uses token2022 program - to handle
  console.log("start")
  console.log({ src, dest })
  console.log("transferInstructions", transferInstructions)
  console.log("end")
  const inputMint = tokenBalances.find(
    (balance) => balance.account === src.accounts.destination
  )?.preMint
  assert(inputMint != null, "inputMint is null")
  const inputAmount = src.data.amount

  const outputMint = tokenBalances.find(
    (balance) => balance.account === dest.accounts.source
  )?.preMint
  assert(outputMint != null, "outputMint is null")
  const outputAmount = dest.data.amount

  // const [inputVault, outputVault] = swap.data.aToB
  //   ? [swap.accounts.tokenVaultA, swap.accounts.tokenVaultB]
  //   : [swap.accounts.tokenVaultB, swap.accounts.tokenVaultA];

  return {
    type: "raydium_cpmm",
    account: src.accounts.owner,
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
  }
}
