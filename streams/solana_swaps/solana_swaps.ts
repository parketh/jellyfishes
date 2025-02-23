import { getInstructionDescriptor } from "@subsquid/solana-stream"
import { AbstractStream, BlockRef } from "../../core/abstract_stream"
import * as clmm from "./abi/clmm/index"
import * as damm from "./abi/damm/index"
import * as dlmm from "./abi/dlmm/index"
import * as cpmm from "./abi/cpmm/index"
import * as whirlpool from "./abi/whirlpool/index"
import { handleMeteoraDamm, handleMeteoraDlmm } from "./handle_meteora"
import { handleWhirlpool } from "./handle_orca"
import { handleRaydiumClmm, handleRaydiumCpmm } from "./handle_raydium"
import { getTransactionHash } from "./utils"

export type SwapType =
  | "orca_whirlpool"
  | "meteora_damm"
  | "meteora_dlmm"
  | "raydium_clmm"
  | "raydium_cpmm"

export type SolanaSwap = {
  id: string
  type: SwapType
  account: string
  transaction: { hash: string; index: number }
  input: {
    amount: bigint
    mint: string
    //vault: string };
  }
  output: {
    amount: bigint
    mint: string
    // vault: string
  }
  instruction: { address: number[] }
  block: BlockRef
  offset: string
  timestamp: Date
}

export type SolanaSwapTransfer = Pick<SolanaSwap, "input" | "output" | "account" | "type">

function isProgramInstruction(ins: any, programId: string, d8: string) {
  return ins.programId === programId && getInstructionDescriptor(ins) === d8
}

export class SolanaSwapsStream extends AbstractStream<
  {
    fromBlock: number
    toBlock?: number
    tokens?: string[]
    type?: SwapType[]
  },
  SolanaSwap,
  { number: number; hash: string }
> {
  async stream(): Promise<ReadableStream<SolanaSwap[]>> {
    const { args } = this.options

    const offset = await this.getState({ number: args.fromBlock, hash: "" })

    const types = args.type || [
      "orca_whirlpool",
      "meteora_damm",
      "meteora_dlmm",
      "raydium_clmm",
      "raydium_cpmm",
    ]

    const source = this.portal.getFinalizedStream({
      type: "solana",
      fromBlock: offset.number,
      toBlock: args.toBlock,
      fields: {
        block: {
          number: true,
          hash: true,
          timestamp: true,
        },
        transaction: {
          transactionIndex: true,
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
      instructions: types.map((type) => {
        switch (type) {
          case "orca_whirlpool":
            return {
              programId: [whirlpool.programId], // where executed by Whirlpool program
              d8: [whirlpool.instructions.swap.d8],
              isCommitted: true,
              innerInstructions: true,
              transaction: true,
              transactionTokenBalances: true,
            }
          case "meteora_damm":
            return {
              programId: [damm.programId],
              d8: [damm.instructions.swap.d8],
              isCommitted: true,
              innerInstructions: true,
              transaction: true,
              transactionTokenBalances: true,
            }
          case "meteora_dlmm":
            return {
              programId: [dlmm.programId],
              d8: [dlmm.instructions.swap.d8],
              isCommitted: true,
              innerInstructions: true,
              transaction: true,
              transactionTokenBalances: true,
            }
          case "raydium_clmm":
            return {
              programId: [clmm.programId],
              d8: [clmm.instructions.swap.d8],
              isCommitted: true,
              innerInstructions: true,
              transaction: true,
              transactionTokenBalances: true,
            }
          case "raydium_cpmm":
            return {
              programId: [cpmm.programId],
              d8: [cpmm.instructions.swapBaseInput.d8, cpmm.instructions.swapBaseOutput.d8],
              isCommitted: true,
              innerInstructions: true,
              transaction: true,
              transactionTokenBalances: true,
            }
        }
      }),
    })

    return source.pipeThrough(
      new TransformStream({
        transform: ({ blocks }, controller) => {
          // FIXME
          const res = blocks.flatMap((block: any) => {
            if (!block.instructions) return []

            const swaps: SolanaSwap[] = []

            const offset = this.encodeOffset({
              number: block.header.number,
              hash: block.header.hash,
            })

            for (const ins of block.instructions) {
              let swap: SolanaSwapTransfer | null = null
              if (isProgramInstruction(ins, whirlpool.programId, whirlpool.instructions.swap.d8)) {
                swap = handleWhirlpool(ins, block)
              } else if (isProgramInstruction(ins, damm.programId, damm.instructions.swap.d8)) {
                swap = handleMeteoraDamm(this.logger, ins, block)
              } else if (isProgramInstruction(ins, dlmm.programId, dlmm.instructions.swap.d8)) {
                swap = handleMeteoraDlmm(ins, block)
              } else if (isProgramInstruction(ins, clmm.programId, clmm.instructions.swap.d8)) {
                swap = handleRaydiumClmm(ins, block)
              } else if (
                isProgramInstruction(ins, cpmm.programId, cpmm.instructions.swapBaseInput.d8) ||
                isProgramInstruction(ins, cpmm.programId, cpmm.instructions.swapBaseOutput.d8)
              ) {
                swap = handleRaydiumCpmm(ins, block)
              }

              if (!swap) continue
              else if (
                args.tokens &&
                !args.tokens.includes(swap.input.mint) &&
                !args.tokens.includes(swap.output.mint)
              ) {
                continue
              }

              const txHash = getTransactionHash(ins, block)

              swaps.push({
                id: `${txHash}/${ins.transactionIndex}`,
                type: swap.type,
                block: { number: block.header.number, hash: block.header.hash },
                instruction: {
                  address: ins.instructionAddress,
                },
                input: swap.input,
                output: swap.output,
                account: swap.account,
                transaction: {
                  hash: txHash,
                  index: ins.transactionIndex,
                },
                timestamp: new Date(block.header.timestamp * 1000),
                offset,
              })
            }

            return swaps
          })

          if (!res.length) return

          controller.enqueue(res)
        },
      })
    )
  }
}
