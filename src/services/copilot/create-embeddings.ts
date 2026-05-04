import consola from "consola"

import { copilotHeaders, copilotBaseUrl } from "~/lib/api-config"
import { HTTPError } from "~/lib/error"
import { state } from "~/lib/state"

export const createEmbeddings = async (payload: EmbeddingRequest) => {
  if (!state.copilotToken && !state.tokenPool)
    throw new Error("Copilot token not found")

  const poolEntry = (await state.tokenPool?.acquire()) ?? null
  try {
    const response = await fetch(
      `${copilotBaseUrl(state, poolEntry ?? undefined)}/embeddings`,
      {
        method: "POST",
        headers: await copilotHeaders(state, undefined, false, poolEntry ?? undefined),
        body: JSON.stringify(payload),
      },
    )

    if (!response.ok) {
      if (poolEntry && response.status === 429 && state.tokenPool) {
        consola.error(`[pool:${poolEntry.label}] 429 rate limited`)
        state.tokenPool.markRateLimited(poolEntry)
      }
      throw new HTTPError("Failed to create embeddings", response)
    }

    return (await response.json()) as EmbeddingResponse
  } finally {
    if (poolEntry) {
      state.tokenPool?.release(poolEntry)
    }
  }
}

export interface EmbeddingRequest {
  input: string | Array<string>
  model: string
}

export interface Embedding {
  object: string
  embedding: Array<number>
  index: number
}

export interface EmbeddingResponse {
  object: string
  data: Array<Embedding>
  model: string
  usage: {
    prompt_tokens: number
    total_tokens: number
  }
}
