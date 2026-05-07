import consola from "consola"
import fs from "node:fs/promises"
import path from "node:path"
import { setTimeout as delay } from "node:timers/promises"

import { state } from "./state"

const REFRESH_POLL_INTERVAL_MS = 15_000
const EARLY_REFRESH_BUFFER_MS = 60_000
const RETRY_REFRESH_DELAY_MS = 15_000
const MIN_REFRESH_DELAY_MS = 1_000
const COOLDOWN_MS = 120_000
const RATE_LIMIT_COOLDOWN_MS = 10 * 60 * 1000
const MAX_CONCURRENT_PER_ENTRY = 8
const ACQUIRE_POLL_MS = 500
const ACQUIRE_TIMEOUT_MS = 600_000
const SESSION_TTL_MS = 10 * 60 * 1000

export interface TokenPoolEntry {
  label: string
  githubToken: string
  copilotToken: string | null
  copilotApiUrl: string | null
  healthy: boolean
  unavailableUntil: number
  refreshController: AbortController | null
  activeRequests: number
}

export class TokenPool {
  private entries: Array<TokenPoolEntry> = []
  private sessionMap: Map<string, { label: string; lastAccess: number }> =
    new Map()

  get size(): number {
    return this.entries.length
  }

  get healthyCount(): number {
    return this.entries.filter((e) => e.healthy).length
  }

  async init(tokenPaths: Array<string>): Promise<void> {
    for (const tokenPath of tokenPaths) {
      const resolvedPath = path.resolve(tokenPath)
      let githubToken: string
      try {
        githubToken = (await fs.readFile(resolvedPath, "utf8")).trim()
      } catch (err: unknown) {
        consola.warn(`[pool] Cannot read ${resolvedPath}: ${String(err)}`)
        continue
      }
      if (!githubToken) {
        consola.warn(`[pool] Empty token file: ${resolvedPath}`)
        continue
      }

      const label = path.basename(path.dirname(resolvedPath))
      this.entries.push({
        label,
        githubToken,
        copilotToken: null,
        copilotApiUrl: null,
        healthy: true,
        unavailableUntil: 0,
        refreshController: null,
        activeRequests: 0,
      })
    }

    if (this.entries.length === 0) {
      throw new Error("[pool] No valid token files found")
    }

    consola.info(`[pool] Initializing ${this.entries.length} credentials...`)

    // Initialize all entries in parallel
    await Promise.allSettled(this.entries.map((entry) => this.initEntry(entry)))

    const healthy = this.healthyCount
    if (healthy === 0) {
      throw new Error("[pool] All credentials failed to initialize")
    }
    consola.success(
      `[pool] ${healthy}/${this.entries.length} credentials ready`,
    )
  }

  private normalizeAffinityKeys(
    affinityKeys: string | Iterable<string | undefined> | undefined,
  ): Array<string> {
    const keys: Array<string> = []
    if (typeof affinityKeys === "string") {
      if (affinityKeys.length > 0) keys.push(affinityKeys)
    } else if (affinityKeys) {
      for (const k of affinityKeys) {
        if (typeof k === "string" && k.length > 0) keys.push(k)
      }
    }
    return keys
  }

  private resolveBoundLabel(candidateKeys: Array<string>): string | null {
    const now = Date.now()
    for (const key of candidateKeys) {
      const mapping = this.sessionMap.get(key)
      if (mapping) {
        mapping.lastAccess = now
        return mapping.label
      }
    }
    return null
  }

  private recoverExpiredEntries(now: number): void {
    for (const entry of this.entries) {
      if (!entry.healthy && now >= entry.unavailableUntil) {
        entry.healthy = true
        this.initEntry(entry).catch(() => {})
      }
    }
  }

  private tryAcquireBound(label: string): TokenPoolEntry | null | undefined {
    // null = entry vanished, undefined = entry exists but not yet usable
    const entry = this.entries.find((e) => e.label === label)
    if (!entry) return null
    if (
      entry.healthy
      && entry.copilotToken
      && entry.activeRequests < MAX_CONCURRENT_PER_ENTRY
    ) {
      entry.activeRequests++
      consola.info(
        `[pool] Acquired ${entry.label} via session affinity (active: ${entry.activeRequests})`,
      )
      return entry
    }
    return undefined
  }

  private pickLeastBusy(): TokenPoolEntry | null {
    let best: TokenPoolEntry | null = null
    for (const entry of this.entries) {
      if (
        entry.healthy
        && entry.copilotToken
        && entry.activeRequests < MAX_CONCURRENT_PER_ENTRY
        && (!best || entry.activeRequests < best.activeRequests)
      ) {
        best = entry
      }
    }
    return best
  }

  async acquire(
    affinityKeys?: string | Iterable<string | undefined>,
  ): Promise<TokenPoolEntry> {
    const deadline = Date.now() + ACQUIRE_TIMEOUT_MS
    const candidateKeys = this.normalizeAffinityKeys(affinityKeys)
    let boundLabel: string | null = null
    let logged = false

    while (true) {
      this.recoverExpiredEntries(Date.now())

      // Resolve affinity binding once per acquire — keep waiting for that
      // specific entry rather than falling back to a different account, since
      // Copilot rejects cross-account input items with
      // "input item does not belong to this connection".
      if (boundLabel === null && candidateKeys.length > 0) {
        boundLabel = this.resolveBoundLabel(candidateKeys)
        if (!logged && boundLabel === null) {
          // Only warn for keys that imply a prior response chain
          // (resp_/rs_/msg_/fc_). Plain client session UUIDs are bound on
          // first use; missing them is normal for a chain's first request.
          const riskyKey = candidateKeys.find((k) =>
            /^(?:resp|rs|msg|fc)_/.test(k),
          )
          if (riskyKey) {
            consola.warn(
              `[pool] No affinity match for response-chain key ${riskyKey.slice(0, 32)}… (sessionMap=${this.sessionMap.size})`,
            )
          }
          logged = true
        }
      }

      if (boundLabel !== null) {
        const acquired = this.tryAcquireBound(boundLabel)
        if (acquired) return acquired
        if (acquired === null) {
          boundLabel = null // entry vanished — fall through to load balancer
        } else {
          if (Date.now() >= deadline) {
            throw new Error(
              `[pool] Session-bound entry ${boundLabel} unavailable`,
            )
          }
          await delay(ACQUIRE_POLL_MS)
          continue
        }
      }

      const best = this.pickLeastBusy()
      if (best) {
        best.activeRequests++
        const riskyKey = candidateKeys.find((k) =>
          /^(?:resp|rs|msg|fc)_/.test(k),
        )
        if (riskyKey) {
          consola.warn(
            `[pool] Load-balanced ${best.label} despite response-chain key ${riskyKey.slice(0, 32)}… — likely "does not belong" risk`,
          )
        } else {
          consola.info(
            `[pool] Acquired ${best.label} (active: ${best.activeRequests})`,
          )
        }
        return best
      }

      if (Date.now() >= deadline) {
        throw new Error(
          `[pool] All credentials are at max concurrency (${MAX_CONCURRENT_PER_ENTRY}) or unavailable`,
        )
      }
      await delay(ACQUIRE_POLL_MS)
    }
  }

  release(entry: TokenPoolEntry): void {
    entry.activeRequests = Math.max(0, entry.activeRequests - 1)
    consola.debug(
      `[pool] Released ${entry.label} (active: ${entry.activeRequests})`,
    )
  }

  markRateLimited(entry: TokenPoolEntry): void {
    entry.healthy = false
    entry.unavailableUntil = Date.now() + RATE_LIMIT_COOLDOWN_MS
    consola.warn(
      `[pool:${entry.label}] Rate limited — disabled for ${RATE_LIMIT_COOLDOWN_MS / 60000} min`,
    )
  }

  bindSession(responseId: string, entry: TokenPoolEntry): void {
    this.sessionMap.set(responseId, {
      label: entry.label,
      lastAccess: Date.now(),
    })
    this.cleanStaleSessions()
  }

  private cleanStaleSessions(): void {
    const cutoff = Date.now() - SESSION_TTL_MS
    for (const [key, val] of this.sessionMap) {
      if (val.lastAccess < cutoff) this.sessionMap.delete(key)
    }
  }

  /** @deprecated Use acquire()/release() for proper concurrency tracking */
  async next(): Promise<TokenPoolEntry> {
    return this.acquire()
  }

  async getCopilotToken(): Promise<string> {
    const entry = await this.acquire()
    if (!entry.copilotToken) {
      throw new Error("[pool] Selected entry has no copilot token")
    }
    return entry.copilotToken
  }

  getCopilotApiUrl(entry?: TokenPoolEntry): string | null {
    if (entry) {
      return entry.copilotApiUrl
    }
    // Fallback: return the first healthy entry's URL
    const healthy = this.entries.find((e) => e.healthy && e.copilotToken)
    return healthy?.copilotApiUrl ?? null
  }

  stop(): void {
    for (const entry of this.entries) {
      entry.refreshController?.abort()
      entry.refreshController = null
    }
  }

  private async initEntry(entry: TokenPoolEntry): Promise<void> {
    try {
      // Get user info
      const userResp = await fetch("https://api.github.com/user", {
        headers: {
          accept: "application/vnd.github+json",
          authorization: `token ${entry.githubToken}`,
          "user-agent": "GitHubCopilotChat/0.46.0",
          "x-github-api-version": "2022-11-28",
        },
      })
      if (!userResp.ok) {
        throw new Error(`GitHub user API: HTTP ${userResp.status}`)
      }
      const user = (await userResp.json()) as { login: string }
      consola.info(`[pool:${entry.label}] Logged in as ${user.login}`)

      // Get copilot usage/endpoint
      const usageResp = await fetch(
        "https://api.github.com/copilot_internal/v2/token",
        {
          headers: {
            authorization: `token ${entry.githubToken}`,
            "user-agent": "GitHubCopilotChat/0.46.0",
            "x-github-api-version": "2022-11-28",
          },
        },
      )
      if (!usageResp.ok) {
        throw new Error(`Copilot token API: HTTP ${usageResp.status}`)
      }
      const tokenData = (await usageResp.json()) as {
        token: string
        refresh_in: number
        expires_at: number
      }

      // eslint-disable-next-line require-atomic-updates
      entry.copilotToken = tokenData.token
      // eslint-disable-next-line require-atomic-updates
      entry.copilotApiUrl = state.copilotApiUrl || null

      // Start refresh loop
      entry.refreshController?.abort()
      const controller = new AbortController()
      entry.refreshController = controller
      this.runRefreshLoop(entry, tokenData.refresh_in, controller.signal).catch(
        () => {},
      )

      consola.success(`[pool:${entry.label}] Ready`)
    } catch (err: unknown) {
      consola.error(`[pool:${entry.label}] Init failed: ${String(err)}`)
      entry.healthy = false
      entry.unavailableUntil = Date.now() + COOLDOWN_MS
    }
  }

  private async runRefreshLoop(
    entry: TokenPoolEntry,
    refreshIn: number,
    signal: AbortSignal,
  ): Promise<void> {
    let refreshAtMs =
      Date.now()
      + Math.max(
        refreshIn * 1000 - EARLY_REFRESH_BUFFER_MS,
        MIN_REFRESH_DELAY_MS,
      )

    while (!signal.aborted) {
      const nextDelayMs = Math.min(
        Math.max(refreshAtMs - Date.now(), 0),
        REFRESH_POLL_INTERVAL_MS,
      )
      if (nextDelayMs > 0) {
        await delay(nextDelayMs, undefined, { signal })
        continue
      }

      try {
        const resp = await fetch(
          "https://api.github.com/copilot_internal/v2/token",
          {
            headers: {
              authorization: `token ${entry.githubToken}`,
              "user-agent": "GitHubCopilotChat/0.46.0",
              "x-github-api-version": "2025-10-01",
            },
          },
        )
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`)
        const data = (await resp.json()) as {
          token: string
          refresh_in: number
        }
        // eslint-disable-next-line require-atomic-updates
        entry.copilotToken = data.token
        // eslint-disable-next-line require-atomic-updates
        entry.healthy = true
        refreshAtMs =
          Date.now()
          + Math.max(
            data.refresh_in * 1000 - EARLY_REFRESH_BUFFER_MS,
            MIN_REFRESH_DELAY_MS,
          )
        consola.debug(`[pool:${entry.label}] Token refreshed`)
      } catch (err: unknown) {
        consola.error(`[pool:${entry.label}] Refresh failed: ${String(err)}`)
        entry.healthy = false
        entry.unavailableUntil = Date.now() + COOLDOWN_MS
        refreshAtMs = Date.now() + RETRY_REFRESH_DELAY_MS
      }
    }
  }
}

export async function* withPoolRelease<T>(
  stream: AsyncIterable<T>,
  pool: TokenPool,
  entry: TokenPoolEntry,
): AsyncGenerator<T> {
  try {
    yield* stream
  } finally {
    pool.release(entry)
  }
}

function tryParseId(data: string, path: "response" | "item"): string | null {
  try {
    const parsed = JSON.parse(data) as Record<
      string,
      { id?: string } | undefined
    >
    const id = parsed[path]?.id
    return typeof id === "string" && id.length > 0 ? id : null
  } catch {
    return null
  }
}

export async function* withSessionBind<
  T extends { event?: string; data?: string },
>(
  stream: AsyncIterable<T>,
  pool: TokenPool,
  opts: { entry: TokenPoolEntry; sessionId?: string },
): AsyncGenerator<T> {
  if (opts.sessionId) {
    pool.bindSession(opts.sessionId, opts.entry)
  }
  let bound = false
  for await (const chunk of stream) {
    if (chunk.data && chunk.event) {
      if (!bound && chunk.event === "response.created") {
        const id = tryParseId(chunk.data, "response")
        if (id) {
          pool.bindSession(id, opts.entry)
          bound = true
        }
      } else if (
        chunk.event === "response.output_item.added"
        || chunk.event === "response.output_item.done"
      ) {
        const id = tryParseId(chunk.data, "item")
        if (id) pool.bindSession(id, opts.entry)
      }
    }
    yield chunk
  }
}

export async function loadPoolConfig(
  configPath: string,
): Promise<Array<string>> {
  const content = await fs.readFile(configPath, "utf8")

  // Support YAML-like format: credentials:\n  - path1\n  - path2
  const lines = content.split("\n")
  const paths: Array<string> = []
  for (const line of lines) {
    const match = line.match(/^\s*-\s(.+)$/)
    if (match) {
      const tokenPath = match[1].trim()
      // Resolve relative paths against config file directory
      if (path.isAbsolute(tokenPath)) {
        paths.push(tokenPath)
      } else {
        paths.push(path.resolve(path.dirname(configPath), tokenPath))
      }
    }
  }
  return paths
}
