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

export interface TokenPoolEntry {
  label: string
  githubToken: string
  copilotToken: string | null
  copilotApiUrl: string | null
  healthy: boolean
  unavailableUntil: number
  refreshController: AbortController | null
}

export class TokenPool {
  private entries: Array<TokenPoolEntry> = []
  private index = 0

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

  next(): TokenPoolEntry {
    const now = Date.now()
    for (let i = 0; i < this.entries.length; i++) {
      const entry = this.entries[this.index % this.entries.length]
      this.index++
      if (entry.healthy && entry.copilotToken) {
        return entry
      }
      // Check cooldown expiry
      if (!entry.healthy && now >= entry.unavailableUntil) {
        entry.healthy = true
        // Re-init in background
        this.initEntry(entry).catch(() => {})
      }
    }
    throw new Error("[pool] All credentials are unavailable")
  }

  getCopilotToken(): string {
    const entry = this.next()
    if (!entry.copilotToken) {
      throw new Error("[pool] Selected entry has no copilot token")
    }
    return entry.copilotToken
  }

  getCopilotApiUrl(): string | null {
    // Use the last selected entry's URL
    const idx = (this.index - 1 + this.entries.length) % this.entries.length
    return this.entries[idx].copilotApiUrl
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
