// 90 s for quota exhaustion (429/5xx) — matches OpenRouter's per-minute reset window.
const DEFAULT_EXHAUSTION_TTL_MS = 90_000;
// 15 s for a first-token timeout — model is alive but slow; back-off briefly so
// the next batch tries different candidates without burning the whole pool.
const SLOW_TTL_MS = 15_000;

export class FreeRouter {
  // modelId → { at: timestamp, ttl: ms }
  private readonly exhausted = new Map<string, { at: number; ttl: number }>();

  constructor(
    private readonly models: readonly string[],
    private readonly exhaustionTtlMs = DEFAULT_EXHAUSTION_TTL_MS,
  ) {}

  nextModel(): string | null {
    return this.nextModels(1)[0] ?? null;
  }

  nextModels(count: number): string[] {
    const now = Date.now();
    const result: string[] = [];

    for (const id of this.models) {
      if (result.length >= count) break;
      const entry = this.exhausted.get(id);
      if (entry !== undefined) {
        if (now - entry.at < entry.ttl) continue;
        this.exhausted.delete(id); // TTL expired — back in rotation
      }
      result.push(id);
    }

    return result;
  }

  /** Mark model as quota-exhausted (429/5xx). Long TTL — don't retry soon. */
  markExhausted(id: string): void {
    if (!this.models.includes(id)) {
      console.warn(`[pi-freerouter] markExhausted called with unknown model ID: ${id}`);
      return;
    }
    this.exhausted.set(id, { at: Date.now(), ttl: this.exhaustionTtlMs });
  }

  /** Mark model as slow (first-token timeout). Short TTL — try others first, recover fast. */
  markSlow(id: string): void {
    if (!this.models.includes(id)) return;
    // Don't downgrade an already-exhausted model to the shorter slow TTL.
    const existing = this.exhausted.get(id);
    if (existing && existing.ttl >= this.exhaustionTtlMs) return;
    this.exhausted.set(id, { at: Date.now(), ttl: SLOW_TTL_MS });
  }
}
