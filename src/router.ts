const DEFAULT_EXHAUSTION_TTL_MS = 90_000;

export class FreeRouter {
  private readonly exhausted = new Map<string, number>(); // modelId → exhausted-at timestamp

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
      const exhaustedAt = this.exhausted.get(id);
      if (exhaustedAt !== undefined) {
        if (now - exhaustedAt < this.exhaustionTtlMs) continue;
        this.exhausted.delete(id); // TTL expired — back in rotation
      }
      result.push(id);
    }

    return result;
  }

  markExhausted(id: string): void {
    if (!this.models.includes(id)) {
      console.warn(`[pi-freerouter] markExhausted called with unknown model ID: ${id}`);
      return;
    }
    this.exhausted.set(id, Date.now());
  }
}
