export class FreeRouter {
  private readonly exhausted = new Set<string>();

  constructor(private readonly models: readonly string[]) {}

  nextModel(): string | null {
    return this.models.find((id) => !this.exhausted.has(id)) ?? null;
  }

  markExhausted(id: string): void {
    if (!this.models.includes(id)) {
      console.warn(`[pi-freerouter] markExhausted called with unknown model ID: ${id}`);
      return;
    }
    this.exhausted.add(id);
  }
}
