export class FreeRouter {
  private readonly exhausted = new Set<string>();

  constructor(private readonly models: readonly string[]) {}

  nextModel(): string | null {
    return this.models.find((id) => !this.exhausted.has(id)) ?? null;
  }

  markExhausted(id: string): void {
    this.exhausted.add(id);
  }
}
