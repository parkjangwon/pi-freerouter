import type { ProviderModelConfig } from "./types.js";

const DEFAULT_CONTEXT_WINDOW = 128_000;
const DEFAULT_MAX_TOKENS = 4_096;

interface OpenRouterModel {
  id: string;
  name: string;
  context_length?: number;
  top_provider?: { max_completion_tokens?: number };
}

interface OpenRouterModelsResponse {
  data: OpenRouterModel[];
}

export async function fetchFreeModels(apiKey: string): Promise<ProviderModelConfig[]> {
  if (!apiKey) {
    throw new Error("OPENROUTER_API_KEY is required");
  }

  const response = await fetch("https://openrouter.ai/api/v1/models", {
    headers: { Authorization: `Bearer ${apiKey}` },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch OpenRouter models: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as OpenRouterModelsResponse;

  const models = payload.data ?? [];
  return models
    .filter((m) => m.id.includes(":free"))
    .map((m) => ({
      id: m.id,
      name: m.name,
      reasoning: false,
      input: ["text"] as ("text" | "image")[],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: m.context_length ?? DEFAULT_CONTEXT_WINDOW,
      maxTokens: m.top_provider?.max_completion_tokens ?? DEFAULT_MAX_TOKENS,
    }));
}
