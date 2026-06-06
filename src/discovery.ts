import type { ProviderModelConfig } from "./types.js";

interface OpenRouterModel {
  id: string;
  name: string;
  context_length?: number;
  top_provider?: { max_completion_tokens?: number };
  pricing: { prompt: string; completion: string };
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

  return payload.data
    .filter((m) => m.id.includes(":free"))
    .map((m) => ({
      id: m.id,
      name: m.name,
      reasoning: false,
      input: ["text"] as ("text" | "image")[],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: m.context_length ?? 128000,
      maxTokens: m.top_provider?.max_completion_tokens ?? 4096,
    }));
}
