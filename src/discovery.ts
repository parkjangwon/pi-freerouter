import type { ProviderModelConfig } from "./types.js";

const DEFAULT_CONTEXT_WINDOW = 128_000;
const DEFAULT_MAX_TOKENS = 4_096;
const MODEL_DISCOVERY_TIMEOUT_MS = 10_000;

// Providers known for low latency on free tier, ordered by preference
const FAST_PROVIDER_PREFIXES = [
  "groq/",
  "cerebras/",
  "fireworks/",
  "together/",
  "mistralai/",
];

const NON_ASSISTANT_MODEL_MARKERS = [
  "content-safety",
  "moderation",
  "guard",
  "-vl",
  "/vl",
  "vision",
] as const;

function speedScore(modelId: string): number {
  const lower = modelId.toLowerCase();
  const idx = FAST_PROVIDER_PREFIXES.findIndex((prefix) => lower.startsWith(prefix));
  return idx === -1 ? FAST_PROVIDER_PREFIXES.length : idx;
}

function isGeneralAssistantModel(modelId: string): boolean {
  const lower = modelId.toLowerCase();
  return !NON_ASSISTANT_MODEL_MARKERS.some((marker) => lower.includes(marker));
}

interface OpenRouterModel {
  id: string;
  name: string;
  context_length?: number;
  top_provider?: { max_completion_tokens?: number };
}

interface OpenRouterModelsResponse {
  data: OpenRouterModel[];
}

export async function fetchFreeModels(
  apiKey: string,
  timeoutMs = MODEL_DISCOVERY_TIMEOUT_MS,
): Promise<ProviderModelConfig[]> {
  if (!apiKey) {
    throw new Error("OPENROUTER_API_KEY is required");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  let response: Response;
  try {
    response = await fetch("https://openrouter.ai/api/v1/models", {
      headers: { Authorization: `Bearer ${apiKey}` },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    throw new Error(`Failed to fetch OpenRouter models: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as OpenRouterModelsResponse;

  const models = (payload.data ?? []).filter((m) =>
    m.id.includes(":free") && isGeneralAssistantModel(m.id)
  );

  // Sort: fast providers first, then by context size ascending (smaller = faster inference)
  models.sort((a, b) => {
    const scoreDiff = speedScore(a.id) - speedScore(b.id);
    if (scoreDiff !== 0) return scoreDiff;
    const aCtx = a.context_length ?? DEFAULT_CONTEXT_WINDOW;
    const bCtx = b.context_length ?? DEFAULT_CONTEXT_WINDOW;
    return aCtx - bCtx;
  });

  return models.map((m) => ({
    id: m.id,
    name: m.name,
    reasoning: false,
    input: ["text"] as ("text" | "image")[],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: m.context_length ?? DEFAULT_CONTEXT_WINDOW,
    maxTokens: m.top_provider?.max_completion_tokens ?? DEFAULT_MAX_TOKENS,
  }));
}
