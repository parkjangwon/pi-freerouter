import { strict as assert } from "node:assert";
import { describe, it } from "node:test";

describe("fetchFreeModels", () => {
  it("filters only :free models and maps fields", async () => {
    const mockPayload = {
      data: [
        {
          id: "meta-llama/llama-3.2-3b:free",
          name: "Llama 3.2 3B (free)",
          context_length: 131072,
          top_provider: { max_completion_tokens: 8192 },
          pricing: { prompt: "0", completion: "0" },
        },
        {
          id: "openai/gpt-4o",
          name: "GPT-4o",
          context_length: 128000,
          pricing: { prompt: "0.000005", completion: "0.000015" },
        },
      ],
    };

    globalThis.fetch = async () =>
      ({
        ok: true,
        json: async () => mockPayload,
      } as any);

    const { fetchFreeModels } = await import("./discovery.js");
    const models = await fetchFreeModels("sk-or-test");

    assert.equal(models.length, 1);
    assert.equal(models[0].id, "meta-llama/llama-3.2-3b:free");
    assert.equal(models[0].contextWindow, 131072);
    assert.equal(models[0].maxTokens, 8192);
    assert.deepEqual(models[0].cost, { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 });
  });

  it("throws if apiKey is empty", async () => {
    const { fetchFreeModels } = await import("./discovery.js");
    await assert.rejects(
      () => fetchFreeModels(""),
      /OPENROUTER_API_KEY/
    );
  });

  it("throws if fetch response is not ok", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 401, statusText: "Unauthorized" } as any);

    const { fetchFreeModels } = await import("./discovery.js");
    await assert.rejects(
      () => fetchFreeModels("sk-or-bad"),
      /401/
    );
  });
});
