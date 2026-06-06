import type { ExtensionAPI, SessionStartEvent, ExtensionContext } from "./types.js";
import { createAssistantMessageEventStream } from "./types.js";
import { fetchFreeModels } from "./discovery.js";
import { FreeRouter } from "./router.js";
import { streamFreeModel, ModelExhaustedError } from "./stream.js";

export default async function (pi: ExtensionAPI): Promise<void> {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    throw new Error("pi-freerouter: OPENROUTER_API_KEY is not set");
  }

  const freeModels = await fetchFreeModels(apiKey);
  if (freeModels.length === 0) {
    throw new Error("pi-freerouter: No free models found on OpenRouter");
  }

  const router = new FreeRouter(freeModels.map((m) => m.id));

  const maxContext = Math.max(...freeModels.map((m) => m.contextWindow));
  const maxTokens = Math.max(...freeModels.map((m) => m.maxTokens));

  pi.registerProvider("freerouter", {
    baseUrl: "https://openrouter.ai/api/v1",
    apiKey: "$OPENROUTER_API_KEY",
    api: "openai-completions",
    models: [
      {
        id: "free-router",
        name: "FreeRouter",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: maxContext,
        maxTokens,
      },
    ],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    streamSimple: ((_model: unknown, context: import("./types.js").Context) => {
      const stream = createAssistantMessageEventStream();

      (async () => {
        let modelId = router.nextModel();

        while (modelId) {
          try {
            await streamFreeModel(modelId, context, apiKey, stream);
            return;
          } catch (err) {
            if (err instanceof ModelExhaustedError) {
              router.markExhausted(err.modelId);
              modelId = router.nextModel();
            } else {
              throw err;
            }
          }
        }

        // All free models exhausted
        const errMsg = "All free models exhausted. Restart Pi to reset.";
        const exhaustedOutput = {
          role: "assistant" as const,
          content: [{ type: "text" as const, text: errMsg }],
          api: "openrouter",
          provider: "freerouter",
          model: "free-router",
          usage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
          stopReason: "error" as const,
          errorMessage: errMsg,
          timestamp: Date.now(),
        };
        stream.push({ type: "error", reason: "error", error: exhaustedOutput });
        stream.end();
      })().catch((err: unknown) => {
        const errOutput = {
          role: "assistant" as const,
          content: [] as never[],
          api: "openrouter",
          provider: "freerouter",
          model: "free-router",
          usage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
          stopReason: "error" as const,
          errorMessage: String(err),
          timestamp: Date.now(),
        };
        stream.push({ type: "error", reason: "error", error: errOutput });
        stream.end();
      });

      // Cast: local stream satisfies the pi-ai class interface at runtime;
      // private fields on the pi-ai class prevent structural assignability.
      return stream as unknown as ReturnType<NonNullable<import("@earendil-works/pi-coding-agent").ProviderConfig["streamSimple"]>>;
    }) as NonNullable<import("@earendil-works/pi-coding-agent").ProviderConfig["streamSimple"]>,
  });

  // Auto-activate FreeRouter as the default model on session start
  pi.on("session_start", async (_event: SessionStartEvent, ctx: ExtensionContext) => {
    const freeRouterModel = ctx.modelRegistry.find("freerouter", "free-router");
    if (freeRouterModel) {
      await pi.setModel(freeRouterModel);
    }
  });
}
