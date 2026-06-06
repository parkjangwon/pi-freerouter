import type {
  ExtensionAPI,
  ExtensionContext,
  AssistantMessageEvent,
  AssistantMessageEventStream,
  Context,
  SimpleStreamOptions,
} from "./types.js";
import { createAssistantMessageEventStream } from "./types.js";
import { fetchFreeModels } from "./discovery.js";
import { FreeRouter } from "./router.js";
import { streamFreeModel, ModelExhaustedError } from "./stream.js";

// Try this many models simultaneously; first to start streaming wins
const RACE_WIDTH = 2;

function mergeSignals(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const controller = new AbortController();
  for (const sig of signals) {
    if (!sig) continue;
    if (sig.aborted) { controller.abort(); return controller.signal; }
    sig.addEventListener("abort", () => controller.abort(), { once: true });
  }
  return controller.signal;
}

/**
 * Race candidateIds against each other: start all simultaneously, forward events
 * from the first model that emits text_start, abort the rest.
 *
 * Returns exhaustedIds (models that returned 429/5xx) regardless of whether a
 * winner was found, so the caller can mark them in the router.
 */
async function raceModels(
  candidateIds: string[],
  context: Context,
  apiKey: string,
  outStream: AssistantMessageEventStream,
  parentSignal?: AbortSignal,
): Promise<{ winner: string | null; exhaustedIds: string[] }> {
  const controllers = candidateIds.map(() => new AbortController());
  const proxyStreams = candidateIds.map(() => createAssistantMessageEventStream());
  const exhaustedIds: string[] = [];

  // Start all candidates concurrently; each writes to its own proxy stream
  candidateIds.forEach((modelId, i) => {
    const sig = parentSignal
      ? mergeSignals(parentSignal, controllers[i].signal)
      : controllers[i].signal;
    streamFreeModel(modelId, context, apiKey, proxyStreams[i], sig).catch((err: unknown) => {
      if (err instanceof ModelExhaustedError) exhaustedIds.push(modelId);
      proxyStreams[i].end(); // ensure iterator terminates if streamFreeModel didn't
    });
  });

  // Multiplex async iterators — resolve winner on first text_start
  const iterators = proxyStreams.map((s) => s[Symbol.asyncIterator]());
  const buffers: AssistantMessageEvent[][] = candidateIds.map(() => []);

  type RaceItem = { idx: number; result: IteratorResult<AssistantMessageEvent> };
  const nextFrom = (idx: number): Promise<RaceItem> =>
    iterators[idx].next().then((result) => ({ idx, result }));

  let pending: Promise<RaceItem>[] = candidateIds.map((_, i) => nextFrom(i));

  while (pending.length > 0) {
    const { idx, result } = await Promise.race(pending);
    pending = pending.filter((_, j) => j !== idx);

    if (result.done) continue; // this candidate finished without winning

    const event = result.value;
    buffers[idx].push(event);

    if (event.type === "text_start") {
      // Abort all losing candidates
      controllers.forEach((c, j) => { if (j !== idx) c.abort(); });

      // Forward buffered events (everything up to and including text_start)
      for (const e of buffers[idx]) outStream.push(e);

      // Forward remaining events from the winner's stream
      for await (const e of { [Symbol.asyncIterator]: () => iterators[idx] }) {
        outStream.push(e);
        if (e.type === "done" || e.type === "error") {
          outStream.end();
          break;
        }
      }
      outStream.end(); // defensive: no-op if already ended via done/error above

      return { winner: candidateIds[idx], exhaustedIds };
    }

    // Not text_start yet — keep consuming from this candidate
    pending.push(nextFrom(idx));
  }

  return { winner: null, exhaustedIds };
}

const BASE_ERROR_OUTPUT = {
  role: "assistant" as const,
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
};

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
    apiKey,
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
    streamSimple: ((_model: unknown, context: Context, options?: SimpleStreamOptions) => {
      const stream = createAssistantMessageEventStream();
      let streamClosed = false;

      (async () => {
        while (true) {
          const candidates = router.nextModels(RACE_WIDTH);
          if (candidates.length === 0) break;

          const { winner, exhaustedIds } = await raceModels(
            candidates, context, apiKey, stream, options?.signal,
          );

          exhaustedIds.forEach((id) => router.markExhausted(id));

          if (winner !== null) {
            streamClosed = true;
            return; // raceModels already wrote done/error and called stream.end()
          }

          // No winner: mark any non-exhausted candidates so we don't retry them
          // immediately (could be network error, not quota — rotate to fresh ones)
          candidates.forEach((id) => {
            if (!exhaustedIds.includes(id)) router.markExhausted(id);
          });
        }

        // All models currently exhausted — they auto-recover after TTL
        const errMsg = "All free models exhausted. They will recover automatically — please try again in a moment.";
        streamClosed = true;
        stream.push({
          type: "error",
          reason: "error",
          error: {
            ...BASE_ERROR_OUTPUT,
            content: [{ type: "text", text: errMsg }],
            errorMessage: errMsg,
            timestamp: Date.now(),
          },
        });
        stream.end();
      })().catch((err: unknown) => {
        if (!streamClosed) {
          const errMsg = String(err);
          stream.push({
            type: "error",
            reason: "error",
            error: {
              ...BASE_ERROR_OUTPUT,
              content: [],
              errorMessage: errMsg,
              timestamp: Date.now(),
            },
          });
          stream.end();
        }
      });

      // Cast: local stream satisfies the pi-ai class interface at runtime;
      // private fields on the pi-ai class prevent structural assignability.
      return stream as unknown as ReturnType<NonNullable<import("@earendil-works/pi-coding-agent").ProviderConfig["streamSimple"]>>;
    }) as NonNullable<import("@earendil-works/pi-coding-agent").ProviderConfig["streamSimple"]>,
  });

  // Auto-activate FreeRouter as the default model on session start
  pi.on("session_start", async (_event: unknown, handlerCtx?: unknown) => {
    try {
      const registry = (handlerCtx as ExtensionContext)?.modelRegistry;
      const freeRouterModel = registry?.find?.("freerouter", "free-router");
      if (freeRouterModel) {
        await pi.setModel(freeRouterModel);
      }
    } catch (err) {
      console.warn("[pi-freerouter] Failed to set FreeRouter as active model:", err);
    }
  });
}
