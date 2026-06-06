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

// Race this many free models simultaneously; first to stream wins.
const RACE_WIDTH = 3;

// If no candidate emits its first token within this window, abort and try the
// next batch. Keeps perceived latency bounded even on universally-slow batches.
const FIRST_TOKEN_TIMEOUT_MS = 5_000;

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
 * Start candidateIds concurrently and forward events from the first model that
 * emits `text_start` (or a valid empty `done`) to outStream; abort the rest.
 *
 * Key correctness invariant: pending uses a Map<candidateIdx, Promise> so that
 * removal is always by candidate index, never by array position (which diverges
 * after the first re-push).
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

  // Start all candidates concurrently; each writes to its own isolated proxy stream.
  candidateIds.forEach((modelId, i) => {
    const sig = parentSignal
      ? mergeSignals(parentSignal, controllers[i].signal)
      : controllers[i].signal;
    streamFreeModel(modelId, context, apiKey, proxyStreams[i], sig).catch((err: unknown) => {
      if (err instanceof ModelExhaustedError) exhaustedIds.push(modelId);
      proxyStreams[i].end(); // ensure iterator terminates if streamFreeModel didn't
    });
  });

  const iterators = proxyStreams.map((s) => s[Symbol.asyncIterator]());
  const buffers: AssistantMessageEvent[][] = candidateIds.map(() => []);

  type RaceItem = { idx: number; result: IteratorResult<AssistantMessageEvent> };
  type TimeoutSentinel = { __timeout: true };

  const nextFrom = (idx: number): Promise<RaceItem> =>
    iterators[idx].next().then((result) => ({ idx, result }));

  // Map<candidateIdx, Promise> so delete(idx) is always correct regardless of
  // insertion order — the old array-based filter((_,j)=>j!==idx) was wrong after
  // any re-push because j (array position) diverges from idx (candidate index).
  const pending = new Map<number, Promise<RaceItem>>(
    candidateIds.map((_, i): [number, Promise<RaceItem>] => [i, nextFrom(i)]),
  );

  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<TimeoutSentinel>((resolve) => {
    timeoutHandle = setTimeout(
      () => resolve({ __timeout: true }),
      FIRST_TOKEN_TIMEOUT_MS,
    );
  });

  try {
    while (pending.size > 0) {
      const resolved = await Promise.race<RaceItem | TimeoutSentinel>([
        ...pending.values(),
        deadline,
      ]);

      if ("__timeout" in resolved) {
        // Batch is too slow — abort all, caller will try the next batch.
        controllers.forEach((c) => c.abort());
        return { winner: null, exhaustedIds };
      }

      const { idx, result } = resolved;
      pending.delete(idx); // safe: keyed by candidate idx, not array position

      if (result.done) continue; // this candidate's stream ended; move on

      const event = result.value;
      buffers[idx].push(event);

      // text_start  → normal streaming win
      // done        → valid but empty response (no text content); still a winner
      if (event.type === "text_start" || event.type === "done") {
        // Abort all losing candidates immediately.
        controllers.forEach((c, j) => { if (j !== idx) c.abort(); });

        // Forward all buffered events (including the winning event itself).
        for (const e of buffers[idx]) outStream.push(e);

        if (event.type === "done") {
          // Empty response is complete — close and return.
          outStream.end();
          return { winner: candidateIds[idx], exhaustedIds };
        }

        // text_start: pipe the remaining events from the winner to outStream.
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

      if (event.type === "error") {
        // This candidate had a non-quota failure; its stream will end next tick.
        // Don't re-add to pending — let it drain via result.done on the next race.
        continue;
      }

      // Any other event (e.g., thinking_start before text_start): keep consuming.
      pending.set(idx, nextFrom(idx));
    }

    return { winner: null, exhaustedIds };
  } finally {
    clearTimeout(timeoutHandle);
  }
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
          // Check abort before starting each batch.
          if (options?.signal?.aborted) {
            const abortMsg = "Request was cancelled.";
            streamClosed = true;
            stream.push({
              type: "error",
              reason: "aborted",
              error: {
                ...BASE_ERROR_OUTPUT,
                content: [],
                errorMessage: abortMsg,
                timestamp: Date.now(),
              },
            });
            stream.end();
            return;
          }

          const candidates = router.nextModels(RACE_WIDTH);
          if (candidates.length === 0) break;

          const { winner, exhaustedIds } = await raceModels(
            candidates, context, apiKey, stream, options?.signal,
          );

          exhaustedIds.forEach((id) => router.markExhausted(id));

          if (winner !== null) {
            streamClosed = true;
            return; // raceModels wrote all events and called stream.end()
          }

          // Check abort again — the race may have ended because all candidates
          // were aborted by the parent signal (not because of quota exhaustion).
          if (options?.signal?.aborted) {
            const abortMsg = "Request was cancelled.";
            streamClosed = true;
            stream.push({
              type: "error",
              reason: "aborted",
              error: {
                ...BASE_ERROR_OUTPUT,
                content: [],
                errorMessage: abortMsg,
                timestamp: Date.now(),
              },
            });
            stream.end();
            return;
          }

          // No winner: mark non-exhausted candidates too, so we don't retry the
          // same slow/error-prone batch immediately (they recover after TTL).
          candidates.forEach((id) => {
            if (!exhaustedIds.includes(id)) router.markExhausted(id);
          });
        }

        // All models currently in TTL cooldown.
        const errMsg =
          "All free models exhausted. They will recover automatically — please try again in a moment.";
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

  // Auto-activate FreeRouter as the default model on session start.
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
