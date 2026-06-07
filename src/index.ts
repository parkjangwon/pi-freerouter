import type {
  ExtensionAPI,
  AssistantMessageEvent,
  AssistantMessageEventStream,
  Context,
  ProviderConfig,
  SimpleStreamOptions,
} from "./types.js";
import { createAssistantMessageEventStream } from "./types.js";
import { fetchFreeModels } from "./discovery.js";
import { FreeRouter } from "./router.js";
import { streamFreeModel, ModelExhaustedError, ModelFatalError } from "./stream.js";

// Race this many free models simultaneously; first to stream wins.
const RACE_WIDTH = 3;

// If no candidate emits its first token within this window, abort and try the
// next batch. Free models routinely take 10-30 s to first token; 30 s ensures
// we don't thrash through the pool on legitimately slow (but live) models.
const FIRST_TOKEN_TIMEOUT_MS = 30_000;

// Re-fetch the free model list every hour so long-running Pi sessions pick up
// newly available models (and stop wasting retries on removed ones).
const REFRESH_INTERVAL_MS = 60 * 60 * 1_000;
const DEFAULT_MODEL_CONTEXT_WINDOW = 128_000;
const DEFAULT_MODEL_MAX_TOKENS = 4_096;
const DEFERRED_API_KEY_PLACEHOLDER = "pi-freerouter-selectable-placeholder";
const FREEROUTER_API = "freerouter" as NonNullable<ProviderConfig["api"]>;

function mergeSignals(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const controller = new AbortController();
  for (const sig of signals) {
    if (!sig) continue;
    if (sig.aborted) { controller.abort(); return controller.signal; }
    sig.addEventListener("abort", () => controller.abort(), { once: true });
  }
  return controller.signal;
}

type RaceResult = {
  winner: string | null;
  exhaustedIds: string[]; // snapshot — safe to read after raceModels returns
  timedOut: boolean;      // true iff the batch ended due to first-token timeout
  fatalError?: Error;     // 402 or similar — propagate immediately, don't retry
};

/**
 * Start candidateIds concurrently and forward events from the first model that
 * emits `text_start` (or a valid empty `done`) to outStream; abort the rest.
 *
 * Correctness notes:
 * - Uses Map<candidateIdx, Promise> so delete(idx) is always by candidate index,
 *   never by array position (which diverges after the first re-push).
 * - Snapshots exhaustedIds at each return so late floating-.catch() pushes after
 *   raceModels returns don't corrupt the caller's view.
 * - Tracks fatalError (e.g. 402) separately from quota exhaustion.
 */
async function raceModels(
  candidateIds: string[],
  context: Context,
  apiKey: string,
  outStream: AssistantMessageEventStream,
  parentSignal?: AbortSignal,
  maxTokens?: number,
): Promise<RaceResult> {
  const controllers = candidateIds.map(() => new AbortController());
  const proxyStreams = candidateIds.map(() => createAssistantMessageEventStream());
  const exhaustedIds: string[] = [];
  let fatalError: Error | undefined;

  // Start all candidates concurrently; each writes to its own isolated proxy stream.
  candidateIds.forEach((modelId, i) => {
    const sig = parentSignal
      ? mergeSignals(parentSignal, controllers[i].signal)
      : controllers[i].signal;
    streamFreeModel(modelId, context, apiKey, proxyStreams[i], sig, maxTokens).catch((err: unknown) => {
      if (err instanceof ModelExhaustedError) {
        exhaustedIds.push(modelId);
      } else if (err instanceof ModelFatalError) {
        fatalError = fatalError ?? (err as Error); // keep first fatal error
      }
      proxyStreams[i].end(); // ensure iterator terminates
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
        controllers.forEach((c) => c.abort());
        return { winner: null, exhaustedIds: [...exhaustedIds], timedOut: true, fatalError };
      }

      const { idx, result } = resolved;
      pending.delete(idx); // safe: keyed by candidate idx, not array position

      if (result.done) continue; // this candidate's stream ended; move on

      const event = result.value;
      buffers[idx].push(event);

      // text_start      → normal text streaming win
      // toolcall_start  → model is making a tool call; also a valid win
      // done            → valid but empty response (no text/tool content); still a winner
      if (event.type === "text_start" || event.type === "toolcall_start" || event.type === "done") {
        controllers.forEach((c, j) => { if (j !== idx) c.abort(); });

        for (const e of buffers[idx]) outStream.push(e);

        if (event.type === "done") {
          outStream.end();
          return { winner: candidateIds[idx], exhaustedIds: [...exhaustedIds], timedOut: false };
        }

        // Pipe remaining events from the winner to outStream.
        // Race each .next() against the same first-token timeout so a stalled
        // winner (HTTP connection open but no new chunks) doesn't hang Pi forever.
        let pipeDone = false;
        while (!pipeDone) {
          let idleHandle!: ReturnType<typeof setTimeout>;
          const idlePromise = new Promise<{ idle: true }>((resolve) => {
            idleHandle = setTimeout(() => resolve({ idle: true }), FIRST_TOKEN_TIMEOUT_MS);
          });
          const raceResult = await Promise.race([
            iterators[idx].next().then((r) => ({ idle: false as const, r })),
            idlePromise,
          ]);
          clearTimeout(idleHandle);

          if (raceResult.idle) {
            // Winner went silent after starting — abort and surface an error.
            console.warn(
              `[pi-freerouter] ${candidateIds[idx]} stalled after winning; closing stream.`,
            );
            controllers[idx].abort();
            outStream.push({
              type: "error",
              reason: "error",
              error: {
                ...BASE_ERROR_OUTPUT,
                content: [],
                errorMessage: `${candidateIds[idx]} stream stalled`,
                timestamp: Date.now(),
              },
            });
            outStream.end();
            pipeDone = true;
          } else {
            const { value: e, done: iterDone } = raceResult.r;
            if (iterDone) {
              pipeDone = true;
            } else {
              outStream.push(e);
              if (e.type === "done" || e.type === "error") {
                outStream.end();
                pipeDone = true;
              }
            }
          }
        }
        outStream.end(); // defensive: no-op if already ended via done/error above

        return { winner: candidateIds[idx], exhaustedIds: [...exhaustedIds], timedOut: false };
      }

      if (event.type === "error") {
        // Non-quota failure; stream ends next tick — don't re-add to pending.
        continue;
      }

      // Any other event (e.g., thinking_start before text_start): keep consuming.
      pending.set(idx, nextFrom(idx));
    }

    return { winner: null, exhaustedIds: [...exhaustedIds], timedOut: false, fatalError };
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
  // `let` so the background refresh can replace it; each streamSimple call
  // captures its own snapshot via `const localRouter = router`.
  let router: FreeRouter | undefined;
  let modelFetchPromise: Promise<FreeRouter> | undefined;
  let maxTokens = DEFAULT_MODEL_MAX_TOKENS;

  function currentApiKey(): string | undefined {
    const apiKey = process.env.OPENROUTER_API_KEY;
    return apiKey && apiKey.length > 0 ? apiKey : undefined;
  }

  async function ensureRouter(apiKey: string): Promise<FreeRouter> {
    if (router) return router;

    modelFetchPromise ??= fetchFreeModels(apiKey)
      .then((freeModels) => {
        if (freeModels.length === 0) {
          throw new Error("pi-freerouter: No free models found on OpenRouter");
        }

        router = new FreeRouter(freeModels.map((m) => m.id));
        maxTokens = Math.max(...freeModels.map((m) => m.maxTokens));
        return router;
      })
      .catch((err: unknown) => {
        modelFetchPromise = undefined;
        throw err;
      });

    return modelFetchPromise;
  }

  // Hourly background refresh — picks up new free models without restarting Pi.
  async function refreshModels(): Promise<void> {
    const apiKey = currentApiKey();
    if (!apiKey) return;

    try {
      const fresh = await fetchFreeModels(apiKey);
      if (fresh.length > 0) {
        router = new FreeRouter(fresh.map((m) => m.id));
        maxTokens = Math.max(...fresh.map((m) => m.maxTokens));
        console.log(`[pi-freerouter] Model list refreshed: ${fresh.length} free models`);
      }
    } catch (err) {
      console.warn("[pi-freerouter] Failed to refresh free model list:", err);
    }
  }

  const refreshTimer = setInterval(() => { void refreshModels(); }, REFRESH_INTERVAL_MS);
  // Don't keep the Node process alive solely for this timer.
  if (typeof (refreshTimer as NodeJS.Timeout).unref === "function") {
    (refreshTimer as NodeJS.Timeout).unref();
  }

  pi.registerProvider("freerouter", {
    baseUrl: "https://openrouter.ai/api/v1",
    apiKey: DEFERRED_API_KEY_PLACEHOLDER,
    api: FREEROUTER_API,
    models: [
      {
        id: "free-router",
        name: "FreeRouter",
        reasoning: false,
        input: ["text"],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: DEFAULT_MODEL_CONTEXT_WINDOW,
        maxTokens,
      },
    ],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    streamSimple: ((_model: unknown, context: Context, options?: SimpleStreamOptions) => {
      const stream = createAssistantMessageEventStream();
      let streamClosed = false;

      // Per-request set: each model is tried at most once per streamSimple call.
      // Prevents an infinite loop when SLOW_TTL_MS < FIRST_TOKEN_TIMEOUT_MS causes
      // timed-out models to re-enter the pool before the next batch completes.
      const triedThisRequest = new Set<string>();

      (async () => {
        const apiKey = currentApiKey();
        if (!apiKey) {
          const errMsg =
            "OPENROUTER_API_KEY is not set. Set it, then retry your prompt.";
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
          return;
        }

        // Capture router at request start so mid-request refreshes don't affect
        // this in-flight request's exhaustion tracking.
        const localRouter = await ensureRouter(apiKey);

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

          // Get all models not currently in TTL cooldown, then exclude those
          // already tried for this specific request to prevent cycling.
          const allAvailable = localRouter.nextModels(1000);
          const candidates = allAvailable
            .filter((id) => !triedThisRequest.has(id))
            .slice(0, RACE_WIDTH);
          if (candidates.length === 0) break;

          // Mark as tried before racing so aborts mid-batch don't cause retries.
          candidates.forEach((id) => triedThisRequest.add(id));

          console.log(`[pi-freerouter] Racing: ${candidates.join(", ")}`);

          const { winner, exhaustedIds, timedOut, fatalError } = await raceModels(
            candidates, context, apiKey, stream, options?.signal, options?.maxTokens,
          );

          // Quota-exceeded models: long TTL (90s).
          exhaustedIds.forEach((id) => localRouter.markExhausted(id));

          if (winner !== null) {
            console.log(`[pi-freerouter] Winner: ${winner}`);
            streamClosed = true;
            return; // raceModels wrote all events and called stream.end()
          }

          // Fatal error (e.g., 402 Insufficient Credits) — surface immediately.
          if (fatalError) {
            const errMsg = fatalError.message;
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
            return;
          }

          // Check abort — race may have ended because parent signal fired.
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

          // No winner — skip candidates we haven't already marked exhausted.
          // Timeout → short TTL (15s): model is alive but slow, recover quickly.
          // Other failure → long TTL (90s): treat as quota/error, avoid for longer.
          candidates.forEach((id) => {
            if (!exhaustedIds.includes(id)) {
              if (timedOut) {
                localRouter.markSlow(id);
              } else {
                localRouter.markExhausted(id);
              }
            }
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

}
