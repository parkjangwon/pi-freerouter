/**
 * Type re-exports for pi-freerouter.
 *
 * All extension types come from @earendil-works/pi-coding-agent.
 * The AI-layer types (AssistantMessageEventStream, Context, Model, etc.) are
 * NOT re-exported by the top-level pi-coding-agent package — they live in the
 * bundled @earendil-works/pi-ai sub-package.  We declare them here as local
 * types that are structurally compatible, derived from the published .d.ts
 * files inspected at /node_modules/.../pi-ai/dist/utils/event-stream.d.ts and
 * /node_modules/.../pi-ai/dist/types.d.ts.
 */

// ── Types directly exported by @earendil-works/pi-coding-agent ───────────────

export type {
  ExtensionAPI,
  ExtensionFactory,
  ExtensionContext,
  ProviderConfig,
  ProviderModelConfig,
  SessionStartEvent,
} from "@earendil-works/pi-coding-agent";

// ── AI-layer types (pi-ai is a bundled sub-dep, not a direct import) ─────────

/** Supported input types for a message. */
export interface TextContent {
  type: "text";
  text: string;
}

export interface ImageContent {
  type: "image";
  data: string;
  mimeType: string;
}

export interface ToolCall {
  type: "toolCall";
  id: string;
  name: string;
  arguments: Record<string, unknown>;
}

export interface ThinkingContent {
  type: "thinking";
  thinking: string;
}

export interface Usage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
  totalTokens: number;
  cost: { input: number; output: number; cacheRead: number; cacheWrite: number; total: number };
}

export type StopReason = "stop" | "length" | "toolUse" | "error" | "aborted";

export interface UserMessage {
  role: "user";
  content: string | (TextContent | ImageContent)[];
  timestamp: number;
}

export interface AssistantMessage {
  role: "assistant";
  content: (TextContent | ThinkingContent | ToolCall)[];
  api: string;
  provider: string;
  model: string;
  usage: Usage;
  stopReason: StopReason;
  timestamp: number;
  errorMessage?: string;
}

export interface ToolResultMessage {
  role: "toolResult";
  toolCallId: string;
  toolName: string;
  content: (TextContent | ImageContent)[];
  isError: boolean;
  timestamp: number;
}

export type Message = UserMessage | AssistantMessage | ToolResultMessage;

/** Context passed to streamSimple (and the ProviderConfig.streamSimple callback). */
export interface Context {
  systemPrompt?: string;
  messages: Message[];
  tools?: Array<{ name: string; description: string; parameters: unknown }>;
}

/** Minimal Model shape used by streamSimple. */
export interface Model {
  id: string;
  name: string;
  api: string;
  provider: string;
  baseUrl: string;
  reasoning: boolean;
  input: ("text" | "image")[];
  cost: { input: number; output: number; cacheRead: number; cacheWrite: number };
  contextWindow: number;
  maxTokens: number;
  headers?: Record<string, string>;
}

export type ThinkingLevel = "minimal" | "low" | "medium" | "high" | "xhigh";

/** Options passed to ProviderConfig.streamSimple. */
export interface SimpleStreamOptions {
  temperature?: number;
  maxTokens?: number;
  signal?: AbortSignal;
  apiKey?: string;
  reasoning?: ThinkingLevel;
}

/**
 * Async-iterable event stream returned by ProviderConfig.streamSimple.
 * Structurally compatible with AssistantMessageEventStream from pi-ai.
 */
export type AssistantMessageEvent =
  | { type: "start"; partial: AssistantMessage }
  | { type: "text_start"; contentIndex: number; partial: AssistantMessage }
  | { type: "text_delta"; contentIndex: number; delta: string; partial: AssistantMessage }
  | { type: "text_end"; contentIndex: number; content: string; partial: AssistantMessage }
  | { type: "thinking_start"; contentIndex: number; partial: AssistantMessage }
  | { type: "thinking_delta"; contentIndex: number; delta: string; partial: AssistantMessage }
  | { type: "thinking_end"; contentIndex: number; content: string; partial: AssistantMessage }
  | { type: "toolcall_start"; contentIndex: number; partial: AssistantMessage }
  | { type: "toolcall_delta"; contentIndex: number; delta: string; partial: AssistantMessage }
  | { type: "toolcall_end"; contentIndex: number; toolCall: ToolCall; partial: AssistantMessage }
  | { type: "done"; reason: "stop" | "length" | "toolUse"; message: AssistantMessage }
  | { type: "error"; reason: "aborted" | "error"; error: AssistantMessage };

export interface AssistantMessageEventStream extends AsyncIterable<AssistantMessageEvent> {
  push(event: AssistantMessageEvent): void;
  end(result?: AssistantMessage): void;
  result(): Promise<AssistantMessage>;
}

/**
 * Factory function for AssistantMessageEventStream.
 *
 * NOTE: The real implementation lives in @earendil-works/pi-ai, which is a
 * bundled sub-dependency not directly importable from extension code.  Pi
 * injects its own stream factory through the ProviderConfig.streamSimple
 * callback signature — extensions never need to instantiate the stream
 * themselves.  If you do need a concrete implementation for testing, create
 * one that satisfies the AsyncIterable interface above.
 *
 * @internal — for testing only. Pi runtime provides the real factory via streamSimple context.
 */
export function createAssistantMessageEventStream(): AssistantMessageEventStream {
  const queue: AssistantMessageEvent[] = [];
  const waiters: Array<(v: IteratorResult<AssistantMessageEvent>) => void> = [];
  let isDone = false;
  let finalResult: AssistantMessage | undefined;
  let resolveResult!: (v: AssistantMessage) => void;
  const resultPromise = new Promise<AssistantMessage>((r) => (resolveResult = r));

  const stream: AssistantMessageEventStream = {
    push(event: AssistantMessageEvent) {
      if (isDone) return;
      if (waiters.length > 0) {
        waiters.shift()!({ value: event, done: false });
      } else {
        queue.push(event);
      }
      if (event.type === "done") {
        finalResult = event.message;
        resolveResult(event.message);
        isDone = true;
        while (waiters.length > 0) {
          waiters.shift()!({ value: undefined as unknown as AssistantMessageEvent, done: true });
        }
      } else if (event.type === "error") {
        finalResult = event.error;
        resolveResult(event.error);
        isDone = true;
        while (waiters.length > 0) {
          waiters.shift()!({ value: undefined as unknown as AssistantMessageEvent, done: true });
        }
      }
    },
    end(result?: AssistantMessage) {
      isDone = true;
      if (result && !finalResult) resolveResult(result);
      while (waiters.length > 0) {
        waiters.shift()!({ value: undefined as unknown as AssistantMessageEvent, done: true });
      }
    },
    result() {
      return resultPromise;
    },
    [Symbol.asyncIterator]() {
      return {
        next(): Promise<IteratorResult<AssistantMessageEvent>> {
          if (queue.length > 0) {
            return Promise.resolve({ value: queue.shift()!, done: false });
          }
          if (isDone) {
            return Promise.resolve({
              value: undefined as unknown as AssistantMessageEvent,
              done: true,
            });
          }
          return new Promise((resolve) => waiters.push(resolve));
        },
      };
    },
  };

  return stream;
}
