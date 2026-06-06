import type {
  AssistantMessageEventStream,
  Context,
  AssistantMessage,
  Message,
  TextContent,
  ToolCall,
  ToolResultMessage,
} from "./types.js";

export class ModelExhaustedError extends Error {
  constructor(public readonly modelId: string, public readonly status: number) {
    super(`Model ${modelId} quota exceeded (HTTP ${status})`);
    this.name = "ModelExhaustedError";
  }
}

/** Thrown when the API key has insufficient credits (HTTP 402). Not retriable. */
export class ModelFatalError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ModelFatalError";
  }
}

// Fix 6: Helper — normalize OpenRouter finish_reason to Pi StopReason
function normalizeStopReason(finishReason: string | null | undefined): "stop" | "length" | "toolUse" {
  if (finishReason === "tool_calls") return "toolUse";
  if (finishReason === "length") return "length";
  return "stop";
}

// Fix 7: Helper — deep-clone output for partial snapshots (content items are shallow-cloned)
function snapshot(output: AssistantMessage): AssistantMessage {
  return { ...output, content: output.content.map((c) => ({ ...c })) };
}

// Fix 6: Helper — get current text for text_end content field
function getCurrentText(output: AssistantMessage, textIndex: number): string {
  const block = output.content[textIndex];
  return block?.type === "text" ? block.text : "";
}

// Fix 6: Helper — emit text_end event
function emitTextEnd(
  outStream: AssistantMessageEventStream,
  output: AssistantMessage,
  contentIndex: number
): void {
  outStream.push({
    type: "text_end",
    contentIndex,
    content: getCurrentText(output, contentIndex),
    partial: snapshot(output),
  });
}

type OpenRouterContent = { type: "text"; text: string } | { type: "image_url"; image_url: { url: string } };
type OpenRouterMessage =
  | { role: "system"; content: string }
  | { role: "user"; content: string | OpenRouterContent[] }
  | { role: "assistant"; content: string | null; tool_calls?: OpenRouterToolCall[] }
  | { role: "tool"; tool_call_id: string; content: string };

type OpenRouterToolCall = {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
};

/**
 * Convert Pi's internal message format to OpenRouter's chat-completion format.
 *
 * Pi uses role:"toolResult" and embeds tool calls inside content[]; OpenRouter
 * expects role:"tool" and tool_calls as a top-level array on assistant messages.
 */
function normalizeMessages(messages: Message[]): OpenRouterMessage[] {
  const out: OpenRouterMessage[] = [];

  for (const msg of messages) {
    if (msg.role === "user") {
      if (typeof msg.content === "string") {
        out.push({ role: "user", content: msg.content });
      } else {
        const parts: OpenRouterContent[] = msg.content.map((c) => {
          if (c.type === "text") return { type: "text", text: c.text };
          // image → data URL
          return { type: "image_url", image_url: { url: `data:${c.mimeType};base64,${c.data}` } };
        });
        out.push({ role: "user", content: parts });
      }
    } else if (msg.role === "assistant") {
      const textParts = (msg.content as Array<{ type: string }>)
        .filter((c): c is TextContent => c.type === "text")
        .map((c) => c.text)
        .join("");
      const toolCalls = (msg.content as Array<{ type: string }>)
        .filter((c): c is ToolCall => c.type === "toolCall")
        .map((tc): OpenRouterToolCall => ({
          id: tc.id,
          type: "function",
          function: { name: tc.name, arguments: JSON.stringify(tc.arguments) },
        }));

      if (toolCalls.length > 0) {
        out.push({ role: "assistant", content: textParts || null, tool_calls: toolCalls });
      } else {
        out.push({ role: "assistant", content: textParts });
      }
    } else if (msg.role === "toolResult") {
      const tr = msg as ToolResultMessage;
      const text = (tr.content as Array<{ type: string }>)
        .filter((c): c is TextContent => c.type === "text")
        .map((c) => c.text)
        .join("\n");
      out.push({ role: "tool", tool_call_id: tr.toolCallId, content: text });
    }
    // unknown roles: silently drop (forward compatibility)
  }

  return out;
}

export async function streamFreeModel(
  modelId: string,
  context: Context,
  apiKey: string,
  outStream: AssistantMessageEventStream,
  signal?: AbortSignal
): Promise<void> {
  const messages: OpenRouterMessage[] = [
    ...(context.systemPrompt ? [{ role: "system" as const, content: context.systemPrompt }] : []),
    ...normalizeMessages(context.messages),
  ];

  const body: Record<string, unknown> = { model: modelId, stream: true, messages };

  // Forward tool definitions if Pi provided them — free models that support
  // function calling will use them; those that don't will return 400 and be
  // temporarily skipped by the router.
  if (context.tools && context.tools.length > 0) {
    body.tools = context.tools.map((t) => ({
      type: "function",
      function: { name: t.name, description: t.description, parameters: t.parameters },
    }));
  }

  const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      "X-Title": "pi-freerouter",
    },
    body: JSON.stringify(body),
    signal,
  });

  if (response.status === 402) {
    throw new ModelFatalError(
      "OpenRouter API key has insufficient credits. Add credits at openrouter.ai/credits.",
    );
  }
  if (response.status === 429 || response.status >= 500) {
    throw new ModelExhaustedError(modelId, response.status);
  }
  // 400/422: model rejected the request (unsupported features, content policy, etc.)
  // Treat like exhaustion so the router skips this model for the current batch.
  if (response.status === 400 || response.status === 422) {
    throw new ModelExhaustedError(modelId, response.status);
  }
  if (!response.ok) {
    throw new Error(`OpenRouter error: ${response.status} ${response.statusText}`);
  }

  // Fix 4: Guard response.body null
  if (!response.body) {
    throw new Error(`OpenRouter returned empty body for model ${modelId}`);
  }

  const now = Date.now();
  const output: AssistantMessage = {
    role: "assistant",
    content: [],
    api: "openrouter",
    provider: "openrouter",
    model: modelId,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    stopReason: "stop",
    timestamp: now,
  };

  outStream.push({ type: "start", partial: snapshot(output) });

  let textStarted = false;
  // Text content is always at index 0 (we only handle text for now)
  const TEXT_INDEX = 0;

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  // Fix 3: Wrap streaming body in try/finally to ensure outStream.end() is always called
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop()!;

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const data = line.slice(6).trim();

        if (data === "[DONE]") {
          if (textStarted) {
            emitTextEnd(outStream, output, TEXT_INDEX);
          }
          outStream.push({ type: "done", reason: output.stopReason as "stop" | "length" | "toolUse", message: snapshot(output) });
          outStream.end();
          return;
        }

        let chunk: any;
        try { chunk = JSON.parse(data); } catch { continue; }

        const choice = chunk.choices?.[0];
        const delta = choice?.delta;
        const finishReason = choice?.finish_reason;
        const usage = chunk.usage;

        if (usage) {
          output.usage.input = usage.prompt_tokens ?? 0;
          output.usage.output = usage.completion_tokens ?? 0;
          output.usage.totalTokens = usage.total_tokens ?? 0;
        }

        if (finishReason) {
          output.stopReason = normalizeStopReason(finishReason);
        }

        if (delta?.content) {
          if (!textStarted) {
            // Insert the text content block before pushing text_start
            output.content.push({ type: "text", text: "" });
            outStream.push({
              type: "text_start",
              contentIndex: TEXT_INDEX,
              partial: snapshot(output),
            });
            textStarted = true;
          }

          const textBlock = output.content[TEXT_INDEX];
          if (textBlock?.type === "text") {
            textBlock.text += delta.content;
          }

          outStream.push({
            type: "text_delta",
            contentIndex: TEXT_INDEX,
            delta: delta.content,
            partial: snapshot(output),
          });
        }
      }
    }

    // Fix 5: Flush TextDecoder at stream end
    const remaining = decoder.decode();
    if (remaining) buffer += remaining;

    // Fallback: stream ended without [DONE]
    if (textStarted) {
      emitTextEnd(outStream, output, TEXT_INDEX);
    }
    outStream.push({ type: "done", reason: output.stopReason as "stop" | "length" | "toolUse", message: snapshot(output) });
    outStream.end();
  } catch (err) {
    // Fix 3: Ensure stream is closed on mid-stream errors
    const isAbort = err instanceof Error && err.name === "AbortError";
    outStream.push({ type: "error", reason: isAbort ? "aborted" : "error", error: output });
    outStream.end();
    throw err;
  }
}
