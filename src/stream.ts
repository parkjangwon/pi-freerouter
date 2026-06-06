import type { AssistantMessageEventStream, Context, AssistantMessage } from "./types.js";

export class ModelExhaustedError extends Error {
  constructor(public readonly modelId: string, public readonly status: number) {
    super(`Model ${modelId} quota exceeded (HTTP ${status})`);
    this.name = "ModelExhaustedError";
  }
}

export async function streamFreeModel(
  modelId: string,
  context: Context,
  apiKey: string,
  outStream: AssistantMessageEventStream
): Promise<void> {
  const response = await fetch("https://openrouter.ai/api/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      "X-Title": "pi-freerouter",
    },
    body: JSON.stringify({
      model: modelId,
      stream: true,
      messages: context.messages,
      ...(context.tools?.length ? { tools: context.tools } : {}),
    }),
  });

  if (response.status === 429 || response.status >= 500) {
    throw new ModelExhaustedError(modelId, response.status);
  }
  if (!response.ok) {
    throw new Error(`OpenRouter error: ${response.status} ${response.statusText}`);
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

  outStream.push({ type: "start", partial: output });

  let textStarted = false;
  // Text content is always at index 0 (we only handle text for now)
  const TEXT_INDEX = 0;

  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

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
          const textContent = output.content[TEXT_INDEX];
          const contentText = textContent?.type === "text" ? textContent.text : "";
          outStream.push({
            type: "text_end",
            contentIndex: TEXT_INDEX,
            content: contentText,
            partial: { ...output },
          });
        }
        const stopReason = output.stopReason === "toolUse" ? "toolUse" : output.stopReason === "length" ? "length" : "stop";
        outStream.push({ type: "done", reason: stopReason, message: { ...output } });
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
        output.stopReason = finishReason === "tool_calls" ? "toolUse" : finishReason === "length" ? "length" : "stop";
      }

      if (delta?.content) {
        if (!textStarted) {
          // Insert the text content block before pushing text_start
          output.content.push({ type: "text", text: "" });
          outStream.push({
            type: "text_start",
            contentIndex: TEXT_INDEX,
            partial: { ...output },
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
          partial: { ...output },
        });
      }
    }
  }

  // Fallback: stream ended without [DONE]
  if (textStarted) {
    const textContent = output.content[TEXT_INDEX];
    const contentText = textContent?.type === "text" ? textContent.text : "";
    outStream.push({
      type: "text_end",
      contentIndex: TEXT_INDEX,
      content: contentText,
      partial: { ...output },
    });
  }
  const stopReason = output.stopReason === "toolUse" ? "toolUse" : output.stopReason === "length" ? "length" : "stop";
  outStream.push({ type: "done", reason: stopReason, message: { ...output } });
  outStream.end();
}
