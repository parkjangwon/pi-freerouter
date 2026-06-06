import { strict as assert } from "node:assert";
import { describe, it, beforeEach, afterEach } from "node:test";
import { ModelExhaustedError, ModelFatalError } from "./stream.js";

function makeStream() {
  const events: any[] = [];
  let ended = false;
  return {
    push: (e: any) => events.push(e),
    end: () => { ended = true; },
    get events() { return events; },
    get ended() { return ended; },
  };
}

function makeSseBody(lines: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  const text = lines.map((l) => `data: ${l}\n\n`).join("");
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
}

describe("streamFreeModel", () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => { originalFetch = globalThis.fetch; });
  afterEach(() => { globalThis.fetch = originalFetch; });

  it("throws ModelExhaustedError on 429", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 429, statusText: "Too Many Requests", body: null } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await assert.rejects(
      () => streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream),
      ModelExhaustedError
    );
  });

  it("throws ModelFatalError on 402", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 402, statusText: "Payment Required", body: null } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await assert.rejects(
      () => streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream),
      ModelFatalError
    );
  });

  it("throws ModelExhaustedError on 503", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 503, statusText: "Service Unavailable", body: null } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await assert.rejects(
      () => streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream),
      ModelExhaustedError
    );
  });

  it("throws ModelExhaustedError on 400 (model rejects request)", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 400, statusText: "Bad Request", body: null } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await assert.rejects(
      () => streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream),
      ModelExhaustedError
    );
  });

  it("throws ModelExhaustedError on 422 (unprocessable)", async () => {
    globalThis.fetch = async () =>
      ({ ok: false, status: 422, statusText: "Unprocessable Entity", body: null } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await assert.rejects(
      () => streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream),
      ModelExhaustedError
    );
  });

  it("emits toolcall events for tool call responses", async () => {
    const sseLines = [
      JSON.stringify({ choices: [{ delta: { role: "assistant", content: null, tool_calls: [{ index: 0, id: "call_abc", type: "function", function: { name: "bash", arguments: "" } }] }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: '{"command' } }] }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: '":"ls"}' } }] }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "tool_calls" }], usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } }),
      "[DONE]",
    ];

    globalThis.fetch = async () =>
      ({ ok: true, status: 200, body: makeSseBody(sseLines) } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream);

    const types = stream.events.map((e: any) => e.type);
    assert.ok(types.includes("start"), "missing start event");
    assert.ok(types.includes("toolcall_start"), "missing toolcall_start");
    assert.ok(types.includes("toolcall_delta"), "missing toolcall_delta");
    assert.ok(types.includes("toolcall_end"), "missing toolcall_end");
    assert.ok(types.includes("done"), "missing done event");
    assert.ok(stream.ended, "stream.end() was not called");

    const tcEnd = stream.events.find((e: any) => e.type === "toolcall_end");
    assert.equal(tcEnd.toolCall.id, "call_abc");
    assert.equal(tcEnd.toolCall.name, "bash");
    assert.deepEqual(tcEnd.toolCall.arguments, { command: "ls" });

    const done = stream.events.find((e: any) => e.type === "done");
    assert.equal(done.reason, "toolUse");
  });

  it("pushes text events and done on success", async () => {
    const sseLines = [
      JSON.stringify({ choices: [{ delta: { role: "assistant", content: "" }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: { content: "Hello" }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: { content: " world" }, finish_reason: null }] }),
      JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }], usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 } }),
      "[DONE]",
    ];

    globalThis.fetch = async () =>
      ({ ok: true, status: 200, body: makeSseBody(sseLines) } as any);

    const { streamFreeModel } = await import("./stream.js");
    const stream = makeStream() as any;
    await streamFreeModel("model:free", { messages: [] } as any, "sk-or-test", stream);

    const types = stream.events.map((e: any) => e.type);
    assert.ok(types.includes("start"), "missing start event");
    assert.ok(types.includes("text_start"), "missing text_start");
    assert.ok(types.includes("text_delta"), "missing text_delta");
    assert.ok(types.includes("done"), "missing done event");
    assert.ok(stream.ended, "stream.end() was not called");

    const deltas = stream.events
      .filter((e: any) => e.type === "text_delta")
      .map((e: any) => e.delta);
    assert.deepEqual(deltas, ["Hello", " world"]);

    const done = stream.events.find((e: any) => e.type === "done");
    assert.equal(done.message.usage.input, 5);
    assert.equal(done.message.usage.output, 2);
  });
});
