import { strict as assert } from "node:assert";
import { afterEach, beforeEach, describe, it } from "node:test";

import { AuthStorage, ModelRegistry } from "@earendil-works/pi-coding-agent";
import freerouterExtension from "./index.js";

function makePi() {
  let providerConfig: any;
  let sessionStartHandler: any;

  return {
    pi: {
      registerProvider: (_name: string, config: any) => {
        providerConfig = config;
      },
      on: (_event: string, handler: any) => {
        sessionStartHandler = handler;
      },
      setModel: async () => {},
    } as any,
    get providerConfig() {
      return providerConfig;
    },
    get sessionStartHandler() {
      return sessionStartHandler;
    },
  };
}

async function collectEvents(stream: AsyncIterable<any>): Promise<any[]> {
  const events: any[] = [];
  for await (const event of stream) {
    events.push(event);
  }
  return events;
}

describe("freerouter extension startup", () => {
  let originalFetch: typeof globalThis.fetch;
  let originalApiKey: string | undefined;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    originalApiKey = process.env.OPENROUTER_API_KEY;
    delete process.env.OPENROUTER_API_KEY;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    if (originalApiKey === undefined) {
      delete process.env.OPENROUTER_API_KEY;
    } else {
      process.env.OPENROUTER_API_KEY = originalApiKey;
    }
  });

  it("registers the provider when OPENROUTER_API_KEY is missing", async () => {
    const fakePi = makePi();
    let fetchCalls = 0;
    globalThis.fetch = async () => {
      fetchCalls += 1;
      throw new Error("fetch should not run during startup");
    };

    await freerouterExtension(fakePi.pi);

    assert.equal(fetchCalls, 0);
    assert.equal(fakePi.providerConfig.apiKey, "pi-freerouter-deferred-openrouter-key");
    assert.equal(fakePi.providerConfig.models[0].id, "free-router");
    assert.equal(typeof fakePi.providerConfig.streamSimple, "function");
    assert.equal(typeof fakePi.sessionStartHandler, "function");
  });

  it("passes Pi provider validation when OPENROUTER_API_KEY is missing", async () => {
    const registry = ModelRegistry.inMemory(AuthStorage.inMemory());
    const fakePi = {
      registerProvider: registry.registerProvider.bind(registry),
      on: () => {},
      setModel: async () => {},
    } as any;

    await freerouterExtension(fakePi);

    const model = registry.find("freerouter", "free-router");
    assert.equal(model?.id, "free-router");
    assert.ok(model);
    assert.equal(registry.hasConfiguredAuth(model), true);
  });

  it("returns a request-time error when OPENROUTER_API_KEY is missing", async () => {
    const fakePi = makePi();
    await freerouterExtension(fakePi.pi);

    const stream = fakePi.providerConfig.streamSimple(
      undefined,
      { messages: [] },
    ) as AsyncIterable<any>;
    const events = await collectEvents(stream);

    assert.equal(events.length, 1);
    assert.equal(events[0].type, "error");
    assert.match(events[0].error.errorMessage, /OPENROUTER_API_KEY/);
  });
});
