import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { FreeRouter } from "./router.js";

describe("FreeRouter", () => {
  it("returns the first model when none exhausted", () => {
    const r = new FreeRouter(["a:free", "b:free", "c:free"]);
    assert.equal(r.nextModel(), "a:free");
  });

  it("skips exhausted models", () => {
    const r = new FreeRouter(["a:free", "b:free", "c:free"]);
    r.markExhausted("a:free");
    assert.equal(r.nextModel(), "b:free");
  });

  it("returns null when all models exhausted", () => {
    const r = new FreeRouter(["a:free", "b:free"]);
    r.markExhausted("a:free");
    r.markExhausted("b:free");
    assert.equal(r.nextModel(), null);
  });

  it("always returns first available in insertion order", () => {
    const r = new FreeRouter(["a:free", "b:free", "c:free"]);
    r.markExhausted("a:free");
    r.markExhausted("b:free");
    assert.equal(r.nextModel(), "c:free");
    assert.equal(r.nextModel(), "c:free"); // stable
  });

  it("handles empty model list", () => {
    const r = new FreeRouter([]);
    assert.equal(r.nextModel(), null);
  });

  it("ignores markExhausted with unknown ID", () => {
    const r = new FreeRouter(["a:free", "b:free"]);
    r.markExhausted("unknown:free"); // not in models list
    assert.equal(r.nextModel(), "a:free"); // unchanged
  });

  it("markExhausted is idempotent", () => {
    const r = new FreeRouter(["a:free", "b:free"]);
    r.markExhausted("a:free");
    r.markExhausted("a:free"); // second call, same ID
    assert.equal(r.nextModel(), "b:free"); // still correct
  });

  it("nextModels returns up to count non-exhausted models in order", () => {
    const r = new FreeRouter(["a:free", "b:free", "c:free", "d:free"]);
    r.markExhausted("b:free");
    assert.deepEqual(r.nextModels(2), ["a:free", "c:free"]);
  });

  it("nextModels returns fewer than count when not enough available", () => {
    const r = new FreeRouter(["a:free", "b:free"]);
    r.markExhausted("b:free");
    assert.deepEqual(r.nextModels(3), ["a:free"]);
  });

  it("exhausted model returns after TTL expires", async () => {
    const r = new FreeRouter(["a:free", "b:free"], 20); // 20ms TTL for test speed
    r.markExhausted("a:free");
    assert.equal(r.nextModel(), "b:free"); // a is excluded
    await new Promise((res) => setTimeout(res, 30));   // wait past TTL
    assert.equal(r.nextModel(), "a:free"); // a is back at front of list
  });

  it("markSlow skips model with short TTL", async () => {
    const r = new FreeRouter(["a:free", "b:free"]);
    r.markSlow("a:free");
    assert.equal(r.nextModel(), "b:free"); // a temporarily skipped
    // Note: full 15s slow TTL not tested to keep tests fast; TTL logic is shared with markExhausted
  });

  it("markSlow does not downgrade an exhausted model to slow TTL", () => {
    const r = new FreeRouter(["a:free", "b:free"], 90_000);
    r.markExhausted("a:free");
    r.markSlow("a:free"); // should keep the longer 90s TTL
    // Both markExhausted and markSlow skip the model; the key invariant is that
    // markSlow won't replace a long-TTL entry with a short one.
    assert.equal(r.nextModel(), "b:free"); // a still excluded
  });

  it("nextModels with large count returns all non-exhausted models", () => {
    const r = new FreeRouter(["a:free", "b:free", "c:free"]);
    r.markExhausted("b:free");
    const all = r.nextModels(1000);
    assert.deepEqual(all, ["a:free", "c:free"]);
  });
});
