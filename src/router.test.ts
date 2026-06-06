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
});
