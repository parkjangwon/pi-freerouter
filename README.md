# pi-freerouter

Pi coding agent extension that routes every request through OpenRouter's free model tier — no paid API key needed beyond your OpenRouter account.

## Quick start

**1. Install**

```bash
pi install npm:pi-freerouter
```

**2. Set your OpenRouter API key**

```bash
export OPENROUTER_API_KEY=sk-or-...
```

Free key at [openrouter.ai/keys](https://openrouter.ai/keys).

**3. Start Pi and select FreeRouter**

FreeRouter appears in the model picker as `auto [free-router]`. Select it when
you want requests routed through OpenRouter's free model tier. If
`OPENROUTER_API_KEY` is not set, Pi still starts normally; pi-freerouter shows
the key error only after you send a prompt with `auto [free-router]` selected.

---

## How free model routing works

OpenRouter exposes dozens of free models (models with a `:free` suffix). Each has its own rate limit — typically a few requests per minute per model. The trick is to spread load across all of them automatically.

### Parallel racing

Every time Pi sends a request, pi-freerouter doesn't pick one model and hope for the best. It picks the **next 3 available models** from its sorted list and fires all three requests simultaneously.

```
Request arrives
    │
    ├── model A ──────────────────── first token ──▶ WINNER → stream to Pi
    ├── model B ───── (slower)                      → aborted
    └── model C ─── (even slower)                   → aborted
```

Whichever model emits its first token wins. The other two are immediately cancelled. Pi sees a single clean stream — it has no idea a race happened.

### Automatic fallback

Failed models are skipped for a short cooldown period and then return to the pool:

| Failure | Cooldown |
|---|---|
| Rate limit (429) or server error (5xx) | 90 s |
| No first token within 30 s | 15 s |
| Request rejected (400/422) | 90 s |

```
Batch 1: [model A, model B, model C]  → all hit quota
Batch 2: [model D, model E, model F]  → model D wins
         ↑ model A–C recover after 90s and rejoin the pool
```

Each model is tried at most once per request. Once a winner is streaming, a 30-second idle window per chunk ensures a stalled connection is aborted promptly rather than left open.

### Provider priority

Free models are sorted so the lowest-latency inference providers are always tried first:

1. Groq
2. Cerebras  
3. Fireworks
4. Together
5. Mistral
6. Everything else (sorted by context window ascending)

### Model list refresh

The list of available free models is fetched when you first send a prompt with
`auto [free-router]` selected, then refreshed every hour in the background.

---

## Requirements

- [Pi coding agent](https://pi.dev) v0.78+
- OpenRouter API key (free tier is sufficient)

## License

MIT
