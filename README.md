# pi-freerouter

A [Pi coding agent](https://pi.dev) extension that automatically routes requests through OpenRouter's free model tier.

## What it does

- Registers a single **FreeRouter** model in Pi's UI — no need to manually pick individual models
- Auto-discovers all free models from OpenRouter (models with `:free` suffix)
- Races **3 models simultaneously** and streams from whichever responds first
- Transparently rotates to the next batch on quota exhaustion (429) or slow responses
- Sets FreeRouter as Pi's default model on every session start
- Exhausted models automatically recover after 90 seconds (matching OpenRouter's per-minute quota reset)

## Setup

### 1. Install

```bash
npm install pi-freerouter
```

### 2. Set your OpenRouter API key

```bash
export OPENROUTER_API_KEY=sk-or-...
```

Get a free key at [openrouter.ai/keys](https://openrouter.ai/keys).

### 3. Register in Pi

In your Pi config, add `pi-freerouter` to your extensions list. FreeRouter will appear as a model in Pi's model picker and is set as the default automatically.

## How it works

```
Pi session start
    │
    └── pi-freerouter activates
            ├── Fetches all :free models from OpenRouter
            ├── Sorts by speed (Groq → Cerebras → Fireworks → others)
            └── Registers "FreeRouter" as a single virtual model

Per request (streamSimple)
    │
    ├── Pick next 3 available models from the sorted list
    ├── Start all 3 simultaneously
    ├── First to emit a response token wins → stream it to Pi
    ├── Abort the other 2
    └── On 429 / 5xx / timeout (5s) → mark exhausted, try next batch
            └── Exhausted models recover after 90s TTL
```

## Model priority

Free models are sorted so the fastest providers are always tried first:

1. Groq
2. Cerebras
3. Fireworks
4. Together
5. Mistral
6. Everything else (sorted by context size ascending)

## Configuration

Only one environment variable is required:

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENROUTER_API_KEY` | ✅ | Your OpenRouter API key |

No other configuration. Rotation, timeout, and TTL are tuned automatically.

## Error handling

| Situation | Behavior |
|-----------|----------|
| 429 Too Many Requests | Mark model exhausted (90s) → try next batch |
| 5xx Server Error | Mark model exhausted (90s) → try next batch |
| No response within 5s | Abort slow batch → try next batch |
| All models exhausted | Return error: "All free models exhausted. Try again in a moment." |
| Request cancelled | Return abort error immediately |
| `OPENROUTER_API_KEY` not set | Throw at startup |

## Development

```bash
npm install
npm test        # run tests (tsc + node --test)
npm run build   # compile to dist/
```

## License

MIT
