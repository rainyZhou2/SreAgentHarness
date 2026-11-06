# go-sre-harness

**English** | [简体中文](README.zh-CN.md)

A cognitive agent runtime for SRE: every reasoning step and tool call runs as a recoverable, auditable distributed transaction.

## Core capabilities

| Area | Capability |
|---|---|
| **Durable execution** | Run/Step/Attempt state machine, step-level leases + fence tokens, resume after a crash |
| **Safe execution** | R0–R3 risk classes, approvals bound to `action_hash`, single-use grants, side-effect ledger; unknown outcomes go to `NEEDS_HUMAN` and are never replayed |
| **Conversations** | Session / Turn / Branch, single-writer queue, branch fork, replayable messages and events |
| **Model gateway** | Alias → hard filters → soft-score routing, circuit breakers, budget and concurrency admission, fallback only while "not yet sent"; Claude, OpenAI-compatible and Gemini |
| **Context & intent** | Prompt manifest (per-section hashes), deterministic compaction; each message is classified as a question, a task or a clarification |
| **Long-term memory** | Full-text + vector + dependency-graph recall, RRF fusion, poisoning checks and conflict resolution |
| **Web** | `web.fetch` works out of the box; `web.search` supports Brave / Tavily / Bing / a generic backend, with output treated as untrusted |
| **Evaluation & evolution** | Datasets recorded from real runs, offline replay, non-inferiority gate, canary with automatic rollback |
| **Event delivery** | Committed event → event log → relay → JetStream → SSE; reconnects resume from the cursor, with no steady-state database reads |
| **Plugins** | Everything is replaceable except the state machine, leases, transactional commit, outbox and core routes |

## How it works

```
clients (web / desktop)
   │ REST + SSE
internal/api ── extensions: memory / eval / extensions
   │
internal/kernel ── Durable Step Engine
   ├─ cognitive  intent analysis, context assembly, prompt manifest, strategies
   ├─ gateway    routing, circuit breakers, budget, request/usage ledger
   ├─ tools      MCP / gRPC / Kubernetes / Prometheus / HTTP
   └─ policy     risk gate and approvals
   │
PostgreSQL (source of truth)  +  NATS JetStream (wake-ups and event delivery)
```

- **Claim in a short transaction → execute outside it → commit with the fence**: a worker that lost its lease discards its result, so stale workers cannot overwrite new state.
- **Gap-free event log + transactional outbox**: state, events and wake-ups commit together; the reconciler recovers anything stuck.
- **Event plane**: a leased relay tails the event log, JetStream fans events out per run, and the SSE gateway reads the log once before streaming live updates.
- **The kernel depends only on interfaces**: it never imports PostgreSQL, NATS or a model SDK, so adapters are replaceable and testable.

## What stands out

- **The database is the only truth; push is an accelerator.** Disconnects, dropped messages and replica failures cannot make state inconsistent.
- **Side effects cannot be duplicated.** Approvals bind an action hash and are re-checked before execution; an unknown outcome goes to a human instead of being retried.
- **Plugins can only tighten, never relax.** The policy floor, risk-class source, trust rules and audit are enforced by the framework.
- **The full path runs without an API key.** The built-in scripted model exercises the ledger, prompt manifest, streaming and tool validation.
- **The evolution loop is complete.** Freeze a dataset → replay offline → gate → human approval → canary → automatic rollback.

## Quick start

Requires Go 1.26+ and PostgreSQL 14+; NATS is optional.

```bash
createdb harness
HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

With a model:

```bash
ANTHROPIC_API_KEY=sk-ant-... HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

Demo (create a run → R2 approval → SSE → conclusion):

```bash
scripts/demo.sh "remediate high latency service=checkout"
```

The default `dev` authentication mode only listens on loopback addresses.

## Desktop client

[`desktop/`](desktop/README.md) is a native Wails client (~9 MB): sessions, streamed reasoning, tool-call cards, inline approvals, an approval inbox, and management of models / MCP servers / HTTP tools / skills.

```bash
desktop/build.sh run
```

## Deployment

```bash
docker compose -f deploy/docker-compose.yml up --build
```

- Roles: `api` / `worker` / `scheduler`; `all` runs them in one process, multiple processes need NATS.
- Kubernetes manifests: `deploy/k8s/harness.yaml`.
- Full plugin and configuration example: `deploy/harness.example.yaml`.
- In production use a non-superuser database role, otherwise RLS has no effect.

## Common settings

| Variable | Purpose |
|---|---|
| `HARNESS_DATABASE_URL` | required |
| `HARNESS_NATS_URL` | cross-process wake-ups and event delivery |
| `HARNESS_CONFIG` | plugin config file (YAML/JSON) |
| `HARNESS_AUTH_MODE` | `dev` / `token` / `oidc` |
| `HARNESS_SSE_MODE` | `push` (default) / `poll` |
| `HARNESS_MODEL_DAILY_BUDGET_USD` | per-tenant daily budget |
| `HARNESS_REQUIRE_DISTINCT_APPROVER` | the approver cannot be the initiator |

See `deploy/harness.example.yaml` for the rest.

## API

`POST /v1/runs`, `GET /v1/runs/{id}/events/stream` (SSE), `POST /v1/runs/{id}/commands` (approve / pause / cancel), `/v1/sessions…`, `/v1/memories…`, `/v1/eval…`, `/v1/evolution…`, `/v1/extensions…`, `/healthz`, `/metrics`.

## Tests

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

Database tests are skipped when the variable is unset.

## Known limitations

- GPU serving (vLLM/SGLang routing, KV offload) is not implemented.
- Artifacts only have a filesystem store; use an S3/MinIO plugin in production.
- Offline evaluation uses its own in-memory ledger; its cost is not charged to the tenant budget.
- Plugin config is read at startup; changes need a restart.
