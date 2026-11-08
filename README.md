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
| **Orchestration DSL** | A profile-embedded flow graph: conditions/loops, parallel child runs, variable interpolation, approvals/human input, subflows and multi-agent |
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

### Orchestration DSL

A profile can set `strategy: "flow"` and embed a `flow` graph. The DSL compiles to the existing step engine: the parent run only coordinates, while agent / tool / subflow nodes become child runs, so leases, fences, approvals, the effect ledger and checkpoint recovery are reused. Parallelism is carried by child runs and joined by a `join` node; a child inherits the parent profile and can only narrow its tools and permissions.

```yaml
strategy: flow
tools: [demo.metrics, demo.restart]
max_steps: 40
flow:
  version: 1
  entry: triage
  limits: {max_steps: 20, max_children: 4, max_total_runs: 8, max_depth: 2}
  nodes:
    - id: triage
      type: agent
      role: triage
      tools: [demo.metrics]
      next: [{to: finish}]
    - id: finish
      type: end
      output: "${nodes.triage.output}"
```

Node types: `agent` / `tool` / `subflow` / `parallel` / `join` / `approval` / `input` / `loop` / `end`. `GET /v1/runs/{id}/flow` returns node and child-run state; the `provide_input` command answers an `input` node. Built-in examples: `sre-diagnosis@8` (parallel diagnosis + approval) and `@9` (human input + subflow).

The desktop client does not ask you to choose a profile. An auto session starts with a durable **plan step**: the planner LLM submits a flow DSL through `submit_flow`; the server compiles it, runs a static safety/budget analysis, repairs malformed plans, and pins the plan as an artifact. A generated plan with side effects (or an R2/R3 tool) gets a **plan review** step whose approval binds the plan hash; changing the graph, inputs or tool set invalidates the approval. Only then is the flow step appended. If the model cannot produce a valid plan, the runtime falls back to the configured runbook (default `sre-diagnosis@8`) or ReAct. Generated plans, flow nodes, child runs, approvals and input gates are rendered in the conversation.

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

[`desktop/`](desktop/README.md) is a native Wails client. It starts in **Local runtime** mode: the app launches a bundled SQLite-backed harness core, writes execution events to an append-only JSONL log, and keeps SQLite only as the query index. No Docker, PostgreSQL, NATS or terminal is needed. Switch to **Team server** to use a shared harness. Sessions, streamed reasoning, tool-call cards, inline approvals, an approval inbox, and model / MCP / HTTP tool / skill management are the same in both modes; local mode implements the same `/v1/extensions`, command, checkpoint/fork, memory, graph and evaluation APIs. Commands are version-guarded and idempotent across restarts, cancelling a flow cascades to child runs, and compaction summaries, memory hits, checkpoints and step output revisions are persisted in the local store.

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
| `HARNESS_AUTO_PROFILE` / `_VERSION` | run profile for auto sessions; defaults to `sre-diagnosis` / `10` |
| `HARNESS_PLANNER_MODEL_ALIAS` / `_EFFORT` | model and effort used by the planner |
| `HARNESS_PLANNER_FALLBACK_PROFILE` / `_VERSION` | runbook used when planning fails; defaults to `sre-diagnosis` / `8` |
| `HARNESS_PLANNER_MAX_REPAIRS` / `_MAX_NODES` | plan repair attempts and generated node limit |
| `HARNESS_PLAN_APPROVAL` | `auto` (default), `always` or `never`; `auto` reviews generated plans with writes |
| `HARNESS_PLAN_APPROVERS` | required plan approvals; R3 plans always require at least two |
| `HARNESS_PLANNER_MAX_CHILDREN` / `_TOTAL_RUNS` / `_DEPTH` / `_ITERATIONS` | generated-graph fan-out limits |
| `HARNESS_PLANNER_MAX_MODEL_CALLS` / `_MAX_ESTIMATED_TOKENS` | static model-call and output-token ceilings |
| `HARNESS_OUTPUT_OFFLOAD_BYTES` | tool results above this size are stored and summarized; the model reads them with `harness.artifact_query` (default 24576, negative disables) |
| `HARNESS_CONTEXT_PROVIDER_TIMEOUT` / `HARNESS_CONTEXT_MAX_FACTS` | per-provider deadline and fact limit for environment facts |
| `HARNESS_PLANNER_REACT_PATTERNS` / `_FLOW_PATTERNS` | fallback routing patterns, `\|\|`-separated regexes; `none` disables a list |

Generated plans emit `plan.generated`, `plan.validation_failed`, `plan.repaired`, `plan.fallback`, `plan.approval_requested`, `plan.approved`, `plan.rejected` and `plan.executed` events. Planner metrics are exposed on `/metrics`: `harness_plan_total`, `harness_plan_duration_seconds`, `harness_plan_repairs_total`, `harness_plan_validation_failures_total` and `harness_plan_fallback_total`.

Plan datasets record the plan artifact, analysis, flow state, approvals, outcome, duration and cost. `POST /v1/eval/datasets/{id}/plan-cases` adds a terminal run, `POST /v1/eval/plan-runs` replays the frozen split offline, and the report groups success, safety, replay completion, steps, duration and cost by variant (`react`, `fallback_flow`, `generated_flow`, `profile_flow`).

See `deploy/harness.example.yaml` for the rest.

## Triggers, policy rules and the action loop

- **Triggers** (`triggers:`): `POST /v1/triggers/{name}` turns webhooks (Alertmanager, Grafana, PagerDuty, CI, …) into runs. Items fan out, `filter` drops noise, `resolve` events only attach evidence. Duplicates within the dedup window are injected into the open run as evidence instead of starting a new one; deduplication is shared through the database, so replicas agree. Optional HMAC signatures (`X-Harness-Signature: sha256=…`) and per-trigger rate limits. `schedule:` triggers create one run per interval across scheduler replicas.
- **Rule-based policy** (`policy: {name: rules}`): matchers over tool, source, labels, risk, environment, target, arguments, time windows, rate and blast radius. The strictest matching rule decides; the framework floor always applies. The environment is resolved per call and bound into the action intent.
- **Time and environment context**: runs carry a pinned `now`, incident window, services and labels (`context` on `POST /v1/runs`, or rendered by a trigger). `context_provider` plugins add topology, ownership and recent changes once per run; facts are pinned for replay and rendered as untrusted data. Tools with `time_args` get the window filled in when the model leaves it out.
- **Verify and compensate** (`tool_overlays.<tool>.lifecycle`): after a side effect the kernel polls the verify tool until `expect` holds. A refuted action proposes the compensation, which always waits for approval.
- **Large outputs**: oversized tool results become a `tool-output` artifact plus a structured summary (log levels, time range, frequent templates; JSON shape and numeric stats). The model queries the rest with `harness.artifact_query` (head, tail, lines, grep, JSON path, time range).
- **Untrusted output**: every tool result, operator evidence block and compacted summary is enclosed in a content-derived `<<<DATA id=…>>>` boundary that the content cannot close; chat-template control tokens are neutralized and instruction-like text (the memory poisoning patterns) is flagged `injection_suspected` in the prompt, the observation and the prompt manifest. Content is never dropped, only marked. Compaction keeps offload handles, verification verdicts and these flags outside the excerpt limit.
- **Routing**: when planning fails, the goal-context hint, the model's requested mode, configured patterns and side-effecting tools named in the goal decide between the fallback flow and ReAct.

The desktop runtime reads the same sections (`policy`, `triggers`, `context_providers`, `tool_overlays`) from `harness.local.json` in its data directory, or from `HARNESS_LOCAL_CONFIG`.

## Measurable, reusable, self-improving reasoning

The agent's explicit decision trace (plan, hypotheses with confidence, verification) is scored, reused and improved. Provider-hidden reasoning is never read. Design: `docs/superpowers/specs/2026-11-07-reasoning-quality-and-self-improvement-design.md`.

- **Measure**: every finished run is scored by the consolidator (and rescored when feedback arrives). The score covers calibration (Brier score of hypotheses that ended VERIFIED/REFUTED), evidence grounding, redundant tool calls, steps to the first verdict, the conclusion verdict and a composite quality. Offline replays are scored the same way, and the release gate fails a candidate whose calibration, grounding, redundancy or quality regresses on the holdout split.
- **Reuse**: completed, undisputed runs with a VERIFIED conclusion are distilled into *reasoning patterns*: procedural memories listing the hypotheses checked in order, their verdicts and the tools used until the first verdict. They follow the normal memory lifecycle (CANDIDATE → approval → ACTIVE), so an operator activates every pattern. Each memory's success rate and Wilson lower bound are measured from the runs that retrieved it.
- **Pattern effectiveness**: a pattern is judged against the baseline success rate of the same service (runs that did not see it), not an absolute rate. Explicit citations (`memory:<id>` in evidence or hypothesis refs) decide once there are enough of them. Patterns significantly and materially worse than the baseline are excluded. Excluded patterns are still shown to a small, deterministic share of runs (`reasoning.pattern_probe_rate`, at most 0.2) and are reinstated when those runs reach the baseline. Stale entries are pruned, and the list keeps the strongest cases when it hits its 500-entry limit. Operators can propose an exclusion or reinstatement by hand.
- **Trace structure**: hypotheses carry a stable `id` used for deduplication across rewording. Confidence coverage and the structured share are measured, gated and diagnosed; a settled hypothesis without a confidence counts as a 0.5 forecast.
- **Improve**: the `reasoning_improver` job (or `POST /v1/reasoning/improve`) diagnoses weak dimensions and underperforming ACTIVE patterns. It proposes a profile candidate that changes only `reasoning.playbook` (fixed, domain-free guidance; at most 2,000 characters, poison-checked, rendered before the trust rules) and `reasoning.patterns_excluded`. Runs behind holdout cases are excluded from the diagnosis. The candidate goes through the existing pipeline: holdout gate, approval by a human other than the proposer, canary with automatic rollback. The canary monitor also rolls back when the canary's reasoning regresses against the control group (default on, at least 20 scored runs per side). The loop never approves, canaries or promotes, and proposes one change at a time.
- **Retention**: reasoning scores are kept for `reasoning_retention` (default 90 days) by the `retention` job.
- **APIs**: `GET /v1/reasoning/summary`, `GET /v1/reasoning/runs/{run_id}`, `GET /v1/reasoning/patterns`, `POST /v1/reasoning/patterns/{memory_id}/exclude|reinstate` (operator), `POST /v1/reasoning/improve` (admin). The desktop runtime scores and distills the same way and improves only on demand.

## API

`POST /v1/runs`, `GET /v1/runs/{id}/events/stream` (SSE), `POST /v1/runs/{id}/commands` (approve / pause / cancel), `/v1/sessions…`, `/v1/memories…`, `/v1/eval…`, `/v1/evolution…`, `/v1/extensions…`, `/v1/triggers…`, `/v1/reasoning…`, `/healthz`, `/metrics`.

## Tests

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

Database tests are skipped when the variable is unset. The live planner probe exercises the real provider tool-call path (and exits non-zero if the model does not call `submit_flow` with a valid plan):

```bash
ANTHROPIC_API_KEY=sk-ant-... go run ./cmd/plancheck -goal "restart checkout after approval"
```

## Known limitations

- GPU serving (vLLM/SGLang routing, KV offload) is not implemented.
- Artifacts only have a filesystem store; use an S3/MinIO plugin in production.
- Offline evaluation uses its own in-memory ledger; its cost is not charged to the tenant budget.
- Plan replay drives the flow state machine with recorded node results; graphs with bounded loops are compiled and safety-checked but not step-replayed.
- Plugin config is read at startup; changes need a restart.
