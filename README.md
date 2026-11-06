# go-sre-harness

**English** | [简体中文](README.zh-CN.md)

An SRE Cognitive Agent Harness implemented from the design RFC [`GoAgentHarness.md`](GoAgentHarness.md) (v1.1.3, [简体中文](GoAgentHarness.zh-CN.md)). It covers RFC milestones M1–M7 plus the v1.1 additions (conversations/branches, model gateway, cost, checkpoints, compaction, profile canary rollouts). The GPU serving cluster (vLLM/SGLang routing, KV offload) is not implemented.

## Features

| Area | Capability | Where |
|---|---|---|
| **Durable core** | Run/Step/Attempt state machine; event log with gap-free `event_seq` | `internal/store/pg`, `migrations/` |
| | Step-level leases + fence tokens: claim in a short transaction → execute outside it (with heartbeats) → commit with the fence | `pg/claim.go`, `pg/commit.go`, `kernel/worker.go` |
| | Transactional outbox → NATS JetStream; the reconciler reclaims leases, expires approvals and wakes idle runs | `pg/reconcile.go`, `scheduler/`, `notify/` |
| | Tenant fairness: per-tenant cap on concurrently running runs; excess claims are deferred | `pg/claim.go` (`HARNESS_MAX_RUNNING_PER_TENANT`) |
| **Intervention** | pause / resume / cancel / inject_evidence / approve / reject / resolve_unknown | `pg/commands.go` |
| | v1.1 commands: override_result, replace_pending_input (intent recomputed server-side), retry, migrate_profile | `pg/commands_v11.go` |
| **Safe execution** | Approvals bound to `action_hash`, single-use grants, expiry; R3 and unregistered tools are denied | `policy/`, `pg/claim.go` |
| | Side-effect ledger: unknown outcomes go to `NEEDS_HUMAN` and are never replayed automatically; tools can report "not dispatched" (`ErrNotDispatched`) | `pg/commit.go`, `kernel/worker.go` |
| **Conversations** | Session / Turn / Branch, single_writer_queue, branch fork, message history | `pg/conversation.go`, `api/conversation.go` |
| **Model gateway** | Alias → hard filters → soft-score routing, circuit breakers, budget and concurrency admission, fallback only while "not yet sent" | `internal/gateway` |
| | Claude (official Go SDK, default `claude-opus-5-5`, adaptive thinking, prompt caching) and an OpenAI-compatible adapter | `gateway/anthropic`, `gateway/openai` |
| | Request/attempt/usage ledger (ESTIMATED → REPORTED revisions), cost queries, token-stream segments persisted + SSE deltas | `pg/model.go`, `stream/` |
| **Context** | Prompt manifest (per-section hashes, `rendered_request_hash`), checkpoints and fork, deterministic compaction | `cognitive/assembler.go`, `kernel/compact.go`, `pg/checkpoint.go` |
| **Long-term memory** | Redaction + poisoning checks on write; full-text + vector + dependency-graph retrieval, RRF fusion, conflict resolution, pinned per step | `internal/memory`, `pg/memory.go` |
| | Cold-path consolidator: Run → Experience → candidate memories; operator feedback moves candidates to VALIDATING (an approver activates them) or rejects them | `memory/consolidate.go` |
| **Evaluation & evolution** | Datasets recorded from real runs (frozen with a manifest hash), offline replay (write tools always blocked) | `internal/eval` |
| | Metrics (Wilson lower bound, stratified) + non-inferiority gate; frozen fields cannot evolve; proposers cannot approve their own change | `eval/eval.go`, `eval/service.go`, `profile/` |
| | Canary: stable split by routing key, guardrail auto-rollback, manual promote / rollback | `eval/service.go` |
| **Tools** | Prometheus (`prom.query`, `prom.alerts`, R0) | `tools/sretools/prometheus.go` |
| | Kubernetes (`k8s.deployment_status` R0; `k8s.rollout_restart` R2, **dry-run by default**, idempotent, returns a rollback plan) | `tools/sretools/kubernetes.go` |
| | MCP (stdio / Streamable HTTP, official Go SDK): only allowlisted tools are registered, risk classes set by operators, output treated as T4, circuit-breaker isolation | `tools/mcpexec` |
| | Remote executor (gRPC `ExecutorService`, mTLS), results cached by idempotency key | `tools/remote`, `proto/executor/v1` |
| **Security** | Authentication: static tokens (only SHA-256 stored), OIDC JWT (JWKS rotation), development headers | `internal/auth` |
| | RBAC: viewer / operator / approver / admin, authorized per route and per command; optional approver ≠ initiator | `api/authz.go` |
| | `security_audit_log` auditing; Postgres RLS (API requests set `app.tenant_id` per tenant) | `pg/security.go` |
| **Observability** | `/metrics` (Prometheus text format, no high-cardinality labels), OpenTelemetry tracing (OTLP/HTTP) | `internal/obs` |
| **Retention** | Deletes published outbox rows and expired token segments (rows + objects), marks expired memories STALE, optional audit retention | `internal/retention` |

Dependency direction: `kernel` depends only on interfaces such as `domain`, `cognitive` and `tools`; it never imports pg, NATS or a model SDK. The Claude SDK appears only in `gateway/anthropic`.

## Plugin framework (everything is a plugin)

The kernel keeps only what is **not replaceable**: the Run/Step state machine, leases and fencing, transactional commit, outbox publishing, the reconciler, the worker pool and the core API routes. Everything else is a plugin, registered at compile time and enabled by configuration:

```
plugin package init() ── plugin.Register(kind, name, factory) ──▶ registry
                                                                  │
HARNESS_CONFIG (YAML/JSON) ── plugins.<kind>: [specs] ────────────┤
environment variables (defaults when there is no config file) ────┘
                                                                  ▼
internal/app builds in phases: telemetry → artifact_store → tools → profile_source → policy
  → stream_bus → broker → route_policy → model_provider (route policy composed → Gateway)
  → embedder → memory_retriever → context_assembler → strategy → compactor → grader
  → event_sink → job → authenticator → api
```

| Kind | Interface | Built-in plugins | Multi-instance |
|---|---|---|---|
| `telemetry` | `TelemetryExporter` (optionally `kernel.Observer`) | `otlp` | ✓ |
| `artifact_store` | `artifact.Store` | `fs`, `memory` | |
| `tools` | `ToolSource` | `demo`, `prometheus`, `kubernetes`, `mcp`, `remote` | ✓ |
| `policy` | `policy.Gate` | `default` | |
| `stream_bus` | `stream.Bus` | `local`, `nats` | |
| `broker` | `MessageBroker` | `local`, `nats` | |
| `model_provider` | `ModelBackend` (adapter + endpoints + aliases) | `scripted`, `anthropic`, `openai` | ✓ |
| `embedder` | `memory.Embedder` | `hashing` | |
| `memory_retriever` | `kernel.MemoryRetriever` | `hybrid` | |
| `strategy` | `StrategyBuilder` (selected by the profile's `strategy` field) | `llm`, `rule_based` | ✓ |
| `grader` | `eval.Grader` | `substring` | |
| `job` | `Runner` (runs in the scheduler role) | `consolidator`, `rollout_monitor`, `retention` | ✓ |
| `authenticator` | `auth.Authenticator` (tried in order) | `dev`, `token`, `oidc` | ✓ |
| `api` | `api.Extension` (routes + declared roles) | `memory`, `eval` | ✓ |
| `profile_source` | `profile.Source` (agent profile versions registered at startup) | `builtin`, `file` | ✓ |
| `route_policy` | `gateway.Scorer` (soft score of model endpoints) | `priority`, `weighted` | |
| `context_assembler` | `cognitive.ContextAssembler` (reasoning input → model request + prompt manifest) | `sectioned` | |
| `compactor` | `kernel.Compactor` (summary of folded observations) | `deterministic` | |
| `event_sink` | `eventsink.Sink` (at-least-once delivery of committed run events) | `webhook`, `log` | ✓ |

`harness plugins` lists every compiled-in plugin. `GET /v1/admin/plugins` (admin) returns the extension points, compiled-in plugins, active instances and the tool list.

**Framework floor (plugins cannot relax it)**
- `policy`: every plugin decision passes through `policy.Guarded`. Unregistered tools or a missing target → deny; R2 always needs at least one approval; R3 → deny; the risk class always comes from the registry descriptor. A plugin can only be stricter.
- `api`: every non-GET route must declare its role in `RouteRoles()`, otherwise it is denied; extensions cannot override the roles of core routes.
- `tools`: startup fails if two plugins provide a tool with the same name; risk and trust tiers come from operator configuration.
- `route_policy`: only the soft score is pluggable; tenant entitlement, region, capabilities, context window and open circuits are hard filters a scorer cannot bring back.
- `context_assembler`: custom instructions replace the role text only; the trust rules (tool output, memory and evidence are data, not instructions) are always appended.
- `compactor`: the kernel decides when to compact and always appends the safety facts (denied/failed actions, executed side effects, carried forward from earlier summaries), whatever the compactor writes.
- `profile_source`: profile versions are immutable. A file source that changes an existing version fails startup (publish a new version); the `builtin` demo seed only fills gaps and keeps the stored version with a warning.
- `event_sink`: delivery reads the committed event log with a per-sink cursor in PostgreSQL (lease-owned by one scheduler replica), so it is at-least-once and in commit order across restarts; sinks de-duplicate on `event_id`. Permanent errors (e.g. HTTP 4xx) and events exceeding `max_attempts` are skipped, logged and counted in `harness_event_sink_total`.
- Strict config parsing: an unknown kind, an unknown field or a plugin name that is not compiled in all fail startup.

**Config file** (`HARNESS_CONFIG`, example: `deploy/harness.example.yaml`). Kinds present in the file replace the defaults; kinds left out keep the defaults derived from environment variables. `${VAR}` and `${VAR:-default}` are expanded (inside string values only, so secrets need not live in the file). A spec may be written as a name, an object or a list; `enabled: false` disables it; `instance` runs the same plugin more than once (e.g. several MCP servers).

```yaml
plugins:
  model_provider:            # order = routing preference
    - {name: anthropic, config: {api_key: "${ANTHROPIC_API_KEY}"}}
    - scripted
  tools:
    - {name: prometheus, config: {url: "http://prometheus:9090"}}
    - {name: mcp, instance: "mcp:runbooks", config: {name: runbooks, url: "http://runbooks:8080/mcp", allow: {search: {risk: R0}}}}
  job: []                    # disable all background jobs
  profile_source:            # builtin demo seed + versioned profiles from git-managed files
    - builtin
    - {name: file, config: {path: /etc/harness/profiles}}
  route_policy: {name: weighted, config: {cost_usd: 100, failures: 20}}
  context_assembler: {name: sectioned, config: {instructions_file: /etc/harness/instructions.md}}
  compactor: {name: deterministic, config: {keep: 3}}
  event_sink:
    - {name: webhook, instance: slack-approvals, config: {url: "${SLACK_WEBHOOK_URL}", format: slack, types: ["approval.requested", "run.failed"]}}
    - {name: webhook, instance: siem, config: {url: "https://siem.internal/hooks/harness", secret: "${SIEM_HMAC}", start_from: earliest}}
settings:
  default_profile: sre-diagnosis   # used when POST /v1/runs names no profile
  default_profile_version: 1
models:
  circuit_failure_threshold: 3
  circuit_cooldown: 30s
```

A profile file (`*.yaml`/`*.yml`/`*.json`, one definition or a list) looks like `{id: ops-triage, version: 3, tenant?: t1, spec: {strategy: llm, tools: [prom.query], max_steps: 12}}`; definitions without a tenant go to the source's `tenants` (default `settings.default_tenant`). Webhook sinks send `X-Harness-Event`, `X-Harness-Event-Id` and, with a `secret`, `X-Harness-Signature: sha256=<HMAC>`.

**Profile enforcement.** A run is bound to the profile version it started with, and its frozen fields are enforced by the kernel, the store and the command API (one shared decision path, `profile.Rules`):

| Field | Enforcement |
|---|---|
| `tools` | Allowlist (names, `prefix.*`, `*`). The model only sees these tools; any other call is planned as a skipped step ("not allowed by profile"), and execution re-checks it. `[]` allows no tools; **omitting the field** keeps the legacy behaviour (every registered tool) and logs a warning at startup. |
| `risk_overrides` | `{tool: R0..R3}`, can only **raise** a tool's risk class (a lower value is ignored). The raised class is bound into the ActionIntent. |
| `approval_rules` | `{"min_approvals": {"R1": 1, "R2": 2, "k8s.rollout_restart": 2}}`: requires approval for allowed tools or raises the approval count; never relaxes a deny. Also applies to `replace_pending_input`. |
| `policy_version` | When set, tool calls are denied unless the active policy reports this version. |
| `max_steps` | Lowers the global `HARNESS_MAX_STEPS_PER_RUN` for runs of this profile. |
| `allowed_aliases` | `model_alias` must be one of them; if the deployment default is not allowed, the first allowed alias is used. |
| `compact_every` / `memory_top_k` | Override the worker / retriever defaults for this profile. |

Invalid values (unknown risk class, bad pattern, unknown `approval_rules` field, alias outside `allowed_aliases`) fail startup when the profile comes from a profile source.

**Writing a plugin**: external modules depend only on the public package `go-sre-harness/pkg/sdk` (type aliases + `Register` + every Kind), then build their own binary with `pkg/harness.Main()`. See `examples/plugins/hello` (a tool, a stricter policy `freeze_window`, an API route) and `examples/custom-harness`:

```go
func init() {
	sdk.Register(sdk.Tools, "hello", "greeting tool",
		func(ctx context.Context, h *sdk.Host, c sdk.Config) (sdk.ToolSource, error) {
			return sdk.StaticTools(&sdk.FuncTool{Desc: sdk.ToolDescriptor{Name: "example.hello", Risk: sdk.RiskR0}, Fn: ...}), nil
		})
}
```

```bash
HARNESS_CONFIG=examples/custom-harness/harness.yaml HARNESS_DATABASE_URL=... go run ./examples/custom-harness all
```

Through `Host`, a plugin can use the shared services of earlier phases (Store, Artifacts, Tools, Gateway, Embedder, Eval, Server, …) and share objects with other plugins via `Host.Provide/Lookup`. Instances implementing `Close()` / `Shutdown(ctx)` are closed in reverse order on exit; instances implementing `Run(ctx) error` are started with the process. External modules can declare their own extension points with `sdk.NewKind`.

## Quick start (single process)

Requires Go 1.26+ and PostgreSQL 14+; NATS is optional.

```bash
createdb harness
```

```bash
HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

Without a model key the gateway routes to the deterministic scripted model, so the full ledger, manifest and streaming path still runs. To use Claude:

```bash
ANTHROPIC_API_KEY=sk-ant-... HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

Run the demo in another terminal (create a run → wait for the R2 approval → approve → SSE events → conclusion):

```bash
scripts/demo.sh "remediate high latency service=checkout"
```

The default authentication mode is `dev` (trusts `X-Tenant-ID` / `X-Actor-ID`) and only listens on loopback addresses.

### Desktop client

[`desktop/`](desktop/README.md) is a native chat client in the style of Codex / Claude desktop (Wails: Go + the system WebView, ~9 MB). You chat with the agent in sessions. Each reply shows a collapsible work log: memory search, reasoning summaries, streamed model output, and tool calls with arguments and results. Risky tool calls show inline Approve / Reject cards, and a Stop button cancels the current reply. **Customize** manages the tenant's model, MCP servers, HTTP tools and skills (see below). Typing `/` in the composer opens a picker of skills and tools. **Approvals** collects pending approvals from every chat, and new ones also arrive as macOS notifications. Chats can be renamed, searched, archived, deleted and exported to Markdown. The client is a separate Go module and talks to the server only through the `/v1` API.

```bash
desktop/build.sh run
```

## Production deployment

### Authentication

Hash a token and put it in the token file (format: `deploy/tokens.example.json`):

```bash
go run ./cmd/harness hash-token 'my-secret-token'
```

```bash
HARNESS_AUTH_MODE=token,oidc HARNESS_AUTH_TOKENS_FILE=/etc/harness/tokens.json HARNESS_OIDC_ISSUER=https://idp.example.com HARNESS_OIDC_AUDIENCE=sre-harness go run ./cmd/harness api
```

`role_bindings` in the database are added on top of the roles from the token/JWT and are managed with `POST /v1/admin/role-bindings` (admin).

| Role | Allowed |
|---|---|
| viewer | every GET |
| operator | create runs/sessions/turns, pause/resume/cancel/inject, feedback, datasets and evaluations, propose evolution candidates |
| approver | approve/reject/resolve_unknown/override_result, activate memories, approve evolution candidates |
| admin | all of the above + migrate_profile, canary start/promote/rollback, service dependencies, roles and audit |

With `HARNESS_REQUIRE_DISTINCT_APPROVER=true`, a run's initiator cannot approve its own tool actions.

> If the local `harness` database role is a superuser, RLS has no effect. In production use a non-superuser role without `BYPASSRLS`.

### Tool configuration

Configure tools under `plugins.tools` in `HARNESS_CONFIG` (see above). The legacy `HARNESS_TOOLS_CONFIG` still works: it points to a JSON file (example: `deploy/tools.example.json`) that is converted into the matching tool plugins at startup. Risk classes, allowlists and target arguments are decided there by operators; MCP/remote self-descriptions never raise privileges. Kubernetes restarts default to `dry_run: true` and only change the cluster when explicitly set to `false`.

### Model (per tenant, at runtime)

By default runs use the models configured in the route policy (the built-in `scripted` provider in development). A tenant admin can set the tenant's own model under **Customize → Model**, or with `PUT /v1/extensions/models/{name}`:

| Provider | Spec |
|---|---|
| `anthropic` | `{"provider":"anthropic","model":"claude-opus-5-5","api_key":"…","effort":"high"}` (model defaults to `claude-opus-5-5`; effort `low`…`max`) |
| `openai` (compatible) | `{"provider":"openai","model":"…","base_url":"https://…/v1","api_key":"…","max_completion_tokens"?:true}`: OpenAI, DeepSeek, Qwen (DashScope), ERNIE (Qianfan v2), Zhipu GLM, Kimi, Doubao (Ark), vLLM/Ollama |
| `gemini` | `{"provider":"gemini","model":"gemini-flash-latest","api_key":"…"}` |

Any model may set `"default": true` to lead the tenant's fallback order. `POST /v1/extensions/models/{name}/list` returns the model IDs the key can use (blank `model` allowed). The desktop client manages models under **Settings → Models** with vendor presets.

The model becomes a gateway endpoint (`tenant:<tenant>:<name>`) entitled to that tenant only. It serves the `sre-reasoning` and `sre-fast` aliases ahead of the global endpoints and falls back to them when it fails. With the `llm` strategy the model chooses the tools itself, under the same profile, policy and approval rules. `api_key` is write-only. `POST /v1/extensions/models/{name}/test` sends a short request without saving, and `GET /v1/extensions` reports `current_model` (the endpoint and model that new runs resolve to). The `model` kind needs migration `0007_tenant_models.sql`.

### MCP servers, HTTP tools and skills (per tenant, at runtime)

Besides the tools compiled in through `plugins.tools`, tenant admins can add extensions at runtime: in the desktop client under **Customize**, or with the `/v1/extensions` API. They are stored per tenant in `tenant_extensions` (RLS), and every process reloads them within `HARNESS_EXTENSIONS_SYNC_INTERVAL`.

| Kind | What it is | Tool names |
|---|---|---|
| MCP server | a streamable-HTTP MCP server. "Connect & list tools" probes it, and only the tools the admin ticks are exposed, each with an operator-chosen risk class (R0–R3) and optional target argument. Local stdio servers are refused unless `HARNESS_MCP_ALLOW_STDIO=true`. | `mcp.<server>.<tool>` |
| HTTP tool | an endpoint that receives the call's JSON arguments (POST, or GET query for R0 tools) with `Idempotency-Key`, `X-Harness-Run-ID` and `X-Harness-Step-ID` headers | `http.<name>` |
| Skill | a playbook (Markdown, ≤16 KiB) rendered into the system prompt, before the trust rules; the prompt manifest records `name@hash`. `mode: "always"` (default) applies it to every run; `mode: "manual"` applies it only when the message (or an earlier one in the chat) mentions `/<name>` | — |

Runtime tools never bypass governance:
- the run's profile must allow them (the default `sre-diagnosis@2` allows `demo.*`, `mcp.*` and `http.*`; `@1` does not);
- policy and the R2/R3 approval floor apply as for any tool;
- MCP descriptions and outputs are marked untrusted (T4);
- a tenant tool never shadows a global tool;
- header and env values are write-only: the API returns them blank, and sending a blank value keeps the stored one, unless the destination (`url`, `base_url`, `provider`, `command`, `args`) changed, in which case the secrets must be re-entered (`422 secrets_required`);
- tenant extensions cannot connect to loopback, link-local or cloud metadata addresses (checked on the resolved address at connect time); `HARNESS_EXTENSIONS_ALLOW_NETWORKS` adds exceptions, metadata addresses stay blocked;
- every change is admin-only and audited.

To try it locally:

```bash
go run ./examples/mcp-demo
```

Start the harness with `HARNESS_EXTENSIONS_ALLOW_NETWORKS=127.0.0.0/8` so tenant extensions may reach it. This serves an MCP server at `http://127.0.0.1:8090/mcp` (runbook_search, oncall_lookup, page_oncall) and a `POST /deploys` endpoint. Add them under Customize, then ask in a chat, for example: `use mcp.demo.runbook_search {"query":"pool"}`.

### Multiple processes / containers

```bash
nats-server -js
```

```bash
export HARNESS_DATABASE_URL=postgres://localhost/harness HARNESS_NATS_URL=nats://127.0.0.1:4222
```

```bash
go run ./cmd/harness scheduler
```

```bash
HARNESS_WORKER_ID=w1 HARNESS_METRICS_ADDR=127.0.0.1:9091 go run ./cmd/harness worker
```

```bash
go run ./cmd/harness api
```

The `scheduler` role runs outbox publishing and the reconciler, plus the enabled jobs (memory consolidator, canary guardrail monitor, retention cleanup).

Docker Compose (Postgres + NATS + migrate + scheduler + 2 workers + api, token auth; create `deploy/tokens.json` first):

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Kubernetes manifests are in `deploy/k8s/harness.yaml` (external Postgres/NATS, Secret `harness`, a ServiceAccount with `get/patch deployments` only in allowlisted namespaces). The FS artifact store needs an RWX volume; for production, implement the same interface on S3/MinIO as an `artifact_store` plugin.

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `HARNESS_DATABASE_URL` | — | required |
| `HARNESS_CONFIG` | empty | plugin config file (YAML/JSON); when empty everything is derived from the variables below |
| `HARNESS_NATS_URL` | empty | empty = in-process broker (mode `all` only) |
| `HARNESS_AUTO_MIGRATE` | `true` | run migrations at startup (advisory lock, safe with replicas) |
| `HARNESS_HTTP_ADDR` | `127.0.0.1:8080` | |
| `HARNESS_METRICS_ADDR` | empty | `/metrics` listener for non-api roles |
| `HARNESS_ARTIFACT_DIR` | `./data/artifacts` | content-addressed file store |
| `HARNESS_DEFAULT_TENANT` | `tenant-a` | tenant of the `builtin` seed profiles `sre-diagnosis@1` and `@2` (the default; allows `mcp.*` / `http.*`) and of tenant-less profile definitions |
| `HARNESS_EVENT_WEBHOOK_URL` / `_FORMAT` / `_SECRET` | empty / `json` / empty | enables a `webhook` event sink (`format: slack` for chat) |
| `HARNESS_EVENT_TYPES` | empty (all) | comma-separated filter for that sink, e.g. `approval.*,run.failed` |
| `HARNESS_AUTH_MODE` | `dev` | comma-separated `token`, `oidc`, `dev`; `dev` on a non-loopback address needs `HARNESS_ALLOW_DEV_AUTH=true` |
| `HARNESS_AUTH_TOKENS_FILE` | — | static token file |
| `HARNESS_OIDC_ISSUER` / `_AUDIENCE` / `_JWKS_URL` / `_TENANT_CLAIM` / `_ROLES_CLAIM` | — / — / discovery / `tenant_id` / `roles` | OIDC |
| `HARNESS_REQUIRE_DISTINCT_APPROVER` | `false` | the approver cannot be the initiator |
| `HARNESS_TOOLS_CONFIG` | empty | legacy tool config file; empty = demo tools only |
| `HARNESS_STRATEGY` | `llm` | `rule` bypasses the model gateway entirely |
| `ANTHROPIC_API_KEY` / `HARNESS_ANTHROPIC_BASE_URL` / `HARNESS_ANTHROPIC_EFFORT` | — | Claude adapter |
| `HARNESS_OPENAI_BASE_URL` / `HARNESS_OPENAI_MODEL` / `HARNESS_OPENAI_API_KEY` | — | OpenAI-compatible adapter |
| `GEMINI_API_KEY` / `HARNESS_GEMINI_MODEL` / `HARNESS_GEMINI_BASE_URL` | — / `gemini-flash-latest` / public API | Gemini adapter (native `streamGenerateContent`) |
| `HARNESS_MODEL_ALIAS` / `HARNESS_MODEL_EFFORT` | `sre-reasoning` / empty | defaults when a profile does not set them |
| `HARNESS_MODEL_DAILY_BUDGET_USD` / `HARNESS_MODEL_MAX_INFLIGHT_PER_TENANT` | `0` (unlimited) / `32` | gateway admission |
| `HARNESS_MEMORY_RETRIEVAL` / `HARNESS_MEMORY_TOP_K` | `true` / `5` | retrieve memories before each run's first reasoning step |
| `HARNESS_CHECKPOINTS` | `true` | write a checkpoint after every successful step |
| `HARNESS_BRANCH_POLICY` / `HARNESS_MAX_PENDING_TURNS` | `single_writer_queue` / `8` | conversation concurrency policy |
| `HARNESS_MAX_RUNNING_PER_TENANT` | `0` (unlimited) | tenant fairness |
| `HARNESS_WORKER_ID` / `HARNESS_WORKER_MAX_INFLIGHT` | `<host>-<pid>` / `90` | |
| `HARNESS_LEASE_TTL` / `HARNESS_STEP_TIMEOUT` | `45s` / `2m` | heartbeat interval is TTL/4 |
| `HARNESS_MAX_STEPS_PER_RUN` / `HARNESS_APPROVAL_TTL` | `100` / `30m` | |
| `HARNESS_RECONCILE_INTERVAL` / `HARNESS_CONSOLIDATE_INTERVAL` / `HARNESS_ROLLOUT_MONITOR_INTERVAL` | `10s` / `10s` / `1m` | |
| `HARNESS_RETENTION_INTERVAL` / `HARNESS_OUTBOX_RETENTION` / `HARNESS_AUDIT_RETENTION` | `5m` / `24h` / `0` (forever) | |
| `HARNESS_MCP_ALLOW_STDIO` / `HARNESS_EXTENSIONS_SYNC_INTERVAL` | `false` / `5s` | allow tenant MCP servers started as local commands / reload period of tenant extensions |
| `HARNESS_EXTENSIONS_ALLOW_NETWORKS` | empty | comma-separated CIDRs or addresses tenant extensions may reach despite the default block (loopback, link-local); e.g. `127.0.0.0/8` for local trials |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | enables OTLP/HTTP tracing when set |

## API overview

| Method | Path | Description |
|---|---|---|
| POST | `/v1/runs` | requires `Idempotency-Key`; `{"goal": "...", "profile_id"?, "profile_version"?}`; without a version, resolved via canary/channel |
| GET | `/v1/runs?status=&limit=` | newest runs of the caller's tenant, with goal text (`limit` 1..200, default 50) |
| GET | `/v1/runs/{id}/steps?expand=io` | steps with their input and output artifacts inlined (tool args, observations, decisions; up to 256 KiB each) |
| GET | `/v1/runs/{id}` · `/steps` · `/approvals` · `/events` · `/events/stream` | SSE supports `Last-Event-ID`; `include_deltas=true` adds the token stream; a finished run's stream ends with a `stream.end` message (no id) |
| POST | `/v1/runs/{id}/commands` | `202` applied / `409` version conflict / `422` rejected (with reason) / `403` insufficient role |
| GET | `/v1/runs/{id}/model-requests` · `/cost` · `/stream-segments` · `/checkpoints` | model ledger and checkpoints |
| POST | `/v1/runs/{id}/fork` | derive a new run from a checkpoint |
| GET | `/v1/sessions?status=active\|archived&q=&limit=` | the tenant's sessions, most recently used first, with turn counts; `q` searches titles |
| PATCH | `/v1/sessions/{id}` | operator; `{"title"?, "status"?: "ACTIVE"\|"ARCHIVED"\|"DELETED"}` (deleted chats are hidden for good; their runs are kept) |
| GET | `/v1/approvals?limit=` | pending approvals across the tenant, with run, session and session title |
| POST/GET | `/v1/sessions…` | sessions, turns, branches, messages |
| POST/GET | `/v1/memories…` · `/v1/runs/{id}/feedback` · `/v1/incidents/{id}/memory-events` | memory lifecycle and feedback |
| POST/GET | `/v1/eval/datasets…` · `/v1/eval/runs` | dataset recording, freezing, offline evaluation |
| POST/GET | `/v1/evolution/candidates…` · `/v1/evolution/rollouts…` | propose → gated evaluation → approve → canary → promote/rollback |
| GET | `/v1/usage` · `/v1/whoami` · `/v1/admin/audit` · `/v1/admin/plugins` | |
| GET | `/v1/extensions` · `/v1/tools` | the tenant's MCP servers, HTTP tools and skills with load status (secrets blank); every tool a run can be offered, with `allowed_by_profile` |
| PUT/DELETE | `/v1/extensions/{models\|mcp-servers\|http-tools\|skills}/{name}` | admin; `{"spec": {...}, "enabled"?, "expected_version"?}` (`0` = create only) |
| POST | `/v1/extensions/mcp-servers/{name}/probe` | admin; connect and list the server's tools without saving |
| POST | `/v1/extensions/models/{name}/test` | admin; send a short request to the model without saving; `{reply, model, latency_ms}` or `502 model_failed` |
| GET | `/healthz` · `/metrics` | no authentication |

Evolution flow example:

```json
POST /v1/evolution/candidates  {"profile_id":"sre-diagnosis","base_version":1,"patch":{"effort":"high"},"rationale":"..."}
POST /v1/evolution/candidates/{id}/evaluate  {"dataset_id":"…"}
POST /v1/evolution/candidates/{id}/approve   (a different approver)
POST /v1/evolution/candidates/{id}/rollout   {"traffic_percent":10,"guardrails":{"min_runs":20,"max_failure_rate":0.2}}
POST /v1/evolution/rollouts/{id}/promote
```

## Tests

Each test uses its own schema; database tests are skipped when `HARNESS_TEST_DATABASE_URL` is not set.

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

The desktop client has its own tests (`cd desktop && go test ./...`).

Coverage highlights:
- **State layer**: idempotency, tenant isolation, concurrent duplicate deliveries yield a single lease, stale fences cannot commit, pause/resume/cancel, `NEEDS_HUMAN` recovery, KV, outbox mutual exclusion, v1.1 command version checks, tenant fairness.
- **End to end** (real worker/outbox/reconciler/API):
  - diagnosis and approval flow, SSE resume;
  - model gateway + Claude adapter (fake HTTP server), compaction/fork/override;
  - pinned memory retrieval, poisoning rejection, consolidation and feedback;
  - evolution gate, self-approval rejection, canary split and auto-rollback, promote;
  - authentication/RBAC/audit/separation of duties, `/metrics`, retention.
- **Plugin framework**: registration, strict config, duplicate instances, reverse-order shutdown, runner collection; YAML + `${VAR}` merging; a full run with the example plugins assembled from a config file, the policy floor and `/v1/admin/plugins` authorization.
- **Adapters**: MCP (in-memory transport), gRPC (bufconn), Prometheus and Kubernetes (fake HTTP servers), OIDC (real RSA-signed JWTs, forged signatures and algorithm confusion).

## Known limitations

- **GPU serving**: vLLM/SGLang routing and KV offload are not implemented.
- **Artifacts**: only the filesystem store exists. Artifacts are content-addressed and shared, so GC deletes only token-stream segment objects.
- **Offline evaluation**: each evaluation uses its own gateway with an in-memory ledger; its cost is reported but not charged to the tenant budget.
- **KV expiry**: expired entries are treated as absent on reads; physical cleanup (writing tombstone versions) is not implemented yet.
- **Python workers**: connect through the gRPC `ExecutorService` or MCP; the repository only ships a Go server-side example.
- **`ErrNotDispatched`**: when a tool rejects a call before dispatch (invalid arguments, namespace not allowlisted), the step fails permanently and the run fails; it does not go to `NEEDS_HUMAN`.
- **The in-process broker** drops messages when full (the reconciler recovers them); it is meant for single-process mode and tests only.
- **Plugin config** is read at startup; changes need a restart.
