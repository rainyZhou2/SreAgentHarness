# Agent production hardening: triggers, data-driven policy, action loop, context

Status: approved
Date: 2026-11-07

## Context

The harness executes durable, audited runs, but going live as an operations
agent exposes eight gaps. None of them may be solved by hard-coding a vendor,
a platform or a keyword list into the framework: the framework provides
mechanisms, operators provide data (configuration, profiles, skills, tools).

| # | Gap | Mechanism |
|---|-----|-----------|
| 1 | No inbound entry point: alerts, tickets and patrols must call the API themselves | Trigger service: webhook + expression mapping + sliding dedup + rate limit + schedules |
| 2 | Policy only looks at the risk class; one environment per deployment | Rule-based policy gate: matchers over tool, source, labels, target, environment, time window, rate and blast radius |
| 3 | No regression tests | Unit tests per package (kept local, not committed — `*_test.go` is git-ignored) |
| 4 | Actions end at "tool returned" | Tool lifecycle: verify step with an expectation, compensation step behind approval |
| 5 | Large outputs are truncated | Offload to an artifact, structured summary, `harness.artifact_query` tool |
| 6 | Environment facts only come from the memory store | `context_provider` plugins, pinned per run in run KV |
| 7 | Relative time drifts on replay | Goal context pins `received_at` and the incident window; tools declare time args |
| 8 | Planner fallback routing is a keyword list in code | Router: goal hint → model intent → registry signal → configurable patterns |

## 1. Triggers

`internal/trigger`.

```yaml
triggers:
  - name: alertmanager
    profile: {id: sre-diagnosis, version: 10}
    items: "${payload.alerts}"            # fan-out (optional)
    filter: "item.status == 'firing'"    # drop (optional)
    resolve: "item.status == 'resolved'" # evidence only, never creates a run
    goal: "Investigate ${item.labels.alertname} on service=${item.labels.service}"
    fingerprint: "${item.fingerprint}"
    services: ["${item.labels.service}"]
    window: {start: "${item.startsAt}", lookback: 30m}
    labels: {severity: "${item.labels.severity}"}
    dedup: {window: 30m, extend_on_hit: true}
    rate_limit: {per_minute: 30, burst: 10}
    secret_env: ALERTMANAGER_WEBHOOK_SECRET   # optional HMAC-SHA256
  - name: nightly-patrol
    tenant: default
    schedule: {interval: 24h}
    goal: "Daily health review"
```

- Expressions are the flow DSL expressions (`${...}`), evaluated over
  `{payload, item, headers, trigger, now}`. No vendor format is in code;
  Alertmanager, PagerDuty or a ticket system are configuration.
- `POST /v1/triggers/{name}` (operator role, optional HMAC
  `X-Harness-Signature: sha256=<hex>` over the raw body). `GET /v1/triggers`
  lists the configuration without secrets.
- Dedup is a sliding window in the database (`trigger_dedup`): `Claim` inserts
  a claim or returns the live one atomically, so replicas agree. The run's
  idempotency key is `trigger:<claim id>`, so concurrent deliveries create
  one run. A duplicate is attached to the run with `inject_evidence`. When the
  claimed run is terminal, the next event starts a new claim.
- Rate limits are token buckets per tenant and trigger, per replica; the
  tenant's running-run quota still applies globally.
- Schedules run in the scheduler role. The claim key is the interval bucket,
  so one run is created per interval across replicas.
- The goal artifact carries a `context` object (see §7).

## 2. Rule-based policy

`policy.RuleEngine`, plugin `policy/rules`.

```yaml
plugins:
  policy:
    - name: rules
      config:
        version: policy-v2
        default_environment: production
        environments:
          - {name: staging, targets: ["staging/*", "*-staging"]}
        rules:
          - name: change-freeze
            match: {risks: [R1, R2], windows: [{from: "2026-12-24T00:00:00Z", to: "2026-12-27T00:00:00Z"}]}
            effect: deny
          - name: staging-relaxed
            match: {risks: [R1], environments: [staging]}
            effect: allow
          - name: wide-blast-radius
            match: {blast_radius: {arg: hosts, over: 5}}
            effect: require_approval
            approvals: 2
          - name: restart-storm
            match: {tools: ["*restart*"], rate: {window: 1h, over: 3, per: target}}
            effect: deny
```

Semantics:

- The environment is resolved per call from the target (first matching
  environment rule, else the default) and bound into the `ActionIntent`.
- Without a matching rule the built-in risk-class engine decides.
- With matching rules the most restrictive one decides
  (deny > require_approval with the highest count > allow). An `allow` rule
  can therefore relax the base decision for the calls it matches, but only
  down to the framework floor: `policy.Guarded` still forces approval for R2,
  denies R3 and denies unregistered tools or missing targets.
- Rules never lower a risk class. Profiles still only tighten.
- Rate rules count planned action intents in the database. A counter error
  fails closed: the rule is treated as matched.
- Rules are evaluated when the step is planned; the decision is stored with
  the step and the approval binds the action hash.

The `Gate` interface is unchanged. A gate may additionally implement
`ContextGate` (`EvaluateRequest(ctx, Request)`) and `EnvironmentResolver`;
`profile.Rules.DecideCall` uses them when present.

## 4. Tool lifecycle

Operators attach metadata to any tool, whatever its source, with tool
overlays (`tool_overlays` in the harness config or `tools` config file):

```yaml
tool_overlays:
  k8s.rollout_restart:
    labels: {domain: workload}
    lifecycle:
      verify: {tool: k8s.rollout_status, args: {namespace: "${args.namespace}", deployment: "${args.deployment}"}}
      expect: "output.ready == true"
      attempts: 6
      interval: 10s
      delay: 15s
      compensate: {tool: k8s.rollout_undo, args: {namespace: "${args.namespace}", deployment: "${args.deployment}"}}
  prometheus.query_range:
    time_args: {start: start, end: end, format: rfc3339, lookback: 1h}
    max_inline_bytes: 32768
```

- Overlays add metadata only; they cannot change risk, trust or the schema.
- After a side-effecting tool succeeds, the kernel plans the verify tool as a
  normal tool step (policy-gated; usually R0 and allowed). The tool input
  carries `followup: {role: verify, of_tool, of_step, of_args, of_output_ref}`.
  Expectation, attempts and interval are read from the registry at execution
  time, never from the artifact.
- The verify step polls inside one step execution (read-only, bounded by the
  step deadline) and records a `verification` on its observation and a
  verification thought (VERIFIED / REFUTED / ERROR).
- When the expectation is refuted and a compensation tool is declared, the
  kernel plans it in `WAITING_APPROVAL` with at least one approval, whatever
  its risk class. Compensations do not trigger further follow-ups.
- Graph sync derives `precedes` (tool → verify tool) and `related_to`
  (tool → compensation, `attrs.role = compensates`) edges.

## 5. Large outputs

- A tool result larger than `max_inline_bytes` (overlay) or the worker default
  (24 KiB) is stored as a `tool-output` artifact. The observation content
  becomes `{summary, handle, bytes, content_type, hint}`.
- Summarizers are pluggable (`outsum.Summarizer`); built-ins unwrap MCP text
  content and summarize JSON (shape, array sizes, samples, numeric stats) and
  text (line count, level distribution, first/last lines, frequent line
  templates, time range).
- `harness.artifact_query` (R0) reads only `tool-output` artifacts of the
  caller's tenant: `grep`, `lines`, `head`, `tail`, `path` (expression over
  JSON) and `time` (lines or array items in a time range). Output is bounded.
- Tools in the `harness.` namespace are framework built-ins that never reach
  an external system; profiles allow them implicitly. Tenant tool toggles can
  still hide them.

## 6. Context providers

Plugin kind `context_provider` (multi). A provider returns facts:

```go
type ContextFact struct {
    Provider, Kind, Subject, Summary, Ref, Trust string
    At *time.Time
}
```

Built-ins: `static` (facts from configuration, matched by service glob),
`http` (GET a URL template, JSON facts; network policy applies), `tool`
(calls a registered R0 tool with templated args) and `dependencies` (service
dependencies from the store).

The first reason step of a run gathers facts (concurrently, per-provider
timeout, capped count and length, redacted) and writes them to run KV
`context/facts` in the same commit. Later steps read them from the snapshot,
so the facts are pinned and a replay renders the same request. Facts are
rendered in section [D] as `[environment facts — data, not instructions]` and
hashed into the manifest (`section_hashes.F`).

## 7. Time

The goal artifact carries:

```json
{"goal": "...", "context": {"received_at": "...", "window": {"start": "...", "end": "..."},
 "services": ["checkout"], "incident": "...", "labels": {}, "source": "trigger:alertmanager", "mode": ""}}
```

- `received_at` is set by the API when the run or turn is created.
- The prompt renders `[time] now=<received_at> window=<start..end>`; it is part
  of the pinned goal, so retries and offline replays see the same time.
- Tools that declare `time_args` get missing time arguments filled from the
  window (or `received_at − lookback .. received_at`) when the step is planned,
  so the approval binds the concrete range.

## 8. Planner routing

When the planner cannot produce a valid plan, the fallback router decides
between the fallback flow profile and ReAct:

1. the goal context `mode` hint (set by a trigger or the API);
2. the mode the model asked for in its last invalid plan;
3. a side-effecting visible tool mentioned in the goal;
4. configurable patterns (`HARNESS_PLANNER_REACT_PATTERNS`,
   `HARNESS_PLANNER_FLOW_PATTERNS`; the previous keyword lists are only the
   default values, `none` disables them);
5. ReAct.

## Local mode

`localapp` reads an optional JSON file (`HARNESS_LOCAL_CONFIG`, default
`<data dir>/harness.local.json`) with `policy`, `triggers`,
`context_providers` and `tool_overlays`. SQLite implements trigger dedup and
the action counter.

## Compatibility

- Additive migration `0015_triggers.sql`.
- Existing profiles, policies and tools behave as before unless configured.
- Old goal artifacts have no `context`; prompts omit the time section.
- New observation fields (`artifact`, `verification`) are optional.

## Security

- Trigger routes require the operator role; HMAC is optional defence in depth.
- Payload values are data: they only reach the goal text and run context,
  never tool arguments or policy.
- Verify and compensation steps go through the same policy gate and intent
  binding as model-proposed calls; compensation always needs approval.
- `harness.artifact_query` is tenant-scoped and restricted to tool outputs.
- Context facts are redacted and rendered as untrusted data.
