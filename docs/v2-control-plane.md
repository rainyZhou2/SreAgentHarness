# v2 control plane: Works, events, world state, actions, outcomes

The v2 control plane turns the run-centric harness into a continuous agent: a
**Work** is a durable goal (an incident, a change, a routine check) that lives
across many runs, waits for events and timers, executes verified actions and
ends only when its outcome is checked. It is a modular-monolith layer on top
of the Durable Step Engine; the kernel is unchanged and never imports it.

Everything is off by default:

| Variable | Default | Effect |
|---|---|---|
| `HARNESS_WORK_ENGINE_ENABLED` | `false` | Work API, orchestrator, watch reconciler, event ingestion |
| `HARNESS_EVENT_SHADOW_MODE` | `true` | events are stored, deduplicated, routed and fire watches, but rules do not create Works; the decision is recorded on the inbox record |
| `HARNESS_ACTION_V2_ENABLED` | `false` | action runtime; without it action stages block with a clear reason and `/v1/actions/*` writes return 503 |
| `HARNESS_ACTION_APPROVAL_TTL` | `30m` | validity of an action plan and its approval |
| `HARNESS_ACTION_REQUIRE_KNOWN_TARGET` | `false` | reject targets the world state cannot resolve uniquely |
| `HARNESS_REQUIRE_DISTINCT_APPROVER` | `false` | the Work owner cannot approve its own actions |
| `HARNESS_WORK_INLINE_TEMPLATES` | `false` | admins may submit template bodies instead of catalog ids |
| `HARNESS_CONTROLPLANE_DB` | `<data dir>/controlplane.db` | desktop runtime: SQLite file of the control plane |

Rollout: enable the engine in shadow mode, compare `GET /v1/events/{id}`
decisions with what the v1 triggers did, then turn shadow mode off; enable
actions last.

## Packages

| Package | Responsibility |
|---|---|
| `internal/work` | Work aggregate, state machine, templates, store port. No SQL, no HTTP. |
| `internal/eventing` | event envelope, whitelisted predicate AST, correlation rules |
| `internal/continuous` | durable watches (event, time, either, periodic, approval) |
| `internal/world` | bitemporal entities/facts/relations, snapshots, diff, identity |
| `internal/action` | action plans, ledger-first execution, approvals, reconcile, compensation |
| `internal/outcome` | success specs and the evaluator (metric/state/event/human) |
| `internal/evidence` | evidence records, hypotheses, multi-agent merge without voting |
| `internal/store/cpsql` | one `database/sql` implementation for PostgreSQL and SQLite |
| `internal/orchestration` | orchestrator, ingestor, reconciler, command service, metrics |
| `internal/workapi` | HTTP extension (`/v1/works`, `/v1/events`, `/v1/world`, `/v1/actions`) |
| `internal/workengine` | assembly behind the flags, shared by server and desktop runtime |
| `pkg/workclient` | typed Go client |
| `plugins/srework` | `sre-sim` simulator (deployments, metrics, rollback/traffic actions) |

## Lifecycle

```
NEW → ACTIVE ⇄ WAITING → VERIFYING → SUCCEEDED
        │  ↘ BLOCKED (human resolves)      ↘ ACTIVE (verify failed → replan)
        └→ SUSPENDED (pause/resume)   any → CANCELLED / FAILED
terminal → ACTIVE only through `reopen` (new generation)
```

- `SUCCEEDED` is reachable only from `VERIFYING`, and the store requires a
  `SATISFIED` outcome evaluation in the same transaction. A model cannot
  declare success: evaluations come from metric/state/event sources or a
  human with the approver role.
- Every change is one `ApplyWork` transaction: version CAS, lease fence,
  validated transition, gap-free timeline event, links, watches, outcome.
- A Work pins its template at creation. Stages: `run`, `agents` (parallel
  runs merged without voting), `wait` (watch), `action`, `verify`,
  `confirm`. `on_failure` is `block` (default), `fail`, `skip` or an earlier
  stage id to replan from (new generation, bounded by `max_replans`).
- A `verify` stage without a success spec (template or goal) falls back to a
  human confirmation. After the last stage, a Work without a satisfied
  outcome of its current generation waits for one.
- Limits per template: `max_runs` (30), `max_replans` (4), `max_duration`
  (48h), `max_agents` (4), `max_actions` (5); `goal.constraints.max_cost_usd`
  blocks the Work when exceeded.

## Events, correlation and watches

`POST /v1/events` (or a plugin's `WorkEnv.Ingest`) stores the envelope in the
inbox; `(tenant, source, event_id)` is unique, so redeliveries are answered
with the stored record. Processing then

1. fires every ACTIVE event watch whose predicate matches (firing id derived
   from the inbox id, so re-processing never fires twice);
2. routes through the first matching correlation rule: `create` opens a Work
   unless the rule's group (`correlation_id`, `dedup_key`, `subject` or
   `label:<name>`) already has an open one within the window, `attach` only
   attaches, `ignore` drops.

Correlation is one transaction over `work_correlations`, so concurrent
deliveries of one group converge on one Work. Failed records are retried by
the sweep and become `DEAD` after 5 attempts (kept for repair).

Timers are rows, not goroutines: the reconciler fires due checks with a
deterministic firing id (`timer:<watch>:<check>`), and `FireWatch` locks the
Work and the watch and inserts the firing under a unique key — an event and a
timeout racing for one watch wake the Work once.

## World state

Facts are bitemporal: `valid_from/valid_to` is when it was true, `observed_at`
when we learned it, `closed_observed_at` when we learned it stopped. A query
"as of T, as known at K" reconstructs exactly what was believable then, which
is what actions and post-incident reviews pin (`world_snapshots`, with a
digest). Re-observing an unchanged value refreshes `confirmed_at`, which only
feeds freshness. API writers are stored as trust tier T4 unless admin;
snapshots prefer higher trust, then recency, and flag disagreements.

## Actions

An action is built by the server from the template (or from a run's proposal,
filtered through the stage's `allowed` list), never submitted by a model.

1. **Propose**: resolve the target, check it is within the Work subjects,
   evaluate the guarded policy (R3 denied, R2 always needs approval), read a
   persisted world snapshot, require fresh required facts, pin the target
   version, run the executor's read-only preflight, seal the plan
   (`plan_hash`, `intent_hash` over every field an approval relies on,
   `effect_key`).
2. **Approve**: `POST /v1/actions/{id}/approve` with the `intent_hash` the
   approver saw; any other hash is refused (422).
3. **Execute**: re-check expiry, policy and target version, take the
   per-target lock, commit `DISPATCHED` *before* calling the executor with
   the effect key. A lost result leaves the action `DISPATCHED`/
   `EFFECT_UNKNOWN`; it is reconciled with the executor's read-only
   `Inspect` or resolved by a human, and never dispatched again
   automatically.
4. **Verify**: the plan's effect spec is evaluated until it holds or times
   out (`VERIFIED` / `DEGRADED`); compensation is proposed as a new action
   and goes through the same gates.

The Work-level ledger is `work_actions`; kernel tool effects stay in
`external_tool_effects`. The two never share keys.

## Outcomes

A success spec is a versioned set of objectives (`all_of`, optional `any_of`,
`stable_for`). Metric objectives query a named, read-only metric source
(plugins register them) over a window with an aggregate and a threshold;
state objectives read the world; event objectives count (or require the
absence of) events; human objectives wait for a confirmation. Missing,
stale or too few samples are `INCONCLUSIVE`, never `SATISFIED`. The
stabilization clock (`satisfied_since`) is bound to the spec hash and resets
on any regression.

## API

See the package comment of `internal/workapi` for the full route list.
Mutating routes need `Idempotency-Key`; Work commands accept `If-Match`
(the Work version, also returned as `ETag`) and answer 412 on a stale
version. Errors: 400 invalid, 403 role, 404 not found (also for other
tenants' objects), 409 conflict/transition, 412 precondition, 422
idempotency or approval mismatch, 503 disabled. `GET
/v1/works/{id}/events/stream` is an SSE stream of the durable timeline,
resumable with `Last-Event-ID`.

## Metrics

`harness_work_active{kind,status}`, `harness_work_transitions_total{kind,to}`,
`harness_work_ticks_total{result}`, `harness_work_stage_total{type,result}`,
`harness_action_state_total{state}`, `harness_outcome_evaluations_total{verdict}`,
`harness_event_ingested_total{decision}`, `harness_watch_fired_total{kind}`.
Labels are bounded enums; tenant, Work id and subjects are in logs only.

## Deviations from the RFC (decisions)

- `work_run_links` has no foreign key to `agent_runs`: the desktop runtime
  keeps runs in a different database file, and the kernel must stay
  deletable/retainable independently. Run ownership is enforced by a partial
  unique index (`uq_work_run_single_owner`).
- The run → Work bridge polls linked run status from the orchestrator instead
  of adding a kernel hook, so the kernel is untouched.
- The control plane runs on PostgreSQL through `database/sql`; RLS policies
  exist on all v2 tables, and every query is explicitly tenant-scoped (the
  pooled `database/sql` connections do not carry the per-request tenant
  setting, so RLS acts as a backstop with an empty setting).
- Local mode uses a separate SQLite file with the same schema (mirrors under
  `internal/store/cpsql/sqlite`), and the same contract tests run on both.

## Demo and tests

```bash
scripts/v2/demo.sh   # offline: builds harness-local with the simulator and drives one incident
go test ./internal/store/cpsql/ ./internal/orchestration/ ./internal/workapi/ ./plugins/srework/
HARNESS_PG_TEST_URL=postgres://postgres@127.0.0.1:55432/harness_test?sslmode=disable go test ./internal/store/cpsql/
```

`scripts/v2/harness.local.json` also contains `investigate-and-rollback`, which
adds a model-driven investigation (needs a model provider) whose proposal
feeds the action stage.
