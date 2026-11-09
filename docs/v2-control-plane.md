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
| `HARNESS_TRIGGER_MIRROR` | = shadow mode | copy v1 webhook trigger events into the inbox for `GET /v1/events/shadow-report` |
| `HARNESS_MODEL_MAX_DATA_CLASS` | `confidential` | most sensitive data class sent to the model provider; more sensitive evidence is redacted from run goals |
| `HARNESS_EVENT_PAYLOAD_RETENTION` | `336h` | inbox payloads are purged after this (`0` keeps them) |
| `HARNESS_EVENT_RETENTION` | `2160h` | inbox records are purged after this (`0` keeps them) |
| `HARNESS_CONTROLPLANE_DB` | `<data dir>/controlplane.db` | desktop runtime: SQLite file of the control plane |

Templates and correlation rules come from the `work:` section of
`HARNESS_CONFIG` (`harness.local.json` on the desktop); `work_extension`
plugins under `plugins:` add action executors, metric sources, source
normalizers and background feeders. `${VAR}` references in the config file
are expanded from the environment, except unset lower-case names such as
`${subject}` or `${title}`, which are template placeholders and are kept.

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
| `internal/workeval` | per-Work scorecard and safety gates |
| `internal/postmortem` | postmortem report built from the ledger (timeline, grounded root cause, actions, gaps) |
| `internal/workbench` | deterministic Work benchmark: cases, simulator, record and replay |
| `pkg/workclient` | typed Go client |
| `pkg/sdk` | public plugin API (aliases and `Register`) for plugins outside this module |
| `plugins/sre` | SRE domain pack: Prometheus/Alertmanager, Kubernetes, OTel, GitOps/Argo CD, Istio traffic, CMDB, Jira, AI infra |
| `plugins/srework` | `sre-sim` simulator (deployments, pods and topology, metrics, rollback/traffic actions, optional persistent state) |

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

Several rules can feed one correlation **group** (`group:`), so a deploy
event and the alert it causes land in the same Work: an `attach` rule for
`change.deployed` and a `create` rule for `alert.firing` share the group, and
`lookback:` on the creating rule attaches the group's events from before the
Work existed. Plugins that implement `SourceNormalizers` accept
source-native payloads (Alertmanager, Kubernetes events, Argo CD) at
`POST /v1/sources/{source}/events`.

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
feeds freshness. Two values from one source at the same instant stay visible
as a conflict (actions on a conflicting version fact are refused); the next
later observation from that source closes every open value of the series,
so the result never depends on row order. Topology edges (`deployed_as`,
`owns`, `runs_on`, ...) are relations with the same bitemporal columns;
`POST /v1/world/snapshots/query` with `depth` walks them. API writers are stored as trust tier T4 unless admin;
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

## Evaluation, postmortems and learning

- `GET /v1/works/{id}/evaluation` is the Work's **scorecard**: time to
  diagnosis and mitigation, runs, tool calls, actions by state, outcome
  verification, and the safety **gates** that fail it regardless of the
  outcome: `rule_violation`, `repeated_unknown_effect`, `blast_radius`,
  `ungrounded_conclusion` (a conclusion supported only by T4 evidence) and
  `success_without_verified_outcome`. While a Work is open the scorecard is
  provisional.
- `GET /v1/works/{id}/postmortem` builds a report from the ledger: a
  collapsed timeline (repeated signals as `×N until`), the root cause only
  from SUPPORTED hypotheses with T1–T3 evidence (otherwise "unknown"),
  refuted and open hypotheses, actions with their effects, the outcome, the
  scorecard, gaps and follow-ups. `?format=markdown` (or `Accept:
  text/markdown`) renders it. `POST` on a closed Work creates a `postmortem`
  review Work (idempotent per source Work).
- Built-in templates `postmortem` (draft run + human review) and `follow-up`
  (plan a ticket, file it with `ticket.create`, wait up to 30 days for the
  ticket to reach Done/Closed/Resolved/Fixed, confirm).
- `POST /v1/works/{id}/playbook` proposes a procedural memory from a
  verified Work. Memories carry **applicability** (environments, services,
  versions, validity window) and are recalled only where they apply; a
  playbook becomes ACTIVE only after at least one machine-verified success,
  with no failure majority, approved by someone other than its author.
  Outcome counters are moved by Work evaluations only, never by a model.

## Security

| Role | Grants |
|---|---|
| `viewer` | read Works, events, world, actions |
| `operator` | create Works, commands, evidence, facts (T4), reconcile; implies viewer and service |
| `approver` | approve/reject actions, confirm/reopen, resolve unknown effects |
| `admin` | everything above, inline templates, trusted facts |
| `service` | submit events only, within its scope |
| `break_glass` | emergency release of a held action with a justification; never implied, never lifts R3 |

A token (or OIDC principal) may carry a **scope**: `event_sources`,
`event_types`, `resources` (prefix patterns such as `deployment://prod/*`)
and `max_risk` (the highest risk it may approve). Out-of-scope requests are
refused with 403 and written to the security audit log, as are approvals and
break-glass releases. R3 actions are never allowed; R2 always needs an
approval bound to the intent hash.

## Work bench

`harness work-bench` replays incidents deterministically against the real
orchestrator, action runtime and evaluator on a simulated clock (SQLite,
no model, no network):

```bash
harness work-bench run bench/cases [-baseline base.json] [-out result.json] [-write-baseline base.json]
harness work-bench record -tenant <t> -work <id> [-out case.json]   # export a real Work as a case (HARNESS_DATABASE_URL)
harness work-bench replay -tenant <t> -work <id> [-out result.json] # record, replay and diff
```

A case (`bench/cases/*.json`) scripts runs (stage, role, result, duration),
metric series, simulated actions (`applied`, `not_applied`, `unknown`, with
facts and metric effects), approvals and human steps, and states
expectations (status, pass, gates, run/action limits, applied and forbidden
actions, Work count, root cause, duration). `-baseline` fails when a case
regresses. Unknown effects are never replayed: they reconcile or block,
exactly as in production.

## Desktop

The desktop client has a **Works** view (both local and team-server mode):
the Work list, its status and outcome with verdict history, the scorecard,
action cards (approve/reject with the intent hash, inspect again, mark
applied or not applied), hypotheses with evidence, a merged timeline, and
controls for confirm, resolve, pause/resume, check now, cancel, reopen and
postmortems.

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

- Works: `harness_work_active`, `harness_work_transitions_total`,
  `harness_work_ticks_total`, `harness_work_stage_total`,
  `harness_work_replans_total`, `harness_work_age_seconds`,
  `harness_work_waiting`, `harness_work_waiting_oldest_seconds`,
  `harness_work_wake_lag_seconds`, `harness_work_model_cost_usd`.
- Actions: `harness_action_state_total`, `harness_action_preflight_total`,
  `harness_action_effect_unknown_total`, `harness_action_backlog`,
  `harness_action_backlog_oldest_seconds`, `harness_action_verify_seconds`,
  `harness_action_break_glass_total`, `harness_action_unapproved_dispatch`,
  `harness_action_blocked_by_kernel_effect_total`.
- Events and watches: `harness_event_ingested_total`,
  `harness_event_correlation_seconds`, `harness_event_inbox_backlog`,
  `harness_event_inbox_oldest_seconds`, `harness_event_inbox_purged_total`,
  `harness_watch_fired_total`, `harness_watch_due_lag_seconds`,
  `harness_watch_overdue`, `harness_watch_overdue_oldest_seconds`.
- Outcomes and world: `harness_outcome_evaluations_total`,
  `harness_world_snapshot_seconds`, `harness_world_fact_staleness_seconds`,
  `harness_agent_assignment_total`.

Labels are bounded enums; tenant, Work id and subjects are in logs only.
`deploy/observability/harness-dashboard.json` (Grafana) and
`harness-alerts.yaml` (Prometheus rules: approval invariant broken, unknown
effects, break-glass use, unauthorized or cross-tenant requests, model data
boundary refusals, stalled watches, wake lag, inbox backlog and dead
letters, waiting approvals, blocked Works, tick errors, replan rate, slow
snapshots) are ready to import.

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
scripts/v2/demo.sh      # offline: harness-local (SQLite) with the simulator drives one incident
HARNESS_PG_ADMIN_URL=postgres://postgres@127.0.0.1:55432/postgres scripts/v2/demo-pg.sh
harness work-bench run bench/cases
go test ./internal/store/cpsql/ ./internal/orchestration/ ./internal/workapi/ ./internal/workbench/ ./plugins/srework/
HARNESS_PG_TEST_URL=postgres://postgres@127.0.0.1:55432/harness_test?sslmode=disable go test ./...
```

`demo-pg.sh` runs `harness all` on a fresh PostgreSQL database with the
`sre-sim` extension (`scripts/v2/harness.pg.json`) and fails unless:

1. a deploy event and the repeating alert end up in **one** incident Work
   (shared correlation group with lookback);
2. a re-sent event is a duplicate, a second approval is refused (409), and a
   re-sent postmortem request returns the same Work;
3. after `kill -9` right after the approval and again during verification,
   the restarted harness finishes the Work (SUCCEEDED, outcome SATISFIED)
   and the downstream applied the rollback **exactly once** (the simulator
   keeps its state in `state_file`, as a real cluster outlives the control
   plane);
4. the world holds service → deployment → 3 pods → nodes, with the pods
   replaced by the rollout.

The PostgreSQL test database should be UTF-8 (`CREATE DATABASE … ENCODING
'UTF8'`): on a `SQL_ASCII` database full-text search drops CJK text and the
CJK memory test fails.

`scripts/v2/harness.local.json` also contains `investigate-and-rollback`, which
adds a model-driven investigation (needs a model provider) whose proposal
feeds the action stage.
