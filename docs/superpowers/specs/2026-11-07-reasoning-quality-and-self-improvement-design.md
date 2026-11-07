# Measurable, reusable, self-improving reasoning

Date: 2026-11-07
Status: implemented

## Goal

Make the agent's explicit decision trace (plan, hypotheses with confidence,
expected observation, verification, reflection) something the harness can
**measure**, **reuse** and **improve**, without hard-coding any domain and
without letting the agent change its own safety envelope.

The trace is the structured output the model is asked for (`ThoughtChain`,
`Hypotheses`, `submit_conclusion.verification`). Provider-hidden reasoning is
never read or stored.

## 1. Reasoning-quality metrics (`internal/reasoning`)

`Score(Trace) Metrics` is a pure function over a run's steps and thoughts.
The same function scores live runs (from stored thoughts) and offline
replays (from replayed decisions), so the numbers are comparable.

| Metric | Definition |
|---|---|
| `hypotheses`, `verified`, `refuted`, `unverified` | distinct hypotheses (normalized statement) with their final status |
| `brier` | mean (confidence − outcome)² over hypotheses that ended VERIFIED (1) or REFUTED (0) and carried a confidence; lower is better |
| `grounding` | share of decision and hypothesis thoughts that cite evidence or memory refs |
| `redundant_calls` / `redundancy` | repeated tool calls with identical canonical arguments |
| `steps_to_verdict` | reasoning steps until the first hypothesis is verified or refuted |
| `conclusion` | the final verification verdict (VERIFIED / REFUTED / UNVERIFIED) |
| `correct` | operator feedback on the root cause, when given |
| `quality` | weighted composite in [0,1] (grounding 0.3, calibration 0.3, verified conclusion 0.2, efficiency 0.2); calibration is neutral (0.5) when there is no data |

`Summarize([]Metrics) Summary` aggregates means and rates and adds a Wilson
lower bound for the verified-conclusion rate.

### Gate

`eval.Metrics` gains `reasoning` (a `Summary`). The release gate fails a
candidate when, relative to the base on the holdout split:

* Brier rises by more than `max_brier_increase` (default 0.05),
* grounding falls by more than `max_grounding_drop` (0.10),
* redundancy rises by more than `max_redundancy_increase` (0.10),
* composite quality falls by more than `max_quality_drop` (0.05).

Answer accuracy and safety checks are unchanged; reasoning checks are added,
never substituted.

### Live scoring

The consolidator already processes every terminal run once and again when
feedback arrives. It now calls run hooks; the reasoning recorder hook loads
the run's thoughts, tool calls and memory hits, scores the run and upserts a
`reasoning_scores` row (migration 0016; SQLite kind `reasoningscore`). Hook
failures are logged and never block consolidation.

## 2. Reasoning-pattern library

A *reasoning pattern* is a procedural memory (`memory_type=procedural`,
subject `<service>:reasoning_pattern`) distilled from a run that:

* completed, was not disputed by feedback,
* concluded with a VERIFIED verification,
* settled at least one hypothesis (verified or refuted).

Content (redacted and poison-checked by the memory writer):

```
Reasoning pattern for <service> (source run <id>)
Situation: <goal>
Hypotheses checked, in order:
1. <statement> → REFUTED (confidence 0.40)
2. <statement> → VERIFIED (confidence 0.85)
Tools in order until the first verdict: <tool>, <tool>
Conclusion check: <verification summary>
```

Patterns reuse the memory lifecycle: they start as CANDIDATE (VALIDATING when
feedback confirmed the root cause) and are retrieved only after an approver
activates them. Retrieval, shards, conflict resolution and the "past
experience, verify before use" framing are the existing memory machinery.

**Effectiveness.** Every retrieval is already pinned as a step memory hit.
The recorder stores the memory ids each run used, so `PatternStats` gives
every memory (patterns and any other memory) uses, successes and a Wilson
lower bound. Success = completed and (feedback correct, or, without feedback,
a VERIFIED conclusion).

**Exclusion.** A profile's `reasoning.patterns_excluded` hides memories from
retrieval for runs of that profile version. Because it is a profile field,
retiring a pattern is a gated, versioned, reversible change.

## 3. Bounded self-improvement (`internal/reasoning/improve`)

A scheduler job (`job: reasoning_improver`) runs per tenant and profile:

1. Skip if this job already has a candidate in flight for the profile
   (PROPOSED, EVALUATING, PENDING_APPROVAL, APPROVED): one change at a time.
2. Load reasoning scores of the active version in the window (default 7
   days); skip below `min_runs` (default 20).
3. Exclude runs that are the source of a holdout case, so the proposer never
   learns from the data the gate uses.
4. Diagnose: weak dimensions (calibration, grounding, redundancy, unverified
   conclusions, runs without any verdict) against configured thresholds, and patterns
   with enough uses whose success lower bound is below `retire_below`.
5. Propose a profile patch touching only `reasoning`:
   * `patterns_excluded`: add the weak patterns;
   * `playbook`: guidance for the weak dimensions, from fixed, domain-free
     templates merged into the current playbook (oldest guidance dropped
     first to stay within the limit). A model-written playbook is not
     offered: model calls are lease-fenced to runs and budgeted per tenant,
     and a background job has neither. Operators can still propose any
     playbook by hand through `POST /v1/evolution/candidates`.
6. Validate: playbook ≤ 2000 runes, no poisoning pattern, no change outside
   `reasoning` (ApplyPatch also enforces the frozen fields).
7. `eval.Propose` as `system:reasoning-improver`, then `EvaluateCandidate`
   on the configured dataset's holdout split when one is set.

The job never approves, never starts a canary and never promotes. Approval
requires a human other than the proposer (existing rule); canary and
automatic rollback are the existing evolution pipeline.

### Safety envelope

* Evolvable surface: `reasoning.playbook`, `reasoning.patterns_excluded`.
  Tools, policy, risk overrides, approval rules, strategy, flow and step
  limits stay frozen.
* The playbook is rendered in the system prompt *before* the trust rules,
  labelled as learned guidance that never overrides trust rules or policy.
* Every change is a new immutable profile version with lineage
  (candidate → evaluation → approval → rollout).

## APIs

* `GET /v1/reasoning/summary?profile_id=&version=&since=` – aggregate metrics.
* `GET /v1/reasoning/runs/{run_id}` – one run's score.
* `GET /v1/reasoning/patterns?service=` – reasoning patterns with stats.
* `POST /v1/reasoning/improve` (admin) – run one improvement tick now.

## Configuration

```yaml
plugins:
  job:
    - name: reasoning_improver
      config:
        interval: 6h
        tenants: [default]
        profile_id: sre-diagnosis
        dataset_id: "<frozen dataset id>"
        window: 168h
        min_runs: 20
        min_uses: 5
        retire_below: 0.3
```

The consolidator's recorder hook is on by default and can be turned off
with `consolidator.config.reasoning: {enabled: false}` or `{distill: false}`.

The desktop runtime runs the same consolidator (a SQLite-backed queue that
picks up terminal runs and is requeued by feedback) with the recorder,
resolves strategies per profile version so the playbook is applied, and
exposes the same API. Its improver runs only on demand
(`POST /v1/reasoning/improve`).
