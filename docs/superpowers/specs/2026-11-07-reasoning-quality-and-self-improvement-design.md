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
   conclusions, runs without any verdict, trace structure) against configured
   thresholds, and plan pattern exclusions (section 4).
5. Propose a profile patch touching only `reasoning`:
   * `patterns_excluded`: retire, reinstate and prune patterns (section 4);
   * `pattern_probe_rate`: set to `probe_rate` (default 0.05) when patterns
     are excluded and the profile has no probe rate yet;
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

* Evolvable surface: `reasoning.playbook`, `reasoning.patterns_excluded`,
  `reasoning.pattern_probe_rate` (at most 0.2).
  Tools, policy, risk overrides, approval rules, strategy, flow and step
  limits stay frozen.
* The playbook is rendered in the system prompt *before* the trust rules,
  labelled as learned guidance that never overrides trust rules or policy.
* Every change is a new immutable profile version with lineage
  (candidate → evaluation → approval → rollout).

## 4. Pattern exclusion lifecycle

### Effect relative to the service baseline

Raw success rates mostly measure how hard a service is. For every memory,
`PatternStats` compares the runs exposed to it with the *unexposed* runs of
the same service: each exposed run contributes its service's baseline
success rate (Beta(1,1) prior) to `expected`; `lift` is
(successes − expected) / n and `z` = (successes − expected) / √Σp(1−p).
Exposed runs of a service without unexposed runs are not comparable and are
left out (`n` counts comparable runs).

Exposure is measured two ways. *Retrieved* means the memory was injected
into the prompt. *Cited* means the model referenced `memory:<id>` in
evidence refs, hypothesis supports or refutes (the plan's automatic
`based_on` links do not count). The cited effect decides once it has
`min_uses` comparable runs, because it is closer to "the pattern was used";
otherwise the retrieval effect decides.

### Retire, probe, reinstate

* **Retire** an ACTIVE reasoning pattern when its decisive effect has
  `z <= -retire_z` (default 1.96) *and* `lift <= -min_lift` (default 0.10):
  significantly and materially worse than the baseline.
* **Probe**: `reasoning.pattern_probe_rate` re-exposes each excluded memory
  to that share of runs, chosen by a SHA-256 hash of run and memory id, so
  retries of a run see the same memories. Runs of a version that excludes a
  pattern only see it through probes, so the stats of the active version are
  probe evidence.
* **Reinstate** an excluded pattern whose probed runs reach
  `z >= reinstate_z` (default 0: at least the baseline). Reinstating needs
  much less evidence of harm to be absent than retiring needs evidence of
  harm to be present, and `reinstate_z` must stay above `-retire_z`, so a
  pattern does not flap.
* **Prune** excluded ids whose memory is gone or no longer ACTIVE (retrieval
  never returns them). Pruning rides along with a real change; it never
  triggers a proposal by itself.
* **Capacity**: the list holds at most 500 ids. Pruning frees space first;
  if retirements still do not fit, the ones with the strongest evidence
  (lowest z) are kept and the rest are reported as `deferred`.
* **Manual**: `POST /v1/reasoning/patterns/{memory_id}/exclude` and
  `/reinstate` (operator) propose the same kind of candidate on behalf of
  the caller; a different human approves it.

## 5. Trace structure

* Hypotheses carry a stable `id` (the conclusion schema now requires `id`,
  `statement`, `status` and `confidence_estimate`). The kernel stores it as
  the `hypothesis_id` thought attribute; scoring deduplicates by id and falls
  back to the normalized statement only when there is none.
* `calibration_coverage` is the share of settled hypotheses that carried a
  confidence. Brier stays defined over stated confidences; for the composite
  quality score a settled hypothesis without confidence counts as the
  no-skill forecast 0.5 (Brier 0.25).
* Summaries report the mean structured share and pooled coverage. The gate
  fails a candidate whose structured share or coverage drops by more than
  0.10 (`max_structured_drop`, `max_coverage_drop`; 0 means the default, so
  configurations written before these checks still get them). The improver
  flags the `structure` dimension below `min_structured` (0.8) or
  `min_coverage` (0.6) and adds guidance to fill the structured fields.

### Canary reasoning guardrails

`RolloutStats` now carries a reasoning `Summary` of each group's scored runs
(canary: runs pinned to the rollout; control: runs of the profile without a
rollout since the canary started). `StartCanary` enables
`guardrails.reasoning` by default with the release-gate margins and
`min_scored_runs: 20` per side; the monitor rolls the canary back when its
reasoning regresses against the control group by more than a margin. Pass
`"reasoning": {"disabled": true}` to opt out; rollouts created earlier carry
no reasoning guardrails and are judged as before.

### Retention

Reasoning scores are derived data. The `retention` job deletes scores last
computed more than `reasoning_retention` ago (default 2160h, 90 days); keep
it longer than the improver's window. The desktop runtime prunes with the
default on its reconcile loop.

## APIs

* `GET /v1/reasoning/summary?profile_id=&version=&since=` – aggregate metrics.
* `GET /v1/reasoning/runs/{run_id}` – one run's score.
* `GET /v1/reasoning/patterns?service=&profile_id=` – reasoning patterns with
  stats, baseline-relative effects and whether the profile excludes them.
* `POST /v1/reasoning/patterns/{memory_id}/exclude|reinstate` (operator) –
  propose an exclusion change.
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
        retire_z: 1.96
        min_lift: 0.10
        reinstate_z: 0
        probe_rate: 0.05
```

The consolidator's recorder hook is on by default and can be turned off
with `consolidator.config.reasoning: {enabled: false}` or `{distill: false}`.

The desktop runtime runs the same consolidator (a SQLite-backed queue that
picks up terminal runs and is requeued by feedback) with the recorder,
resolves strategies per profile version so the playbook is applied, and
exposes the same API. Its improver runs only on demand
(`POST /v1/reasoning/improve`).
