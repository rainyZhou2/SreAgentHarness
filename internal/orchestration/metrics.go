package orchestration

import (
	"context"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// Metrics are the v2 control-plane metrics (RFC §21.1). Labels are bounded
// enums only: never tenant, work id, run id, subject or free text — those
// go to spans, logs and the audit log.
type Metrics struct {
	ticks       obs.Counter
	transitions obs.Counter
	stages      obs.Counter
	actions     obs.Counter
	outcomes    obs.Counter
	events      obs.Counter
	watches     obs.Counter
	inboxPurge  obs.Counter

	workAge        obs.Histogram
	workCost       obs.Histogram
	replans        obs.Counter
	correlation    obs.Histogram
	watchLag       obs.Histogram
	wakeLag        obs.Histogram
	factStaleness  obs.Histogram
	snapshot       obs.Histogram
	preflight      obs.Counter
	effectUnknown  obs.Counter
	verify         obs.Histogram
	assignments    obs.Counter
	guardDenials   obs.Counter
	breakGlass     obs.Counter
	effectsBlocked obs.Counter
	queueClaimed   obs.Counter
	poolDeferrals  obs.Counter
}

var (
	_ action.Observer = (*Metrics)(nil)
	_ world.Observer  = (*Metrics)(nil)
)

// Latency buckets (seconds).
var (
	fastBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
	lagBuckets  = []float64{.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}
	ageBuckets  = []float64{60, 300, 900, 1800, 3600, 4 * 3600, 12 * 3600, 86400, 3 * 86400, 7 * 86400, 30 * 86400}
	costBuckets = []float64{.01, .05, .1, .5, 1, 2, 5, 10, 25, 50, 100}
)

// NewMetrics registers the metrics on reg and a gauge of Works by status.
func NewMetrics(reg *obs.Registry, store *cpsql.Store) *Metrics {
	m := &Metrics{
		ticks:       reg.Counter("harness_work_ticks_total", "Work processing passes by result.", "result"),
		transitions: reg.Counter("harness_work_transitions_total", "Work status transitions.", "kind", "to"),
		stages:      reg.Counter("harness_work_stage_total", "Work stage lifecycle events.", "type", "result"),
		actions:     reg.Counter("harness_action_state_total", "Work action state changes observed.", "state"),
		outcomes:    reg.Counter("harness_outcome_evaluations_total", "Outcome evaluations by verdict.", "verdict"),
		events:      reg.Counter("harness_event_ingested_total", "Ingested events by decision.", "decision"),
		watches:     reg.Counter("harness_watch_fired_total", "Watch firings by kind.", "kind"),
		inboxPurge:  reg.Counter("harness_event_inbox_purged_total", "Inbox retention: payloads stripped and rows deleted.", "op"),

		workAge: reg.Histogram("harness_work_age_seconds", "Time from Work creation to its terminal status.",
			ageBuckets, "kind", "status"),
		workCost: reg.Histogram("harness_work_model_cost_usd", "Model cost of a Work at its terminal status (USD).",
			costBuckets, "kind"),
		replans: reg.Counter("harness_work_replans_total", "Replans by cause (stage_failure, operator, reopen, retry).", "cause"),
		correlation: reg.Histogram("harness_event_correlation_seconds", "Event routing and correlation latency.",
			fastBuckets, "decision"),
		watchLag: reg.Histogram("harness_watch_due_lag_seconds", "Delay from a watch being due to its firing.",
			lagBuckets, "kind"),
		wakeLag: reg.Histogram("harness_work_wake_lag_seconds", "Delay from a Work's wake time to its processing.",
			lagBuckets),
		factStaleness: reg.Histogram("harness_world_fact_staleness_seconds", "Age of World facts served in snapshots.",
			ageBuckets, "trust"),
		snapshot: reg.Histogram("harness_world_snapshot_seconds", "World snapshot build latency.", fastBuckets, "result"),
		preflight: reg.Counter("harness_action_preflight_total", "Action preflight results (ready, approval, rejected).",
			"result"),
		effectUnknown: reg.Counter("harness_action_effect_unknown_total",
			"Actions whose effect is unknown (dispatch_error, lost_result). Alert on any increase.", "cause"),
		verify: reg.Histogram("harness_action_verify_seconds", "Time from dispatch to a settled effect verification.",
			lagBuckets, "state"),
		assignments: reg.Counter("harness_agent_assignment_total", "Agent assignments by stage type and result.",
			"stage_type", "result"),
		guardDenials: reg.Counter("harness_guard_denials_total", "Requests refused by a guard, by guard and reason.",
			"guard", "reason"),
		breakGlass: reg.Counter("harness_action_break_glass_total", "Actions released through break-glass."),
		effectsBlocked: reg.Counter("harness_action_blocked_by_kernel_effect_total",
			"Action dispatches deferred by an unresolved kernel tool effect."),
		queueClaimed: reg.Counter("harness_work_queue_claimed_total", "Works claimed for processing, by scheduling queue.", "queue"),
		poolDeferrals: reg.Counter("harness_pool_deferred_total",
			"Work steps postponed because a resource pool was full (model_runs, actions).", "pool"),
	}
	if store != nil {
		registerBacklog(reg, store)
		reg.Collect("harness_work_active", "Works by kind and status.", func(ctx context.Context, emit func(string, map[string]string, float64)) {
			counts, err := store.CountByStatus(ctx)
			if err != nil {
				return
			}
			for kind, by := range counts {
				for status, n := range by {
					emit("harness_work_active", map[string]string{"kind": kind, "status": status}, float64(n))
				}
			}
		})
	}
	return m
}

func (m *Metrics) claimed(queue string) {
	if m == nil {
		return
	}
	m.queueClaimed.Inc(queue)
}

func (m *Metrics) poolDeferred(pool string) {
	if m == nil {
		return
	}
	m.poolDeferrals.Inc(pool)
}

// RegisterPools exports the in-flight slots of the per-replica pools.
func (m *Metrics) RegisterPools(reg *obs.Registry, p *Pools) {
	if reg == nil || p == nil {
		return
	}
	reg.Collect("harness_pool_inflight", "Held slots of the per-replica resource pools (actions, snapshots).",
		func(_ context.Context, emit func(string, map[string]string, float64)) {
			emit("harness_pool_inflight", map[string]string{"pool": "actions"}, float64(p.Actions.InFlight()))
			emit("harness_pool_inflight", map[string]string{"pool": "snapshots"}, float64(p.Snapshots.InFlight()))
		})
}

func (m *Metrics) tick(result string) {
	if m != nil {
		m.ticks.Inc(result)
	}
}

func (m *Metrics) transition(kind, to string) {
	if m != nil {
		m.transitions.Inc(kind, to)
	}
}

// closed records the age and cost of a Work that reached a terminal status.
func (m *Metrics) closed(w work.Work, at time.Time) {
	if m == nil || !w.Status.Terminal() {
		return
	}
	m.workAge.Observe(at.Sub(w.CreatedAt).Seconds(), string(w.Kind), string(w.Status))
	m.workCost.Observe(float64(w.CostMicros)/1e6, string(w.Kind))
}

func (m *Metrics) replan(cause string) {
	if m != nil {
		m.replans.Inc(cause)
	}
}

func (m *Metrics) stage(typ, result string) {
	if m != nil {
		m.stages.Inc(typ, result)
	}
}

func (m *Metrics) action(state string) {
	if m != nil {
		m.actions.Inc(state)
	}
}

func (m *Metrics) outcome(verdict string) {
	if m != nil {
		m.outcomes.Inc(verdict)
	}
}

func (m *Metrics) event(decision string) {
	if m != nil {
		m.events.Inc(decision)
	}
}

func (m *Metrics) correlated(decision string, d time.Duration) {
	if m != nil {
		m.correlation.Observe(d.Seconds(), decision)
	}
}

func (m *Metrics) watch(kind string) {
	if m != nil {
		m.watches.Inc(kind)
	}
}

func (m *Metrics) watchDue(kind string, lag time.Duration) {
	if m != nil {
		m.watchLag.Observe(max(lag, 0).Seconds(), kind)
	}
}

func (m *Metrics) woke(lag time.Duration) {
	if m != nil {
		m.wakeLag.Observe(max(lag, 0).Seconds())
	}
}

func (m *Metrics) assignment(stageType, result string) {
	if m != nil {
		m.assignments.Inc(stageType, assignmentClass(result))
	}
}

// assignmentClass bounds a run outcome to a small label set.
func assignmentClass(outcome string) string {
	switch {
	case strings.HasPrefix(outcome, "completed"):
		return "completed"
	case outcome == "failed", outcome == "cancelled":
		return outcome
	}
	return "other"
}

func (m *Metrics) inboxPurged(stripped, deleted int64) {
	if m != nil {
		m.inboxPurge.Add(float64(stripped), "payload")
		m.inboxPurge.Add(float64(deleted), "record")
	}
}

// GuardDenied counts a refusal by a guard outside the action runtime:
// scope (principal scope), data_class (model data boundary), role.
func (m *Metrics) GuardDenied(guard, reason string) {
	if m != nil {
		m.guardDenials.Inc(guard, reason)
	}
}

// ---- action.Observer ----

// Preflight implements action.Observer.
func (m *Metrics) Preflight(result string) {
	if m != nil {
		m.preflight.Inc(result)
	}
}

// EffectUnknown implements action.Observer.
func (m *Metrics) EffectUnknown(cause string) {
	if m != nil {
		m.effectUnknown.Inc(cause)
	}
}

// Verify implements action.Observer.
func (m *Metrics) Verify(state action.State, d time.Duration) {
	if m != nil {
		m.verify.Observe(max(d, 0).Seconds(), string(state))
	}
}

// Denied implements action.Observer.
func (m *Metrics) Denied(reason string) {
	if m == nil {
		return
	}
	if reason == "pending_effect" {
		m.effectsBlocked.Inc()
	}
	m.guardDenials.Inc("action", reason)
}

// BreakGlass implements action.Observer.
func (m *Metrics) BreakGlass() {
	if m != nil {
		m.breakGlass.Inc()
	}
}

// ---- world.Observer ----

// Snapshot implements world.Observer.
func (m *Metrics) Snapshot(d time.Duration, result string, _, _ int) {
	if m != nil {
		m.snapshot.Observe(d.Seconds(), result)
	}
}

// FactAge implements world.Observer.
func (m *Metrics) FactAge(trust string, age time.Duration) {
	if m == nil {
		return
	}
	switch trust {
	case world.TrustAuthoritative, world.TrustFirstParty, world.TrustTelemetry, world.TrustUntrusted:
	default:
		trust = "unknown"
	}
	m.factStaleness.Observe(max(age, 0).Seconds(), trust)
}

// registerBacklog exports the operator backlog (RFC §21.3) at scrape time.
func registerBacklog(reg *obs.Registry, store *cpsql.Store) {
	gauges := map[string]string{
		"harness_action_backlog":                "Work actions waiting for a decision, dispatch result, verification or recovery, by state.",
		"harness_action_backlog_oldest_seconds": "Age of the oldest action in each backlog state.",
		"harness_work_waiting":                  "Works waiting on a human (WAITING, BLOCKED, SUSPENDED).",
		"harness_work_waiting_oldest_seconds":   "Age of the oldest waiting Work by status.",
		"harness_event_inbox_backlog":           "Inbox events not yet processed (NEW, FAILED) or dead-lettered (DEAD).",
		"harness_event_inbox_oldest_seconds":    "Age of the oldest inbox event by state.",
		"harness_watch_overdue":                 "Active watches due for more than a minute (the reconciler is not progressing).",
		"harness_watch_overdue_oldest_seconds":  "How long the most overdue watch has been due.",
		"harness_action_unapproved_dispatch":    "Dispatched actions that required approval but have neither an approval nor break-glass (must be 0).",
		"harness_work_queue_due":                "Works due for processing and not leased, by scheduling queue.",
		"harness_work_queue_due_oldest_seconds": "How long the oldest due Work of each queue has waited.",
	}
	for n, h := range gauges {
		reg.Describe(n, h)
	}
	reg.Collect("harness_action_backlog", gauges["harness_action_backlog"], func(ctx context.Context, emit func(string, map[string]string, float64)) {
		b, err := store.Backlog(ctx, time.Now())
		if err != nil {
			return
		}
		emitItems := func(count, oldest, label string, items map[string]cpsql.BacklogItem, keys []string) {
			for _, k := range keys {
				it := items[k]
				emit(count, map[string]string{label: k}, float64(it.Count))
				emit(oldest, map[string]string{label: k}, it.Oldest.Seconds())
			}
		}
		emitItems("harness_action_backlog", "harness_action_backlog_oldest_seconds", "state", b.Actions,
			[]string{"WAITING_APPROVAL", "READY", "DISPATCHED", "EFFECT_UNKNOWN", "MANUAL_RECOVERY", "VERIFYING", "DEGRADED", "COMPENSATION_PENDING"})
		emitItems("harness_work_waiting", "harness_work_waiting_oldest_seconds", "status", b.Works,
			[]string{"WAITING", "BLOCKED", "SUSPENDED"})
		queues := make([]string, len(work.Queues))
		for i, q := range work.Queues {
			queues[i] = string(q)
		}
		emitItems("harness_work_queue_due", "harness_work_queue_due_oldest_seconds", "queue", b.Due, queues)
		emitItems("harness_event_inbox_backlog", "harness_event_inbox_oldest_seconds", "state", b.Inbox,
			[]string{"NEW", "FAILED", "DEAD"})
		emit("harness_watch_overdue", nil, float64(b.OverdueWatches.Count))
		emit("harness_watch_overdue_oldest_seconds", nil, b.OverdueWatches.Oldest.Seconds())
		emit("harness_action_unapproved_dispatch", nil, float64(b.UnapprovedDispatches))
	})
}
