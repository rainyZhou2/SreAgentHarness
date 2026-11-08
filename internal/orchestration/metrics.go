package orchestration

import (
	"context"

	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/store/cpsql"
)

// Metrics are the v2 control-plane metrics (RFC §21). Labels are bounded
// enums only: never tenant, work id, subject or free text.
type Metrics struct {
	ticks       obs.Counter
	transitions obs.Counter
	stages      obs.Counter
	actions     obs.Counter
	outcomes    obs.Counter
	events      obs.Counter
	watches     obs.Counter
}

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
	}
	if store != nil {
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

func (m *Metrics) watch(kind string) {
	if m != nil {
		m.watches.Inc(kind)
	}
}
