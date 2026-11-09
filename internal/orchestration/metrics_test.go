package orchestration

import (
	"context"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// §21.1: every RFC metric is exported with bounded labels only.
func TestControlPlaneMetrics(t *testing.T) {
	reg := obs.NewRegistry()
	m := NewMetrics(reg, nil)
	now := time.Now()
	m.closed(work.Work{Kind: work.KindIncident, Status: work.StatusSucceeded, CreatedAt: now.Add(-time.Hour), CostMicros: 2_500_000}, now)
	m.closed(work.Work{Kind: work.KindIncident, Status: work.StatusActive, CreatedAt: now}, now) // not terminal: ignored
	m.replan("operator")
	m.correlated("created", 3*time.Millisecond)
	m.watchDue("time", 2*time.Second)
	m.woke(-time.Second) // clock skew is clamped
	m.assignment("investigate", "completed:confirmed")
	m.GuardDenied("scope", "event.ingest")
	m.Preflight("approval")
	m.EffectUnknown("lost_result")
	m.Verify(action.StateVerified, 40*time.Second)
	m.Denied("pending_effect")
	m.BreakGlass()
	m.Snapshot(12*time.Millisecond, "ok", 3, 1)
	m.FactAge(world.TrustTelemetry, 90*time.Second)
	m.FactAge("free text", time.Second)

	var b strings.Builder
	reg.Write(context.Background(), &b)
	out := b.String()
	for _, want := range []string{
		`harness_work_age_seconds_count{kind="incident",status="SUCCEEDED"} 1`,
		`harness_work_model_cost_usd_sum{kind="incident"} 2.5`,
		`harness_work_replans_total{cause="operator"} 1`,
		`harness_event_correlation_seconds_count{decision="created"} 1`,
		`harness_watch_due_lag_seconds_count{kind="time"} 1`,
		`harness_work_wake_lag_seconds_count 1`,
		`harness_agent_assignment_total{stage_type="investigate",result="completed"} 1`,
		`harness_guard_denials_total{guard="scope",reason="event.ingest"} 1`,
		`harness_guard_denials_total{guard="action",reason="pending_effect"} 1`,
		`harness_action_preflight_total{result="approval"} 1`,
		`harness_action_effect_unknown_total{cause="lost_result"} 1`,
		`harness_action_verify_seconds_count{state="VERIFIED"} 1`,
		`harness_action_break_glass_total 1`,
		`harness_action_blocked_by_kernel_effect_total 1`,
		`harness_world_snapshot_seconds_count{result="ok"} 1`,
		`harness_world_fact_staleness_seconds_count{trust="T3"} 1`,
		`harness_world_fact_staleness_seconds_count{trust="unknown"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out, `status="ACTIVE"`) {
		t.Error("a non-terminal Work was observed as closed")
	}
	var nilM *Metrics
	nilM.closed(work.Work{}, now) // nil-safe
	nilM.EffectUnknown("x")
}
