package orchestration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidenceplan"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
)

// fakeChecks is a tool catalog with a profile that hides one tool.
type fakeChecks struct{ calls int }

func (f *fakeChecks) Checks(_ context.Context, _, profileID string, _ int64, _ string) ([]tools.Descriptor, evidenceplan.Constraints, error) {
	f.calls++
	d := func(name, topics string, risk domain.RiskClass) tools.Descriptor {
		return tools.Descriptor{Name: name, Risk: risk, Trust: "T3", Labels: map[string]string{evidenceplan.LabelTopics: topics}}
	}
	ds := []tools.Descriptor{
		d("k8s.cpu_throttling", "cpu,throttling,cfs", domain.RiskR0),
		d("trace.rpc_latency", "downstream,rpc,latency,payment", domain.RiskR0),
		d("prom.cpu_usage", "cpu,usage", domain.RiskR0),
		d("deployment.restart", "cpu,throttling,restart", domain.RiskR1),
		d("secret.cpu_dump", "cpu,throttling,dump", domain.RiskR0),
	}
	visible := func(t tools.Descriptor) (tools.Descriptor, bool) { return t, t.Name != "secret.cpu_dump" }
	return ds, evidenceplan.Constraints{Visible: visible, Gate: policy.Guarded(policy.Engine{Version: "t", Environment: "production"})}, nil
}

var competing = map[string]any{
	"answer": "Either CPU throttling or the payment dependency.",
	"hypotheses": []map[string]any{
		{"id": "h1", "statement": "CPU throttling on checkout pods", "status": "OPEN"},
		{"id": "h2", "statement": "Downstream payment RPC latency", "status": "OPEN"},
	},
	"next_checks": []any{"trace.rpc_latency", map[string]any{"tool": "secret.cpu_dump"}},
	"loop":        map[string]any{"cost_usd": 0.01},
}

func twoRunTemplate(second work.Stage) work.Template {
	return work.Template{ID: "rca", Version: 1, Kind: work.KindIncident, Stages: []work.Stage{
		{ID: "triage", Type: work.StageRun, Phase: "investigate", Run: &work.RunSpec{Goal: "Triage ${subjects}"}},
		second,
		{ID: "confirm", Type: work.StageConfirm},
	}}
}

// tickUntil ticks with the clock advancing until n runs started.
func (h *harness) tickUntil(n int) {
	h.t.Helper()
	for i := 0; i < 20 && h.runs.started() < n; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			h.t.Fatal(err)
		}
		h.clock.Add(6 * time.Second)
	}
}

func (h *harness) goals() []string {
	h.runs.mu.Lock()
	defer h.runs.mu.Unlock()
	var out []string
	for _, id := range h.runs.order {
		out = append(out, h.runs.runs[id].req.Goal)
	}
	return out
}

func (h *harness) plans(workID string) []map[string]any {
	evs, err := h.store.ListWorkEvents(h.ctx, "t1", workID, 0, 1000)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []map[string]any
	for _, e := range evs {
		if e.Type == EventEvidencePlanned {
			var p map[string]any
			_ = json.Unmarshal(e.Payload, &p)
			out = append(out, p)
		}
	}
	return out
}

// RFC §12.4: once earlier runs left competing hypotheses, the next run is
// told which read-only checks tell them apart; tools the profile hides or
// that are not read-only never appear, even when a run suggested them.
func TestEvidencePlanGuidesNextRun(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	checks := &fakeChecks{}
	h.orch.Checks = checks
	h.useTemplates(twoRunTemplate(work.Stage{ID: "dig", Type: work.StageRun, Phase: "investigate",
		Run: &work.RunSpec{Goal: "Find the root cause of ${subjects}"}}))
	w := h.create("rca", nil)
	h.settle(w.ID, work.StatusActive)
	if checks.calls != 0 || len(h.plans(w.ID)) != 0 {
		t.Fatal("no plan before there are hypotheses")
	}
	h.runs.finishAll(competing, true)
	h.tickUntil(2)
	goals := h.goals()
	if len(goals) != 2 {
		t.Fatalf("runs: %d", len(goals))
	}
	g := goals[1]
	if !strings.Contains(g, "Evidence plan (framework)") || !strings.Contains(g, "k8s.cpu_throttling") ||
		!strings.Contains(g, "trace.rpc_latency") {
		t.Fatalf("goal lacks the plan:\n%s", g)
	}
	for _, banned := range []string{"deployment.restart", "secret.cpu_dump"} {
		if strings.Contains(g, banned) {
			t.Fatalf("plan names %s:\n%s", banned, g)
		}
	}
	if !strings.Contains(g, "<<<DATA") || !strings.Contains(g, "CPU throttling on checkout pods") {
		t.Fatalf("hypotheses must be quoted as data:\n%s", g)
	}
	ps := h.plans(w.ID)
	if len(ps) != 1 {
		t.Fatalf("plan events: %d", len(ps))
	}
	ex, _ := ps[0]["excluded"].(map[string]any)
	if ex[evidenceplan.ExclNotReadOnly] != float64(1) || ex[evidenceplan.ExclNotVisible] != float64(1) {
		t.Fatalf("excluded: %v", ex)
	}
	plan, _ := ps[0]["plan"].(map[string]any)
	if plan["live_hypotheses"] != float64(2) || len(plan["checks"].([]any)) == 0 {
		t.Fatalf("plan: %v", plan)
	}
}

// Parallel agents of one profile get different checks, so they test
// different things instead of repeating each other.
func TestEvidencePlanSplitsChecksAcrossAgents(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.orch.Checks = &fakeChecks{}
	h.useTemplates(twoRunTemplate(work.Stage{ID: "dig", Type: work.StageAgents, Phase: "investigate",
		EvidencePlan: &work.EvidencePlanSpec{MaxChecks: 1},
		Agents: []work.AgentSpec{{Role: "infra", RunSpec: work.RunSpec{Goal: "infra view"}, Required: true},
			{Role: "deps", RunSpec: work.RunSpec{Goal: "dependency view"}, Required: true}}}))
	w := h.create("rca", nil)
	h.settle(w.ID, work.StatusActive)
	h.runs.finishAll(competing, true)
	h.tickUntil(3)
	goals := h.goals()
	if len(goals) != 3 {
		t.Fatalf("runs: %d", len(goals))
	}
	first := func(g string) string {
		for _, name := range []string{"k8s.cpu_throttling", "trace.rpc_latency", "prom.cpu_usage"} {
			if strings.Contains(g, "1. "+name) {
				return name
			}
		}
		return ""
	}
	a, b := first(goals[1]), first(goals[2])
	if a == "" || b == "" || a == b {
		t.Fatalf("agents got %q and %q", a, b)
	}
	if strings.Contains(goals[1], "2. ") || strings.Contains(goals[2], "2. ") {
		t.Fatal("max_checks 1 per agent")
	}
}

// Planning can be turned off per stage and never blocks a run.
func TestEvidencePlanDisabled(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	checks := &fakeChecks{}
	h.orch.Checks = checks
	h.useTemplates(twoRunTemplate(work.Stage{ID: "dig", Type: work.StageRun, EvidencePlan: &work.EvidencePlanSpec{Disabled: true},
		Run: &work.RunSpec{Goal: "dig"}}))
	w := h.create("rca", nil)
	h.settle(w.ID, work.StatusActive)
	h.runs.finishAll(competing, true)
	h.tickUntil(2)
	if g := h.goals(); len(g) != 2 || strings.Contains(g[1], "Evidence plan") || checks.calls != 0 {
		t.Fatalf("disabled planning: calls=%d goals=%v", checks.calls, g)
	}
}
