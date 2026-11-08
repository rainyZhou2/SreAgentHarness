package reasoning

import (
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
)

func TestDistillPlanExemplar(t *testing.T) {
	run := domain.Run{ID: "r1", TenantID: "t", Status: domain.RunCompleted}
	p := &planArt{Mode: "flow", Rationale: "check then restart",
		Flow: json.RawMessage(`{ "entry": "q", "nodes": [ {"id":"q","type":"end"} ] }`)}
	pm := &PlanMetrics{Mode: "flow", Succeeded: true, Replans: 1}
	nm, ok := DistillPlanExemplar(run, "checkout is slow", "checkout", p, pm, "", nil)
	if !ok || nm.Status != "CANDIDATE" || nm.Subject != "checkout:"+PlanExemplarSubject || nm.ScopeID != "checkout" {
		t.Fatalf("%+v %v", nm, ok)
	}
	for _, want := range []string{`Flow: {"entry":"q","nodes":[{"id":"q","type":"end"}]}`, "after 1 replan", "Goal: checkout is slow"} {
		if !strings.Contains(nm.Content, want) {
			t.Fatalf("missing %q:\n%s", want, nm.Content)
		}
	}
	if strings.Contains(nm.Content, "r1") {
		t.Fatal("content must not carry the run id")
	}
	yes, no := true, false
	if nm, _ := DistillPlanExemplar(run, "g", "", p, pm, "", &yes); nm.Status != "VALIDATING" || nm.Scope != "organization" {
		t.Fatalf("correct feedback: %+v", nm)
	}
	reject := []func(){
		func() { p.Fallback = true },
		func() { pm.Injection = true },
		func() { pm.ReviewRejected = true },
	}
	for i, f := range reject {
		p2, pm2 := *p, *pm
		p, pm = &p2, &pm2
		f()
		if _, ok := DistillPlanExemplar(run, "g", "s", p, pm, "", nil); ok {
			t.Fatalf("case %d must not distill", i)
		}
		p.Fallback, pm.Injection, pm.ReviewRejected = false, false, false
	}
	if _, ok := DistillPlanExemplar(run, "g", "s", p, pm, "DISPUTED", nil); ok {
		t.Fatal("disputed")
	}
	if _, ok := DistillPlanExemplar(run, "g", "s", p, pm, "", &no); ok {
		t.Fatal("wrong feedback")
	}
	failed := run
	failed.Status = domain.RunFailed
	if _, ok := DistillPlanExemplar(failed, "g", "s", p, pm, "", nil); ok {
		t.Fatal("failed run")
	}
}

func TestPlanSummaryAndGate(t *testing.T) {
	mk := func(n int, fallback, success int) []Metrics {
		var ms []Metrics
		for i := 0; i < n; i++ {
			ms = append(ms, Metrics{Plan: &PlanMetrics{Mode: "flow", Fallback: i < fallback, Succeeded: i < success}})
		}
		return append(ms, Metrics{}) // runs without a plan are ignored
	}
	base, cand := SummarizePlans(mk(10, 1, 9)), SummarizePlans(mk(10, 4, 6))
	if base.N != 10 || base.FallbackRate != 0.1 || base.SuccessRate != 0.9 {
		t.Fatalf("summary %+v", base)
	}
	f := PlanGate(base, cand, PlanGateConfig{})
	if len(f) != 2 || !strings.Contains(f[0], "fallback") || !strings.Contains(f[1], "success") {
		t.Fatalf("gate %v", f)
	}
	if f := PlanGate(base, SummarizePlans(mk(3, 3, 0)), PlanGateConfig{}); f != nil {
		t.Fatalf("too few runs must not gate: %v", f)
	}
	if SummarizePlans([]Metrics{{}}) != nil {
		t.Fatal("no plans → nil")
	}
	// The run-level gate includes plan regressions.
	if fs := Gate(Summary{N: 10, Plan: base}, Summary{N: 10, Plan: cand}, GateConfig{}); !strings.Contains(strings.Join(fs, ";"), "fallback") {
		t.Fatalf("Gate did not include plan checks: %v", fs)
	}
}
