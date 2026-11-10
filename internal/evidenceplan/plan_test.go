package evidenceplan

import (
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
)

var now = time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)

func hyp(key, statement, status string) evidence.Hypothesis {
	return evidence.Hypothesis{Key: key, Statement: statement, Status: status}
}

func desc(name, description string, risk domain.RiskClass, labels map[string]string) tools.Descriptor {
	return tools.Descriptor{Name: name, Description: description, Risk: risk, Trust: "T3", Labels: labels}
}

var catalog = []tools.Descriptor{
	desc("k8s.cpu_throttling", "CFS throttled periods per container", domain.RiskR0, map[string]string{LabelTopics: "cpu,throttling,cfs,limits"}),
	desc("trace.rpc_latency", "Downstream RPC latency from traces", domain.RiskR0, map[string]string{LabelTopics: "downstream,rpc,latency,trace,payment"}),
	desc("prom.cpu_usage", "CPU usage", domain.RiskR0, nil),
	desc("logs.search", "Full-text log search", domain.RiskR0, map[string]string{LabelExclude: "true"}),
	desc("deployment.restart", "Restart a deployment (cpu throttling fix)", domain.RiskR1, nil),
	desc("harness.read_artifact", "Read a run artifact", domain.RiskR0, nil),
	desc("db.slow_queries", "Slow query log of the database", domain.RiskR0, map[string]string{LabelCost: "high"}),
}

func gate() policy.Gate {
	return policy.Guarded(policy.Engine{Version: "t", Environment: "production"})
}

// The RFC §12.4 example: with CPU throttling and downstream RPC latency
// competing, the plan prefers checks that tell the two apart over a CPU
// usage query.
func TestPlanPrefersDiscriminatingChecks(t *testing.T) {
	cands, ex := Filter(catalog, Constraints{Gate: gate(), Target: "service://prod/checkout", BudgetLeft: 1})
	got := map[string]string{}
	for _, e := range ex {
		got[e.Tool] = e.Reason
	}
	if got["deployment.restart"] != ExclNotReadOnly || got["logs.search"] != ExclOperator || got["harness.read_artifact"] != ExclBuiltin {
		t.Fatalf("exclusions: %v", got)
	}
	hs := []evidence.Hypothesis{
		hyp("a", "CPU throttling on checkout pods", evidence.HypOpen),
		hyp("b", "Downstream payment RPC latency", evidence.HypOpen),
	}
	p := Score(Input{Hypotheses: hs, Candidates: cands, Now: now, K: 2})
	if len(p.Checks) != 2 || p.Live != 2 {
		t.Fatalf("plan: %+v", p)
	}
	tools := map[string]bool{p.Checks[0].Tool: true, p.Checks[1].Tool: true}
	if !tools["k8s.cpu_throttling"] || !tools["trace.rpc_latency"] {
		t.Fatalf("expected the discriminating checks, got %+v", p.Checks)
	}
	for _, c := range p.Checks {
		if len(c.Bears) != 1 || c.Discrimination < 0.9 || c.Rationale == "" {
			t.Fatalf("check: %+v", c)
		}
	}
}

// A check that bears on every hypothesis alike cannot separate them.
func TestCheckCoveringAllHypothesesDoesNotDiscriminate(t *testing.T) {
	both := desc("svc.overview", "", domain.RiskR0, map[string]string{LabelTopics: "cpu,throttling,downstream,rpc,latency"})
	hs := []evidence.Hypothesis{hyp("a", "CPU throttling", evidence.HypOpen), hyp("b", "downstream RPC latency", evidence.HypOpen)}
	p := Score(Input{Hypotheses: hs, Candidates: []tools.Descriptor{both}, Now: now})
	if len(p.Checks) != 0 || p.Reason == "" {
		t.Fatalf("plan: %+v", p)
	}
	// With one hypothesis left, confirming it is what counts.
	p = Score(Input{Hypotheses: hs[:1], Candidates: []tools.Descriptor{both}, Now: now})
	if len(p.Checks) != 1 {
		t.Fatalf("single hypothesis: %+v", p)
	}
}

// Refuted hypotheses no longer compete; challenged ones weigh more.
func TestLiveHypotheses(t *testing.T) {
	cands, _ := Filter(catalog, Constraints{Gate: gate(), Target: "x", BudgetLeft: 1})
	p := Score(Input{Hypotheses: []evidence.Hypothesis{hyp("a", "CPU throttling", evidence.HypRefuted)}, Candidates: cands, Now: now})
	if len(p.Checks) != 0 || p.Live != 0 || p.Reason != "no competing hypotheses yet" {
		t.Fatalf("plan: %+v", p)
	}
	if weight(evidence.HypChallenged) <= weight(evidence.HypOpen) {
		t.Fatal("challenged hypotheses must weigh more")
	}
}

// Repeating a check against an unchanged hypothesis state brings nothing
// new; after the evidence moved, the check is worth doing again.
func TestRepeatedCheckIsDiscounted(t *testing.T) {
	cands, _ := Filter(catalog, Constraints{Gate: gate(), Target: "x", BudgetLeft: 1})
	hs := []evidence.Hypothesis{hyp("a", "CPU throttling on checkout pods", evidence.HypOpen),
		hyp("b", "Downstream payment RPC latency", evidence.HypOpen)}
	first := Score(Input{Hypotheses: hs, Candidates: cands, Now: now, K: 1})
	again := Score(Input{Hypotheses: hs, Candidates: cands, Now: now.Add(time.Hour), K: 1, Previous: &first})
	if again.Checks[0].Tool == first.Checks[0].Tool {
		t.Fatalf("same check planned twice for the same state: %+v", again.Checks)
	}
	hs[0].Supporting = []string{"ev:1"}
	hs[0].Status = evidence.HypSupported
	moved := Score(Input{Hypotheses: hs, Candidates: cands, Now: now.Add(time.Hour), K: 3, Previous: &first})
	for _, c := range moved.Checks {
		if c.Tool == first.Checks[0].Tool && c.Freshness != 1 {
			t.Fatalf("freshness after the evidence changed: %+v", c)
		}
	}
}

// The framework filters are hard: invisible tools, approval-gated tools and
// expensive checks over a spent budget never enter a plan, and model
// suggestions cannot add a tool.
func TestHardConstraints(t *testing.T) {
	visible := func(d tools.Descriptor) (tools.Descriptor, bool) {
		if d.Name == "trace.rpc_latency" {
			return d, false
		}
		if d.Name == "prom.cpu_usage" {
			d.Risk = domain.RiskR1 // profile risk override
		}
		return d, true
	}
	cands, ex := Filter(catalog, Constraints{Visible: visible, Gate: gate(), Target: "x", BudgetLeft: 0.1})
	reasons := map[string]string{}
	for _, e := range ex {
		reasons[e.Tool] = e.Reason
	}
	if reasons["trace.rpc_latency"] != ExclNotVisible || reasons["prom.cpu_usage"] != ExclNotReadOnly ||
		reasons["db.slow_queries"] != ExclBudget {
		t.Fatalf("exclusions: %v", reasons)
	}
	hs := []evidence.Hypothesis{hyp("a", "CPU throttling", evidence.HypOpen), hyp("b", "downstream RPC latency", evidence.HypOpen)}
	p := Score(Input{Hypotheses: hs, Candidates: cands, Now: now, K: 5,
		Suggested: map[string]float64{"trace.rpc_latency": 1, "deployment.restart": 1}})
	for _, c := range p.Checks {
		if c.Tool == "trace.rpc_latency" || c.Tool == "deployment.restart" {
			t.Fatalf("a suggestion added a filtered tool: %+v", p.Checks)
		}
	}
	// A policy that requires approval for reads keeps them out.
	strict := policy.Guarded(denyAll{})
	if c, _ := Filter(catalog, Constraints{Gate: strict, Target: "x", BudgetLeft: 1}); len(c) != 0 {
		t.Fatalf("policy-gated tools planned: %v", c)
	}
	// Tools that need a target cannot be checked without one.
	targeted := desc("k8s.pod_events", "pod events", domain.RiskR0, nil)
	targeted.TargetArg = "pod"
	if c, ex := Filter([]tools.Descriptor{targeted}, Constraints{Gate: gate(), BudgetLeft: 1}); len(c) != 0 || ex[0].Reason != ExclPolicy {
		t.Fatalf("untargeted: %v %v", c, ex)
	}
}

// A model suggestion raises the score of an allowed candidate.
func TestSuggestionAdjustsScore(t *testing.T) {
	cands, _ := Filter(catalog, Constraints{Gate: gate(), Target: "x", BudgetLeft: 1})
	hs := []evidence.Hypothesis{hyp("a", "database slow queries", evidence.HypOpen)}
	base := Score(Input{Hypotheses: hs, Candidates: cands, Now: now})
	sugg := Score(Input{Hypotheses: hs, Candidates: cands, Now: now, Suggested: map[string]float64{"prom.cpu_usage": 1}})
	found := false
	for _, c := range sugg.Checks {
		if c.Tool == "prom.cpu_usage" {
			found = true
		}
	}
	for _, c := range base.Checks {
		if c.Tool == "prom.cpu_usage" {
			t.Fatal("unrelated check planned without a suggestion")
		}
	}
	if !found {
		t.Fatalf("suggested check: %+v", sugg.Checks)
	}
}

type denyAll struct{}

func (denyAll) Evaluate(d *tools.Descriptor, _ string) policy.Decision {
	return policy.Decision{Verdict: policy.RequireApproval, RequiredApprovals: 1, Risk: d.Risk}
}
func (denyAll) BuildIntent(tools.Descriptor, domain.ToolCall, string, policy.Decision) domain.ActionIntent {
	return domain.ActionIntent{}
}
