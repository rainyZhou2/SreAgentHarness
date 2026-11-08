package planner

import (
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/tools"
)

var catalog = []tools.Descriptor{
	{Name: "prom.query", Risk: domain.RiskR0},
	{Name: "logs.search", Risk: domain.RiskR0},
	{Name: "k8s.rollout_restart", Risk: domain.RiskR2},
	{Name: "db.drop_table", Risk: domain.RiskR3},
}

func compileSpec(t *testing.T, raw string) flow.Spec {
	t.Helper()
	fs, _, _, err := flow.Compile([]byte(raw), flow.CompileOptions{AllowTool: func(string) bool { return true }})
	if err != nil {
		t.Fatalf("compile: %v\n%s", err, raw)
	}
	return fs
}

func findingCodes(a Analysis) map[string]Finding {
	out := map[string]Finding{}
	for _, f := range a.Findings {
		out[f.Code] = f
	}
	return out
}

func TestAnalyzeReadOnlyPlanNeedsNoReview(t *testing.T) {
	fs := compileSpec(t, `{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[{"to":"done"}]},
		{"id":"done","type":"end","output":"${nodes.q.output}"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog})
	if len(a.Findings) != 0 || a.RequiresReview || a.HighestRisk != domain.RiskR0 || a.ReviewRisk() != domain.RiskR0 {
		t.Fatalf("read-only plan: %+v", a)
	}
	if a.SideEffects != 0 || a.EstimatedRuns != 1 {
		t.Fatalf("estimates: %+v", a)
	}
}

func TestAnalyzeApprovalCoversEveryPath(t *testing.T) {
	gated := compileSpec(t, `{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[{"to":"ok"}]},
		{"id":"ok","type":"approval","risk":"R2","next":[{"to":"restart"}]},
		{"id":"restart","type":"tool","tool":"k8s.rollout_restart","next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(gated, AnalysisOptions{Tools: catalog})
	if _, bad := findingCodes(a)["side_effect_without_approval"]; bad {
		t.Fatalf("gated restart flagged: %+v", a.Findings)
	}
	if !a.RequiresReview || a.ReviewRisk() != domain.RiskR2 || a.SideEffects != 1 || a.ApprovalNodes != 1 {
		t.Fatalf("gated plan: %+v", a)
	}

	// One branch reaches the restart around the approval: the dataflow AND
	// over incoming paths must catch it.
	bypass := compileSpec(t, `{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"prom.query","next":[
			{"to":"ok","when":"input.careful == true"},{"to":"restart"}]},
		{"id":"ok","type":"approval","risk":"R2","next":[{"to":"restart"}]},
		{"id":"restart","type":"tool","tool":"k8s.rollout_restart","next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a = Analyze(bypass, AnalysisOptions{Tools: catalog})
	f, ok := findingCodes(a)["side_effect_without_approval"]
	if !ok || f.NodeID != "restart" || f.Severity != "warning" {
		t.Fatalf("bypass not flagged: %+v", a.Findings)
	}
}

func TestAnalyzeUnknownToolIsAnError(t *testing.T) {
	fs := compileSpec(t, `{"entry":"q","nodes":[
		{"id":"q","type":"tool","tool":"made.up","next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog})
	if errs := a.Errors(); len(errs) != 1 || !strings.Contains(errs[0], "made.up") {
		t.Fatalf("errors: %v", errs)
	}
	// Without a catalogue the name cannot be checked: assume a write.
	a = Analyze(fs, AnalysisOptions{})
	if len(a.Errors()) != 0 || a.HighestRisk != domain.RiskR2 || !a.RequiresReview {
		t.Fatalf("no catalogue: %+v", a)
	}
}

func TestAnalyzeUnpinnedAgentAssumesHighestRisk(t *testing.T) {
	fs := compileSpec(t, `{"entry":"a","nodes":[
		{"id":"a","type":"agent","role":"investigator","max_steps":4,"next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog})
	codes := findingCodes(a)
	if _, ok := codes["agent_tools_unbounded"]; !ok {
		t.Fatalf("missing unbounded warning: %+v", a.Findings)
	}
	if a.HighestRisk != domain.RiskR3 || a.ReviewRisk() != domain.RiskR3 {
		t.Fatalf("risk: %+v", a)
	}
	if len(a.Tools) != 1 || a.Tools[0].Tool != "*" || a.Tools[0].Approved {
		t.Fatalf("tool uses: %+v", a.Tools)
	}
}

func TestAnalyzeR3ToolRequiresReview(t *testing.T) {
	fs := compileSpec(t, `{"entry":"ok","nodes":[
		{"id":"ok","type":"approval","risk":"R3","approvers":2,"next":[{"to":"drop"}]},
		{"id":"drop","type":"tool","tool":"db.drop_table","next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog})
	codes := findingCodes(a)
	if _, ok := codes["r3_tool"]; !ok {
		t.Fatalf("missing r3 warning: %+v", a.Findings)
	}
	if _, ok := codes["side_effect_without_approval"]; ok {
		t.Fatalf("approved R3 tool flagged as unapproved")
	}
	if a.ReviewRisk() != domain.RiskR3 {
		t.Fatalf("review risk %s", a.ReviewRisk())
	}
}

func TestAnalyzeBudgets(t *testing.T) {
	fs := compileSpec(t, `{"entry":"fan","limits":{"max_children":4},"nodes":[
		{"id":"fan","type":"parallel","branches":["a","b","c"],"next":[{"to":"j"}]},
		{"id":"a","type":"agent","role":"metrics","tools":["prom.query"],"max_steps":10},
		{"id":"b","type":"agent","role":"logs","tools":["logs.search"],"max_steps":10},
		{"id":"c","type":"tool","tool":"prom.query"},
		{"id":"j","type":"join","wait":["fan"],"next":[{"to":"done"}]},
		{"id":"done","type":"end"}]}`)
	a := Analyze(fs, AnalysisOptions{Tools: catalog, Budget: Budget{MaxChildren: 2, MaxModelCalls: 15,
		MaxOutputTokens: 1000, MaxEstimatedTokens: 5000}})
	codes := findingCodes(a)
	for _, want := range []string{"child_budget", "child_limit", "model_call_budget", "token_budget"} {
		if f, ok := codes[want]; !ok || f.Severity != "error" {
			t.Errorf("missing %s error: %+v", want, a.Findings)
		}
	}
	if a.ParallelWidth != 3 || a.EstimatedModelCalls != 20 {
		t.Fatalf("estimates: width=%d calls=%d", a.ParallelWidth, a.EstimatedModelCalls)
	}
	if !a.RequiresReview {
		t.Fatal("a plan with errors must require review")
	}

	ok := Analyze(fs, AnalysisOptions{Tools: catalog, Budget: Budget{MaxChildren: 4, MaxModelCalls: 40}})
	if errs := ok.Errors(); len(errs) != 0 {
		t.Fatalf("within budget: %v", errs)
	}
}
