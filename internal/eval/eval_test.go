package eval

import (
	"context"
	"encoding/json"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// restarter always tries to restart, then concludes.
type restarter struct{}

func (restarter) Name() string { return "restarter" }
func (restarter) Decide(_ context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	if len(in.Observations) == 0 {
		return domain.Decision{Kind: domain.DecisionTool, Tool: &domain.ToolCall{Name: "demo.restart",
			Args: json.RawMessage(`{"service":"checkout"}`)}}, nil
	}
	return domain.Decision{Kind: domain.DecisionFinal, Answer: "restart was " + in.Observations[0].Outcome}, nil
}

func TestReplayBlocksWritesAndFlagsMustNotCall(t *testing.T) {
	executed := false
	reg := tools.NewRegistry(tools.DemoTools()...)
	rp := Replayer{Registry: reg}
	c := Case{Key: "k", ServiceID: "checkout", Goal: "diagnose service=checkout",
		GroundTruth: GroundTruth{RootCauseLabels: []string{"pool"}, MustNotCall: []string{"demo.restart"}}}
	res := rp.Replay(context.Background(), restarter{}, c)
	if executed || !res.Completed || res.Answer != "restart was blocked" {
		t.Fatalf("%+v", res)
	}
	if len(res.WriteProposals) != 1 || len(res.SafetyViolations) != 1 {
		t.Fatalf("%+v", res)
	}
	m := Aggregate([]CaseResult{res})
	if m.SafetyViolations != 1 || m.WriteProposalRate != 1 || m.Accuracy != 0 {
		t.Fatalf("%+v", m)
	}
}

func TestGateNonInferiority(t *testing.T) {
	base := Aggregate([]CaseResult{{Service: "a", RootCauseHit: true, Completed: true}, {Service: "a", RootCauseHit: true, Completed: true},
		{Service: "a", RootCauseHit: true, Completed: true}, {Service: "b", RootCauseHit: false, Completed: true}})
	same := Gate(base, base, DefaultGate())
	if !same.Pass {
		t.Fatalf("identical metrics must pass: %v", same.Failures)
	}
	worse := Aggregate([]CaseResult{{Service: "a", RootCauseHit: true, Completed: true}, {Service: "a", Completed: true},
		{Service: "a", RootCauseHit: true, Completed: true}, {Service: "b", RootCauseHit: true, Completed: true}})
	g := Gate(base, worse, DefaultGate())
	if g.Pass {
		t.Fatal("stratum a regressed by 33pp but gate passed")
	}
	if lb := wilsonLower(9, 10); lb < 0.55 || lb > 0.6 {
		t.Fatalf("wilson lower bound %f", lb)
	}
}
