package eval

import (
	"testing"

	"go-sre-harness/internal/reasoning"
)

func TestGateChecksReasoning(t *testing.T) {
	cfg := DefaultGate()
	cfg.MinCases = 1
	base := Metrics{N: 10, Accuracy: 0.8, CompletionRate: 1, Reasoning: &reasoning.Summary{N: 10, Grounding: 0.9, MeanQuality: 0.8}}
	cand := base
	cand.Reasoning = &reasoning.Summary{N: 10, Grounding: 0.5, MeanQuality: 0.8}
	if g := Gate(base, cand, cfg); g.Pass || len(g.Failures) != 1 {
		t.Fatalf("grounding regression must fail the gate: %+v", g.Failures)
	}
	cand.Reasoning = base.Reasoning
	if g := Gate(base, cand, cfg); !g.Pass {
		t.Fatalf("equal reasoning must pass: %+v", g.Failures)
	}
}
