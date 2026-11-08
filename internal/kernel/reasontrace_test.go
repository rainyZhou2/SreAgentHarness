package kernel

import (
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/reasoning"
)

func TestReasonTraceCarriesHypothesisID(t *testing.T) {
	d := domain.Decision{Kind: domain.DecisionFinal, Answer: "a", ReasonSummary: "r",
		Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "x", Status: "VERIFIED", ConfidenceEstimate: 0.8}, {Statement: "y"}}}
	th, _, _ := reasonTrace(d, cognitive.ReasoningInput{}, "t", nil)
	var got []map[string]any
	for _, x := range th {
		if x.Kind == domain.ThoughtHypothesis {
			got = append(got, x.Attrs)
		}
	}
	if len(got) != 2 || got[0][reasoning.HypothesisIDAttr] != "h1" || got[1] != nil {
		t.Fatalf("attrs: %v", got)
	}
}

func TestReasonTraceResolvesCitations(t *testing.T) {
	d := domain.Decision{Kind: domain.DecisionFinal, Answer: "a", ReasonSummary: "r", EvidenceRefs: []string{"obs:3", "made-up"},
		Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "x", Status: "VERIFIED", ConfidenceEstimate: 0.8,
			Supports: []string{"obs:prom.query"}, Refutes: []string{"obs:step7"}}}}
	in := cognitive.ReasoningInput{Observations: []domain.Observation{{StepSeq: 3, Tool: "prom.query"}}}
	th, _, unresolved := reasonTrace(d, in, "t", nil)
	if unresolved != 2 { // "made-up" (evidence, resolved once) and "obs:step7"
		t.Fatalf("unresolved=%d", unresolved)
	}
	for _, x := range th {
		if x.Kind != domain.ThoughtHypothesis {
			continue
		}
		if len(x.Refs) != 2 || x.Refs[0].Ref != "obs:step3" || x.Refs[1].Resolved() {
			t.Fatalf("hypothesis refs: %+v", x.Refs)
		}
	}
	m := reasoning.Score(reasoning.Trace{Completed: true, Steps: []reasoning.Step{{Reason: true, Thoughts: th}}})
	if m.Unresolved != 2 || m.UnsupportedVerdicts != 0 {
		t.Fatalf("score: %+v", m)
	}
}
