package eval

import (
	"strings"
	"testing"

	"go-sre-harness/internal/reasoning"
)

func TestReasoningBreaches(t *testing.T) {
	good := &reasoning.Summary{N: 30, MeanQuality: 0.8, Grounding: 0.9}
	bad := &reasoning.Summary{N: 30, MeanQuality: 0.5, Grounding: 0.9}
	g := &ReasoningGuardrails{}
	if b := reasoningBreaches(g, RolloutStats{Reasoning: bad}, RolloutStats{Reasoning: good}); len(b) != 1 ||
		!strings.Contains(b[0], "reasoning vs control") {
		t.Fatalf("quality drop must breach: %v", b)
	}
	if b := reasoningBreaches(g, RolloutStats{Reasoning: good}, RolloutStats{Reasoning: bad}); len(b) != 0 {
		t.Fatalf("improvement must not breach: %v", b)
	}
	few := &reasoning.Summary{N: 5, MeanQuality: 0.1}
	if b := reasoningBreaches(g, RolloutStats{Reasoning: few}, RolloutStats{Reasoning: good}); len(b) != 0 {
		t.Fatal("too few scored canary runs must not be judged")
	}
	if b := reasoningBreaches(nil, RolloutStats{Reasoning: bad}, RolloutStats{Reasoning: good}); len(b) != 0 {
		t.Fatal("legacy rollouts without reasoning guardrails must not be judged")
	}
	if b := reasoningBreaches(&ReasoningGuardrails{Disabled: true}, RolloutStats{Reasoning: bad}, RolloutStats{Reasoning: good}); len(b) != 0 {
		t.Fatal("disabled guardrails")
	}
	if b := reasoningBreaches(g, RolloutStats{Reasoning: bad}, RolloutStats{}); len(b) != 0 {
		t.Fatal("missing control summary")
	}
	loose := &ReasoningGuardrails{GateConfig: reasoning.GateConfig{MaxQualityDrop: 0.5, MaxGroundingDrop: 1, MaxBrierIncrease: 1, MaxRedundancyIncrease: 1}}
	if b := reasoningBreaches(loose, RolloutStats{Reasoning: bad}, RolloutStats{Reasoning: good}); len(b) != 0 {
		t.Fatalf("configured margins must apply: %v", b)
	}
}
