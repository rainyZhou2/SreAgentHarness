package evidence

import (
	"slices"
	"testing"
	"time"
)

func TestAdjudicationIndependentOfAgentArrivalOrder(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	records := []Record{{ID: "e", TenantID: "t", WorkID: "w", CapturedAt: at, ContentHash: "e", Quality: QualityValid, TrustTier: "T2", Relation: RelCausedBy}}
	envs := []ResultEnvelope{
		{RunID: "b", AgentRole: "rca", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "same cause", Category: CatRootCause, Supports: []string{"e"}}}},
		{RunID: "a", AgentRole: "diagnosis", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "same cause", Category: CatContributing, Supports: []string{"e"}}}},
	}
	_, a, err := Adjudicate(scope, envs, records, at)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(envs)
	_, b, err := Adjudicate(scope, envs, records, at)
	if err != nil {
		t.Fatal(err)
	}
	if a[0].Category != "" || b[0].Category != "" {
		t.Fatal("conflicting causal roles silently selected by arrival order")
	}
	if a[0].Decision.InputDigest != b[0].Decision.InputDigest {
		t.Fatal("arrival order changes decision digest")
	}
	if !a[0].CurrentDecisionFor(scope) || !b[0].CurrentDecisionFor(scope) {
		t.Fatal("canonical decision cannot replay")
	}
}

func TestVersionTwoDecisionRemainsHistoricallyReplayable(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	_, hs, err := adjudicate(scope, []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause"}}}}, nil, at, "2")
	if err != nil {
		t.Fatal(err)
	}
	d := *hs[0].Decision
	replayed, err := ReplayDecision(d)
	if err != nil || replayed.InputDigest != d.InputDigest || replayed.AdjudicatorVersion != "2" {
		t.Fatalf("legacy replay: %+v %v", replayed, err)
	}
	if hs[0].CurrentDecisionFor(scope) {
		t.Fatal("legacy adjudication is current without migration")
	}
}
