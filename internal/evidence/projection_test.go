package evidence

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestCanonicalDecisionProjectionRemainsReplayable(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	records := []Record{{ID: "e", TenantID: "t", WorkID: "w", CapturedAt: at, ContentHash: "e", Quality: QualityValid, TrustTier: "T2", Relation: RelCausedBy}}
	for _, statements := range [][2]string{{"same cause", "same cause"}, {"same cause", "Same cause"}} {
		envs := []ResultEnvelope{
			{RunID: "same", Status: "COMPLETED", Summary: "a", Hypotheses: []ResultHypothesis{{Statement: statements[0], Category: CatRootCause, Supports: []string{"e"}}}},
			{RunID: "same", Status: "COMPLETED", Summary: "z", Hypotheses: []ResultHypothesis{{Statement: statements[1], Category: CatContributing, Supports: []string{"e"}}}},
		}
		_, hs, err := Adjudicate(scope, envs, records, at)
		if err != nil {
			t.Fatal(err)
		}
		if !hs[0].CurrentDecisionFor(scope) || !hs[0].ValidDecisionFor(scope, records, at) {
			t.Fatalf("fresh decision cannot replay after projection: %+v", hs[0])
		}
		if _, err := ValidateAdjudication(hs[0], records); err != nil {
			t.Fatalf("fresh decision cannot persist: %v", err)
		}
		slices.Reverse(envs)
		_, reordered, err := Adjudicate(scope, envs, records, at)
		if err != nil {
			t.Fatal(err)
		}
		if !sameClaim(hs[0], reordered[0]) || !reflect.DeepEqual(hs[0].Decision, reordered[0].Decision) {
			t.Fatal("arrival order changed canonical claim or verdict")
		}
	}
}

func TestVersionTwoProjectionPreservesHistoricalOrdering(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	envs := []ResultEnvelope{
		{RunID: "same", Status: "COMPLETED", Summary: "a", Hypotheses: []ResultHypothesis{{Statement: "same cause", Category: CatRootCause}}},
		{RunID: "same", Status: "COMPLETED", Summary: "z", Hypotheses: []ResultHypothesis{{Statement: "Same cause", Category: CatContributing}}},
	}
	_, hs, err := adjudicate(scope, envs, nil, at, "2")
	if err != nil {
		t.Fatal(err)
	}
	original := *hs[0].Decision
	if len(original.Input.Results) != 2 || original.Input.Results[0].Hypotheses[0].Statement != "same cause" || original.Input.Results[1].Hypotheses[0].Statement != "Same cause" {
		t.Fatal("v2 historical input projection was reordered")
	}
	got, err := ReplayDecision(original)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, original) {
		t.Fatal("v2 historical ordering or digest changed")
	}
	if hs[0].Statement != "same cause" || hs[0].Category != CatRootCause {
		t.Fatal("v2 historical first-claim selection changed")
	}
}
