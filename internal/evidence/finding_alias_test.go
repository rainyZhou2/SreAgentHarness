package evidence

import (
	"slices"
	"testing"
	"time"
)

func TestMultipleHypothesisGapsRemainIndependentAndReplayable(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{TenantID: "tenant", WorkID: "work", Generation: 1}
	env := ParseResult("run", "investigator", "COMPLETED", []byte(`{
		"uncertainties": ["source coverage", "time window", "alternative causes"],
		"hypotheses": [
			{"statement": "correlation hypothesis", "supports": ["evidence:observation"]},
			{"statement": "unsupported hypothesis", "status": "VERIFIED"}
		]
	}`))
	ledger := []Record{{ID: "observation", TenantID: scope.TenantID, WorkID: scope.WorkID,
		SourceRef: "obs://metric", ContentHash: "observation", CapturedAt: at,
		TrustTier: "T2", Quality: QualityValid, Relation: RelCorrelates}}
	syn, hypotheses, err := Adjudicate(scope, []ResultEnvelope{env}, ledger, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(syn.Findings) != 2 || len(hypotheses) != 2 {
		t.Fatalf("expected two findings and hypotheses: %+v %+v", syn, hypotheses)
	}
	correlationGap := "the cited evidence only correlates; compare versions or observe a rollback"
	missingGap := "the conclusion cites no usable evidence"
	if !slices.Contains(syn.Findings[0].Gaps, correlationGap) || slices.Contains(syn.Findings[0].Gaps, missingGap) {
		t.Fatalf("another hypothesis changed correlation gaps: %v", syn.Findings[0].Gaps)
	}
	if !slices.Contains(syn.Findings[1].Gaps, missingGap) || slices.Contains(syn.Findings[1].Gaps, correlationGap) {
		t.Fatalf("another hypothesis changed missing-evidence gaps: %v", syn.Findings[1].Gaps)
	}
	for _, h := range hypotheses {
		if !h.CurrentDecisionFor(scope) {
			t.Errorf("freshly adjudicated hypothesis cannot replay: %q", h.Statement)
		}
	}
}
