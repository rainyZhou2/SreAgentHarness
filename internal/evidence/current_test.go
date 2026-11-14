package evidence

import (
	"testing"
	"time"
)

func TestCurrentDecisionRevalidatesLiveLedger(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	record := Record{ID: "old", TenantID: "t", WorkID: "w", SourceRef: "obs://cause", ContentHash: "original", CapturedAt: at, TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy}
	envs := []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{record.SourceRef}}}}}
	_, hs, err := Adjudicate(scope, envs, []Record{record}, at)
	if err != nil {
		t.Fatal(err)
	}
	h := hs[0]
	for _, tc := range []struct {
		name   string
		ledger []Record
		valid  bool
	}{
		{"unchanged", []Record{record}, true},
		{"removed", nil, false},
		{"superseded later", []Record{record, {ID: "new", TenantID: "t", WorkID: "w", CapturedAt: at.Add(time.Minute), Supersedes: "old"}}, false},
		{"ambiguous alias later", []Record{record, {ID: "other", TenantID: "t", WorkID: "w", SourceRef: record.SourceRef, CapturedAt: at.Add(time.Minute)}}, false},
		{"unrelated", []Record{record, {ID: "other", TenantID: "t", WorkID: "w", SourceRef: "obs://different", CapturedAt: at.Add(time.Minute)}}, true},
		{"foreign supersession", []Record{record, {ID: "new", TenantID: "foreign", WorkID: "w", CapturedAt: at.Add(time.Minute), Supersedes: "old"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.ValidDecisionFor(scope, tc.ledger, at.Add(2*time.Minute)); got != tc.valid {
				t.Fatalf("valid=%v want %v", got, tc.valid)
			}
			if !h.CurrentDecisionFor(scope) {
				t.Fatal("live validation changed historical replay")
			}
		})
	}
	changed := record
	changed.Quality = QualityStale
	if h.ValidDecisionFor(scope, []Record{changed}, at.Add(time.Minute)) {
		t.Fatal("stale ledger record accepted")
	}
	if h.ValidDecisionFor(scope, []Record{record}, at.Add(-time.Minute)) {
		t.Fatal("future verdict accepted")
	}
	if h.ValidDecisionFor(scope, make([]Record, 1000), at.Add(time.Minute)) {
		t.Fatal("bounded, possibly truncated ledger accepted")
	}
}
