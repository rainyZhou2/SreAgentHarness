package evidence

import (
	"testing"
	"time"
)

func TestSupersessionReferenceIdentitySurvivesReplay(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{TenantID: "tenant", WorkID: "work", Generation: 1}
	for _, tc := range []struct {
		name           string
		supersedes     string
		includeID      bool
		citeCorrection bool
		want           string
	}{
		{"canonical ID wins over source alias", "evidence:other", true, false, DecisionConfirmed},
		{"raw ID wins over source alias", "other", true, false, DecisionConfirmed},
		{"missing canonical ID cannot become source alias", "evidence:other", false, false, DecisionConfirmed},
		{"raw source alias still supersedes", "other", false, false, DecisionInconclusive},
		{"cited canonical superseder preserves identity", "evidence:other", true, true, DecisionConfirmed},
		{"cited raw superseder preserves identity", "other", true, true, DecisionConfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := Record{ID: "old", TenantID: scope.TenantID, WorkID: scope.WorkID,
				SourceType: SourceMetric, SourceRef: "other", ContentHash: "old", CapturedAt: at,
				TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy}
			other := old
			other.ID, other.SourceRef, other.ContentHash = "other", "obs://other", "other"
			correction := old
			correction.ID, correction.SourceRef, correction.ContentHash = "correction", "obs://correction", "correction"
			correction.Supersedes = tc.supersedes
			ledger := []Record{old, correction}
			if tc.includeID {
				ledger = append(ledger, other)
			}
			envs := []ResultEnvelope{{RunID: "run", AgentRole: "investigator", Status: "COMPLETED",
				Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"evidence:old"}}}}}
			if tc.citeCorrection {
				envs[0].Hypotheses[0].Supports = append(envs[0].Hypotheses[0].Supports, "evidence:correction")
			}
			_, hypotheses, err := Adjudicate(scope, envs, ledger, at)
			if err != nil || len(hypotheses) != 1 {
				t.Fatalf("adjudicate: %+v %v", hypotheses, err)
			}
			h := hypotheses[0]
			if h.Decision.Verdict != tc.want {
				t.Fatalf("got %s, want %s", h.Decision.Verdict, tc.want)
			}
			if !h.CurrentDecisionFor(scope) {
				replayed, err := ReplayDecision(*h.Decision)
				t.Fatalf("fresh decision cannot replay: stored=%s replay=%s err=%v", h.Decision.Verdict, replayed.Verdict, err)
			}
			if _, err := ValidateAdjudication(h, ledger); err != nil {
				t.Fatalf("fresh decision does not match durable evidence: %v", err)
			}
		})
	}
}

func TestSupersessionIdentityAnchorsRemainTransitive(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{TenantID: "tenant", WorkID: "work", Generation: 1}
	for _, cyclic := range []bool{false, true} {
		old := Record{ID: "old", TenantID: scope.TenantID, WorkID: scope.WorkID,
			SourceType: SourceMetric, SourceRef: "last", ContentHash: "old", CapturedAt: at,
			TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy}
		correction := old
		correction.ID, correction.SourceRef, correction.ContentHash, correction.Supersedes = "correction", "obs://correction", "correction", "middle"
		middle := old
		middle.ID, middle.SourceRef, middle.ContentHash, middle.Supersedes = "middle", "obs://middle", "middle", "last"
		last := old
		last.ID, last.SourceRef, last.ContentHash = "last", "obs://last", "last"
		if cyclic {
			last.Supersedes = "middle"
		}
		ledger := []Record{old, correction, middle, last}
		envs := []ResultEnvelope{{RunID: "run", AgentRole: "investigator", Status: "COMPLETED",
			Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"evidence:old", "evidence:correction"}}}}}
		_, hypotheses, err := Adjudicate(scope, envs, ledger, at)
		if err != nil || len(hypotheses) != 1 {
			t.Fatalf("cyclic=%v adjudicate: %+v %v", cyclic, hypotheses, err)
		}
		h := hypotheses[0]
		if h.Decision.Verdict != DecisionConfirmed || h.Decision.IndependentSupportCount != 2 || !h.CurrentDecisionFor(scope) {
			t.Fatalf("cyclic=%v transitive supersession changed support on replay: %+v", cyclic, h.Decision)
		}
		if _, err := ValidateAdjudication(h, ledger); err != nil {
			t.Fatalf("cyclic=%v durable evidence mismatch: %v", cyclic, err)
		}
	}
}
