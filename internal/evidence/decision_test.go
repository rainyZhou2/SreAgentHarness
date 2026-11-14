package evidence

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAdjudicateEvidenceDimensions(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{TenantID: "tenant", WorkID: "work", Generation: 2}
	for _, tt := range []struct{ name, quality, trust, relation, want string }{
		{"causal", QualityValid, "T2", RelCausedBy, DecisionConfirmed},
		{"support", QualityValid, "T2", RelSupports, DecisionProbable},
		{"correlation", QualityValid, "T2", RelCorrelates, DecisionInconclusive},
		{"precedence", QualityValid, "T2", RelPrecedes, DecisionInconclusive},
		{"stale", QualityStale, "T2", RelCausedBy, DecisionInconclusive},
		{"partial", QualityPartial, "T2", RelCausedBy, DecisionInconclusive},
		{"injected", QualityInjected, "T2", RelCausedBy, DecisionInconclusive},
		{"unknown quality", "", "T2", RelCausedBy, DecisionInconclusive},
		{"untrusted", QualityValid, "T4", RelCausedBy, DecisionInconclusive},
		{"unknown relation", QualityValid, "T2", "", DecisionInconclusive},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := Record{ID: "id", TenantID: scope.TenantID, WorkID: scope.WorkID, SourceRef: "obs://metric", ContentHash: "hash", CapturedAt: at, Quality: tt.quality, TrustTier: tt.trust, Relation: tt.relation}
			env := ResultEnvelope{RunID: "run", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"evidence:id"}, Status: "VERIFIED"}}}
			_, hs, err := Adjudicate(scope, []ResultEnvelope{env}, []Record{r}, at)
			if err != nil || len(hs) != 1 {
				t.Fatalf("adjudicate: %v %+v", err, hs)
			}
			if hs[0].Decision.Verdict != tt.want {
				t.Fatalf("got %+v want %s", hs[0].Decision, tt.want)
			}
			if hs[0].Decision.InputDigest == "" {
				t.Fatal("missing reproducible input digest")
			}
		})
	}
}

func TestCanonicalReferencesAndIndependence(t *testing.T) {
	at := time.Now().UTC()
	scope := DecisionScope{"tenant", "work", 1}
	r := Record{ID: "id", TenantID: "tenant", WorkID: "work", SourceRef: "obs://metric", ContentHash: "hash", CapturedAt: at, Quality: QualityValid, TrustTier: "T2", Relation: RelCausedBy, IndependenceGroup: "observation"}
	env := ResultEnvelope{RunID: "run", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"id", "evidence:id", "obs://metric"}}}}
	other := env
	other.RunID = "other"
	syn, hs, err := Adjudicate(scope, []ResultEnvelope{env, other}, []Record{r}, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(syn.Findings) != 2 || hs[0].Decision.IndependentSupportCount != 1 || len(hs[0].Supporting) != 1 || hs[0].Supporting[0] != "evidence:id" {
		t.Fatalf("duplicate observation: %+v %+v", syn, hs)
	}
	for _, foreign := range []Record{
		{ID: "id", TenantID: "foreign", WorkID: "work"},
		{ID: "id", TenantID: "tenant", WorkID: "other"},
	} {
		_, hs, err = Adjudicate(scope, []ResultEnvelope{env}, []Record{foreign}, at)
		if err != nil {
			t.Fatal(err)
		}
		if hs[0].Decision.Verdict != DecisionInconclusive {
			t.Fatal("foreign evidence accepted")
		}
	}
	copy := r
	copy.ID = "second"
	_, hs, err = Adjudicate(scope, []ResultEnvelope{{RunID: "run", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{r.SourceRef}}}}}, []Record{r, copy}, at)
	if err != nil || hs[0].Decision.Verdict != DecisionInconclusive {
		t.Fatalf("ambiguous source accepted: %+v %v", hs, err)
	}
}

func TestCausalSupportCannotOutvoteRefutation(t *testing.T) {
	at := time.Now().UTC()
	scope := DecisionScope{"tenant", "work", 1}
	ledger := []Record{
		{ID: "yes", TenantID: "tenant", WorkID: "work", ContentHash: "yes", CapturedAt: at, Quality: QualityValid, TrustTier: "T2", Relation: RelCausedBy},
		{ID: "no", TenantID: "tenant", WorkID: "work", ContentHash: "no", CapturedAt: at, Quality: QualityValid, TrustTier: "T2", Relation: RelRefutes},
	}
	envs := []ResultEnvelope{
		{RunID: "a", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"yes"}}}},
		{RunID: "b", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"yes"}}}},
		{RunID: "c", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Refutes: []string{"no"}}}},
	}
	syn, hs, err := Adjudicate(scope, envs, ledger, at)
	if err != nil || hs[0].Decision.Verdict != DecisionContested || hs[0].Status != HypChallenged || len(syn.Contested) != 1 {
		t.Fatalf("conflict: %+v %+v %v", syn, hs, err)
	}
	if len(hs[0].Decision.RequiredNextChecks) == 0 {
		t.Fatal("missing distinguishing check")
	}
}

func TestDecisionReplayAndAsOfSupersession(t *testing.T) {
	at := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	scope := DecisionScope{"t", "w", 1}
	ledger := []Record{
		{ID: "old", TenantID: "t", WorkID: "w", SourceRef: "obs://old", TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy, CapturedAt: at, ContentHash: "old"},
		{ID: "new", TenantID: "t", WorkID: "w", SourceRef: "obs://new", TrustTier: "T2", Quality: QualityValid, Relation: RelRefutes, CapturedAt: at.Add(time.Hour), ContentHash: "new", Supersedes: "old"},
	}
	envs := []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Category: CatRootCause, Status: "VERIFIED", Supports: []string{"obs://old"}}}}}
	_, hs, err := Adjudicate(scope, envs, ledger, at)
	if err != nil || hs[0].Decision.Verdict != DecisionConfirmed {
		t.Fatalf("future supersession changed historical decision: %+v %v", hs, err)
	}
	d := *hs[0].Decision
	replay, err := ReplayDecision(d)
	if err != nil || replay.InputDigest != d.InputDigest || replay.Verdict != d.Verdict || replay.IndependentSupportCount != 1 {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	d.Input.Evidence[0].Quality = QualityStale
	if _, err := ReplayDecision(d); err == nil {
		t.Fatal("tampered replay input accepted")
	}
	_, hs, err = Adjudicate(scope, envs, ledger, at.Add(2*time.Hour))
	if err != nil || hs[0].Decision.Verdict != DecisionInconclusive {
		t.Fatalf("superseded evidence still accepted: %+v %v", hs, err)
	}
}

func TestCorrelationAndRefutationRemainConsistent(t *testing.T) {
	at := time.Now().UTC()
	scope := DecisionScope{"t", "w", 1}
	ledger := []Record{
		{ID: "correlation", TenantID: "t", WorkID: "w", TrustTier: "T2", Quality: QualityValid, Relation: RelCorrelates, CapturedAt: at, ContentHash: "c"},
		{ID: "refutation", TenantID: "t", WorkID: "w", TrustTier: "T2", Quality: QualityValid, Relation: RelRefutes, CapturedAt: at, ContentHash: "r"},
	}
	syn, hs, err := Adjudicate(scope, []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"correlation"}, Refutes: []string{"refutation"}}}}}, ledger, at)
	if err != nil || syn.Findings[0].Verdict != FindingRefuted || hs[0].Status != HypRefuted || hs[0].Decision.Verdict != DecisionInconclusive {
		t.Fatalf("inconsistent: %+v %+v %v", syn, hs, err)
	}
}

func TestReplayPreservesSourceAliasSupersession(t *testing.T) {
	at := time.Now().UTC()
	scope := DecisionScope{"t", "w", 1}
	ledger := []Record{
		{ID: "old", TenantID: "t", WorkID: "w", SourceRef: "obs://old", TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy, CapturedAt: at, ContentHash: "old"},
		{ID: "new", TenantID: "t", WorkID: "w", Supersedes: "obs://old", CapturedAt: at, ContentHash: "new"},
	}
	_, hs, err := Adjudicate(scope, []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: []string{"evidence:old"}}}}}, ledger, at)
	if err != nil || len(hs) != 1 || hs[0].Decision.Verdict != DecisionInconclusive {
		t.Fatalf("adjudicate: %+v %v", hs, err)
	}
	replay, err := ReplayDecision(*hs[0].Decision)
	if err != nil || replay.Verdict != hs[0].Decision.Verdict || replay.InputDigest != hs[0].Decision.InputDigest {
		t.Fatalf("supersession lost on replay: %+v %v", replay, err)
	}
}

func TestAdjudicationPreflightsDurableAuditBound(t *testing.T) {
	at := time.Now().UTC()
	scope := DecisionScope{"t", "w", 1}
	var ledger []Record
	var refs []string
	for i := 0; i < 25; i++ {
		id := fmt.Sprintf("obs-%d", i)
		ledger = append(ledger, Record{ID: id, TenantID: "t", WorkID: "w", SourceRef: "obs://" + id, TrustTier: "T2", Quality: QualityValid, Relation: RelCausedBy, CapturedAt: at, ContentHash: id, Summary: strings.Repeat("x", 2000)})
		refs = append(refs, id)
	}
	_, hs, err := Adjudicate(scope, []ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []ResultHypothesis{{Statement: "cause", Supports: refs}}}}, ledger, at)
	if !errors.Is(err, ErrDecisionLimit) || len(hs) != 0 {
		t.Fatalf("oversized audit must be refused before commit: %d %v", len(hs), err)
	}
}
