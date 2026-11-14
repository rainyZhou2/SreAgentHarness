package workapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

func TestDecisionViewsUseOneInjectedClockReading(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	code, created, _ := e.do("POST", "/v1/works", "t1", "operator", map[string]any{"kind": "incident", "goal": map[string]any{"intent": "clock scoped decision"}}, map[string]string{"Idempotency-Key": "decision-clock"})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, created)
	}
	id := created["id"].(string)
	w, err := e.store.GetWork(ctx, "t1", id)
	if err != nil {
		t.Fatal(err)
	}
	// The injected horizon intentionally differs from wall time.
	at := time.Now().UTC().Add(24 * time.Hour)
	rec, _, err := e.store.AddEvidence(ctx, evidence.Record{TenantID: "t1", WorkID: id, SourceType: evidence.SourceTool, SourceRef: "experiment://clock", TrustTier: "T2", Quality: evidence.QualityValid, Relation: evidence.RelCausedBy, Kind: evidence.KindObservation, CapturedAt: at, Summary: "controlled observation"})
	if err != nil {
		t.Fatal(err)
	}
	syn, hyps, err := evidence.Adjudicate(evidence.DecisionScope{TenantID: "t1", WorkID: id, Generation: w.Generation}, []evidence.ResultEnvelope{{RunID: cpkit.NewID(), AgentRole: "investigator", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{{Statement: "cause one", Supports: []string{rec.SourceRef}}, {Statement: "cause two", Supports: []string{rec.SourceRef}}}}}, []evidence.Record{rec}, at)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.store.ApplyWork(ctx, work.Mutation{TenantID: "t1", WorkID: id, ExpectedVersion: w.Version, ActorType: "system", Now: at, Findings: syn.Findings, Hypotheses: hyps})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	a := &API{Store: e.store, Clock: func() time.Time {
		calls++
		if calls > 1 {
			return at.Add(-time.Hour)
		}
		return at
	}}
	mux := http.NewServeMux()
	a.Register(mux)
	e.mux = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), auth.Principal{Tenant: "t1", Subject: "viewer", Roles: []auth.Role{auth.Viewer}})))
	})
	for _, suffix := range []string{"/decisions", "/hypotheses"} {
		t.Run(suffix, func(t *testing.T) {
			calls = 0
			code, body, _ := e.do("GET", "/v1/works/"+id+suffix, "t1", "viewer", nil, nil)
			items, _ := body["items"].([]any)
			if code != http.StatusOK || len(items) != 2 {
				t.Fatalf("injected horizon ignored: %d %v", code, body)
			}
			if suffix == "/hypotheses" {
				for _, item := range items {
					if item.(map[string]any)["decision_current"] != true {
						t.Fatalf("decision incorrectly obsolete: %v", item)
					}
				}
			}
			if calls != 1 {
				t.Fatalf("clock read %d times, want one request horizon", calls)
			}
		})
	}
}
