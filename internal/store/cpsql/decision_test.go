package cpsql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

func TestLegacyHypothesisWritesFailClosed(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		h := evidence.Hypothesis{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, Statement: "bad deploy", Supporting: []string{"invented"}, Status: evidence.HypSupported}
		got, err := s.UpsertHypothesis(ctx, h, 0)
		if err != nil || got.Status != evidence.HypInconclusive || got.Decision != nil {
			t.Fatalf("unadjudicated references must fail closed: %+v, %v", got, err)
		}
		got.Decision = &evidence.DecisionVerdict{DecisionScope: evidence.DecisionScope{TenantID: tn, WorkID: w.ID, Generation: w.Generation},
			DecisionKey: got.Key, Profile: evidence.DecisionProfile, ProfileVersion: 1, AdjudicatorVersion: evidence.AdjudicatorVersion,
			Verdict: evidence.DecisionConfirmed, IndependentSupportCount: 1, AcceptedEvidenceRefs: []string{"invented"}, InputDigest: "forged", AdjudicatedAt: t0}
		updated, err := s.UpsertHypothesis(ctx, got, got.Version)
		if err != nil || updated.Status != evidence.HypInconclusive || updated.Decision != nil {
			t.Fatalf("legacy API trusted a caller verdict: %+v %v", updated, err)
		}
	})
}

func decisionMutation(t *testing.T, w work.Work) work.Mutation {
	t.Helper()
	scope := evidence.DecisionScope{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation}
	syn, hs, err := evidence.Adjudicate(scope, []evidence.ResultEnvelope{{RunID: cpkit.NewID(), AgentRole: "investigator", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{{Statement: "bad deploy"}, {Statement: "resource pressure"}}}}, nil, t0)
	if err != nil {
		t.Fatal(err)
	}
	return work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", Now: t0,
		Findings: syn.Findings, Hypotheses: hs, StageState: json.RawMessage(`{"adjudicated":true}`),
		Events: []work.NewEvent{{Type: "test.stage_advanced"}}}
}

func assertDecisionRollback(t *testing.T, s *Store, w work.Work) {
	t.Helper()
	ctx := context.Background()
	got, err := s.GetWork(ctx, w.TenantID, w.ID)
	if err != nil || got.Version != w.Version || got.LastSeq != w.LastSeq || string(got.StageState) != string(w.StageState) {
		t.Fatalf("work changed after failed batch: %+v %v", got, err)
	}
	fs, err := s.ListFindings(ctx, w.TenantID, w.ID, 100)
	if err != nil || len(fs) != 0 {
		t.Fatalf("partial findings: %+v %v", fs, err)
	}
	hs, err := s.ListHypotheses(ctx, w.TenantID, w.ID)
	if err != nil || len(hs) != 0 {
		t.Fatalf("partial decisions: %+v %v", hs, err)
	}
	evs, err := s.ListWorkEvents(ctx, w.TenantID, w.ID, w.LastSeq, 100)
	if err != nil || len(evs) != 0 {
		t.Fatalf("partial events: %+v %v", evs, err)
	}
}

func TestDecisionBatchCommitAndRetry(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		m := decisionMutation(t, w)
		got, err := s.ApplyWork(ctx, m)
		if err != nil || got.Version != w.Version+1 {
			t.Fatalf("commit: %+v %v", got, err)
		}
		fs, err := s.ListFindings(ctx, tn, w.ID, 100)
		if err != nil || len(fs) != 2 {
			t.Fatalf("findings: %+v %v", fs, err)
		}
		hs, err := s.ListHypotheses(ctx, tn, w.ID)
		if err != nil || len(hs) != 2 {
			t.Fatalf("hypotheses: %+v %v", hs, err)
		}
		for _, h := range hs {
			if !cpkit.ValidID(h.ID) || h.Version != 1 || !h.Decision.CurrentFor(evidence.DecisionScope{TenantID: tn, WorkID: w.ID, Generation: w.Generation}, h.Key) {
				t.Fatalf("missing persisted decision: %+v", h)
			}
		}
		evs, err := s.ListWorkEvents(ctx, tn, w.ID, w.LastSeq, 100)
		if err != nil || len(evs) != 2 || evs[1].Type != "work.decision_adjudicated" {
			t.Fatalf("events: %+v %v", evs, err)
		}
		if _, err := s.ApplyWork(ctx, m); !errors.Is(err, work.ErrVersionConflict) {
			t.Fatalf("retry must not duplicate: %v", err)
		}
		m.ExpectedVersion, m.Hypotheses = got.Version, hs
		updated, err := s.ApplyWork(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		fs, err = s.ListFindings(ctx, tn, w.ID, 100)
		if err != nil || len(fs) != 2 {
			t.Fatalf("finding dedup: %+v %v", fs, err)
		}
		hs, err = s.ListHypotheses(ctx, tn, w.ID)
		if err != nil || len(hs) != 2 || hs[0].Version != 2 {
			t.Fatalf("CAS update: %+v %v", hs, err)
		}
		m.ExpectedVersion = updated.Version
		if _, err := s.ApplyWork(ctx, m); !errors.Is(err, evidence.ErrVersionConflict) {
			t.Fatalf("stale hypothesis CAS: %v", err)
		}
	})
}

func TestDecisionBatchRollback(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		for _, failure := range []string{"second finding", "second hypothesis"} {
			t.Run(failure, func(t *testing.T) {
				w := mustCreate(t, s, tenant())
				m := decisionMutation(t, w)
				if failure == "second finding" {
					m.Findings[1].Verdict = "invalid"
				} else {
					m.Hypotheses[1].Version = 99
				}
				if _, err := s.ApplyWork(context.Background(), m); err == nil {
					t.Fatal("invalid batch accepted")
				}
				assertDecisionRollback(t, s, w)
			})
		}
	})
}

func TestDecisionBatchRejectsStaleAndForeignWrites(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		for _, failure := range []string{"no work CAS", "stale work CAS", "lost lease", "expired lease", "stale generation", "foreign decision", "foreign hypothesis", "foreign finding", "finding generation", "missing decision", "empty decision key", "generation change"} {
			t.Run(failure, func(t *testing.T) {
				w := mustCreate(t, s, tenant())
				m := decisionMutation(t, w)
				switch failure {
				case "no work CAS":
					m.ExpectedVersion = 0
				case "stale work CAS":
					m.ExpectedVersion++
				case "lost lease":
					m.LeaseOwner = "absent"
				case "expired lease":
					m.LeaseOwner = "owner"
					if _, err := s.db.ExecContext(context.Background(), s.q(`UPDATE agent_works SET lease_owner=?, lease_until=? WHERE tenant_id=? AND id=?`), "owner", s.t(t0.Add(-time.Second)), w.TenantID, w.ID); err != nil {
						t.Fatal(err)
					}
				case "stale generation":
					m.Hypotheses[1].Decision.Generation++
				case "foreign decision":
					m.Hypotheses[1].Decision.TenantID = "other"
				case "foreign hypothesis":
					m.Hypotheses[1].WorkID = cpkit.NewID()
				case "foreign finding":
					m.Findings[1].TenantID = "other"
				case "finding generation":
					m.Findings[1].Generation++
				case "missing decision":
					m.Hypotheses[1].Decision = nil
				case "empty decision key":
					m.Hypotheses[1].Key, m.Hypotheses[1].Decision.DecisionKey = "", ""
				case "generation change":
					gen := w.Generation + 1
					m.Generation = &gen
				}
				if _, err := s.ApplyWork(context.Background(), m); err == nil {
					t.Fatal("unsafe batch accepted")
				}
				assertDecisionRollback(t, s, w)
			})
		}
	})
}

func TestLegacyHypothesisJSONRead(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		h, err := s.UpsertHypothesis(ctx, evidence.Hypothesis{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, Statement: "legacy"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		body := map[string]any{"id": h.ID, "tenant_id": tn, "work_id": w.ID, "key": h.Key, "statement": h.Statement, "status": "SUPPORTED"}
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE work_hypotheses SET record=? WHERE tenant_id=? AND work_id=?`), js(body), tn, w.ID); err != nil {
			t.Fatal(err)
		}
		hs, err := s.ListHypotheses(ctx, tn, w.ID)
		if err != nil || len(hs) != 1 || hs[0].Decision != nil || hs[0].Version != 1 || hs[0].Status != evidence.HypInconclusive {
			t.Fatalf("legacy read: %+v %v", hs, err)
		}
	})
}
