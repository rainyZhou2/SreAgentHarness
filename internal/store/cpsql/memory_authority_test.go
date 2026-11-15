package cpsql

import (
	"context"
	"testing"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

func TestMemoryVerificationAuthority(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		m := decisionMutation(t, w)
		updated, err := s.ApplyWork(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		hs, err := s.ListHypotheses(ctx, tn, w.ID)
		if err != nil || len(hs) == 0 {
			t.Fatalf("hypotheses=%d err=%v", len(hs), err)
		}
		e := outcome.Evaluation{ID: cpkit.NewID(), SpecHash: "human", Verdict: outcome.Satisfied,
			RawVerdict: outcome.Satisfied, EvaluatedAt: t0, EvidenceRefs: []string{"evidence:outcome"}}
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ExpectedVersion: updated.Version,
			ActorType: "system", Now: t0, Outcome: &e}); err != nil {
			t.Fatal(err)
		}
		decision, err := s.MemoryDecision(ctx, tn, "decision:"+w.ID+":"+hs[0].Key)
		if err != nil || decision.WorkID != w.ID || decision.Generation != w.Generation || decision.Verdict == "" {
			t.Fatalf("decision=%+v err=%v", decision, err)
		}
		got, err := s.MemoryOutcome(ctx, tn, "outcome:"+w.ID+":"+hs[0].ID)
		if err == nil {
			t.Fatalf("foreign outcome reference was accepted: %+v", got)
		}
		outs, err := s.ListOutcomes(ctx, tn, w.ID, 1)
		if err != nil || len(outs) != 1 {
			t.Fatalf("outcomes=%+v err=%v", outs, err)
		}
		got, err = s.MemoryOutcome(ctx, tn, "outcome:"+w.ID+":"+outs[0].ID)
		if err != nil || got.Verdict != string(outcome.Satisfied) || got.WorkID != w.ID {
			t.Fatalf("outcome=%+v err=%v", got, err)
		}
		for _, ref := range []string{"", "decision:" + w.ID, "decision:other:" + hs[0].Key, "outcome:" + w.ID + ":missing"} {
			if _, err := s.MemoryDecision(ctx, tn, ref); err == nil {
				t.Fatalf("invalid decision ref %q accepted", ref)
			}
		}
	})
}
