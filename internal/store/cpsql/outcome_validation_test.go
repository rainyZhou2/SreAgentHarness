package cpsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

func TestOutcomeScopeValidation(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		for _, tc := range []struct {
			name   string
			change func(*outcome.Evaluation)
		}{
			{"tenant", func(e *outcome.Evaluation) { e.TenantID = "foreign" }},
			{"work", func(e *outcome.Evaluation) { e.WorkID = "foreign" }},
			{"generation", func(e *outcome.Evaluation) { e.Generation = w.Generation + 1 }},
			{"raw verdict", func(e *outcome.Evaluation) { e.RawVerdict = "bad" }},
			{"future", func(e *outcome.Evaluation) { e.EvaluatedAt = t0.Add(time.Minute) }},
			{"missing time", func(e *outcome.Evaluation) { e.EvaluatedAt = time.Time{} }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				e := outcome.Evaluation{SpecHash: "human", Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: t0, Generation: w.Generation}
				tc.change(&e)
				_, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Outcome: &e})
				if !errors.Is(err, work.ErrInvalid) {
					t.Fatalf("accepted invalid outcome: %v", err)
				}
				got, _ := s.GetWork(ctx, tn, w.ID)
				if got.Version != w.Version {
					t.Fatal("rejected outcome mutated Work")
				}
			})
		}
		e := outcome.Evaluation{SpecHash: "human", Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: t0}
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Outcome: &e}); err != nil {
			t.Fatal(err)
		}
		if e.TenantID != "" || e.WorkID != "" || e.ID != "" {
			t.Fatal("store mutated caller evaluation")
		}
		outs, err := s.ListOutcomes(ctx, tn, w.ID, 10)
		if err != nil || len(outs) != 1 || outs[0].Generation != w.Generation || outs[0].TenantID != tn || outs[0].WorkID != w.ID {
			t.Fatalf("persisted scope: %+v %v", outs, err)
		}
	})
}

func TestSuccessRequiresCurrentGoalOutcome(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		n := newWork(tn, cpkit.NewID())
		n.Goal.Success = &outcome.Spec{StableFor: cpkit.Duration(time.Minute), AllOf: []outcome.Objective{{ID: "ready", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}
		w, _, err := s.CreateWork(ctx, n)
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range []work.Status{work.StatusActive, work.StatusVerifying} {
			w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Status: st(status)})
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			name   string
			change func(*outcome.Evaluation)
		}{
			{"human bypass", func(e *outcome.Evaluation) { e.SpecHash = "human" }},
			{"other spec", func(e *outcome.Evaluation) { e.SpecHash = "sha256:other" }},
			{"raw failure", func(e *outcome.Evaluation) { e.RawVerdict = outcome.NotSatisfied }},
			{"missing objectives", func(e *outcome.Evaluation) { e.Objectives = nil }},
			{"contradictory objective", func(e *outcome.Evaluation) { e.Objectives[0].Verdict = outcome.NotSatisfied }},
			{"unknown objective", func(e *outcome.Evaluation) { e.Objectives[0].ID = "other" }},
			{"optional bypass", func(e *outcome.Evaluation) { e.Objectives[0].Optional = true }},
			{"stale", func(e *outcome.Evaluation) {
				e.EvaluatedAt = t0.Add(-time.Hour)
				since := e.EvaluatedAt.Add(-time.Minute)
				e.SatisfiedSince = &since
			}},
			{"missing stability", func(e *outcome.Evaluation) { e.SatisfiedSince = nil }},
			{"insufficient stability", func(e *outcome.Evaluation) { x := t0; e.SatisfiedSince = &x }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				since := t0.Add(-time.Minute)
				e := outcome.Evaluation{SpecHash: w.Goal.Success.Hash(), Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: t0, SatisfiedSince: &since, Generation: w.Generation, Objectives: []outcome.ObjectiveResult{{ID: "ready", Kind: outcome.KindState, Verdict: outcome.Satisfied}}}
				tc.change(&e)
				_, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Status: st(work.StatusSucceeded), Outcome: &e})
				if !errors.Is(err, work.ErrInvalid) {
					t.Fatalf("accepted invalid success: %v", err)
				}
			})
		}
	})
}

func TestReplanPreservesObservedOutcomeGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		next := w.Generation + 1
		e := outcome.Evaluation{SpecHash: "human", Verdict: outcome.NotSatisfied, RawVerdict: outcome.NotSatisfied, EvaluatedAt: t0, Generation: w.Generation}
		updated, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Generation: &next, Outcome: &e})
		if err != nil {
			t.Fatal(err)
		}
		outs, err := s.ListOutcomes(ctx, tn, w.ID, 10)
		if err != nil || len(outs) != 1 || outs[0].Generation != w.Generation || updated.Generation != next {
			t.Fatalf("wrong generation: %+v %+v %v", updated, outs, err)
		}
		var sqlGeneration int
		err = s.db.QueryRowContext(ctx, s.q(`SELECT generation FROM work_outcomes WHERE tenant_id=? AND work_id=?`), tn, w.ID).Scan(&sqlGeneration)
		if err != nil || sqlGeneration != w.Generation {
			t.Fatalf("SQL generation=%d: %v", sqlGeneration, err)
		}
	})
}
