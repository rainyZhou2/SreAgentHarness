package cpsql

import (
	"context"
	"testing"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

func TestWatchEventConsumptionIsBoundToGeneration(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		build := func(generation int) continuous.Watch {
			t.Helper()
			wt, err := continuous.Build(continuous.Spec{Mode: continuous.OnEvent, Predicate: &eventing.Predicate{EventTypes: []string{"anomaly"}}}, tn, w.ID, generation, w.Goal.SubjectRefs, t0)
			if err != nil {
				t.Fatal(err)
			}
			return wt
		}
		add := func(generation int, watches ...continuous.Watch) {
			t.Helper()
			var err error
			w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Generation: &generation, Watches: watches})
			if err != nil {
				t.Fatal(err)
			}
		}
		fire := func(wt continuous.Watch, event string, want bool) {
			t.Helper()
			before, err := s.GetWork(ctx, tn, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			oldWatch, err := s.GetWatch(ctx, tn, wt.ID)
			if err != nil {
				t.Fatal(err)
			}
			oldFirings, err := s.ListFirings(ctx, tn, wt.ID)
			if err != nil {
				t.Fatal(err)
			}
			got, err := s.FireWatch(ctx, continuous.Firing{TenantID: tn, WorkID: w.ID, WatchID: wt.ID, FiringID: event, Kind: "event", FiredAt: t0})
			if err != nil || got != want {
				t.Errorf("generation %d event %s fired=%v want=%v err=%v", wt.Generation, event, got, want, err)
			}
			if want {
				return
			}
			after, err := s.GetWork(ctx, tn, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			newWatch, err := s.GetWatch(ctx, tn, wt.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Version != before.Version || after.LastSeq != before.LastSeq || newWatch.Version != oldWatch.Version || newWatch.Status != oldWatch.Status {
				t.Error("rejected firing mutated Work/watch")
			}
			newFirings, err := s.ListFirings(ctx, tn, wt.ID)
			if err != nil || len(newFirings) != len(oldFirings) {
				t.Fatalf("rejected firing changed the ledger: %+v %v", newFirings, err)
			}
		}
		first, sibling, stale := build(1), build(1), build(1)
		add(1, first, sibling, stale)
		fire(first, "event:old", true)
		fire(sibling, "event:old", true) // same generation may have multiple matching watches
		next, nextSibling, future := build(2), build(2), build(3)
		add(2, next, nextSibling, future)
		fire(next, "event:old", false)
		fire(next, "event:new", true)
		fire(nextSibling, "event:new", true)
		fire(stale, "event:late", false)
		fire(future, "event:future", false)
		terminal := build(2)
		add(2, terminal)
		var err error
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0, Status: st(work.StatusCancelled)})
		if err != nil {
			t.Fatal(err)
		}
		fire(terminal, "event:closed", false)
		// Consumption is scoped to a Work, so other Works can consume the
		// same tenant inbox event independently.
		w = mustCreate(t, s, tn)
		other := build(w.Generation)
		add(w.Generation, other)
		fire(other, "event:old", true)
	})
}
