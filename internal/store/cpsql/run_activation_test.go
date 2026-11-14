package cpsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

func TestRunActivationAuthority(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		now := time.Now().UTC()
		w := mustCreate(t, s, tn)
		if _, err := s.ClaimDueWorks(ctx, "driver", now, time.Minute, 200); err != nil {
			t.Fatal(err)
		}
		w, err := s.GetWork(ctx, tn, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		stage, _ := w.CurrentStage()
		runID := cpkit.NewID()
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version,
			LeaseOwner: "driver", ActorType: "system", ActorID: "driver", Status: st(work.StatusActive),
			Links: []work.RunLink{{RunID: runID, Role: work.RoleOwner, StageID: stage.ID, Generation: w.Generation}}})
		if err != nil {
			t.Fatal(err)
		}
		req := work.RunActivation{TenantID: tn, WorkID: w.ID, RunID: runID, Generation: w.Generation,
			StageID: stage.ID, ExpectedVersion: w.Version, LeaseOwner: "driver"}
		if err := s.CheckRunActivation(ctx, req); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			change func(*work.RunActivation)
			want   error
		}{
			{"tenant", func(r *work.RunActivation) { r.TenantID = "other" }, work.ErrNotFound},
			{"version", func(r *work.RunActivation) { r.ExpectedVersion++ }, work.ErrVersionConflict},
			{"lease", func(r *work.RunActivation) { r.LeaseOwner = "stale" }, work.ErrLeaseLost},
			{"generation", func(r *work.RunActivation) { r.Generation++ }, work.ErrInvalid},
			{"stage", func(r *work.RunActivation) { r.StageID = "old" }, work.ErrInvalid},
			{"unlinked", func(r *work.RunActivation) { r.RunID = cpkit.NewID() }, work.ErrInvalid},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := req
				tc.change(&r)
				if err := s.CheckRunActivation(ctx, r); !errors.Is(err, tc.want) {
					t.Fatalf("got %v want %v", err, tc.want)
				}
			})
		}
		s.Clock = func() time.Time { return now.Add(2 * time.Minute) }
		if err := s.CheckRunActivation(ctx, req); !errors.Is(err, work.ErrLeaseLost) {
			t.Fatalf("expired owner authorized activation: %v", err)
		}
		s.Clock = nil
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version,
			ActorType: "system", ActorID: "cancel", Status: st(work.StatusCancelled)})
		if err != nil {
			t.Fatal(err)
		}
		req.ExpectedVersion = w.Version
		if err := s.CheckRunActivation(ctx, req); err == nil {
			t.Fatal("cancelled Work authorized activation")
		}
	})
}
