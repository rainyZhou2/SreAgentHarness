package cpsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

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
		calls := 0
		serialization := &pgconn.PgError{Code: "40001", Message: "callback result may already be committed"}
		s.TxRetries = 3
		if err := s.WithRunActivation(ctx, req, func(context.Context) error {
			calls++
			return serialization
		}); !errors.Is(err, serialization) || calls != 1 {
			t.Fatalf("activation callback replayed: calls=%d err=%v", calls, err)
		}
		if err := s.WithRunActivation(ctx, req, nil); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("nil activation accepted: %v", err)
		}
		// Cancelling the caller must not release the Work lock until an
		// already-entered Kernel database callback has returned.
		activationCtx, stopActivation := context.WithCancel(ctx)
		err = s.WithRunActivation(activationCtx, req, func(context.Context) error {
			stopActivation()
			cancelCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			defer stop()
			_, err := s.ApplyWork(cancelCtx, work.Mutation{TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version,
				ActorType: "system", ActorID: "cancel", Status: st(work.StatusCancelled)})
			if err == nil || !errors.Is(cancelCtx.Err(), context.DeadlineExceeded) {
				t.Errorf("caller cancellation released authority before callback returned: err=%v context=%v", err, cancelCtx.Err())
			}
			return activationCtx.Err()
		})
		stopActivation()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("activation cancellation: %v", err)
		}
		if err := s.CheckRunActivation(ctx, req); err != nil {
			t.Fatalf("failed callback changed Work or leaked the lock: %v", err)
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
				called := false
				if err := s.WithRunActivation(ctx, r, func(context.Context) error { called = true; return nil }); !errors.Is(err, tc.want) || called {
					t.Fatalf("invalid authority entered callback=%v err=%v want=%v", called, err, tc.want)
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
