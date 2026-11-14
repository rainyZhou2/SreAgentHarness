package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

func readyAction(t *testing.T, s *Store, w work.Work) action.Record {
	t.Helper()
	p := action.Plan{ID: cpkit.NewID(), TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation,
		ActionName: "deployment.rollback", ActionVersion: "1", Risk: domain.RiskR1,
		TargetRef: "deployment://prod/checkout", Args: json.RawMessage(`{}`),
		CreatedAt: t0, ExpiresAt: t0.Add(time.Hour)}
	p.Seal()
	r, _, err := s.CreateAction(context.Background(), action.Record{Plan: p, State: action.StateReady, UpdatedAt: t0})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func dispatchAction(s *Store, r action.Record) (action.Record, error) {
	r.State, r.MayHaveDispatched, r.DispatchedAt = action.StateDispatched, true, &t0
	return s.UpdateAction(context.Background(), r, r.Version)
}

func TestDispatchRequiresCurrentWorkAuthority(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		for _, tc := range []string{"new", "cancelled", "paused", "replanned", "writes forbidden"} {
			t.Run(tc, func(t *testing.T) {
				ctx, tn := context.Background(), tenant()
				n := newWork(tn, cpkit.NewID())
				if tc == "writes forbidden" {
					n.Goal.Constraints.ProductionWrites = work.WritesForbidden
				}
				w, _, err := s.CreateWork(ctx, n)
				if err != nil {
					t.Fatal(err)
				}
				if tc != "new" {
					w, err = s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Status: st(work.StatusActive), Now: t0})
					if err != nil {
						t.Fatal(err)
					}
				}
				r := readyAction(t, s, w)
				m := work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0}
				switch tc {
				case "cancelled":
					m.Status = st(work.StatusCancelled)
				case "paused":
					m.Status = st(work.StatusSuspended)
				case "replanned":
					generation := w.Generation + 1
					m.Generation = &generation
				}
				if tc == "cancelled" || tc == "paused" || tc == "replanned" {
					if _, err := s.ApplyWork(ctx, m); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := dispatchAction(s, r); !errors.Is(err, action.ErrPrecondition) {
					t.Fatalf("%s dispatch returned %v, want precondition refusal", tc, err)
				}
				got, err := s.GetAction(ctx, tn, r.ID)
				if err != nil || got.State != action.StateReady || got.Version != r.Version {
					t.Fatalf("rejected dispatch changed action: %+v %v", got, err)
				}
			})
		}
	})
}

func TestDispatchAndCancellationSerialize(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		for i := 0; i < 12; i++ {
			w := mustCreate(t, s, tn)
			w, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Status: st(work.StatusActive), Now: t0})
			if err != nil {
				t.Fatal(err)
			}
			r := readyAction(t, s, w)
			start := make(chan struct{})
			var wg sync.WaitGroup
			var dispatchErr, cancelErr error
			var atCancel action.Record
			wg.Add(2)
			go func() { defer wg.Done(); <-start; _, dispatchErr = dispatchAction(s, r) }()
			go func() {
				defer wg.Done()
				<-start
				cancelErr = s.tx(ctx, func(tx *sql.Tx) error {
					if _, err := s.lockWork(ctx, tx, tn, w.ID); err != nil {
						return err
					}
					var err error
					atCancel, err = scanRecord(tx.QueryRowContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE tenant_id=? AND id=?`), tn, r.ID))
					if err != nil {
						return err
					}
					_, err = tx.ExecContext(ctx, s.q(`UPDATE agent_works SET status='CANCELLED', version=version+1 WHERE tenant_id=? AND id=?`), tn, w.ID)
					return err
				})
			}()
			close(start)
			wg.Wait()
			if cancelErr != nil {
				t.Fatal(cancelErr)
			}
			if atCancel.State == action.StateReady && !errors.Is(dispatchErr, action.ErrPrecondition) {
				t.Fatalf("cancel won but dispatch accepted: %v", dispatchErr)
			}
			if atCancel.State == action.StateDispatched && dispatchErr != nil {
				t.Fatalf("dispatch won but failed: %v", dispatchErr)
			}
			if dispatchErr == nil {
				// A cancelled Work cannot erase the result of an authorized dispatch.
				got, err := s.GetAction(ctx, tn, r.ID)
				if err != nil {
					t.Fatal(err)
				}
				got.State, got.DispatchClass = action.StateApplied, action.ConfirmedApplied
				if _, err := s.UpdateAction(ctx, got, got.Version); err != nil {
					t.Fatalf("historical result blocked after cancel: %v", err)
				}
			}
		}
	})
}

func TestDispatchedResultSurvivesWorkCancellation(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		w, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn,
			WorkID: w.ID, ExpectedVersion: w.Version, Status: st(work.StatusActive), Now: t0})
		if err != nil {
			t.Fatal(err)
		}
		r, err := dispatchAction(s, readyAction(t, s, w))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "user", ActorID: "operator", TenantID: tn,
			WorkID: w.ID, ExpectedVersion: w.Version, Status: st(work.StatusCancelled), Now: t0}); err != nil {
			t.Fatal(err)
		}
		// A recovery inspection can settle a historical dispatch even though
		// the owning Work no longer authorizes any new execution.
		r.State, r.DispatchClass, r.Result = action.StateApplied, action.ConfirmedApplied, json.RawMessage(`{"inspected":true}`)
		if _, err := s.UpdateAction(ctx, r, r.Version); err != nil {
			t.Fatalf("historical result rejected: %v", err)
		}
		got, err := s.GetAction(ctx, tn, r.ID)
		var result struct {
			Inspected bool `json:"inspected"`
		}
		decodeErr := json.Unmarshal(got.Result, &result)
		if err != nil || got.State != action.StateApplied || decodeErr != nil || !result.Inspected {
			t.Fatalf("historical result missing: %+v %v", got, err)
		}
	})
}
