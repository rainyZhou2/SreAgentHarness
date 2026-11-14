package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

type recoveryRuns struct {
	*fakeRuns
	items      []WorkRun
	failCancel bool
}

func (r *recoveryRuns) ListWorkRuns(_ context.Context, after string, limit int) ([]WorkRun, error) {
	var out []WorkRun
	for _, x := range r.items {
		if x.RunID > after && len(out) < limit {
			out = append(out, x)
		}
	}
	return out, nil
}
func (r *recoveryRuns) CancelRun(ctx context.Context, tenant, id, actor, reason, key string) error {
	if r.failCancel {
		return errors.New("kernel unavailable")
	}
	return r.fakeRuns.CancelRun(ctx, tenant, id, actor, reason, key)
}

func TestRunRecoveryCancelsAbandonedReservation(t *testing.T) {
	for _, mode := range []string{"cancel", "replan", "changed-stage", "past-final-stage", "unlinked-current", "blocked", "suspended"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, domain.RiskR1)
			w := h.create("mitigate", nil)
			stage, _ := w.CurrentStage()
			r := &recoveryRuns{fakeRuns: h.runs}
			id, err := r.ReserveRun(h.ctx, RunRequest{TenantID: w.TenantID, IdempotencyKey: "reservation"})
			if err != nil {
				t.Fatal(err)
			}
			r.items = []WorkRun{{TenantID: w.TenantID, WorkID: w.ID, RunID: id, Generation: w.Generation, StageID: stage.ID}}
			if mode == "cancel" {
				if _, err := h.command(w.ID, work.CmdCancel, nil); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "replan" {
				if _, err := h.command(w.ID, work.CmdReplan, nil); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "changed-stage" || mode == "past-final-stage" {
				next := 1
				if mode == "past-final-stage" {
					next = len(w.Template.Stages)
				}
				if _, err := h.store.ApplyWork(h.ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", ActorID: "driver", Stage: &next}); err != nil {
					t.Fatal(err)
				}
			}
			preserve := mode == "unlinked-current" || mode == "blocked" || mode == "suspended"
			if mode == "blocked" || mode == "suspended" {
				active := work.StatusActive
				var err error
				w, err = h.store.ApplyWork(h.ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", ActorID: "driver", Status: &active})
				if err != nil {
					t.Fatal(err)
				}
				status := work.StatusBlocked
				if mode == "suspended" {
					status = work.StatusSuspended
				}
				if _, err := h.store.ApplyWork(h.ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", ActorID: "driver", Status: &status}); err != nil {
					t.Fatal(err)
				}
			}
			rec := &Reconciler{Store: h.store, Runs: r, Inventory: r}
			r.failCancel = true
			err = rec.RecoverRuns(h.ctx)
			if !preserve && err == nil {
				t.Fatal("cancellation failure hidden")
			}
			r.failCancel = false
			if err := rec.RecoverRuns(h.ctx); err != nil {
				t.Fatal(err)
			}
			state, err := r.RunState(h.ctx, w.TenantID, id)
			if err != nil {
				t.Fatal(err)
			}
			if preserve {
				if state.Status != "PENDING_ACTIVATION" {
					t.Fatalf("adoptable reservation lost: %+v", state)
				}
			} else if !state.Terminal {
				t.Fatalf("abandoned reservation alive: %+v", state)
			}
		})
	}
}

func TestRunRecoveryCursorDoesNotStarveLaterReservations(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	w := h.create("mitigate", nil)
	stage, _ := w.CurrentStage()
	r := &recoveryRuns{fakeRuns: h.runs}
	for i := 0; i < 101; i++ {
		id, err := r.ReserveRun(h.ctx, RunRequest{TenantID: w.TenantID, IdempotencyKey: fmt.Sprint(i)})
		if err != nil {
			t.Fatal(err)
		}
		r.items = append(r.items, WorkRun{TenantID: w.TenantID, WorkID: w.ID, RunID: id, Generation: w.Generation, StageID: stage.ID})
	}
	sort.Slice(r.items, func(i, j int) bool { return r.items[i].RunID < r.items[j].RunID })
	r.items[100].StageID = "abandoned"
	rec := &Reconciler{Store: h.store, Runs: r, Inventory: r}
	if err := rec.RecoverRuns(h.ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.cancelled) != 0 {
		t.Fatal("cancelled current first page")
	}
	if err := rec.RecoverRuns(h.ctx); err != nil {
		t.Fatal(err)
	}
	if len(r.cancelled) != 1 || r.cancelled[0] != r.items[100].RunID {
		t.Fatal("later abandoned reservation starved")
	}
	if rec.runCursor != "" {
		t.Fatal("cursor did not wrap")
	}
}
