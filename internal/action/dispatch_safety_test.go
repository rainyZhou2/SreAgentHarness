package action_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

func TestDispatchRechecksRuntimeAuthorization(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		for _, change := range []string{"allowlist", "scope"} {
			t.Run(change, func(t *testing.T) {
				e.clock.add(time.Second)
				ex := e.deploy("deployment.rollback", domain.RiskR1)
				rt := e.runtime(ex)
				rt.Allowed, rt.Scope = []string{ex.name}, []string{target}
				e.observe("checkout:v2", 1, e.clock.now())
				r := e.propose(rt, e.work(), ex.name, "checkout:v1")
				if change == "allowlist" {
					rt.Allowed = []string{"another.action"}
				} else {
					rt.Scope = []string{"deployment://prod/another"}
				}
				out, err := rt.Execute(e.ctx, e.tn, r.ID)
				_ = e.store.UnlockTarget(e.ctx, e.tn, r.LockKey(), r.ID)
				if !errors.Is(err, action.ErrPrecondition) || out.State != action.StateRejected || ex.callCount() != 0 {
					t.Fatalf("changed %s: state=%s err=%v executor calls=%d", change, out.State, err, ex.callCount())
				}
			})
		}
	})
}

// Simulate cancellation arriving after the dispatch intent commits but before
// the runtime invokes an external executor. The store itself remains real.
type cancelAfterDispatchStore struct {
	action.Store
	cancel        context.CancelFunc
	resultBounded bool
}

func (s *cancelAfterDispatchStore) UpdateAction(ctx context.Context, r action.Record, version int64) (action.Record, error) {
	if r.State == action.StateFailed || r.State == action.StateApplied {
		deadline, ok := ctx.Deadline()
		s.resultBounded = ok && time.Until(deadline) > 0 && time.Until(deadline) <= 30*time.Second
	}
	out, err := s.Store.UpdateAction(ctx, r, version)
	if err == nil && r.State == action.StateDispatched {
		s.cancel()
	}
	return out, err
}

func TestCancelledCallerCannotStartExecutor(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		e.observe("checkout:v2", 1, e.clock.now())
		r := e.propose(rt, e.work(), ex.name, "checkout:v1")
		ctx, cancel := context.WithCancel(e.ctx)
		defer cancel()
		store := &cancelAfterDispatchStore{Store: e.store, cancel: cancel}
		rt.Store = store
		out, err := rt.Execute(ctx, e.tn, r.ID)
		if ex.callCount() != 0 || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled dispatch: calls=%d err=%v state=%s", ex.callCount(), err, out.State)
		}
		got, err := e.store.GetAction(e.ctx, e.tn, r.ID)
		if err != nil || got.State != action.StateFailed || got.DispatchClass != action.DefinitelyNotSent || !store.resultBounded {
			t.Fatalf("cancel result: state=%s class=%s bounded=%v err=%v", got.State, got.DispatchClass, store.resultBounded, err)
		}
	})
}

func TestCancelledWorkRefusesDispatchAndReleasesTarget(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		e.observe("checkout:v2", 1, e.clock.now())
		id := e.work()
		r := e.propose(rt, id, ex.name, "checkout:v1")
		w, err := e.store.GetWork(e.ctx, e.tn, id)
		if err != nil {
			t.Fatal(err)
		}
		cancelled := work.StatusCancelled
		if _, err := e.store.ApplyWork(e.ctx, work.Mutation{TenantID: e.tn, WorkID: id, ExpectedVersion: w.Version,
			ActorType: "user", ActorID: "operator", Status: &cancelled, Now: e.clock.now()}); err != nil {
			t.Fatal(err)
		}
		out, err := rt.Execute(e.ctx, e.tn, r.ID)
		if !errors.Is(err, action.ErrPrecondition) || out.State != action.StateRejected || ex.callCount() != 0 {
			t.Fatalf("cancelled Work dispatch: state=%s err=%v calls=%d", out.State, err, ex.callCount())
		}
		locked, _, err := e.store.LockTarget(e.ctx, e.tn, r.LockKey(), cpkit.NewID(), e.clock.now().Add(time.Minute), e.clock.now())
		if err != nil || !locked {
			t.Fatalf("refused dispatch retained target lock: %v", err)
		}
	})
}

func TestResultPersistenceHasIndependentDeadline(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		e.observe("checkout:v2", 1, e.clock.now())
		r := e.propose(rt, e.work(), ex.name, "checkout:v1")
		store := &cancelAfterDispatchStore{Store: e.store, cancel: func() {}}
		rt.Store = store
		out, err := rt.Execute(e.ctx, e.tn, r.ID)
		if err != nil || out.State != action.StateApplied || !store.resultBounded {
			t.Fatalf("result persistence: %s bounded=%v err=%v", out.State, store.resultBounded, err)
		}
	})
}
