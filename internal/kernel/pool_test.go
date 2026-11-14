package kernel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

// A claim-only store makes admission observable without running a model or
// tool; there is no runnable Step after the optional in-flight barrier.
type poolClaimStore struct {
	StateStore
	claims  atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (s *poolClaimStore) ClaimStep(context.Context, string, string, time.Duration) (domain.Lease, error) {
	s.claims.Add(1)
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return domain.Lease{}, domain.ErrNoRunnableStep
}

func poolWorker(s StateStore) *Worker {
	return &Worker{store: s, cfg: Config{MaxLocalSteps: 1, Observer: nopObserver{}}}
}

func TestPoolDrainRejectsLaterAdmission(t *testing.T) {
	s := &poolClaimStore{}
	p := NewPool(poolWorker(s), 1)
	p.Wait()
	done := make(chan bool, 1)
	p.Handle(context.Background(), "run", func(ack bool, _ time.Duration) { done <- ack })
	select {
	case ack := <-done:
		if ack {
			t.Error("drained pool acknowledged new work instead of returning it for redelivery")
		}
	case <-time.After(time.Second):
		t.Fatal("late notification did not settle")
	}
	if n := s.claims.Load(); n != 0 {
		t.Fatalf("drained pool claimed %d steps", n)
	}
}

func TestPoolDrainWaitsForInFlightExecution(t *testing.T) {
	s := &poolClaimStore{entered: make(chan struct{}), release: make(chan struct{})}
	p := NewPool(poolWorker(s), 1)
	completed := make(chan bool, 1)
	p.Handle(context.Background(), "run", func(ack bool, _ time.Duration) { completed <- ack })
	<-s.entered
	drained := make(chan struct{})
	late := make(chan bool, 1)
	go p.Handle(context.Background(), "late-run", func(ack bool, _ time.Duration) { late <- ack })
	go func() { p.Wait(); close(drained) }()
	select {
	case ack := <-late:
		if ack {
			t.Error("notification waiting for a slot was acknowledged during drain")
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not release a notification waiting for capacity")
	}
	select {
	case <-drained:
		t.Fatal("drain returned while a Step was still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(s.release)
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain did not finish after execution settled")
	}
	if !<-completed {
		t.Error("existing execution lost its normal disposition")
	}
	// Concurrent repeated drains are safe and cannot reopen admission.
	p.Wait()
}
