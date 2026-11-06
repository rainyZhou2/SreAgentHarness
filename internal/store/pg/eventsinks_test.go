package pg_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

type recSink struct {
	mu    sync.Mutex
	got   []domain.Event
	fail  map[string]int // event type → remaining failures
	perm  map[string]bool
	calls int
}

func (r *recSink) Deliver(_ context.Context, e domain.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.perm[e.Type] {
		return eventsink.Permanent(errors.New("rejected"))
	}
	if r.fail[e.Type] > 0 {
		r.fail[e.Type]--
		return errors.New("temporary")
	}
	r.got = append(r.got, e)
	return nil
}

func (r *recSink) types() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.got {
		out = append(out, e.Type)
	}
	return out
}

func dispatcher(s *pg.Store, id, owner string, sink eventsink.Sink, o eventsink.Options) *eventsink.Dispatcher {
	return &eventsink.Dispatcher{ID: id, Sink: sink, Store: s, Owner: owner, Options: o,
		Backoff: func(int) time.Duration { return time.Millisecond }}
}

// drain runs batches until the cursor stops moving.
func drain(t *testing.T, d *eventsink.Dispatcher) {
	t.Helper()
	for i := 0; i < 50; i++ {
		n, err := d.Once(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("dispatcher did not drain")
}

func TestEventSinkDeliversCommittedEventsOnceInOrder(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	early := newRun(t, s, "sink-early")

	// "latest" (default) starts after what is already committed.
	latest := &recSink{}
	dl := dispatcher(s, "latest", "w1", latest, eventsink.Options{})
	drain(t, dl)
	if n := len(latest.types()); n != 0 {
		t.Fatalf("latest sink replayed %d old events", n)
	}
	// "earliest" replays the log, with type filters.
	all := &recSink{}
	da := dispatcher(s, "earliest", "w1", all, eventsink.Options{StartFrom: "earliest", Types: []string{"run.*"}})
	drain(t, da)
	if got := all.types(); len(got) != 1 || got[0] != "run.created" {
		t.Fatalf("earliest = %v", got)
	}

	late := newRun(t, s, "sink-late")
	drain(t, dl)
	drain(t, da)
	drain(t, da) // idempotent: the cursor does not move back
	if got := all.types(); len(got) != 2 {
		t.Fatalf("earliest after second run = %v", got)
	}
	var runs []string
	for _, e := range latest.got {
		runs = append(runs, e.RunID)
	}
	for _, r := range runs {
		if r == early.ID {
			t.Fatal("latest sink delivered an event committed before it started")
		}
	}
	if len(runs) == 0 || runs[0] != late.ID {
		t.Fatalf("latest sink events = %v", latest.types())
	}
}

func TestEventSinkRetriesSkipsAndLeases(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sink := &recSink{fail: map[string]int{"run.created": 2}, perm: map[string]bool{"step.planned": true}}
	d := dispatcher(s, "flaky", "w1", sink, eventsink.Options{StartFrom: "earliest", MaxAttempts: 3})
	other := dispatcher(s, "flaky", "w2", &recSink{}, eventsink.Options{StartFrom: "earliest"})

	newRun(t, s, "sink-retry")
	drain(t, d)
	if got := sink.types(); len(got) == 0 || got[0] != "run.created" {
		t.Fatalf("temporary failures must be retried: %v", got)
	}
	for _, typ := range sink.types() {
		if typ == "step.planned" {
			t.Fatal("permanently rejected event was recorded")
		}
	}
	// w1 holds the lease: w2 cannot take the sink over.
	if n, err := other.Once(ctx); err != nil || n != 0 {
		t.Fatalf("second owner processed %d (err %v) while the lease is held", n, err)
	}
	st, err := s.EventSinkStatuses(ctx)
	if err != nil || len(st) != 1 || st[0].Skipped == 0 || st[0].Delivered == 0 {
		t.Fatalf("status = %+v %v", st, err)
	}
}
