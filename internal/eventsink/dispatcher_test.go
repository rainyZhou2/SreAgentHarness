package eventsink

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

type leaseTestStore struct {
	now, until                   time.Time
	owner                        string
	cursor                       Cursor
	events                       []Delivery
	claimErr                     error
	delivered, skipped, advances int
}

func (s *leaseTestStore) ClaimEventSink(_ context.Context, _, owner string, ttl time.Duration, _ bool) (Cursor, bool, error) {
	if s.claimErr != nil {
		return s.cursor, false, s.claimErr
	}
	if s.owner != "" && s.owner != owner && s.until.After(s.now) {
		return s.cursor, false, nil
	}
	s.owner, s.until = owner, s.now.Add(ttl)
	return s.cursor, true, nil
}
func (s *leaseTestStore) ReadEventLog(_ context.Context, after Cursor, _ int) ([]Delivery, error) {
	if s.events != nil {
		var out []Delivery
		for _, e := range s.events {
			if e.Cursor.GlobalSeq > after.GlobalSeq {
				out = append(out, e)
			}
		}
		return out, nil
	}
	if after.GlobalSeq > 0 {
		return nil, nil
	}
	return []Delivery{{Cursor: Cursor{TxID: 1, GlobalSeq: 1}, Event: domain.Event{ID: "event", Type: "run.completed"}}}, nil
}

func TestDispatcherRenewsAfterFilteredPrefix(t *testing.T) {
	s := &leaseTestStore{now: time.Unix(1, 0), events: []Delivery{
		{Cursor: Cursor{TxID: 1, GlobalSeq: 1}, Event: domain.Event{ID: "ignored", Type: "run.started"}},
		{Cursor: Cursor{TxID: 1, GlobalSeq: 2}, Event: domain.Event{ID: "selected", Type: "run.completed"}},
	}}
	calls := 0
	d := Dispatcher{ID: "sink", Owner: "owner", Store: s, Options: Options{Types: []string{"run.completed"}}, Sink: testSinkFunc(func(_ context.Context, e domain.Event) error {
		calls++
		if e.ID != "selected" {
			t.Fatalf("unexpected delivery: %s", e.ID)
		}
		return nil
	})}
	if n, err := d.Once(context.Background()); n != 2 || err != nil {
		t.Fatalf("filtered prefix: %d %v", n, err)
	}
	if calls != 1 || s.cursor.GlobalSeq != 2 || s.delivered != 1 {
		t.Fatalf("calls=%d store=%+v", calls, s)
	}
}
func (s *leaseTestStore) AdvanceEventSink(_ context.Context, _, owner string, c Cursor, delivered, skipped int, ttl time.Duration) error {
	s.advances++
	if owner != s.owner || !s.until.After(s.now) {
		return domain.ErrLeaseLost
	}
	s.cursor, s.until = c, s.now.Add(ttl)
	s.delivered += delivered
	s.skipped += skipped
	return nil
}

type testSinkFunc func(context.Context, domain.Event) error

func (f testSinkFunc) Deliver(ctx context.Context, e domain.Event) error { return f(ctx, e) }

func TestDispatcherRenewsBetweenSlowRetries(t *testing.T) {
	s := &leaseTestStore{now: time.Unix(1, 0)}
	calls := 0
	d := Dispatcher{ID: "sink", Owner: "owner", Store: s, LeaseTTL: time.Minute, Backoff: func(int) time.Duration { return 0 }, Sink: testSinkFunc(func(ctx context.Context, _ domain.Event) error {
		calls++
		s.now = s.now.Add(20 * time.Second)
		if calls < 5 {
			return errors.New("transient delivery failure")
		}
		return nil
	})}
	if n, err := d.Once(context.Background()); n != 1 || err != nil {
		t.Fatalf("slow delivery lost progress: %d %v", n, err)
	}
	if calls != 5 || s.cursor.GlobalSeq != 1 || s.delivered != 1 || s.skipped != 0 {
		t.Fatalf("calls=%d store=%+v", calls, s)
	}
}

func TestDispatcherStopsAfterRenewalFailure(t *testing.T) {
	for _, dbFailure := range []bool{false, true} {
		name := "takeover"
		if dbFailure {
			name = "database_failure"
		}
		t.Run(name, func(t *testing.T) {
			s := &leaseTestStore{now: time.Unix(1, 0)}
			calls := 0
			unavailable := errors.New("database unavailable")
			d := Dispatcher{ID: "sink", Owner: "owner", Store: s, Backoff: func(int) time.Duration { return 0 }, Sink: testSinkFunc(func(context.Context, domain.Event) error {
				calls++
				if dbFailure {
					s.claimErr = unavailable
				} else {
					s.owner = "replacement"
				}
				return errors.New("delivery result lost")
			})}
			n, err := d.Once(context.Background())
			if n != 0 || !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("lost authority: %d %v", n, err)
			}
			if dbFailure && !errors.Is(err, unavailable) {
				t.Fatalf("lost database cause: %v", err)
			}
			if calls != 1 || s.advances != 0 || s.skipped != 0 || s.cursor.GlobalSeq != 0 {
				t.Fatalf("delivery continued after authority loss: calls=%d store=%+v", calls, s)
			}
		})
	}
}
