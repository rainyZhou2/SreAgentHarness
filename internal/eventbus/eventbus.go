// Package eventbus distributes committed run events to API replicas.
//
// Delivery is best effort: the durable log in PostgreSQL is the source of
// truth and every consumer resumes from its own cursor. A dropped message
// costs latency, never correctness.
package eventbus

import (
	"context"
	"errors"
	"sync"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventlog"
)

// ErrClosed is returned when the bus is shutting down.
var ErrClosed = errors.New("event bus closed")

// Bus publishes committed events and subscribes to one run.
type Bus interface {
	// Publish fans an event out to live subscribers. Implementations must be
	// safe for concurrent use and must return before the caller commits its
	// cursor.
	Publish(ctx context.Context, e domain.Event) error
	// Subscribe delivers events published after the call for one run. The
	// channel is closed when the subscription fails or ctx is cancelled;
	// consumers then fall back to the durable log.
	Subscribe(ctx context.Context, tenantID, runID string) (<-chan domain.Event, func(), error)
	// Healthy reports whether live delivery is available.
	Healthy() bool
	// Shared reports whether the bus reaches other processes.
	Shared() bool
}

// Local is a process-local bus. It is only valid when the publisher and the
// subscribers share a process.
type Local struct {
	mu   sync.RWMutex
	subs map[string]map[chan domain.Event]struct{}
}

func NewLocal() *Local { return &Local{subs: map[string]map[chan domain.Event]struct{}{}} }

func (l *Local) Healthy() bool { return true }

func (l *Local) Shared() bool { return false }

func (l *Local) Publish(_ context.Context, e domain.Event) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for ch := range l.subs[eventlog.Subject(e.TenantID, e.RunID)] {
		select {
		case ch <- e:
		default:
		}
	}
	return nil
}

func (l *Local) Subscribe(_ context.Context, tenantID, runID string) (<-chan domain.Event, func(), error) {
	ch := make(chan domain.Event, 256)
	key := eventlog.Subject(tenantID, runID)
	l.mu.Lock()
	if l.subs[key] == nil {
		l.subs[key] = map[chan domain.Event]struct{}{}
	}
	l.subs[key][ch] = struct{}{}
	l.mu.Unlock()
	return ch, func() {
		l.mu.Lock()
		if _, ok := l.subs[key][ch]; ok {
			delete(l.subs[key], ch)
			close(ch)
			if len(l.subs[key]) == 0 {
				delete(l.subs, key)
			}
		}
		l.mu.Unlock()
	}, nil
}
