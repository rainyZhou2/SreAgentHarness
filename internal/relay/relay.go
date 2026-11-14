package relay

// Package relay publishes committed run events to the event bus.
//
// The relay is a single-writer consumer of the durable log: replicas compete
// for a leased cursor, so one of them publishes at a time and a standby
// takes over without duplicating the cursor. Publishing is at least once;
// consumers de-duplicate by event id and by run sequence.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventlog"
)

// Publisher writes one event to the bus.
type Publisher interface {
	Publish(ctx context.Context, e domain.Event) error
}

// Observer receives relay metrics.
type Observer interface {
	ObserveRelay(outcome string)
	ObserveRelayLag(d time.Duration)
}

// Relay moves events from the durable log to the bus.
type Relay struct {
	ID        string
	Owner     string
	Store     eventlog.Store
	Publisher Publisher
	Log       *slog.Logger
	Observer  Observer

	Batch    int
	Interval time.Duration
	LeaseTTL time.Duration
	// PublishTimeout bounds one bus publish so an unavailable broker cannot
	// stall the cursor.
	PublishTimeout time.Duration
	StartFrom      string
}

func (r *Relay) defaults() {
	if r.ID == "" {
		r.ID = "event-relay"
	}
	if r.Batch <= 0 {
		r.Batch = 1000
	}
	if r.Interval <= 0 {
		r.Interval = 250 * time.Millisecond
	}
	if r.LeaseTTL <= 0 {
		r.LeaseTTL = 15 * time.Second
	}
	if r.PublishTimeout <= 0 {
		r.PublishTimeout = 5 * time.Second
	}
	if r.Log == nil {
		r.Log = slog.Default()
	}
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.defaults()
	for {
		n, err := r.Once(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if !errors.Is(err, domain.ErrLeaseLost) {
				r.Log.Warn("event relay", "err", err)
			}
			if r.Observer != nil {
				r.Observer.ObserveRelay("error")
			}
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(r.Interval):
			}
		}
	}
}

// Once publishes one batch and advances the cursor.
func (r *Relay) Once(ctx context.Context) (int, error) {
	r.defaults()
	cur, ok, err := r.Store.ClaimEventSink(ctx, r.ID, r.Owner, r.LeaseTTL, r.StartFrom == "earliest")
	if err != nil || !ok {
		return 0, err
	}
	batch, err := r.Store.ReadEventLog(ctx, cur, r.Batch)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	advanced := 0
	for _, it := range batch {
		pctx, cancel := context.WithTimeout(ctx, min(r.PublishTimeout, r.LeaseTTL/2))
		err := r.Publisher.Publish(pctx, it.Event)
		cancel()
		if err != nil {
			return advanced, fmt.Errorf("publish event %s: %w", it.Event.ID, err)
		}
		// Checkpoint only published facts, renewing the lease between calls.
		// A lost commit can redeliver one event; consumers still deduplicate.
		if err := r.Store.AdvanceEventSink(ctx, r.ID, r.Owner, it.Cursor, 1, 0, r.LeaseTTL); err != nil {
			return advanced, err
		}
		advanced++
		if r.Observer != nil {
			r.Observer.ObserveRelay("published")
		}
	}
	last := batch[len(batch)-1]
	if r.Observer != nil && !last.Event.CommittedAt.IsZero() {
		r.Observer.ObserveRelayLag(time.Since(last.Event.CommittedAt))
	}
	return advanced, nil
}
