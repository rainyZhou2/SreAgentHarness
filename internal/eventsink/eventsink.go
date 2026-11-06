// Package eventsink delivers committed run events to external systems
// (Slack, webhooks, paging, SIEM) — the event_sink plugin kind.
//
// Delivery is driven from the durable event log, not from in-process
// callbacks: each sink has a cursor row in PostgreSQL, so events are
// delivered at least once, in commit order, across restarts and scheduler
// replicas (one replica owns a sink at a time through a lease). A sink must
// therefore be idempotent; Event.ID is a stable de-duplication key.
package eventsink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
)

// Sink receives events. Returning an error retries the event (with backoff,
// up to Options.MaxAttempts); wrap it with Permanent to skip retries.
type Sink interface {
	Deliver(ctx context.Context, e domain.Event) error
}

// Options are the framework-level delivery settings. Built-in sinks embed
// them in their config, so they appear as top-level config fields.
type Options struct {
	// Types selects event types: exact ("run.failed") or prefix ("approval.*").
	// Empty means every event.
	Types []string `json:"types,omitempty"`
	// Tenants restricts delivery to these tenants (empty = all).
	Tenants []string `json:"tenants,omitempty"`
	// MaxAttempts per event before it is skipped and logged (default 5).
	MaxAttempts int `json:"max_attempts,omitempty"`
	// StartFrom applies when the sink has no cursor yet: "latest" (default)
	// delivers only new events, "earliest" replays the whole log.
	StartFrom string `json:"start_from,omitempty"`
}

// Configurable is implemented by sinks that expose Options.
type Configurable interface {
	SinkOptions() Options
}

// Matches reports whether the event passes the filters.
func (o Options) Matches(e domain.Event) bool {
	if len(o.Tenants) > 0 && !contains(o.Tenants, e.TenantID) {
		return false
	}
	if len(o.Types) == 0 {
		return true
	}
	for _, t := range o.Types {
		if t == e.Type || t == "*" || (strings.HasSuffix(t, ".*") && strings.HasPrefix(e.Type, strings.TrimSuffix(t, "*"))) {
			return true
		}
	}
	return false
}

func (o Options) Validate() error {
	switch o.StartFrom {
	case "", "latest", "earliest":
	default:
		return fmt.Errorf("start_from must be latest or earliest, got %q", o.StartFrom)
	}
	if o.MaxAttempts < 0 {
		return errors.New("max_attempts must be >= 0")
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

type permanent struct{ error }

func (p permanent) Unwrap() error { return p.error }

// Permanent marks a delivery error that retrying cannot fix (e.g. HTTP 400).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Cursor is a position in the global event log: (transaction id, global
// sequence). Only events of transactions older than every running
// transaction are read, so a cursor never skips a late commit.
type Cursor struct {
	TxID      int64 `json:"tx_id"`
	GlobalSeq int64 `json:"global_seq"`
}

// Delivery is an event with its log position.
type Delivery struct {
	Cursor Cursor
	Event  domain.Event
}

// Store is the cursor and log access the dispatcher needs (store/pg).
type Store interface {
	// ClaimEventSink creates the cursor if missing and takes or renews the
	// lease; ok is false while another owner holds it.
	ClaimEventSink(ctx context.Context, sinkID, owner string, ttl time.Duration, earliest bool) (c Cursor, ok bool, err error)
	ReadEventLog(ctx context.Context, after Cursor, limit int) ([]Delivery, error)
	// AdvanceEventSink moves the cursor and renews the lease; it fails with
	// domain.ErrLeaseLost when the lease was taken over.
	AdvanceEventSink(ctx context.Context, sinkID, owner string, c Cursor, delivered, skipped int, ttl time.Duration) error
}

// Counters are optional delivery metrics.
type Counters interface {
	ObserveSink(sink, outcome string)
}

// Dispatcher drives one sink.
type Dispatcher struct {
	ID       string // sink instance id (the cursor key)
	Sink     Sink
	Options  Options
	Store    Store
	Owner    string
	Log      *slog.Logger
	Metrics  Counters
	Poll     time.Duration // idle poll interval (default 1s)
	LeaseTTL time.Duration // default 60s
	Batch    int           // default 100
	// Backoff before retry n (1-based); default 1s·2^(n-1), capped at 30s.
	Backoff func(n int) time.Duration
}

func (d *Dispatcher) defaults() {
	if d.Poll <= 0 {
		d.Poll = time.Second
	}
	if d.LeaseTTL <= 0 {
		d.LeaseTTL = time.Minute
	}
	if d.Batch <= 0 {
		d.Batch = 100
	}
	if d.Options.MaxAttempts <= 0 {
		d.Options.MaxAttempts = 5
	}
	if d.Backoff == nil {
		d.Backoff = func(n int) time.Duration {
			b := time.Second << (n - 1)
			if b > 30*time.Second || b <= 0 {
				b = 30 * time.Second
			}
			return b
		}
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
}

// Run delivers events until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.defaults()
	for {
		n, err := d.Once(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, domain.ErrLeaseLost) {
			d.Log.Warn("event sink", "sink", d.ID, "err", err)
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.Poll):
			}
		}
	}
}

// Once claims the sink and processes one batch; it returns how many log
// entries the cursor moved past.
func (d *Dispatcher) Once(ctx context.Context) (int, error) {
	d.defaults()
	cur, ok, err := d.Store.ClaimEventSink(ctx, d.ID, d.Owner, d.LeaseTTL, d.Options.StartFrom == "earliest")
	if err != nil || !ok {
		return 0, err
	}
	batch, err := d.Store.ReadEventLog(ctx, cur, d.Batch)
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	delivered, skipped := 0, 0
	for _, it := range batch {
		if d.Options.Matches(it.Event) {
			if err := d.deliver(ctx, it.Event); err != nil {
				if ctx.Err() != nil {
					return 0, ctx.Err() // cursor stays: the event is redelivered
				}
				skipped++
				d.observe("skipped")
				d.Log.Error("event sink gave up on event", "sink", d.ID, "event_id", it.Event.ID,
					"event_type", it.Event.Type, "run_id", it.Event.RunID, "err", err)
			} else {
				delivered++
				d.observe("delivered")
			}
			// Persist progress after each delivery so a crash redelivers at
			// most one event.
			if err := d.Store.AdvanceEventSink(ctx, d.ID, d.Owner, it.Cursor, delivered, skipped, d.LeaseTTL); err != nil {
				return 0, err
			}
			delivered, skipped = 0, 0
		}
		cur = it.Cursor
	}
	return len(batch), d.Store.AdvanceEventSink(ctx, d.ID, d.Owner, cur, delivered, skipped, d.LeaseTTL)
}

func (d *Dispatcher) deliver(ctx context.Context, e domain.Event) error {
	var err error
	for n := 1; n <= d.Options.MaxAttempts; n++ {
		if err = d.Sink.Deliver(ctx, e); err == nil {
			return nil
		}
		if IsPermanent(err) || ctx.Err() != nil {
			return err
		}
		d.observe("retried")
		if n < d.Options.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.Backoff(n)):
			}
		}
	}
	return err
}

func (d *Dispatcher) observe(outcome string) {
	if d.Metrics != nil {
		d.Metrics.ObserveSink(d.ID, outcome)
	}
}
