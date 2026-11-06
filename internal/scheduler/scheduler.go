// Package scheduler runs the background loops of the control plane: the
// outbox publisher (DB → broker) and the reconciler (expired leases, approval
// timeouts, lost wake-ups).
package scheduler

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/notify"
)

type OutboxStore interface {
	ClaimOutbox(ctx context.Context, limit int, claimFor time.Duration) ([]domain.OutboxMessage, error)
	MarkPublished(ctx context.Context, id string) error
	MarkPublishFailed(ctx context.Context, id string, backoff time.Duration) error
}

// Publisher drains the outbox. A message is marked published only after the
// broker accepted it; a crash in between re-publishes with the same msg id,
// which JetStream de-duplicates (and consumers tolerate anyway).
type Publisher struct {
	Store    OutboxStore
	Broker   notify.Publisher
	Interval time.Duration
	Batch    int
	Log      *slog.Logger
}

func (p *Publisher) Run(ctx context.Context) error {
	if p.Interval == 0 {
		p.Interval = 100 * time.Millisecond
	}
	if p.Batch == 0 {
		p.Batch = 100
	}
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		n, err := p.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			p.Log.Error("outbox publish", "err", err)
		}
		if n == p.Batch {
			continue // backlog: drain without waiting
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *Publisher) Tick(ctx context.Context) (int, error) {
	msgs, err := p.Store.ClaimOutbox(ctx, p.Batch, 30*time.Second)
	if err != nil {
		return 0, err
	}
	for _, m := range msgs {
		if err := p.Broker.Publish(ctx, m.Subject, m.ID, m.Payload); err != nil {
			backoff := min(time.Duration(1<<min(m.Attempt, 8))*100*time.Millisecond, time.Minute)
			backoff += time.Duration(rand.Int64N(int64(backoff/2) + 1)) // jitter
			if ferr := p.Store.MarkPublishFailed(ctx, m.ID, backoff); ferr != nil {
				return 0, ferr
			}
			continue
		}
		if err := p.Store.MarkPublished(ctx, m.ID); err != nil {
			return 0, err
		}
	}
	return len(msgs), nil
}

type ReconcileStore interface {
	RecoverExpiredLeases(ctx context.Context, limit int) (int, error)
	ExpireApprovals(ctx context.Context, limit int) (int, error)
	RewakeStaleQueued(ctx context.Context, idle time.Duration, limit int) (int, error)
}

type Reconciler struct {
	Store     ReconcileStore
	Interval  time.Duration
	StaleIdle time.Duration
	Batch     int
	Log       *slog.Logger
}

func (r *Reconciler) Run(ctx context.Context) error {
	if r.Interval == 0 {
		r.Interval = 10 * time.Second
	}
	if r.StaleIdle == 0 {
		r.StaleIdle = 30 * time.Second
	}
	if r.Batch == 0 {
		r.Batch = 100
	}
	for {
		if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
			r.Log.Error("reconcile", "err", err)
		}
		// Jitter avoids synchronized scans across replicas (recovery storms).
		wait := r.Interval + time.Duration(rand.Int64N(int64(r.Interval/5)+1))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (r *Reconciler) Tick(ctx context.Context) error {
	if n, err := r.Store.RecoverExpiredLeases(ctx, r.Batch); err != nil {
		return err
	} else if n > 0 {
		r.Log.Info("recovered expired leases", "runs", n)
	}
	if n, err := r.Store.ExpireApprovals(ctx, r.Batch); err != nil {
		return err
	} else if n > 0 {
		r.Log.Info("expired approvals", "count", n)
	}
	_, err := r.Store.RewakeStaleQueued(ctx, r.StaleIdle, r.Batch)
	return err
}
