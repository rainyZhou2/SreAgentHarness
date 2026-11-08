package orchestration

import (
	"context"
	"log/slog"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/store/cpsql"
)

// Reconciler fires due timers and runs the recovery sweeps: inbox records
// left unprocessed, actions stuck in DISPATCHED, expired correlation
// groups. Every sweep is idempotent, so all replicas may run it.
type Reconciler struct {
	Store    *cpsql.Store
	Ingestor *Ingestor
	Actions  *action.Runtime
	Clock    cpkit.Clock
	Log      *slog.Logger
	Metrics  *Metrics
	// Interval is the timer resolution (default 2s); SweepEvery the
	// recovery cadence (default 30s).
	Interval   time.Duration
	SweepEvery time.Duration
	// StaleDispatch is when a DISPATCHED action is reconciled (default 5m).
	StaleDispatch time.Duration
}

func (r *Reconciler) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

// Run loops until ctx ends.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		r.Interval = 2 * time.Second
	}
	if r.SweepEvery <= 0 {
		r.SweepEvery = 30 * time.Second
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	lastSweep := time.Time{}
	for {
		if _, err := r.FireDue(ctx); err != nil && ctx.Err() == nil {
			r.log().Warn("watch reconciler failed", "err", err)
		}
		if time.Since(lastSweep) >= r.SweepEvery {
			lastSweep = time.Now()
			r.Sweep(ctx)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// FireDue fires timed watch checks that are due. The firing id is
// deterministic per check, so concurrent reconcilers fire each check once.
func (r *Reconciler) FireDue(ctx context.Context) (int, error) {
	now := r.Clock.Now()
	ws, err := r.Store.DueWatches(ctx, now, 200)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, w := range ws {
		kind, ok := continuous.DueReason(w, now)
		if !ok {
			continue
		}
		fired, err := r.Store.FireWatch(ctx, continuous.Firing{TenantID: w.TenantID, WatchID: w.ID, WorkID: w.WorkID,
			FiringID: continuous.TimerFiringID(w.ID, w.Checks), Kind: string(kind), FiredAt: now})
		if err != nil {
			r.log().Warn("fire watch failed", "watch_id", w.ID, "err", err)
			continue
		}
		if fired {
			n++
			r.Metrics.watch(string(kind))
		}
	}
	return n, nil
}

// Sweep runs the recovery sweeps once.
func (r *Reconciler) Sweep(ctx context.Context) {
	if r.Ingestor != nil {
		if _, err := r.Ingestor.Sweep(ctx, 30*time.Second, 200); err != nil && ctx.Err() == nil {
			r.log().Warn("inbox sweep failed", "err", err)
		}
	}
	if r.Actions != nil {
		age := r.StaleDispatch
		if age <= 0 {
			age = 5 * time.Minute
		}
		if _, err := r.Actions.RecoverStale(ctx, age, 50); err != nil && ctx.Err() == nil {
			r.log().Warn("action recovery failed", "err", err)
		}
	}
	if _, err := r.Store.PurgeCorrelations(ctx, r.Clock.Now(), 1000); err != nil && ctx.Err() == nil {
		r.log().Warn("correlation purge failed", "err", err)
	}
}
