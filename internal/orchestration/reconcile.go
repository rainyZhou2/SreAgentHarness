package orchestration

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/store/cpsql"
)

// Reconciler fires due timers and runs the recovery sweeps: inbox records
// left unprocessed, actions stuck in DISPATCHED, expired correlation
// groups. Every sweep is idempotent, so all replicas may run it.
type Reconciler struct {
	Runs      Runs
	Inventory RunInventory
	runMu     sync.Mutex
	runCursor string
	Store     *cpsql.Store
	Ingestor  *Ingestor
	Actions   *action.Runtime
	Clock     cpkit.Clock
	Log       *slog.Logger
	Metrics   *Metrics
	// Interval is the timer resolution (default 2s); SweepEvery the
	// recovery cadence (default 30s).
	Interval   time.Duration
	SweepEvery time.Duration
	// StaleDispatch is when a DISPATCHED action is reconciled (default 5m).
	StaleDispatch time.Duration
	// Retention is the event inbox retention; the zero value keeps events
	// forever.
	Retention cpsql.InboxRetention
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
			if w.NextCheckAt != nil {
				r.Metrics.watchDue(string(kind), now.Sub(*w.NextCheckAt))
			}
		}
	}
	return n, nil
}

// Sweep runs the recovery sweeps once.
func (r *Reconciler) Sweep(ctx context.Context) {
	if err := r.RecoverRuns(ctx); err != nil && ctx.Err() == nil {
		r.log().Warn("work run recovery failed", "err", err)
	}
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
	// Model-run reservations whose owner died: a reservation whose runs are
	// really in flight is renewed, the others return their units.
	if n, err := r.Store.ReapReservations(ctx, r.Clock.Now(), 500, func(ctx context.Context, res quota.Reservation) (bool, error) {
		return r.Store.WorkHasActiveRuns(ctx, res.TenantID, res.WorkID)
	}, 10*time.Minute); err != nil && ctx.Err() == nil {
		r.log().Warn("reservation reaper failed", "err", err)
	} else if n > 0 {
		r.log().Info("expired admission reservations reaped", "count", n)
		r.Metrics.reaped(n)
	}
	// Terminal Works whose dependents were not settled (the process died
	// between the commit and the graph update).
	if pending, err := r.Store.UnsettledTerminalWorks(ctx, 200); err != nil && ctx.Err() == nil {
		r.log().Warn("work graph sweep failed", "err", err)
	} else {
		for _, p := range pending {
			if err := r.Store.CompleteChild(ctx, p[0], p[1], 0); err != nil && ctx.Err() == nil {
				r.log().Warn("settle work graph", "tenant", p[0], "work_id", p[1], "err", err)
			}
		}
	}
	if _, err := r.Store.PurgeCorrelations(ctx, r.Clock.Now(), 1000); err != nil && ctx.Err() == nil {
		r.log().Warn("correlation purge failed", "err", err)
	}
	if r.Retention.Payload > 0 || r.Retention.Record > 0 {
		res, err := r.Store.PurgeEvents(ctx, r.Clock.Now(), r.Retention)
		switch {
		case err != nil && ctx.Err() == nil:
			r.log().Warn("inbox retention failed", "err", err)
		case res.PayloadsStripped > 0 || res.Deleted > 0:
			r.log().Info("inbox retention", "payloads_stripped", res.PayloadsStripped, "deleted", res.Deleted)
			r.Metrics.inboxPurged(res.PayloadsStripped, res.Deleted)
		}
	}
}
