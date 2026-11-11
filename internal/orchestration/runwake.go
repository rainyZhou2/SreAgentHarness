package orchestration

import (
	"context"
	"log/slog"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
	"go-sre-harness/internal/store/cpsql"
)

// RunWaker turns the kernel's run events into Work wake-ups (design §6.4).
// The kernel commits a run's terminal status together with its event; the
// event log delivers it at least once to this sink, which makes the owning
// Work due at once instead of at its next poll.
//
//	kernel commit (run terminal + event)
//	   └─ event sink dispatcher (cursor, retries)
//	        └─ RunWaker.Deliver ── WorkForRun ── wake_at = now
//	             └─ orchestrator claims the Work and reads the run
//
// The signal carries no state: the orchestrator still reads the run
// itself. So a duplicate, late or out-of-order delivery is harmless, and a
// lost one only costs the poll interval — the stage's own poll and the
// reconciler remain the safety net. The kernel does not know this sink
// exists; it only writes its event log.
type RunWaker struct {
	Store   *cpsql.Store
	Clock   cpkit.Clock
	Log     *slog.Logger
	Metrics *Metrics
}

// runWakeTypes are the run events a Work stage reacts to: the terminal
// ones, and the ones that mean a human is needed.
var runWakeTypes = []string{"run.completed", "run.failed", "run.cancelled", "run.waiting_approval", "run.waiting_input"}

// SinkOptions limits delivery to the run events that matter.
func (r *RunWaker) SinkOptions() eventsink.Options {
	return eventsink.Options{Types: append([]string(nil), runWakeTypes...), MaxAttempts: 8}
}

// Deliver wakes the Work that owns the run, if any.
func (r *RunWaker) Deliver(ctx context.Context, e domain.Event) error {
	link, ok, err := r.Store.WorkForRun(ctx, e.TenantID, e.RunID)
	if err != nil {
		r.Metrics.RunWake("error")
		return err // retried by the dispatcher
	}
	if !ok {
		r.Metrics.RunWake("no_owner") // a v1 run, or an evidence-only link
		return nil
	}
	woken, err := r.Store.WakeWorkQuiet(ctx, e.TenantID, link.WorkID, r.Clock.Now())
	switch {
	case err != nil:
		r.Metrics.RunWake("error")
		return err
	case woken:
		r.Metrics.RunWake("woken")
	default:
		r.Metrics.RunWake("terminal") // closed, or already due
	}
	if r.Log != nil {
		r.Log.Debug("work woken by run event", "tenant", e.TenantID, "work_id", link.WorkID, "run_id", e.RunID, "event", e.Type)
	}
	return nil
}
