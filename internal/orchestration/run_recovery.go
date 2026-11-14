package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/work"
)

// RecoverRuns cancels reservations whose immutable owner no longer describes
// the current Work. An unlinked current reservation remains adoptable after a
// crash between reserve and link commit.
func (r *Reconciler) RecoverRuns(ctx context.Context) error {
	if r.Runs == nil || r.Inventory == nil {
		return nil
	}
	if !r.runMu.TryLock() {
		return nil
	}
	defer r.runMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	const limit = 100
	runs, err := r.Inventory.ListWorkRuns(ctx, r.runCursor, limit)
	if err != nil {
		return fmt.Errorf("list work reservations: %w", err)
	}
	var failures []error
	for _, run := range runs {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		w, err := r.Store.GetWork(ctx, run.TenantID, run.WorkID)
		missing := errors.Is(err, work.ErrNotFound)
		if err != nil && !missing {
			failures = append(failures, fmt.Errorf("read run owner: %w", err))
			r.runCursor = run.RunID
			continue
		}
		stage, current := w.CurrentStage()
		if missing || w.Status.Terminal() || w.Generation != run.Generation || !current || stage.ID != run.StageID {
			callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			err := r.Runs.CancelRun(callCtx, run.TenantID, run.RunID, "work-recovery", "work authority revoked", "work-recover:"+run.RunID)
			stop()
			if err != nil {
				failures = append(failures, fmt.Errorf("cancel abandoned run %s: %w", run.RunID, err))
			}
		}
		r.runCursor = run.RunID
	}
	if len(runs) < limit {
		r.runCursor = ""
	}
	return errors.Join(failures...)
}
