package cpsql

import (
	"context"
	"database/sql"
	"fmt"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/work"
)

// validateActionDispatch locks the owning Work before the action row. A
// cancellation or replan and a new dispatch intent therefore have one database
// ordering. This lock is held only for the ledger write, never for remote I/O.
func (s *Store) validateActionDispatch(ctx context.Context, tx *sql.Tx, rec action.Record) error {
	w, err := s.lockWork(ctx, tx, rec.TenantID, rec.WorkID)
	if err != nil {
		return err
	}
	if w.Status != work.StatusActive {
		return fmt.Errorf("%w: Work is %s, not ACTIVE", action.ErrPrecondition, w.Status)
	}
	if rec.Generation != w.Generation {
		return fmt.Errorf("%w: action generation %d differs from Work generation %d", action.ErrPrecondition, rec.Generation, w.Generation)
	}
	if w.Goal.Constraints.ProductionWrites == work.WritesForbidden && rec.Risk.HasSideEffects() {
		return fmt.Errorf("%w: the Work forbids production writes", action.ErrPrecondition)
	}
	return nil
}
