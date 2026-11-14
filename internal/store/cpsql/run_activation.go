package cpsql

import (
	"context"
	"database/sql"
	"fmt"

	"go-sre-harness/internal/work"
)

// CheckRunActivation authorizes a linked reservation against the current leased
// Work. Kernel activation follows outside this transaction; recovery cancels
// reservations whose authority is subsequently revoked.
func (s *Store) CheckRunActivation(ctx context.Context, req work.RunActivation) error {
	if req.ExpectedVersion <= 0 || req.LeaseOwner == "" || req.Generation <= 0 || req.StageID == "" || req.RunID == "" {
		return fmt.Errorf("%w: incomplete run activation authority", work.ErrInvalid)
	}
	return s.tx(ctx, func(tx *sql.Tx) error {
		w, err := s.lockWork(ctx, tx, req.TenantID, req.WorkID)
		if err != nil {
			return err
		}
		if w.Version != req.ExpectedVersion {
			return work.ErrVersionConflict
		}
		if w.LeaseOwner != req.LeaseOwner || w.LeaseUntil == nil || !w.LeaseUntil.After(s.now()) {
			return work.ErrLeaseLost
		}
		stage, ok := w.CurrentStage()
		if w.Status != work.StatusActive || w.Generation != req.Generation || !ok || stage.ID != req.StageID {
			return fmt.Errorf("%w: inactive run owner", work.ErrInvalid)
		}
		var n int
		if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM work_run_links
			WHERE tenant_id=? AND work_id=? AND run_id=? AND role='owner' AND generation=? AND stage_id=?`),
			req.TenantID, req.WorkID, req.RunID, req.Generation, req.StageID).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("%w: run reservation has no matching owner link", work.ErrInvalid)
		}
		return nil
	})
}
