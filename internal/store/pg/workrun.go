package pg

import (
	"context"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

func (s *Store) ActivateWorkRun(ctx context.Context, tenantID, runID string, owner domain.WorkRunOwner) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockRun(ctx, tx, tenantID, runID)
		if err != nil {
			return err
		}
		status, err := domain.WorkRunActivationStatus(r.run, tenantID, owner)
		if err != nil {
			return err
		}
		if status == r.run.Status {
			return nil
		}
		r.setStatus(status)
		if err := r.event(ctx, "run.activated", "system", "", nil, nil, map[string]any{"work_owner": owner}); err != nil {
			return err
		}
		if status == domain.RunQueued {
			if err := r.wake(ctx, r.run.NotBefore); err != nil {
				return err
			}
		}
		return r.save(ctx)
	})
}

// ListWorkRuns is an internal all-tenant recovery inventory. Callers use the
// internal store connection; tenant-scoped connections retain their RLS policy.
func (s *Store) ListWorkRuns(ctx context.Context, after string, limit int) ([]domain.Run, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	// UUID ordering is identical to canonical UUID string ordering.
	var cursor *string
	if after != "" {
		if !validID(after) {
			return nil, &domain.RejectedError{Reason: "invalid_run_cursor"}
		}
		cursor = &after
	}
	rows, err := s.pool.Query(ctx, `SELECT `+runCols+` FROM agent_runs WHERE work_owner IS NOT NULL
 AND status NOT IN ('COMPLETED','FAILED','CANCELLED') AND ($1::uuid IS NULL OR id>$1::uuid) ORDER BY id LIMIT $2`, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}
