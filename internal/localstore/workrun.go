package localstore

import (
	"context"
	"encoding/json"

	"go-sre-harness/internal/domain"
)

func copyWorkOwner(owner *domain.WorkRunOwner) *domain.WorkRunOwner {
	if owner == nil {
		return nil
	}
	copied := *owner
	return &copied
}
func copyWorkRun(run domain.Run) domain.Run {
	run.WorkOwner = copyWorkOwner(run.WorkOwner)
	return run
}

func (s *Store) ActivateWorkRun(ctx context.Context, tenantID, runID string, owner domain.WorkRunOwner) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withStateTxLocked(ctx, func() error {
		r, ok := s.runs[runID]
		if !ok {
			return domain.ErrNotFound
		}
		status, err := domain.WorkRunActivationStatus(r, tenantID, owner)
		if err != nil {
			return err
		}
		if status == r.Status {
			return nil
		}
		r.Status = status
		r.Version++
		r.UpdatedAt = now()
		s.saveRun(r)
		s.appendEvent(tenantID, runID, "", "", "run.activated", "system", "", map[string]any{"work_owner": owner})
		return nil
	})
}

// ListWorkRuns is an internal recovery inventory, bounded before decoding rows.
func (s *Store) ListWorkRuns(ctx context.Context, after string, limit int) ([]domain.Run, error) {
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT data FROM objects WHERE kind='run' AND id>?
 AND json_extract(data,'$.work_owner') IS NOT NULL
 AND json_extract(data,'$.status') NOT IN ('COMPLETED','FAILED','CANCELLED') ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Run
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var run domain.Run
		if err := json.Unmarshal(data, &run); err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}
