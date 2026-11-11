package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/runbook"
)

var _ runbook.Store = (*Store)(nil)

const runbookCols = `spec, status, last_rehearsed_at, executions, successes, unknown_effects, approvals, row_version, updated_at`

func scanRunbook(r rowScanner, tenantID string) (runbook.Runbook, error) {
	var spec, approvals jb
	var rb runbook.Runbook
	var rehearsed, updated ts
	err := r.Scan(&spec, &rb.Status, &rehearsed, &rb.Executions, &rb.Successes, &rb.UnknownEffects, &approvals, &rb.RowVersion, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return rb, runbook.ErrNotFound
	}
	if err != nil {
		return rb, err
	}
	if err := json.Unmarshal(spec, &rb.Spec); err != nil {
		return rb, fmt.Errorf("decode runbook: %w", err)
	}
	if len(approvals) > 0 {
		_ = json.Unmarshal(approvals, &rb.Approvals)
	}
	rb.TenantID, rb.LastRehearsedAt, rb.UpdatedAt = tenantID, rehearsed.ptr(), updated.T
	return rb, nil
}

// PutRunbook creates or CAS-updates one version of a runbook.
func (s *Store) PutRunbook(ctx context.Context, tenantID string, spec runbook.Spec, status string, expected int64) (runbook.Runbook, error) {
	if err := spec.Normalize(); err != nil {
		return runbook.Runbook{}, err
	}
	switch status {
	case "":
		status = runbook.Draft
	case runbook.Draft, runbook.Active, runbook.Expired, runbook.Retired:
	default:
		return runbook.Runbook{}, fmt.Errorf("%w: unknown status %q", runbook.ErrInvalid, status)
	}
	now := s.now()
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var cur int64
		err := tx.QueryRowContext(ctx, s.q(`SELECT row_version FROM runbooks WHERE tenant_id=? AND id=? AND version=?`+s.forUpdate()),
			tenantID, spec.ID, spec.Version).Scan(&cur)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if expected != 0 {
				return runbook.ErrVersionConflict
			}
			_, err = tx.ExecContext(ctx, s.q(`INSERT INTO runbooks (tenant_id, id, version, spec, status, row_version, updated_at)
				VALUES (?,?,?,?,?,1,?)`), tenantID, spec.ID, spec.Version, js(spec), status, s.t(now))
			if isUnique(err, "") {
				return runbook.ErrVersionConflict
			}
			return err
		case err != nil:
			return err
		case expected == 0 || cur != expected:
			return runbook.ErrVersionConflict
		}
		_, err = tx.ExecContext(ctx, s.q(`UPDATE runbooks SET spec=?, status=?, row_version=row_version+1, updated_at=?
			WHERE tenant_id=? AND id=? AND version=?`), js(spec), status, s.t(now), tenantID, spec.ID, spec.Version)
		return err
	})
	if err != nil {
		return runbook.Runbook{}, err
	}
	return s.GetRunbook(ctx, tenantID, spec.ID, spec.Version)
}

// GetRunbook reads one version (0: the newest).
func (s *Store) GetRunbook(ctx context.Context, tenantID, id string, version int) (runbook.Runbook, error) {
	if version > 0 {
		return scanRunbook(s.db.QueryRowContext(ctx, s.q(`SELECT `+runbookCols+` FROM runbooks WHERE tenant_id=? AND id=? AND version=?`),
			tenantID, id, version), tenantID)
	}
	return scanRunbook(s.db.QueryRowContext(ctx, s.q(`SELECT `+runbookCols+` FROM runbooks WHERE tenant_id=? AND id=?
		ORDER BY version DESC LIMIT 1`), tenantID, id), tenantID)
}

// ListRunbooks lists every version of the tenant's runbooks.
func (s *Store) ListRunbooks(ctx context.Context, tenantID string) ([]runbook.Runbook, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+runbookCols+` FROM runbooks WHERE tenant_id=? ORDER BY id, version DESC LIMIT 500`), tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []runbook.Runbook
	for rows.Next() {
		rb, err := scanRunbook(rows, tenantID)
		if err != nil {
			return nil, err
		}
		out = append(out, rb)
	}
	return out, rows.Err()
}

// RecordRehearsal stamps a rehearsal (and an approval, when given).
func (s *Store) RecordRehearsal(ctx context.Context, tenantID, id string, version int, at time.Time, approval *runbook.Approval) (runbook.Runbook, error) {
	var out runbook.Runbook
	err := s.tx(ctx, func(tx *sql.Tx) error {
		rb, err := scanRunbook(tx.QueryRowContext(ctx, s.q(`SELECT `+runbookCols+` FROM runbooks WHERE tenant_id=? AND id=? AND version=?`+
			s.forUpdate()), tenantID, id, version), tenantID)
		if err != nil {
			return err
		}
		if approval != nil {
			rb.Approvals = append(rb.Approvals, *approval)
			if len(rb.Approvals) > 50 {
				rb.Approvals = rb.Approvals[len(rb.Approvals)-50:]
			}
		}
		rb.LastRehearsedAt = &at
		_, err = tx.ExecContext(ctx, s.q(`UPDATE runbooks SET last_rehearsed_at=?, approvals=?, row_version=row_version+1
			WHERE tenant_id=? AND id=? AND version=?`), s.t(at), js(rb.Approvals), tenantID, id, version)
		rb.RowVersion++
		out = rb
		return err
	})
	return out, err
}

// RecordExecution adds one execution to a runbook's track record.
func (s *Store) RecordExecution(ctx context.Context, tenantID, id string, version int, r runbook.Result) error {
	ok, unknown := 0, 0
	if r.Succeeded {
		ok = 1
	}
	if r.UnknownEffect {
		unknown = 1
	}
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE runbooks SET executions=executions+1, successes=successes+?,
		unknown_effects=unknown_effects+? WHERE tenant_id=? AND id=? AND version=?`), ok, unknown, tenantID, id, version)
	return err
}
