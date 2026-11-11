package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"go-sre-harness/internal/impact"
)

var _ impact.Store = (*Store)(nil)

// RecordChange upserts a change and its subject index. created is false
// when the change id was already recorded (a later phase of a rollout).
func (s *Store) RecordChange(ctx context.Context, c impact.Change) (impact.Change, bool, error) {
	if err := c.Normalize(s.now()); err != nil {
		return c, false, err
	}
	created := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, s.q(`UPDATE change_records SET category=?, source=?, actor=?, started_at=?, ended_at=?,
			record=? WHERE tenant_id=? AND change_id=?`), c.Category, c.Source, nullStr(c.Actor), s.t(c.StartedAt),
			s.tp(c.EndedAt), js(c), c.TenantID, c.ID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO change_records (tenant_id, change_id, category, source, actor,
				started_at, ended_at, record, created_at) VALUES (?,?,?,?,?,?,?,?,?)`), c.TenantID, c.ID, c.Category, c.Source,
				nullStr(c.Actor), s.t(c.StartedAt), s.tp(c.EndedAt), js(c), s.t(c.RecordedAt)); err != nil {
				if isUnique(err, "") {
					return errRetryCreate
				}
				return err
			}
			created = true
		}
		for _, ref := range c.AffectedSubjects {
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO change_subjects (tenant_id, change_id, subject_ref, started_at)
				VALUES (?,?,?,?) ON CONFLICT (tenant_id, change_id, subject_ref) DO UPDATE SET started_at=excluded.started_at`),
				c.TenantID, c.ID, ref, s.t(c.StartedAt)); err != nil {
				return err
			}
		}
		return nil
	})
	if err == errRetryCreate {
		return s.RecordChange(ctx, c)
	}
	return c, created, err
}

// ChangesFor lists changes touching any of subjects in [from, to].
func (s *Store) ChangesFor(ctx context.Context, tenantID string, subjects []string, from, to time.Time, limit int) ([]impact.Change, error) {
	if len(subjects) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args := append([]any{tenantID}, anys(subjects)...)
	args = append(args, s.t(from), s.t(to), limit)
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT r.record, r.started_at FROM change_subjects cs
		JOIN change_records r ON r.tenant_id = cs.tenant_id AND r.change_id = cs.change_id
		WHERE cs.tenant_id=? AND cs.subject_ref IN (`+placeholders(len(subjects))+`) AND cs.started_at >= ? AND cs.started_at <= ?
		ORDER BY r.started_at DESC LIMIT ?`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []impact.Change
	for rows.Next() {
		var body jb
		var at ts
		if err := rows.Scan(&body, &at); err != nil {
			return nil, err
		}
		var c impact.Change
		if err := json.Unmarshal(body, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
