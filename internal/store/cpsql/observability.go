package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"go-sre-harness/internal/observability"
)

var (
	_ observability.Catalog = (*Store)(nil)
	_ observability.Audit   = (*Store)(nil)
)

func scanSource(r rowScanner) (observability.Source, error) {
	var spec, health jb
	var status string
	var v int64
	var updated ts
	err := r.Scan(&spec, &status, &v, &health, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return observability.Source{}, observability.ErrNotFound
	}
	if err != nil {
		return observability.Source{}, err
	}
	var s observability.Source
	if err := json.Unmarshal(spec, &s); err != nil {
		return s, fmt.Errorf("decode observability source: %w", err)
	}
	s.State, s.Version, s.UpdatedAt = status, v, updated.T
	if len(health) > 0 {
		var h observability.Health
		if json.Unmarshal(health, &h) == nil {
			s.Health = &h
		}
	}
	return s, nil
}

const sourceCols = `spec, status, version, health, updated_at`

// PutSource creates or CAS-updates a source and appends its revision in
// one transaction.
func (s *Store) PutSource(ctx context.Context, src observability.Source, expectedVersion int64, actorID, reason string) (observability.Source, error) {
	if err := src.Normalize(); err != nil {
		return src, err
	}
	if actorID == "" {
		return src, fmt.Errorf("%w: actor is required", observability.ErrInvalid)
	}
	now := s.now()
	src.Health = nil // health is written by probes only
	err := s.tx(ctx, func(tx *sql.Tx) error {
		cur, err := scanSource(tx.QueryRowContext(ctx, s.q(`SELECT `+sourceCols+` FROM observability_sources
			WHERE tenant_id=? AND id=?`+s.forUpdate()), src.TenantID, src.ID))
		switch {
		case errors.Is(err, observability.ErrNotFound):
			if expectedVersion != 0 {
				return observability.ErrVersionConflict
			}
			src.Version = 1
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO observability_sources (id, tenant_id, kind, backend, endpoint_ref,
				credentials_ref, environment, spec, status, version, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,?,?)`),
				src.ID, src.TenantID, string(src.Kind), src.Backend, src.EndpointRef, src.CredentialID, src.Environment,
				js(src), src.State, s.t(now), s.t(now)); err != nil {
				if isUnique(err, "") {
					return observability.ErrVersionConflict
				}
				return err
			}
		case err != nil:
			return err
		default:
			if expectedVersion == 0 || cur.Version != expectedVersion {
				return observability.ErrVersionConflict
			}
			src.Version = cur.Version + 1
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE observability_sources SET kind=?, backend=?, endpoint_ref=?,
				credentials_ref=?, environment=?, spec=?, status=?, version=?, updated_at=? WHERE tenant_id=? AND id=?`),
				string(src.Kind), src.Backend, src.EndpointRef, src.CredentialID, src.Environment, js(src), src.State,
				src.Version, s.t(now), src.TenantID, src.ID); err != nil {
				return err
			}
		}
		src.UpdatedAt = now
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO observability_source_revisions (tenant_id, source_id, version, spec,
			status, actor_id, reason, created_at) VALUES (?,?,?,?,?,?,?,?)`), src.TenantID, src.ID, src.Version, js(src),
			src.State, actorID, nullStr(reason), s.t(now))
		return err
	})
	return src, err
}

// GetSource reads a source of the tenant.
func (s *Store) GetSource(ctx context.Context, tenantID, id string) (observability.Source, error) {
	return scanSource(s.db.QueryRowContext(ctx, s.q(`SELECT `+sourceCols+` FROM observability_sources
		WHERE tenant_id=? AND id=?`), tenantID, id))
}

// ListSources lists the tenant's sources.
func (s *Store) ListSources(ctx context.Context, tenantID string, f observability.Filter) ([]observability.Source, error) {
	q, args := `SELECT `+sourceCols+` FROM observability_sources WHERE tenant_id=?`, []any{tenantID}
	if f.Kind != "" {
		q, args = q+` AND kind=?`, append(args, string(f.Kind))
	}
	if f.Environment != "" {
		q, args = q+` AND environment=?`, append(args, f.Environment)
	}
	if f.UsableOnly {
		q += ` AND status IN ('ACTIVE','DEGRADED')`
	}
	rows, err := s.db.QueryContext(ctx, s.q(q+` ORDER BY id LIMIT 500`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []observability.Source
	for rows.Next() {
		src, err := scanSource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

// SourceRevisions lists the recorded versions of a source, newest first.
func (s *Store) SourceRevisions(ctx context.Context, tenantID, id string, limit int) ([]observability.Revision, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT version, spec, status, actor_id, reason, created_at
		FROM observability_source_revisions WHERE tenant_id=? AND source_id=? ORDER BY version DESC LIMIT ?`), tenantID, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []observability.Revision
	for rows.Next() {
		var r observability.Revision
		var spec jb
		var status string
		var reason ns
		var at ts
		if err := rows.Scan(&r.Version, &spec, &status, &r.ActorID, &reason, &at); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(spec, &r.Source); err != nil {
			return nil, err
		}
		r.Source.State, r.Source.Version, r.Reason, r.CreatedAt = status, r.Version, reason.S(), at.T
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetSourceHealth records a probe and the resulting state; it is not a
// revision (health is an observation, not configuration).
func (s *Store) SetSourceHealth(ctx context.Context, tenantID, id string, h observability.Health, state string) error {
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE observability_sources SET health=?, status=CASE WHEN status='DISABLED'
		THEN status ELSE ? END WHERE tenant_id=? AND id=?`), js(h), state, tenantID, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return observability.ErrNotFound
	}
	return nil
}

// RecordQuery appends a query audit row.
func (s *Store) RecordQuery(ctx context.Context, r observability.QueryRecord) error {
	_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO observability_queries (tenant_id, id, source_id, work_id, query_digest,
		subject_ref, time_start, time_end, result_status, result_ref, result_digest, result_count, duration_ms, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), r.TenantID, r.ID, r.SourceID, nullStr(r.WorkID), r.QueryDigest,
		nullStr(r.SubjectRef), s.t(r.Start), s.t(r.End), r.Status, nullStr(r.ResultRef), nullStr(r.ResultDigest), r.Count,
		r.DurationMS, s.t(r.CreatedAt))
	return err
}

// CountWorkQueries counts the backend queries of a Work.
func (s *Store) CountWorkQueries(ctx context.Context, tenantID, workID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM observability_queries WHERE tenant_id=? AND work_id=?`),
		tenantID, workID).Scan(&n)
	return n, err
}

// ListQueries lists the query audit of a Work (or of a source when workID
// is empty), newest first.
func (s *Store) ListQueries(ctx context.Context, tenantID, workID, sourceID string, limit int) ([]observability.QueryRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	q, args := `SELECT id, source_id, work_id, query_digest, subject_ref, time_start, time_end, result_status, result_ref,
		result_digest, result_count, duration_ms, created_at FROM observability_queries WHERE tenant_id=?`, []any{tenantID}
	if workID != "" {
		q, args = q+` AND work_id=?`, append(args, workID)
	}
	if sourceID != "" {
		q, args = q+` AND source_id=?`, append(args, sourceID)
	}
	rows, err := s.db.QueryContext(ctx, s.q(q+` ORDER BY created_at DESC LIMIT ?`), append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []observability.QueryRecord
	for rows.Next() {
		r := observability.QueryRecord{TenantID: tenantID}
		var work, subj, ref, dig ns
		var from, to, at ts
		if err := rows.Scan(&r.ID, &r.SourceID, &work, &r.QueryDigest, &subj, &from, &to, &r.Status, &ref, &dig, &r.Count,
			&r.DurationMS, &at); err != nil {
			return nil, err
		}
		r.WorkID, r.SubjectRef, r.ResultRef, r.ResultDigest = work.S(), subj.S(), ref.S(), dig.S()
		r.Start, r.End, r.CreatedAt = from.T, to.T, at.T
		out = append(out, r)
	}
	return out, rows.Err()
}
