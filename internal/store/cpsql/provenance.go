package cpsql

import (
	"context"
	"database/sql"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/provenance"
)

var _ provenance.HealthStore = (*Store)(nil)

// RecordCollection records one collection attempt. A failure keeps the
// last success and counts up; a success resets the count.
func (s *Store) RecordCollection(ctx context.Context, r provenance.Report) error {
	at := r.At
	if at.IsZero() {
		at = s.now()
	}
	maxAge := 3 * r.Every
	if maxAge < 2*time.Minute {
		maxAge = 2 * time.Minute
	}
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO world_collector_health (tenant_id, collector, error_count, max_age_ms, updated_at)
			VALUES (?,?,0,?,?) ON CONFLICT (tenant_id, collector) DO NOTHING`), r.TenantID, r.Collector, maxAge.Milliseconds(), s.t(at)); err != nil {
			return err
		}
		if r.Err != nil {
			_, err := tx.ExecContext(ctx, s.q(`UPDATE world_collector_health SET last_attempt_at=?, last_error=?,
				error_count=error_count+1, max_age_ms=?, updated_at=? WHERE tenant_id=? AND collector=?`), s.t(at),
				cpkit.Truncate(r.Err.Error(), 500), maxAge.Milliseconds(), s.t(at), r.TenantID, r.Collector)
			return err
		}
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE world_collector_health SET last_attempt_at=?, last_success_at=?,
			last_error=NULL, error_count=0, max_age_ms=?, watch_cursor=?, source_version=?, updated_at=?
			WHERE tenant_id=? AND collector=?`), s.t(at), s.t(at), maxAge.Milliseconds(), nullStr(r.WatchCursor),
			nullStr(r.SourceVersion), s.t(at), r.TenantID, r.Collector); err != nil {
			return err
		}
		if r.Full {
			_, err := tx.ExecContext(ctx, s.q(`UPDATE world_collector_health SET last_full_sync=? WHERE tenant_id=? AND collector=?`),
				s.t(at), r.TenantID, r.Collector)
			return err
		}
		return nil
	})
}

// CollectorHealth lists the tenant's collectors.
func (s *Store) CollectorHealth(ctx context.Context, tenantID string) ([]provenance.CollectorHealth, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT collector, last_success_at, last_attempt_at, last_full_sync, last_error,
		error_count, max_age_ms, watch_cursor, source_version FROM world_collector_health WHERE tenant_id=? ORDER BY collector`), tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []provenance.CollectorHealth
	for rows.Next() {
		h := provenance.CollectorHealth{TenantID: tenantID}
		var ok, at, full ts
		var lastErr, cursor, ver ns
		var maxAge int64
		if err := rows.Scan(&h.Collector, &ok, &at, &full, &lastErr, &h.ErrorCount, &maxAge, &cursor, &ver); err != nil {
			return nil, err
		}
		h.LastSuccessAt, h.LastAttemptAt, h.LastFullSync = ok.ptr(), at.ptr(), full.ptr()
		h.LastError, h.WatchCursor, h.SourceVersion = lastErr.S(), cursor.S(), ver.S()
		h.MaxAge = time.Duration(maxAge) * time.Millisecond
		out = append(out, h)
	}
	return out, rows.Err()
}
