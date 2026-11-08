package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

const watchCols = `id, tenant_id, work_id, mode, predicate, subjects, purpose, generation, cursor, status, fired_by,
	checks, interval_ms, not_before, next_check_at, deadline, version, created_at, updated_at`

func scanWatch(r rowScanner) (continuous.Watch, error) {
	var w continuous.Watch
	var pred, subj jb
	var purpose, cursor, firedBy ns
	var mode, status string
	var intervalMS int64
	var notBefore, next, deadline, created, updated ts
	err := r.Scan(&w.ID, &w.TenantID, &w.WorkID, &mode, &pred, &subj, &purpose, &w.Generation, &cursor, &status,
		&firedBy, &w.Checks, &intervalMS, &notBefore, &next, &deadline, &w.Version, &created, &updated)
	if err != nil {
		return w, err
	}
	w.Mode, w.Status = continuous.Mode(mode), continuous.Status(status)
	if len(pred) > 0 {
		var p eventing.Predicate
		if err := json.Unmarshal(pred, &p); err != nil {
			return w, err
		}
		w.Predicate = &p
	}
	if len(subj) > 0 {
		_ = json.Unmarshal(subj, &w.Subjects)
	}
	w.Purpose, w.Cursor, w.FiredBy = purpose.S(), cursor.S(), firedBy.S()
	w.Interval = time.Duration(intervalMS) * time.Millisecond
	w.NotBefore, w.NextCheckAt, w.Deadline = notBefore.T, next.ptr(), deadline.ptr()
	w.CreatedAt, w.UpdatedAt = created.T, updated.T
	return w, nil
}

func scanWatches(rows *sql.Rows) ([]continuous.Watch, error) {
	defer rows.Close()
	var out []continuous.Watch
	for rows.Next() {
		w, err := scanWatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) insertWatch(ctx context.Context, tx *sql.Tx, w continuous.Watch) error {
	if !cpkit.ValidID(w.ID) {
		return errors.New("watch id must be a uuid")
	}
	var pred, subj any
	if w.Predicate != nil {
		pred = js(w.Predicate)
	}
	if len(w.Subjects) > 0 {
		subj = js(w.Subjects)
	}
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_watches (id, tenant_id, work_id, mode, predicate, subjects, purpose,
		generation, cursor, status, checks, interval_ms, not_before, next_check_at, deadline, version, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
		w.ID, w.TenantID, w.WorkID, string(w.Mode), pred, subj, nullStr(w.Purpose), w.Generation, nullStr(w.Cursor),
		string(w.Status), w.Checks, w.Interval.Milliseconds(), s.t(w.NotBefore), s.tp(w.NextCheckAt), s.tp(w.Deadline),
		w.Version, s.t(w.CreatedAt), s.t(w.UpdatedAt))
	return err
}

// GetWatch reads one watch.
func (s *Store) GetWatch(ctx context.Context, tenantID, id string) (continuous.Watch, error) {
	w, err := scanWatch(s.db.QueryRowContext(ctx, s.q(`SELECT `+watchCols+` FROM work_watches WHERE tenant_id=? AND id=?`), tenantID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return w, work.ErrNotFound
	}
	return w, err
}

// ActiveEventWatches returns the tenant's ACTIVE event/either watches
// (bounded); the matcher filters them in memory with the predicate.
func (s *Store) ActiveEventWatches(ctx context.Context, tenantID string, limit int) ([]continuous.Watch, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+watchCols+` FROM work_watches WHERE tenant_id=? AND status='ACTIVE'
		AND mode IN ('event','either') ORDER BY created_at LIMIT ?`), tenantID, limit)
	if err != nil {
		return nil, err
	}
	return scanWatches(rows)
}

// DueWatches returns ACTIVE watches whose next check has passed (all tenants;
// the reconciler fires them).
func (s *Store) DueWatches(ctx context.Context, now time.Time, limit int) ([]continuous.Watch, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+watchCols+` FROM work_watches WHERE status='ACTIVE'
		AND next_check_at IS NOT NULL AND next_check_at <= ? ORDER BY next_check_at LIMIT ?`), s.t(now), limit)
	if err != nil {
		return nil, err
	}
	return scanWatches(rows)
}

// FireWatch records a firing and wakes the Work, atomically. It returns
// false (no-op) when the watch is no longer ACTIVE or this firing id was
// already recorded — so a timer and an event racing for one watch wake the
// Work once (T06). Periodic watches stay ACTIVE and advance next_check_at;
// a timeout expires the watch.
func (s *Store) FireWatch(ctx context.Context, f continuous.Firing) (bool, error) {
	now := f.FiredAt
	if now.IsZero() {
		now = s.now()
		f.FiredAt = now
	}
	var fired bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		fired = false
		// Lock order: work, then watch (the same as ApplyWork).
		var workID string
		if err := tx.QueryRowContext(ctx, s.q(`SELECT work_id FROM work_watches WHERE tenant_id=? AND id=?`),
			f.TenantID, f.WatchID).Scan(&workID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		wk, err := s.lockWork(ctx, tx, f.TenantID, workID)
		if err != nil {
			return err
		}
		w, err := scanWatch(tx.QueryRowContext(ctx, s.q(`SELECT `+watchCols+` FROM work_watches WHERE tenant_id=? AND id=?`+s.forUpdate()),
			f.TenantID, f.WatchID))
		if err != nil {
			return err
		}
		if w.Status != continuous.Active {
			return nil
		}
		res, err := tx.ExecContext(ctx, s.q(`INSERT INTO watch_firings (tenant_id, watch_id, firing_id, work_id, kind, payload, fired_at)
			VALUES (?,?,?,?,?,?,?) ON CONFLICT (tenant_id, watch_id, firing_id) DO NOTHING`),
			f.TenantID, f.WatchID, f.FiringID, w.WorkID, f.Kind, js(f.Payload), s.t(now))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		status := continuous.Fired
		next := (*time.Time)(nil)
		switch {
		case f.Kind == string(continuous.DueTimeout):
			status = continuous.Expired
		case f.Kind == string(continuous.DuePeriodic):
			status = continuous.Active
			n := now.Add(w.Interval)
			if w.Deadline != nil && n.After(*w.Deadline) {
				n = *w.Deadline
			}
			next = &n
		}
		res, err = tx.ExecContext(ctx, s.q(`UPDATE work_watches SET status=?, fired_by=?, checks=checks+1, next_check_at=?,
			version=version+1, updated_at=? WHERE tenant_id=? AND id=? AND version=?`),
			string(status), f.FiringID, s.tp(next), s.t(now), f.TenantID, f.WatchID, w.Version)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return work.ErrVersionConflict
		}
		fired = true
		if wk.Status.Terminal() {
			return nil
		}
		if err := s.appendEvents(ctx, tx, &wk, "system", "watch", now, []work.NewEvent{{Type: "work.watch_fired",
			CausationID: f.FiringID, Payload: map[string]any{"watch_id": w.ID, "kind": f.Kind, "mode": w.Mode,
				"purpose": w.Purpose, "status": status}}}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`UPDATE agent_works SET wake_at=?, last_event_seq=?, version=version+1, updated_at=?
			WHERE tenant_id=? AND id=?`), s.t(now), wk.LastSeq, s.t(now), f.TenantID, wk.ID)
		return err
	})
	return fired, err
}

// ListFirings lists the firings of a watch.
func (s *Store) ListFirings(ctx context.Context, tenantID, watchID string) ([]continuous.Firing, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT firing_id, work_id, kind, payload, fired_at FROM watch_firings
		WHERE tenant_id=? AND watch_id=? ORDER BY fired_at`), tenantID, watchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []continuous.Firing
	for rows.Next() {
		f := continuous.Firing{TenantID: tenantID, WatchID: watchID}
		var p jb
		var at ts
		if err := rows.Scan(&f.FiringID, &f.WorkID, &f.Kind, &p, &at); err != nil {
			return nil, err
		}
		f.Payload, f.FiredAt = json.RawMessage(p), at.T
		out = append(out, f)
	}
	return out, rows.Err()
}
