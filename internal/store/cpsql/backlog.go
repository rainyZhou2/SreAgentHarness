package cpsql

import (
	"context"
	"time"
)

// BacklogItem is a count with the age of its oldest member.
type BacklogItem struct {
	Count  int
	Oldest time.Duration
}

// Backlog is the operator view of what waits (RFC §21.3): actions needing
// a decision or recovery, Works waiting on humans, unprocessed events,
// overdue watches, and an approval invariant that must stay zero.
type Backlog struct {
	Actions map[string]BacklogItem // by action state
	Works   map[string]BacklogItem // WAITING / BLOCKED / SUSPENDED
	Inbox   map[string]BacklogItem // NEW / FAILED / DEAD
	// Due are Works ready to be processed and not leased, per queue: a
	// growing count or age shows a starved queue.
	Due map[string]BacklogItem
	// OverdueWatches are ACTIVE watches due for longer than the grace
	// period: the reconciler is not making progress.
	OverdueWatches BacklogItem
	// UnapprovedDispatches are actions past READY that required approval
	// but carry neither an approval nor a break-glass grant. Must be 0.
	UnapprovedDispatches int
}

// WatchGrace is how long a due watch may wait before it counts as overdue.
const WatchGrace = time.Minute

// Backlog reads the control-plane backlog (all tenants: a metrics source).
func (s *Store) Backlog(ctx context.Context, now time.Time) (Backlog, error) {
	b := Backlog{Actions: map[string]BacklogItem{}, Works: map[string]BacklogItem{}, Inbox: map[string]BacklogItem{},
		Due: map[string]BacklogItem{}}
	group := func(dst map[string]BacklogItem, q string, args ...any) error {
		rows, err := s.db.QueryContext(ctx, s.q(q), args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k string
			var n int
			var oldest ts
			if err := rows.Scan(&k, &n, &oldest); err != nil {
				return err
			}
			it := BacklogItem{Count: n}
			if oldest.Valid {
				it.Oldest = max(now.Sub(oldest.T), 0)
			}
			dst[k] = it
		}
		return rows.Err()
	}
	if err := group(b.Actions, `SELECT state, count(*), min(updated_at) FROM work_actions
		WHERE state IN ('WAITING_APPROVAL','READY','DISPATCHED','EFFECT_UNKNOWN','MANUAL_RECOVERY','VERIFYING','DEGRADED','COMPENSATION_PENDING')
		GROUP BY state`); err != nil {
		return b, err
	}
	if err := group(b.Works, `SELECT status, count(*), min(updated_at) FROM agent_works
		WHERE status IN ('WAITING','BLOCKED','SUSPENDED') GROUP BY status`); err != nil {
		return b, err
	}
	if err := group(b.Due, `SELECT queue, count(*), min(wake_at) FROM agent_works WHERE wake_at IS NOT NULL AND wake_at <= ?
		AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED') AND (lease_until IS NULL OR lease_until < ?)
		GROUP BY queue`, s.t(now), s.t(now)); err != nil {
		return b, err
	}
	if err := group(b.Inbox, `SELECT processing_state, count(*), min(received_at) FROM event_inbox
		WHERE processing_state IN ('NEW','FAILED','DEAD') GROUP BY processing_state`); err != nil {
		return b, err
	}
	var n int
	var oldest ts
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*), min(next_check_at) FROM work_watches
		WHERE status='ACTIVE' AND next_check_at < ?`), s.t(now.Add(-WatchGrace))).Scan(&n, &oldest); err != nil {
		return b, err
	}
	b.OverdueWatches = BacklogItem{Count: n}
	if oldest.Valid {
		b.OverdueWatches.Oldest = max(now.Sub(oldest.T), 0)
	}
	q := `SELECT count(*) FROM work_actions WHERE state IN ('DISPATCHED','APPLIED','VERIFYING','VERIFIED','DEGRADED',
		'EFFECT_UNKNOWN','MANUAL_RECOVERY','COMPENSATION_PENDING','COMPENSATED') AND dispatched_at > ?
		AND coalesce(json_extract(record, '$.required_approvals'), 0) > 0
		AND coalesce(json_array_length(json_extract(record, '$.approvals')), 0) = 0
		AND json_extract(record, '$.break_glass') IS NULL`
	if s.d == Postgres {
		q = `SELECT count(*) FROM work_actions WHERE state IN ('DISPATCHED','APPLIED','VERIFYING','VERIFIED','DEGRADED',
			'EFFECT_UNKNOWN','MANUAL_RECOVERY','COMPENSATION_PENDING','COMPENSATED') AND dispatched_at > $1
			AND coalesce((record->>'required_approvals')::int, 0) > 0
			AND coalesce(jsonb_array_length(record->'approvals'), 0) = 0
			AND (record->'break_glass') IS NULL`
	}
	if err := s.db.QueryRowContext(ctx, q, s.t(now.Add(-7*24*time.Hour))).Scan(&b.UnapprovedDispatches); err != nil {
		return b, err
	}
	return b, nil
}
