package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

// InsertEvent stores a normalized envelope in the inbox. A redelivery of the
// same (tenant, source, event id) returns the stored record with
// created=false and is not processed again (T04).
func (s *Store) InsertEvent(ctx context.Context, e eventing.Envelope, receivedAt time.Time) (eventing.Record, bool, error) {
	rec := eventing.Record{InboxID: cpkit.NewID(), Envelope: e, State: eventing.StateNew, ReceivedAt: receivedAt, UpdatedAt: receivedAt}
	res, err := s.db.ExecContext(ctx, s.q(`INSERT INTO event_inbox (id, tenant_id, source, external_event_id, event_type,
		subject_ref, correlation_id, dedup_key, envelope, payload_ref, payload_hash, occurred_at, observed_at, received_at,
		processing_state, attempts, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'NEW',0,?)
		ON CONFLICT (tenant_id, source, external_event_id) DO NOTHING`),
		rec.InboxID, e.TenantID, e.Source, e.ID, e.Type, e.SubjectRef, nullStr(e.CorrelationID), nullStr(e.DedupKey),
		js(e), nullStr(e.PayloadRef), nullStr(e.PayloadHash), s.t(e.OccurredAt), s.t(e.ObservedAt), s.t(receivedAt), s.t(receivedAt))
	if err != nil {
		return rec, false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return rec, true, nil
	}
	existing, err := s.getEventBy(ctx, `tenant_id=? AND source=? AND external_event_id=?`, e.TenantID, e.Source, e.ID)
	return existing, false, err
}

const inboxCols = `id, envelope, processing_state, attempts, last_error, work_id, decision, received_at, updated_at`

func scanInbox(r rowScanner) (eventing.Record, error) {
	var rec eventing.Record
	var env jb
	var lastErr, workID, decision ns
	var recv, upd ts
	if err := r.Scan(&rec.InboxID, &env, &rec.State, &rec.Attempts, &lastErr, &workID, &decision, &recv, &upd); err != nil {
		return rec, err
	}
	if err := json.Unmarshal(env, &rec.Envelope); err != nil {
		return rec, err
	}
	rec.LastError, rec.WorkID, rec.Decision = lastErr.S(), workID.S(), decision.S()
	rec.ReceivedAt, rec.UpdatedAt = recv.T, upd.T
	return rec, nil
}

func (s *Store) getEventBy(ctx context.Context, where string, args ...any) (eventing.Record, error) {
	rec, err := scanInbox(s.db.QueryRowContext(ctx, s.q(`SELECT `+inboxCols+` FROM event_inbox WHERE `+where), args...))
	if errors.Is(err, sql.ErrNoRows) {
		return rec, work.ErrNotFound
	}
	return rec, err
}

// GetEvent reads an inbox record by inbox id.
func (s *Store) GetEvent(ctx context.Context, tenantID, inboxID string) (eventing.Record, error) {
	if !cpkit.ValidID(inboxID) {
		return eventing.Record{}, work.ErrNotFound
	}
	return s.getEventBy(ctx, `tenant_id=? AND id=?`, tenantID, inboxID)
}

// MarkEvent records the processing outcome of an inbox record.
func (s *Store) MarkEvent(ctx context.Context, tenantID, inboxID, state, workID, decision, errMsg string, maxAttempts int) error {
	now := s.now()
	if state == eventing.StateFailed {
		// Repeated failures become DEAD (kept for repair, never dropped).
		_, err := s.db.ExecContext(ctx, s.q(`UPDATE event_inbox SET attempts=attempts+1, last_error=?, updated_at=?,
			processing_state=CASE WHEN attempts+1 >= ? THEN 'DEAD' ELSE 'FAILED' END WHERE tenant_id=? AND id=?`),
			cpkit.Truncate(errMsg, 2000), s.t(now), maxAttempts, tenantID, inboxID)
		return err
	}
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE event_inbox SET processing_state=?, work_id=?, decision=?, last_error=NULL,
		attempts=attempts+1, updated_at=? WHERE tenant_id=? AND id=?`),
		state, nullStr(workID), nullStr(decision), s.t(now), tenantID, inboxID)
	return err
}

// PendingEvents returns NEW and FAILED inbox records for (re)processing.
func (s *Store) PendingEvents(ctx context.Context, olderThan time.Time, limit int) ([]eventing.Record, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+inboxCols+` FROM event_inbox
		WHERE processing_state IN ('NEW','FAILED') AND updated_at <= ? ORDER BY received_at LIMIT ?`), s.t(olderThan), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventing.Record
	for rows.Next() {
		r, err := scanInbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// EventFilter selects inbox records.
type EventFilter struct {
	TenantID string
	WorkID   string
	Subject  string
	Type     string
	State    string
	Limit    int
}

// ListEvents lists inbox records newest first.
func (s *Store) ListEvents(ctx context.Context, f EventFilter) ([]eventing.Record, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	q := `SELECT ` + inboxCols + ` FROM event_inbox WHERE tenant_id=?`
	args := []any{f.TenantID}
	if f.WorkID != "" {
		if !cpkit.ValidID(f.WorkID) {
			return nil, nil
		}
		q += ` AND work_id=?`
		args = append(args, f.WorkID)
	}
	if f.Subject != "" {
		q += ` AND subject_ref=?`
		args = append(args, f.Subject)
	}
	if f.Type != "" {
		q += ` AND event_type=?`
		args = append(args, f.Type)
	}
	if f.State != "" {
		q += ` AND processing_state=?`
		args = append(args, f.State)
	}
	args = append(args, f.Limit)
	rows, err := s.db.QueryContext(ctx, s.q(q+` ORDER BY received_at DESC, id DESC LIMIT ?`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventing.Record
	for rows.Next() {
		r, err := scanInbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CountEvents counts processed-or-pending events of a type for a subject
// in [since, until] by occurrence time (outcome.EventSource).
func (s *Store) CountEvents(ctx context.Context, tenantID, eventType, subjectRef string, since, until time.Time) (int, error) {
	q := `SELECT count(*) FROM event_inbox WHERE tenant_id=? AND event_type=? AND occurred_at >= ? AND occurred_at <= ?`
	args := []any{tenantID, eventType, s.t(since), s.t(until)}
	if subjectRef != "" {
		q += ` AND subject_ref=?`
		args = append(args, subjectRef)
	}
	var n int
	err := s.db.QueryRowContext(ctx, s.q(q), args...).Scan(&n)
	return n, err
}

// Correlation decisions.
const (
	CorrCreated  = "created"
	CorrAttached = "attached"
	CorrNoWork   = "no_open_work"
)

// CorrelateRequest asks for the Work of a correlation group.
type CorrelateRequest struct {
	TenantID string
	GroupKey string
	Rule     string
	Window   time.Duration
	Now      time.Time
	// Create, when set, creates a Work if the group has no open one.
	Create *work.NewWork
	// EventID is recorded on the Work timeline when attaching.
	EventID string
	Payload any
}

// Correlate attaches an event to the open Work of its group or creates one,
// atomically: concurrent deliveries of one group converge on one Work (T05).
// A group whose Work closed, or whose window lapsed, starts a new Work.
func (s *Store) Correlate(ctx context.Context, req CorrelateRequest) (work.Work, string, error) {
	now := req.Now
	if now.IsZero() {
		now = s.now()
	}
	var out work.Work
	var decision string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		decision = ""
		var workID string
		var exp ts
		err := tx.QueryRowContext(ctx, s.q(`SELECT work_id, expires_at FROM work_correlations WHERE tenant_id=? AND group_key=?`+s.forUpdate()),
			req.TenantID, req.GroupKey).Scan(&workID, &exp)
		existing := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if existing && exp.T.After(now) {
			w, err := s.lockWork(ctx, tx, req.TenantID, workID)
			if err != nil {
				return err
			}
			if !w.Status.Terminal() {
				if err := s.appendEvents(ctx, tx, &w, "system", "correlator", now, []work.NewEvent{{Type: "work.event_attached",
					CausationID: req.EventID, Payload: req.Payload}}); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET last_event_seq=?, wake_at=?, version=version+1,
					updated_at=? WHERE tenant_id=? AND id=?`), w.LastSeq, s.t(now), s.t(now), w.TenantID, w.ID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_correlations SET hits=hits+1, last_seen=?, expires_at=?
					WHERE tenant_id=? AND group_key=?`), s.t(now), s.t(now.Add(req.Window)), req.TenantID, req.GroupKey); err != nil {
					return err
				}
				w.Version++
				out, decision = w, CorrAttached
				return nil
			}
		}
		if req.Create == nil {
			decision = CorrNoWork
			return nil
		}
		n := *req.Create
		n.TenantID = req.TenantID
		n.Now = now
		if n.IdempotencyKey == "" {
			n.IdempotencyKey = "corr:" + req.GroupKey + ":" + now.Format("20060102T150405.000000")
		}
		w, err := s.createWorkTx(ctx, tx, n)
		if err != nil {
			return err
		}
		if existing {
			_, err = tx.ExecContext(ctx, s.q(`UPDATE work_correlations SET work_id=?, rule=?, hits=1, first_seen=?, last_seen=?,
				expires_at=? WHERE tenant_id=? AND group_key=?`), w.ID, req.Rule, s.t(now), s.t(now), s.t(now.Add(req.Window)),
				req.TenantID, req.GroupKey)
		} else {
			_, err = tx.ExecContext(ctx, s.q(`INSERT INTO work_correlations (tenant_id, group_key, work_id, rule, hits, first_seen,
				last_seen, expires_at) VALUES (?,?,?,?,1,?,?,?)`), req.TenantID, req.GroupKey, w.ID, req.Rule, s.t(now), s.t(now),
				s.t(now.Add(req.Window)))
		}
		if isUnique(err, "") {
			return errRetryCreate // a concurrent delivery created the group; re-run attaches
		}
		if err != nil {
			return err
		}
		out, decision = w, CorrCreated
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		return s.Correlate(ctx, req)
	}
	return out, decision, err
}

// createWorkTx creates a Work inside an existing transaction (no
// idempotent replay: callers own the key's uniqueness).
func (s *Store) createWorkTx(ctx context.Context, tx *sql.Tx, n work.NewWork) (work.Work, error) {
	if err := n.Prepare(); err != nil {
		return work.Work{}, err
	}
	now := n.Now
	w := work.Work{ID: cpkit.NewID(), TenantID: n.TenantID, Kind: n.Kind, Status: work.StatusNew, Title: n.Title,
		Goal: n.Goal, GoalHash: cpkit.MustHash(n.Goal), Template: n.Template, Generation: 1, Priority: n.Priority,
		OwnerActor: n.ActorID, SourceEvent: n.SourceEvent, Version: 1, CreatedAt: now, UpdatedAt: now, WakeAt: &now}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO agent_works (id, tenant_id, kind, status, title, goal, goal_hash,
		template, template_id, template_version, generation, stage, replans, runs_started, cost_micros, priority,
		owner_actor, source_event, version, last_event_seq, wake_at, create_idempotency_key, create_request_hash,
		created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,1,0,0,0,0,?,?,?,1,0,?,?,?,?,?)`),
		w.ID, w.TenantID, string(w.Kind), string(w.Status), w.Title, js(w.Goal), w.GoalHash, js(w.Template),
		w.Template.ID, w.Template.Version, w.Priority, nullStr(w.OwnerActor), nullStr(w.SourceEvent),
		s.t(now), n.IdempotencyKey, n.RequestHash(), s.t(now), s.t(now)); err != nil {
		if isUnique(err, "") {
			return w, errRetryCreate
		}
		return w, err
	}
	subjects := append([]work.SubjectLink(nil), n.Subjects...)
	for _, ref := range w.Goal.SubjectRefs {
		subjects = append(subjects, work.SubjectLink{Kind: "subject", ID: ref})
	}
	for _, sl := range subjects {
		if err := s.insertSubject(ctx, tx, w.TenantID, w.ID, sl, now); err != nil {
			return w, err
		}
	}
	if err := s.appendEvents(ctx, tx, &w, n.ActorType, n.ActorID, now, []work.NewEvent{{Type: "work.created",
		CausationID: n.SourceEvent, Payload: map[string]any{"kind": w.Kind, "title": w.Title, "goal_hash": w.GoalHash,
			"template": fmt.Sprintf("%s@%d", w.Template.ID, w.Template.Version), "subjects": w.Goal.SubjectRefs}}}); err != nil {
		return w, err
	}
	return w, s.saveWorkCounters(ctx, tx, w)
}

// PurgeCorrelations deletes expired correlation groups.
func (s *Store) PurgeCorrelations(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	_ = limit
	res, err := s.db.ExecContext(ctx, s.q(`DELETE FROM work_correlations WHERE expires_at < ?`), s.t(before))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
