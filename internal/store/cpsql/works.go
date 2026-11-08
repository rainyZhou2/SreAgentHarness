package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

var _ work.Store = (*Store)(nil)

const workCols = `id, tenant_id, kind, status, title, goal, goal_hash, template, generation, stage, stage_state,
	replans, runs_started, cost_micros, reason, priority, owner_actor, source_event, version, last_event_seq,
	wake_at, lease_owner, lease_until, created_at, updated_at, closed_at`

func scanWork(r rowScanner) (work.Work, error) {
	var w work.Work
	var goal, tpl, state jb
	var reason, owner, source, leaseOwner ns
	var wake, leaseUntil, created, updated, closed ts
	var kind, status string
	err := r.Scan(&w.ID, &w.TenantID, &kind, &status, &w.Title, &goal, &w.GoalHash, &tpl, &w.Generation, &w.Stage,
		&state, &w.Replans, &w.RunsStarted, &w.CostMicros, &reason, &w.Priority, &owner, &source, &w.Version,
		&w.LastSeq, &wake, &leaseOwner, &leaseUntil, &created, &updated, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return w, work.ErrNotFound
	}
	if err != nil {
		return w, err
	}
	w.Kind, w.Status = work.Kind(kind), work.Status(status)
	if err := json.Unmarshal(goal, &w.Goal); err != nil {
		return w, fmt.Errorf("work %s goal: %w", w.ID, err)
	}
	if err := json.Unmarshal(tpl, &w.Template); err != nil {
		return w, fmt.Errorf("work %s template: %w", w.ID, err)
	}
	if len(state) > 0 {
		w.StageState = json.RawMessage(state)
	}
	w.Reason, w.OwnerActor, w.SourceEvent, w.LeaseOwner = reason.S(), owner.S(), source.S(), leaseOwner.S()
	w.WakeAt, w.LeaseUntil, w.ClosedAt = wake.ptr(), leaseUntil.ptr(), closed.ptr()
	w.CreatedAt, w.UpdatedAt = created.T, updated.T
	return w, nil
}

// CreateWork inserts a Work (status NEW, wake now) with its creation event.
func (s *Store) CreateWork(ctx context.Context, n work.NewWork) (work.Work, bool, error) {
	if err := n.Prepare(); err != nil {
		return work.Work{}, false, err
	}
	now := n.Now
	if now.IsZero() {
		now = s.now()
	}
	reqHash := n.RequestHash()
	var out work.Work
	var created bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		row := tx.QueryRowContext(ctx, s.q(`SELECT `+workCols+`, create_request_hash FROM agent_works
			WHERE tenant_id=? AND create_idempotency_key=?`), n.TenantID, n.IdempotencyKey)
		var existingHash string
		w, err := scanWork(scanAppend(row, &existingHash))
		switch {
		case err == nil:
			if existingHash != reqHash {
				return work.ErrIdempotencyConflict
			}
			out = w
			return nil
		case !errors.Is(err, work.ErrNotFound):
			return err
		}
		n.Now = now
		w, err = s.createWorkTx(ctx, tx, n)
		if err != nil {
			return err
		}
		out, created = w, true
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		return s.CreateWork(ctx, n)
	}
	return out, created, err
}

var errRetryCreate = errors.New("retry create")

type appendScanner struct {
	r     rowScanner
	extra []any
}

func (a appendScanner) Scan(dest ...any) error { return a.r.Scan(append(dest, a.extra...)...) }

func scanAppend(r rowScanner, extra ...any) rowScanner { return appendScanner{r, extra} }

func (s *Store) insertSubject(ctx context.Context, tx *sql.Tx, tenant, workID string, sl work.SubjectLink, now time.Time) error {
	if sl.Kind == "" || sl.ID == "" || len(sl.Kind) > 64 || len(sl.ID) > 512 {
		return fmt.Errorf("%w: subject link needs kind and id", work.ErrInvalid)
	}
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_subject_links (tenant_id, work_id, subject_kind, subject_id, created_at)
		VALUES (?,?,?,?,?) ON CONFLICT (tenant_id, work_id, subject_kind, subject_id) DO NOTHING`),
		tenant, workID, sl.Kind, sl.ID, s.t(now))
	return err
}

// appendEvents writes events with consecutive sequence numbers after
// w.LastSeq and advances it (the caller saves the row).
func (s *Store) appendEvents(ctx context.Context, tx *sql.Tx, w *work.Work, actorType, actorID string, now time.Time, evs []work.NewEvent) error {
	for _, e := range evs {
		w.LastSeq++
		payload := js(e.Payload)
		if p, ok := payload.(string); ok && len(p) > 64<<10 {
			payload = js(map[string]any{"truncated": true, "bytes": len(p)})
		}
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_events (tenant_id, work_id, event_seq, event_id, type,
			actor_type, actor_id, causation_id, payload, occurred_at) VALUES (?,?,?,?,?,?,?,?,?,?)`),
			w.TenantID, w.ID, w.LastSeq, cpkit.NewID(), e.Type, actorType, nullStr(actorID), nullStr(e.CausationID),
			payload, s.t(now)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) saveWorkCounters(ctx context.Context, tx *sql.Tx, w work.Work) error {
	_, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET last_event_seq=? WHERE tenant_id=? AND id=?`),
		w.LastSeq, w.TenantID, w.ID)
	return err
}

// GetWork reads one Work.
func (s *Store) GetWork(ctx context.Context, tenantID, workID string) (work.Work, error) {
	if !cpkit.ValidID(workID) {
		return work.Work{}, work.ErrNotFound
	}
	return scanWork(s.db.QueryRowContext(ctx, s.q(`SELECT `+workCols+` FROM agent_works WHERE tenant_id=? AND id=?`),
		tenantID, workID))
}

func (s *Store) lockWork(ctx context.Context, tx *sql.Tx, tenantID, workID string) (work.Work, error) {
	if !cpkit.ValidID(workID) {
		return work.Work{}, work.ErrNotFound
	}
	return scanWork(tx.QueryRowContext(ctx, s.q(`SELECT `+workCols+` FROM agent_works WHERE tenant_id=? AND id=?`+s.forUpdate()),
		tenantID, workID))
}

// ListWorks lists Works newest-updated first.
func (s *Store) ListWorks(ctx context.Context, f work.Filter) ([]work.Work, error) {
	if f.Limit <= 0 || f.Limit > 200 {
		f.Limit = 50
	}
	where := []string{"tenant_id=?"}
	args := []any{f.TenantID}
	if len(f.Status) > 0 {
		where = append(where, "status IN ("+placeholders(len(f.Status))+")")
		for _, st := range f.Status {
			args = append(args, string(st))
		}
	}
	if f.Kind != "" {
		where = append(where, "kind=?")
		args = append(args, string(f.Kind))
	}
	if f.Subject != "" {
		where = append(where, `id IN (SELECT work_id FROM work_subject_links WHERE tenant_id=? AND subject_kind='subject' AND subject_id=?)`)
		args = append(args, f.TenantID, f.Subject)
	}
	if f.BeforeUpdated != nil {
		where = append(where, "(updated_at < ? OR (updated_at = ? AND id < ?))")
		args = append(args, s.t(*f.BeforeUpdated), s.t(*f.BeforeUpdated), f.BeforeID)
	}
	args = append(args, f.Limit)
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+workCols+` FROM agent_works WHERE `+strings.Join(where, " AND ")+
		` ORDER BY updated_at DESC, id DESC LIMIT ?`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []work.Work
	for rows.Next() {
		w, err := scanWork(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ApplyWork applies one mutation atomically (see work.Mutation).
func (s *Store) ApplyWork(ctx context.Context, m work.Mutation) (work.Work, error) {
	if err := work.CheckMutation(&m); err != nil {
		return work.Work{}, err
	}
	now := m.Now
	if now.IsZero() {
		now = s.now()
	}
	var out work.Work
	err := s.tx(ctx, func(tx *sql.Tx) error {
		w, err := s.lockWork(ctx, tx, m.TenantID, m.WorkID)
		if err != nil {
			return err
		}
		if c := m.Command; c != nil {
			var hash string
			var result jb
			err := tx.QueryRowContext(ctx, s.q(`SELECT request_hash, result FROM work_commands
				WHERE tenant_id=? AND work_id=? AND idempotency_key=?`), m.TenantID, m.WorkID, c.Key).Scan(&hash, &result)
			switch {
			case err == nil:
				if hash != c.RequestHash {
					return work.ErrIdempotencyConflict
				}
				var prev work.Work
				if err := json.Unmarshal(result, &prev); err != nil {
					return err
				}
				out = prev
				return nil
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		if m.ExpectedVersion != 0 && m.ExpectedVersion != w.Version {
			return fmt.Errorf("%w: expected %d, current %d", work.ErrVersionConflict, m.ExpectedVersion, w.Version)
		}
		if m.LeaseOwner != "" && (w.LeaseOwner != m.LeaseOwner || w.LeaseUntil == nil || !w.LeaseUntil.After(now)) {
			return work.ErrLeaseLost
		}
		from := w.Status
		if m.Status != nil {
			cmd := ""
			if m.Command != nil {
				cmd = m.Command.Type
			}
			newGen := m.Generation != nil && *m.Generation > w.Generation
			if err := work.TransitionFor(from, *m.Status, cmd, newGen); err != nil {
				return err
			}
			w.Status = *m.Status
		}
		if m.Reason != nil {
			w.Reason = cpkit.Truncate(*m.Reason, 2000)
		}
		if m.Stage != nil {
			if *m.Stage < 0 || *m.Stage > len(w.Template.Stages) {
				return fmt.Errorf("%w: stage %d out of range", work.ErrInvalid, *m.Stage)
			}
			w.Stage = *m.Stage
		}
		if m.ClearState {
			w.StageState = nil
		}
		if len(m.StageState) > 0 {
			if len(m.StageState) > 256<<10 || !json.Valid(m.StageState) {
				return fmt.Errorf("%w: stage state must be JSON up to 256 KiB", work.ErrInvalid)
			}
			w.StageState = m.StageState
		}
		if m.Generation != nil {
			if *m.Generation < w.Generation {
				return fmt.Errorf("%w: generation cannot go back", work.ErrInvalid)
			}
			w.Generation = *m.Generation
		}
		w.Replans += m.AddReplans
		w.RunsStarted += m.AddRuns
		w.CostMicros += m.AddCost
		if m.ClearWake || w.Status.Terminal() {
			w.WakeAt = nil
		}
		if m.WakeAt != nil {
			t := m.WakeAt.UTC()
			w.WakeAt = &t
		}
		if m.ReleaseLease {
			w.LeaseOwner, w.LeaseUntil = "", nil
		}
		if w.Status.Terminal() && w.ClosedAt == nil {
			w.ClosedAt = &now
		}
		if !w.Status.Terminal() {
			w.ClosedAt = nil
		}
		for _, l := range m.Links {
			if err := s.insertLink(ctx, tx, w, l, now); err != nil {
				return err
			}
		}
		for runID, st := range m.LinkStatus {
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_run_links SET status=? WHERE tenant_id=? AND work_id=? AND run_id=?`),
				st, w.TenantID, w.ID, runID); err != nil {
				return err
			}
		}
		if m.CancelAllWatches {
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_watches SET status='CANCELLED', version=version+1, updated_at=?
				WHERE tenant_id=? AND work_id=? AND status='ACTIVE'`), s.t(now), w.TenantID, w.ID); err != nil {
				return err
			}
		}
		for _, id := range m.CancelWatches {
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_watches SET status='CANCELLED', version=version+1, updated_at=?
				WHERE tenant_id=? AND work_id=? AND id=? AND status='ACTIVE'`), s.t(now), w.TenantID, w.ID, id); err != nil {
				return err
			}
		}
		for _, wt := range m.Watches {
			if wt.TenantID != w.TenantID || wt.WorkID != w.ID {
				return fmt.Errorf("%w: watch belongs to another work", work.ErrInvalid)
			}
			if err := s.insertWatch(ctx, tx, wt); err != nil {
				return err
			}
		}
		if ev := m.Outcome; ev != nil {
			if ev.ID == "" {
				ev.ID = cpkit.NewID()
			}
			ev.TenantID, ev.WorkID = w.TenantID, w.ID
			if !ev.Verdict.Valid() {
				return fmt.Errorf("%w: verdict %q", work.ErrInvalid, ev.Verdict)
			}
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_outcomes (id, tenant_id, work_id, spec_hash, verdict,
				raw_verdict, stage_id, generation, evaluation, evaluated_at) VALUES (?,?,?,?,?,?,?,?,?,?)`),
				ev.ID, w.TenantID, w.ID, ev.SpecHash, string(ev.Verdict), string(ev.RawVerdict), nullStr(ev.Stage),
				w.Generation, js(ev), s.t(ev.EvaluatedAt)); err != nil {
				return err
			}
		}
		for _, sl := range m.Subjects {
			if err := s.insertSubject(ctx, tx, w.TenantID, w.ID, sl, now); err != nil {
				return err
			}
		}
		evs := m.Events
		if w.Status != from {
			evs = append([]work.NewEvent{{Type: "work.status_changed", Payload: map[string]any{
				"from": from, "to": w.Status, "reason": w.Reason, "stage": w.Stage, "generation": w.Generation}}}, evs...)
		}
		if m.Command != nil {
			evs = append(evs, work.NewEvent{Type: "work.command_applied", Payload: map[string]any{
				"command": m.Command.Type, "idempotency_key": m.Command.Key}})
		}
		if err := s.appendEvents(ctx, tx, &w, m.ActorType, m.ActorID, now, evs); err != nil {
			return err
		}
		w.Version++
		w.UpdatedAt = now
		var state any
		if len(w.StageState) > 0 {
			state = string(w.StageState)
		}
		res, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET status=?, reason=?, stage=?, stage_state=?, generation=?,
			replans=?, runs_started=?, cost_micros=?, wake_at=?, lease_owner=?, lease_until=?, version=?,
			last_event_seq=?, updated_at=?, closed_at=? WHERE tenant_id=? AND id=? AND version=?`),
			string(w.Status), nullStr(w.Reason), w.Stage, state, w.Generation, w.Replans, w.RunsStarted, w.CostMicros,
			s.tp(w.WakeAt), nullStr(w.LeaseOwner), s.tp(w.LeaseUntil), w.Version, w.LastSeq, s.t(now), s.tp(w.ClosedAt),
			w.TenantID, w.ID, w.Version-1)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return work.ErrVersionConflict
		}
		if c := m.Command; c != nil {
			if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_commands (tenant_id, work_id, idempotency_key,
				command_type, request_hash, actor_id, expected_version, result, created_at) VALUES (?,?,?,?,?,?,?,?,?)`),
				w.TenantID, w.ID, c.Key, c.Type, c.RequestHash, nullStr(m.ActorID), m.ExpectedVersion, js(w), s.t(now)); err != nil {
				return err
			}
		}
		out = w
		return nil
	})
	return out, err
}

func (s *Store) insertLink(ctx context.Context, tx *sql.Tx, w work.Work, l work.RunLink, now time.Time) error {
	if !cpkit.ValidID(l.RunID) {
		return fmt.Errorf("%w: run id %q", work.ErrInvalid, l.RunID)
	}
	if l.Role == work.RoleOwner {
		var other string
		err := tx.QueryRowContext(ctx, s.q(`SELECT work_id FROM work_run_links WHERE tenant_id=? AND run_id=? AND role='owner'`),
			w.TenantID, l.RunID).Scan(&other)
		switch {
		case err == nil && other != w.ID:
			return fmt.Errorf("%w: run %s is owned by work %s", work.ErrOwnerConflict, l.RunID, other)
		case err == nil:
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
	}
	gen := l.Generation
	if gen == 0 {
		gen = w.Generation
	}
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_run_links (tenant_id, work_id, run_id, role, phase, stage_id,
		agent_role, generation, required, status, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (tenant_id, work_id, run_id) DO NOTHING`),
		w.TenantID, w.ID, l.RunID, l.Role, l.Phase, nullStr(l.StageID), nullStr(l.AgentRole), gen, l.Required,
		nullStr(l.Status), s.t(now))
	if isUnique(err, "") {
		return fmt.Errorf("%w: run %s", work.ErrOwnerConflict, l.RunID)
	}
	return err
}

// ListWorkEvents pages the timeline after a sequence number.
func (s *Store) ListWorkEvents(ctx context.Context, tenantID, workID string, afterSeq int64, limit int) ([]work.Event, error) {
	if !cpkit.ValidID(workID) {
		return nil, work.ErrNotFound
	}
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT event_seq, event_id, type, actor_type, actor_id, causation_id,
		payload, occurred_at FROM work_events WHERE tenant_id=? AND work_id=? AND event_seq>? ORDER BY event_seq LIMIT ?`),
		tenantID, workID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []work.Event
	for rows.Next() {
		e := work.Event{TenantID: tenantID, WorkID: workID}
		var actor, cause ns
		var payload jb
		var at ts
		if err := rows.Scan(&e.Seq, &e.ID, &e.Type, &e.ActorType, &actor, &cause, &payload, &at); err != nil {
			return nil, err
		}
		e.ActorID, e.CausationID, e.OccurredAt = actor.S(), cause.S(), at.T
		if len(payload) > 0 {
			e.Payload = json.RawMessage(payload)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) scanLinks(rows *sql.Rows, tenantID string) ([]work.RunLink, error) {
	defer rows.Close()
	var out []work.RunLink
	for rows.Next() {
		l := work.RunLink{TenantID: tenantID}
		var stage, role, status ns
		var at ts
		if err := rows.Scan(&l.WorkID, &l.RunID, &l.Role, &l.Phase, &stage, &role, &l.Generation, &l.Required, &status, &at); err != nil {
			return nil, err
		}
		l.StageID, l.AgentRole, l.Status, l.CreatedAt = stage.S(), role.S(), status.S(), at.T
		out = append(out, l)
	}
	return out, rows.Err()
}

const linkCols = `work_id, run_id, role, phase, stage_id, agent_role, generation, required, status, created_at`

// ListRunLinks lists a Work's runs in creation order.
func (s *Store) ListRunLinks(ctx context.Context, tenantID, workID string) ([]work.RunLink, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+linkCols+` FROM work_run_links WHERE tenant_id=? AND work_id=?
		ORDER BY created_at, run_id`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	return s.scanLinks(rows, tenantID)
}

// WorkForRun returns the owner link of a run.
func (s *Store) WorkForRun(ctx context.Context, tenantID, runID string) (work.RunLink, bool, error) {
	if !cpkit.ValidID(runID) {
		return work.RunLink{}, false, nil
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+linkCols+` FROM work_run_links WHERE tenant_id=? AND run_id=? AND role='owner'`),
		tenantID, runID)
	if err != nil {
		return work.RunLink{}, false, err
	}
	links, err := s.scanLinks(rows, tenantID)
	if err != nil || len(links) == 0 {
		return work.RunLink{}, false, err
	}
	return links[0], true, nil
}

// ListSubjects lists a Work's subject links.
func (s *Store) ListSubjects(ctx context.Context, tenantID, workID string) ([]work.SubjectLink, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT subject_kind, subject_id FROM work_subject_links
		WHERE tenant_id=? AND work_id=? ORDER BY subject_kind, subject_id`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []work.SubjectLink
	for rows.Next() {
		var l work.SubjectLink
		if err := rows.Scan(&l.Kind, &l.ID); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ListOutcomes lists recent outcome evaluations, newest first.
func (s *Store) ListOutcomes(ctx context.Context, tenantID, workID string, limit int) ([]outcome.Evaluation, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT evaluation FROM work_outcomes WHERE tenant_id=? AND work_id=?
		ORDER BY evaluated_at DESC, id DESC LIMIT ?`), tenantID, workID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []outcome.Evaluation
	for rows.Next() {
		var b jb
		var ev outcome.Evaluation
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &ev); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// ClaimDueWorks leases due Works for one orchestrator replica.
func (s *Store) ClaimDueWorks(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]work.Work, error) {
	if limit <= 0 {
		limit = 16
	}
	var out []work.Work
	err := s.tx(ctx, func(tx *sql.Tx) error {
		out = out[:0]
		rows, err := tx.QueryContext(ctx, s.q(`SELECT id FROM agent_works WHERE wake_at IS NOT NULL AND wake_at <= ?
			AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED') AND (lease_until IS NULL OR lease_until < ?)
			ORDER BY priority DESC, wake_at LIMIT ?`+s.skipLocked()), s.t(now), s.t(now), limit)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		until := now.Add(lease)
		for _, id := range ids {
			row := tx.QueryRowContext(ctx, s.q(`UPDATE agent_works SET lease_owner=?, lease_until=?
				WHERE id=? AND (lease_until IS NULL OR lease_until < ?) RETURNING `+workCols),
				owner, s.t(until), id, s.t(now))
			w, err := scanWork(row)
			if errors.Is(err, work.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			out = append(out, w)
		}
		return nil
	})
	return out, err
}

// ReleaseWork drops a lease without changing the Work (e.g. after a
// processing error); the wake time stays, so it is retried.
func (s *Store) ReleaseWork(ctx context.Context, tenantID, workID, owner string, retryAt time.Time) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET lease_owner=NULL, lease_until=NULL,
		wake_at=CASE WHEN status IN ('SUCCEEDED','FAILED','CANCELLED') THEN NULL ELSE ? END
		WHERE tenant_id=? AND id=? AND lease_owner=?`), s.t(retryAt), tenantID, workID, owner)
	return err
}

// WakeWork asks the orchestrator to look at a Work now (e.g. an approval
// decision, a linked run finished). It appends a timeline event.
func (s *Store) WakeWork(ctx context.Context, tenantID, workID, reason, actorType, actorID string, payload any) error {
	now := s.now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		w, err := s.lockWork(ctx, tx, tenantID, workID)
		if err != nil {
			return err
		}
		if w.Status.Terminal() {
			return nil
		}
		if err := s.appendEvents(ctx, tx, &w, actorType, actorID, now, []work.NewEvent{{Type: "work.wake", Payload: map[string]any{
			"reason": reason, "detail": payload}}}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`UPDATE agent_works SET wake_at=?, last_event_seq=?, version=version+1, updated_at=?
			WHERE tenant_id=? AND id=?`), s.t(now), w.LastSeq, s.t(now), tenantID, workID)
		return err
	})
}

// CountByStatus counts Works by kind and status (low-cardinality gauge).
func (s *Store) CountByStatus(ctx context.Context) (map[string]map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT kind, status, count(*) FROM agent_works
		WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELLED') GROUP BY kind, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]int{}
	for rows.Next() {
		var k, st string
		var n int
		if err := rows.Scan(&k, &st, &n); err != nil {
			return nil, err
		}
		if out[k] == nil {
			out[k] = map[string]int{}
		}
		out[k][st] = n
	}
	return out, rows.Err()
}

// ListWatches lists a Work's watches (active only when asked).
func (s *Store) ListWatches(ctx context.Context, tenantID, workID string, activeOnly bool) ([]continuous.Watch, error) {
	q := `SELECT ` + watchCols + ` FROM work_watches WHERE tenant_id=? AND work_id=?`
	if activeOnly {
		q += ` AND status='ACTIVE'`
	}
	rows, err := s.db.QueryContext(ctx, s.q(q+` ORDER BY created_at, id`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	return scanWatches(rows)
}

// CommandResult returns the stored result of an operator command, so a
// replay is answered with the original outcome even after the Work moved on.
func (s *Store) CommandResult(ctx context.Context, tenantID, workID, key string) (work.Work, string, bool, error) {
	var hash string
	var result jb
	err := s.db.QueryRowContext(ctx, s.q(`SELECT request_hash, result FROM work_commands
		WHERE tenant_id=? AND work_id=? AND idempotency_key=?`), tenantID, workID, key).Scan(&hash, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return work.Work{}, "", false, nil
	}
	if err != nil {
		return work.Work{}, "", false, err
	}
	var w work.Work
	if err := json.Unmarshal(result, &w); err != nil {
		return w, "", false, err
	}
	return w, hash, true, nil
}
