package cpsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

var (
	_ action.Store           = (*Store)(nil)
	_ evidence.Store         = (*Store)(nil)
	_ action.EffectLedger    = (*Store)(nil)
	_ action.UnsettledFinder = (*Store)(nil)
)

// UnsettledActions lists actions on any of the refs whose external effect
// is still open: dispatched without a result, unknown, or waiting for a
// human. Both the name the action was planned under and the canonical ref
// its lock was taken on are matched.
func (s *Store) UnsettledActions(ctx context.Context, tenantID string, targetRefs []string, exceptID string, limit int) ([]action.Record, error) {
	if len(targetRefs) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	args := append([]any{tenantID}, anys(targetRefs)...)
	args = append(args, exceptID, limit)
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE tenant_id=?
		AND target_ref IN (`+placeholders(len(targetRefs))+`) AND state IN ('DISPATCHED','EFFECT_UNKNOWN','MANUAL_RECOVERY')
		AND id <> ? ORDER BY updated_at LIMIT ?`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRecords(rows)
}

// KernelEffects reads the kernel's tool effect ledger (PostgreSQL; the
// SQLite control plane has no kernel tables and returns none).
func (s *Store) KernelEffects(ctx context.Context, tenantID string, targets []string, openOnly bool, limit int) ([]action.KernelEffect, error) {
	if s.d != Postgres || len(targets) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id::text, run_id::text, step_id::text, target, state, external_operation_id, last_check_at
		FROM external_tool_effects WHERE tenant_id=$1 AND target = ANY($2)`
	if openOnly {
		q += ` AND state IN ('PREPARED','DISPATCHED','UNKNOWN')`
	}
	q += ` ORDER BY last_check_at DESC NULLS LAST, id DESC LIMIT $3`
	rows, err := s.db.QueryContext(ctx, q, tenantID, targets, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []action.KernelEffect
	for rows.Next() {
		var e action.KernelEffect
		var op ns
		var at ts
		if err := rows.Scan(&e.ID, &e.RunID, &e.StepID, &e.Target, &e.State, &op, &at); err != nil {
			return nil, err
		}
		e.ExternalOperationID, e.LastCheckAt = op.S(), at.ptr()
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanRecord(r rowScanner) (action.Record, error) {
	var body jb
	var v int64
	err := r.Scan(&body, &v)
	if errors.Is(err, sql.ErrNoRows) {
		return action.Record{}, action.ErrNotFound
	}
	if err != nil {
		return action.Record{}, err
	}
	var rec action.Record
	if err := json.Unmarshal(body, &rec); err != nil {
		return rec, fmt.Errorf("decode action: %w", err)
	}
	rec.Version = v
	return rec, nil
}

// CreateAction inserts a sealed action; a plan already recorded for the Work
// (same plan hash) is returned instead (created=false).
func (s *Store) CreateAction(ctx context.Context, r action.Record) (action.Record, bool, error) {
	if r.ID == "" || r.PlanHash == "" || r.EffectKey == "" || r.IntentHash == "" {
		return r, false, errors.New("action must be sealed before it is stored")
	}
	r.Version = 1
	if r.UpdatedAt.IsZero() {
		r.UpdatedAt = s.now()
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = r.UpdatedAt
	}
	var out action.Record
	created := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		existing, err := scanRecord(tx.QueryRowContext(ctx, s.q(`SELECT record, version FROM work_actions
			WHERE tenant_id=? AND work_id=? AND plan_hash=?`), r.TenantID, r.WorkID, r.PlanHash))
		if err == nil {
			out = existing
			return nil
		}
		if !errors.Is(err, action.ErrNotFound) {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO work_actions (id, tenant_id, work_id, run_id, stage_id, generation,
			action_name, action_version, target_ref, plan_hash, intent_hash, effect_key, state, record, version,
			dispatched_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)`),
			r.ID, r.TenantID, r.WorkID, nullStr(r.RunID), nullStr(r.StageID), r.Generation, r.ActionName, r.ActionVersion,
			r.TargetRef, r.PlanHash, r.IntentHash, r.EffectKey, string(r.State), js(r), s.tp(r.DispatchedAt),
			s.t(r.CreatedAt), s.t(r.UpdatedAt))
		if isUnique(err, "") {
			return errRetryCreate
		}
		if err != nil {
			return err
		}
		out, created = r, true
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		// A concurrent writer inserted the same plan: return its record.
		existing, gerr := scanRecord(s.db.QueryRowContext(ctx, s.q(`SELECT record, version FROM work_actions
			WHERE tenant_id=? AND work_id=? AND plan_hash=?`), r.TenantID, r.WorkID, r.PlanHash))
		if gerr != nil {
			return r, false, fmt.Errorf("action conflicts with an existing effect key: %w", action.ErrVersionConflict)
		}
		return existing, false, nil
	}
	return out, created, err
}

// GetAction reads an action.
func (s *Store) GetAction(ctx context.Context, tenantID, id string) (action.Record, error) {
	return scanRecord(s.db.QueryRowContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE tenant_id=? AND id=?`),
		tenantID, id))
}

// ListActions lists the actions of a Work, oldest first.
func (s *Store) ListActions(ctx context.Context, tenantID, workID string) ([]action.Record, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE tenant_id=? AND work_id=?
		ORDER BY created_at, id LIMIT 500`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	return scanRecords(rows)
}

func scanRecords(rows *sql.Rows) ([]action.Record, error) {
	defer rows.Close()
	var out []action.Record
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateAction CAS-writes the record. The immutable plan columns are never
// rewritten; a record whose plan hash differs from the stored one is
// rejected.
func (s *Store) UpdateAction(ctx context.Context, r action.Record, expectedVersion int64) (action.Record, error) {
	r.Version = expectedVersion + 1
	if r.UpdatedAt.IsZero() {
		r.UpdatedAt = s.now()
	}
	err := s.tx(ctx, func(tx *sql.Tx) error {
		// Read immutable ownership before locking: all dispatches acquire the
		// Work lock first, matching Work command/mutation lock order.
		stored, err := scanRecord(tx.QueryRowContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE tenant_id=? AND id=?`), r.TenantID, r.ID))
		if err != nil {
			return err
		}
		if stored.Version != expectedVersion || stored.PlanHash != r.PlanHash || stored.EffectKey != r.EffectKey {
			return action.ErrVersionConflict
		}
		if r.State == action.StateDispatched && stored.State != action.StateDispatched {
			if stored.State != action.StateReady {
				return action.ErrInvalidState
			}
			if err := s.validateActionDispatch(ctx, tx, stored); err != nil {
				return err
			}
		}
		// Callers update lifecycle data; the sealed plan stays immutable even
		// if a caller edits a plan field while retaining its original hash.
		r.Plan = stored.Plan
		res, err := tx.ExecContext(ctx, s.q(`UPDATE work_actions SET state=?, record=?, version=?, dispatched_at=?, updated_at=?
			WHERE tenant_id=? AND id=? AND version=? AND plan_hash=? AND effect_key=?`),
			string(r.State), js(r), r.Version, s.tp(r.DispatchedAt), s.t(r.UpdatedAt), r.TenantID, r.ID, expectedVersion,
			r.PlanHash, r.EffectKey)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return action.ErrVersionConflict
		}
		return nil
	})
	return r, err
}

// ListStaleDispatched returns actions stuck in DISPATCHED since before the
// cutoff (all tenants; the recovery sweep reconciles them).
func (s *Store) ListStaleDispatched(ctx context.Context, before time.Time, limit int) ([]action.Record, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT record, version FROM work_actions WHERE state='DISPATCHED'
		AND dispatched_at < ? ORDER BY dispatched_at LIMIT ?`), s.t(before), limit)
	if err != nil {
		return nil, err
	}
	return scanRecords(rows)
}

// LockTarget takes (or renews) the per-resource lock. An expired lock held by
// another action is taken over.
func (s *Store) LockTarget(ctx context.Context, tenantID, targetRef, actionID string, until, now time.Time) (bool, string, error) {
	var ok bool
	var holder string
	err := s.tx(ctx, func(tx *sql.Tx) error {
		ok, holder = false, ""
		var exp ts
		err := tx.QueryRowContext(ctx, s.q(`SELECT action_id, expires_at FROM action_target_locks
			WHERE tenant_id=? AND target_ref=?`+s.forUpdate()), tenantID, targetRef).Scan(&holder, &exp)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			_, err = tx.ExecContext(ctx, s.q(`INSERT INTO action_target_locks (tenant_id, target_ref, action_id, expires_at)
				VALUES (?,?,?,?)`), tenantID, targetRef, actionID, s.t(until))
			if isUnique(err, "") {
				return errRetryCreate
			}
			if err != nil {
				return err
			}
			ok, holder = true, actionID
			return nil
		case err != nil:
			return err
		}
		if holder != actionID && exp.T.After(now) {
			return nil
		}
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE action_target_locks SET action_id=?, expires_at=?
			WHERE tenant_id=? AND target_ref=?`), actionID, s.t(until), tenantID, targetRef); err != nil {
			return err
		}
		ok, holder = true, actionID
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		return s.LockTarget(ctx, tenantID, targetRef, actionID, until, now)
	}
	return ok, holder, err
}

// UnlockTarget releases the lock if actionID holds it.
func (s *Store) UnlockTarget(ctx context.Context, tenantID, targetRef, actionID string) error {
	_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM action_target_locks WHERE tenant_id=? AND target_ref=? AND action_id=?`),
		tenantID, targetRef, actionID)
	return err
}

// ---- evidence ----

func scanEvidence(r rowScanner) (evidence.Record, error) {
	var body jb
	if err := r.Scan(&body); err != nil {
		return evidence.Record{}, err
	}
	var rec evidence.Record
	err := json.Unmarshal(body, &rec)
	return rec, err
}

// AddEvidence appends evidence; the same content for the same Work is
// returned instead of duplicated.
func (s *Store) AddEvidence(ctx context.Context, r evidence.Record) (evidence.Record, bool, error) {
	if err := r.Normalize(s.now()); err != nil {
		return r, false, err
	}
	get := func(q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}) (evidence.Record, error) {
		return scanEvidence(q.QueryRowContext(ctx, s.q(`SELECT record FROM work_evidence WHERE tenant_id=? AND work_id=?
			AND content_hash=?`), r.TenantID, r.WorkID, r.ContentHash))
	}
	var out evidence.Record
	created := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		created = false
		if e, err := get(tx); err == nil {
			out = e
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// Bounded per Work (RFC §20.3); locking the Work serializes the
		// check with concurrent appends.
		w, err := s.lockWork(ctx, tx, r.TenantID, r.WorkID)
		if err != nil {
			return err
		}
		size := `length(record)`
		if s.d == Postgres {
			size = `octet_length(record::text)`
		}
		var n, used int
		if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*), coalesce(sum(`+size+`), 0) FROM work_evidence
			WHERE tenant_id=? AND work_id=?`), r.TenantID, r.WorkID).Scan(&n, &used); err != nil {
			return err
		}
		body, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if lim := w.Template.Limits.Evidence(); n >= lim {
			return &work.LimitError{Limit: "max_evidence", Detail: fmt.Sprintf("%d records, limit %d", n, lim)}
		}
		if lim := w.Template.Limits.EvidenceBytes(); used+len(body) > lim {
			return &work.LimitError{Limit: "max_evidence_bytes", Detail: fmt.Sprintf("%d + %d bytes, limit %d", used, len(body), lim)}
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO work_evidence (id, tenant_id, work_id, source_type, source_ref,
			content_hash, supersedes, record, captured_at) VALUES (?,?,?,?,?,?,?,?,?)`),
			r.ID, r.TenantID, r.WorkID, r.SourceType, r.SourceRef, r.ContentHash, nullStr(r.Supersedes), js(r), s.t(r.CapturedAt))
		if isUnique(err, "") {
			return errRetryCreate
		}
		if err != nil {
			return err
		}
		out, created = r, true
		return nil
	})
	if errors.Is(err, errRetryCreate) {
		e, gerr := get(s.db)
		return e, false, gerr
	}
	return out, created, err
}

// ListEvidence lists a Work's evidence, oldest first.
func (s *Store) ListEvidence(ctx context.Context, tenantID, workID string, limit int) ([]evidence.Record, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT record FROM work_evidence WHERE tenant_id=? AND work_id=?
		ORDER BY captured_at, id LIMIT ?`), tenantID, workID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []evidence.Record
	for rows.Next() {
		e, err := scanEvidence(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ErrEvidenceNotFound is returned by GetEvidence.
var ErrEvidenceNotFound = errors.New("evidence not found")

// GetEvidence reads one evidence record.
func (s *Store) GetEvidence(ctx context.Context, tenantID, id string) (evidence.Record, error) {
	e, err := scanEvidence(s.db.QueryRowContext(ctx, s.q(`SELECT record FROM work_evidence WHERE tenant_id=? AND id=?`),
		tenantID, id))
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrEvidenceNotFound
	}
	return e, err
}

// UpsertHypothesis inserts (expectedVersion 0) or CAS-updates a hypothesis
// keyed by (tenant, work, key).
func (s *Store) UpsertHypothesis(ctx context.Context, h evidence.Hypothesis, expectedVersion int64) (evidence.Hypothesis, error) {
	// This compatibility API has no Work CAS or adjudication inputs. Agent
	// references and caller-supplied decisions cannot authorize a conclusion.
	h.Status, h.Decision = evidence.HypInconclusive, nil
	return s.upsertHypothesis(ctx, s.db, h, expectedVersion)
}

func (s *Store) upsertHypothesis(ctx context.Context, db decisionWriter, h evidence.Hypothesis, expectedVersion int64) (evidence.Hypothesis, error) {
	if h.Key == "" {
		h.Key = evidence.HypothesisKey(h.Statement)
	}
	if h.TenantID == "" || h.WorkID == "" || h.Key == "" || expectedVersion < 0 {
		return h, fmt.Errorf("%w: hypothesis needs tenant, work and key", evidence.ErrInvalid)
	}
	if h.UpdatedAt.IsZero() {
		h.UpdatedAt = s.now()
	}
	if expectedVersion == 0 {
		if h.ID == "" {
			h.ID = cpkit.NewID()
		}
		h.Version = 1
		_, err := db.ExecContext(ctx, s.q(`INSERT INTO work_hypotheses (id, tenant_id, work_id, hyp_key, status, record,
			version, updated_at) VALUES (?,?,?,?,?,?,1,?)`), h.ID, h.TenantID, h.WorkID, h.Key, h.Status, js(h), s.t(h.UpdatedAt))
		if isUnique(err, "") {
			return h, evidence.ErrVersionConflict
		}
		return h, err
	}
	var id string
	if err := db.QueryRowContext(ctx, s.q(`SELECT id FROM work_hypotheses WHERE tenant_id=? AND work_id=? AND hyp_key=? AND version=?`),
		h.TenantID, h.WorkID, h.Key, expectedVersion).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return h, evidence.ErrVersionConflict
		}
		return h, err
	}
	if h.ID != "" && h.ID != id {
		return h, evidence.ErrVersionConflict
	}
	h.ID = id
	h.Version = expectedVersion + 1
	res, err := db.ExecContext(ctx, s.q(`UPDATE work_hypotheses SET status=?, record=?, version=?, updated_at=?
		WHERE tenant_id=? AND work_id=? AND hyp_key=? AND version=?`), h.Status, js(h), h.Version, s.t(h.UpdatedAt),
		h.TenantID, h.WorkID, h.Key, expectedVersion)
	if err != nil {
		return h, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return h, evidence.ErrVersionConflict
	}
	return h, nil
}

// ListHypotheses lists a Work's hypotheses.
func (s *Store) ListHypotheses(ctx context.Context, tenantID, workID string) ([]evidence.Hypothesis, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT h.record, h.version, w.generation FROM work_hypotheses h
 JOIN agent_works w ON w.tenant_id=h.tenant_id AND w.id=h.work_id
 WHERE h.tenant_id=? AND h.work_id=? ORDER BY h.hyp_key LIMIT 500`), tenantID, workID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []evidence.Hypothesis
	for rows.Next() {
		var body jb
		var v int64
		var generation int
		if err := rows.Scan(&body, &v, &generation); err != nil {
			return nil, err
		}
		var h evidence.Hypothesis
		if err := json.Unmarshal(body, &h); err != nil {
			return nil, err
		}
		h.Version = v
		h.Status = evidence.HypInconclusive
		if h.Decision.CurrentFor(evidence.DecisionScope{TenantID: tenantID, WorkID: workID, Generation: generation}, h.Key) {
			h.Status = h.Decision.HypothesisStatus()
		}
		out = append(out, h)
	}
	return out, rows.Err()
}
