package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// EnsureProfile registers an immutable profile version (no-op if it exists).
func (s *Store) EnsureProfile(ctx context.Context, tenantID, profileID string, version int64, spec json.RawMessage) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,$2,$3,$4,$5,'ACTIVE') ON CONFLICT DO NOTHING`,
		tenantID, profileID, version, jsonb(spec), hashStrings(string(spec)))
	return err
}

// RegisterProfile registers a profile version from a profile source. Versions
// are immutable: re-registering the same content is a no-op, different
// content under an existing version is an error (bump the version instead).
func (s *Store) RegisterProfile(ctx context.Context, tenantID, profileID string, version int64, spec json.RawMessage) (created bool, err error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,$2,$3,$4,$5,'ACTIVE') ON CONFLICT DO NOTHING`,
		tenantID, profileID, version, jsonb(spec), hashStrings(string(spec)))
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	var same bool
	if err := s.pool.QueryRow(ctx, `SELECT spec = $4::jsonb FROM agent_profiles
		WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`, tenantID, profileID, version, jsonb(spec)).Scan(&same); err != nil {
		return false, err
	}
	if !same {
		return false, fmt.Errorf("profile %s@%d (tenant %s) already exists with different content; profile versions are immutable, publish a new version: %w",
			profileID, version, tenantID, domain.ErrVersionConflict)
	}
	return false, nil
}

func (s *Store) CreateIncident(ctx context.Context, tenantID, serviceID, environment string) (string, error) {
	id := newID()
	_, err := s.pool.Exec(ctx, `INSERT INTO incidents (id, tenant_id, service_id, environment, status)
		VALUES ($1,$2,$3,$4,'OPEN')`, id, tenantID, serviceID, environment)
	return id, err
}

type CreateRunRequest = domain.CreateRunRequest

// CreateRun creates the run, its first step(s), run.created/step.planned
// events and the wake-up outbox in one transaction (§21.1). It is idempotent
// on (tenant_id, idempotency key): a retry returns the original run.
func (s *Store) CreateRun(ctx context.Context, req CreateRunRequest) (domain.Run, bool, error) {
	var run domain.Run
	created := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, isNew, err := s.createRunTx(ctx, tx, req)
		if err != nil {
			return err
		}
		created, run = isNew, r.run
		if !isNew {
			return nil
		}
		if err := r.save(ctx); err != nil {
			return err
		}
		run = r.run
		return nil
	})
	return run, created, err
}

// createRunTx inserts a run inside an existing transaction and returns it
// locked (caller must save). Existing runs (same idempotency key) are
// returned with isNew=false and must not be modified.
func (s *Store) createRunTx(ctx context.Context, tx pgx.Tx, req CreateRunRequest) (*runTx, bool, error) {
	if req.InitiatorType == "" {
		req.InitiatorType = "user"
	}
	id := newID()
	status := domain.RunQueued
	if req.PendingActivation {
		status = domain.RunPendingActivation
	}
	if err := s.checkRunQuota(ctx, tx, req.TenantID); err != nil {
		return nil, false, err
	}
	var inserted string
	err := tx.QueryRow(ctx, `INSERT INTO agent_runs (id, tenant_id, incident_id, profile_id, profile_version,
		status, create_idempotency_key, goal_ref, initiator_type, initiator_id, session_id, branch_id, turn_id,
		forked_from_checkpoint_id, rollout_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (tenant_id, create_idempotency_key) DO NOTHING RETURNING id`,
		id, req.TenantID, req.IncidentID, req.ProfileID, req.ProfileVersion, status, req.IdempotencyKey,
		req.GoalRef, req.InitiatorType, nullable(req.InitiatorID), req.SessionID, req.BranchID, req.TurnID,
		req.ForkedFrom, req.RolloutID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runCols+` FROM agent_runs
			WHERE tenant_id=$1 AND create_idempotency_key=$2`, req.TenantID, req.IdempotencyKey))
		return &runTx{s: s, tx: tx, run: run}, false, err
	}
	if err != nil {
		return nil, false, err
	}
	r, err := s.lockRun(ctx, tx, req.TenantID, id)
	if err != nil {
		return nil, false, err
	}
	if req.StepSeqStart > r.run.NextStepSeq {
		r.run.NextStepSeq = req.StepSeqStart
	}
	if err := r.event(ctx, "run.created", req.InitiatorType, req.InitiatorID, nil, nil, map[string]any{
		"profile_id": req.ProfileID, "profile_version": req.ProfileVersion, "incident_id": req.IncidentID,
		"session_id": req.SessionID, "turn_id": req.TurnID, "forked_from_checkpoint_id": req.ForkedFrom,
		"rollout_id": req.RolloutID, "pending_activation": req.PendingActivation,
	}); err != nil {
		return nil, false, err
	}
	steps := req.InitialSteps
	if len(steps) == 0 {
		steps = []PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: req.GoalRef,
			InputHash: req.GoalHash, Status: domain.StepPending}}
		if s.cfg.MemoryRetrieval {
			steps = append([]PlannedStep{{Kind: domain.StepRetrieve, Executor: "memory", InputRef: req.GoalRef,
				InputHash: req.GoalHash, Status: domain.StepPending}}, steps...)
		}
	}
	for _, p := range steps {
		if _, err := r.insertStep(ctx, p); err != nil {
			return nil, false, err
		}
	}
	if !req.PendingActivation {
		if err := r.wake(ctx, nil); err != nil {
			return nil, false, err
		}
	}
	return r, true, nil
}

// checkRunQuota enforces tenant_active_runs_limit / tenant_queued_runs_limit (§14.4, §14.6).
func (s *Store) checkRunQuota(ctx context.Context, tx pgx.Tx, tenantID string) error {
	var maxActive, maxQueued int
	err := tx.QueryRow(ctx, `SELECT max_active_runs, max_queued_runs FROM tenant_quotas WHERE tenant_id=$1`,
		tenantID).Scan(&maxActive, &maxQueued)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var active, queued int
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status IN ('RUNNING','WAITING_APPROVAL','PAUSED','NEEDS_HUMAN')),
		count(*) FILTER (WHERE status IN ('QUEUED','PENDING_ACTIVATION'))
		FROM agent_runs WHERE tenant_id=$1 AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`, tenantID).
		Scan(&active, &queued); err != nil {
		return err
	}
	if active >= maxActive || queued >= maxQueued {
		return domain.ErrQuotaExceeded
	}
	return nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// ---------- reads (tenant-scoped) ----------

func (s *Store) GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error) {
	if !validID(runID) {
		return domain.Run{}, domain.ErrNotFound
	}
	return scanRun(s.pool.QueryRow(ctx, `SELECT `+runCols+` FROM agent_runs WHERE id=$1 AND tenant_id=$2`, runID, tenantID))
}

const stepCols = `id, tenant_id, run_id, seq, kind, executor_name, status, input_ref, input_hash, output_ref,
	output_hash, status_reason, context_snapshot_id, current_attempt, next_attempt_at, output_revision, created_at, updated_at`

func scanStep(row pgx.Row) (domain.Step, error) {
	var st domain.Step
	err := row.Scan(&st.ID, &st.TenantID, &st.RunID, &st.Seq, &st.Kind, &st.ExecutorName, &st.Status, &st.InputRef,
		&st.InputHash, &st.OutputRef, &st.OutputHash, &st.StatusReason, &st.ContextSnapshotID, &st.CurrentAttempt,
		&st.NextAttemptAt, &st.OutputRevision, &st.CreatedAt, &st.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, domain.ErrNotFound
	}
	return st, err
}

func (s *Store) ListSteps(ctx context.Context, tenantID, runID string) ([]domain.Step, error) {
	return s.listSteps(ctx, s.pool, tenantID, runID, 1<<62)
}

func (s *Store) listSteps(ctx context.Context, q querier, tenantID, runID string, beforeSeq int64) ([]domain.Step, error) {
	if !validID(runID) {
		return nil, domain.ErrNotFound
	}
	rows, err := q.Query(ctx, `SELECT `+stepCols+` FROM agent_steps
		WHERE run_id=$1 AND tenant_id=$2 AND seq < $3 ORDER BY seq`, runID, tenantID, beforeSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Step
	for rows.Next() {
		st, err := scanStep(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

func (s *Store) ListEvents(ctx context.Context, tenantID, runID string, afterSeq int64, limit int) ([]domain.Event, error) {
	if !validID(runID) {
		return nil, domain.ErrNotFound
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, run_id, event_seq, event_id, step_id, attempt_id, event_type,
		actor_type, actor_id, payload, occurred_at, committed_at FROM run_events
		WHERE run_id=$1 AND tenant_id=$2 AND event_seq > $3 ORDER BY event_seq LIMIT $4`, runID, tenantID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		if err := rows.Scan(&e.TenantID, &e.RunID, &e.Seq, &e.ID, &e.StepID, &e.AttemptID, &e.Type, &e.ActorType,
			&e.ActorID, &e.Payload, &e.OccurredAt, &e.CommittedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type AttemptView struct {
	ID         string               `json:"id"`
	StepID     string               `json:"step_id"`
	AttemptNo  int                  `json:"attempt_no"`
	WorkerID   string               `json:"worker_id"`
	FenceToken int64                `json:"fence_token"`
	Status     domain.AttemptStatus `json:"status"`
	ErrorCode  *string              `json:"error_code,omitempty"`
}

func (s *Store) ListAttempts(ctx context.Context, tenantID, runID string) ([]AttemptView, error) {
	rows, err := s.pool.Query(ctx, `SELECT a.id, a.step_id, a.attempt_no, a.worker_id, a.fence_token, a.status, a.error_code
		FROM step_attempts a JOIN agent_steps s ON s.id = a.step_id
		WHERE a.run_id=$1 AND a.tenant_id=$2 ORDER BY s.seq, a.attempt_no`, runID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AttemptView
	for rows.Next() {
		var a AttemptView
		if err := rows.Scan(&a.ID, &a.StepID, &a.AttemptNo, &a.WorkerID, &a.FenceToken, &a.Status, &a.ErrorCode); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) ListApprovals(ctx context.Context, tenantID, runID string) ([]domain.ApprovalRequest, error) {
	if !validID(runID) {
		return nil, domain.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT a.id, a.action_intent_id, i.step_id, i.action_hash, i.risk_class, i.tool_name,
		i.target, a.required_approvals, a.status, a.expires_at, a.created_at, a.decided_at
		FROM approval_requests a JOIN action_intents i ON i.id = a.action_intent_id
		WHERE i.run_id=$1 AND a.tenant_id=$2 ORDER BY a.created_at`, runID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ApprovalRequest
	for rows.Next() {
		var a domain.ApprovalRequest
		if err := rows.Scan(&a.ID, &a.ActionIntentID, &a.StepID, &a.ActionHash, &a.RiskClass, &a.ToolName, &a.Target,
			&a.RequiredApprovals, &a.Status, &a.ExpiresAt, &a.CreatedAt, &a.DecidedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

type SnapshotView struct {
	ID                    string           `json:"id"`
	StepID                string           `json:"step_id"`
	ProfileID             string           `json:"profile_id"`
	ProfileVersion        int64            `json:"profile_version"`
	IncidentMemoryVersion *int64           `json:"incident_memory_version,omitempty"`
	KVVersions            map[string]int64 `json:"kv_versions"`
	ContextHash           *string          `json:"context_hash,omitempty"`
}

func (s *Store) GetSnapshot(ctx context.Context, tenantID, id string) (SnapshotView, error) {
	var v SnapshotView
	var kv []byte
	err := s.pool.QueryRow(ctx, `SELECT id, step_id, profile_id, profile_version, incident_memory_version, kv_versions,
		context_hash FROM context_snapshots WHERE id=$1 AND tenant_id=$2`, id, tenantID).
		Scan(&v.ID, &v.StepID, &v.ProfileID, &v.ProfileVersion, &v.IncidentMemoryVersion, &kv, &v.ContextHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, domain.ErrNotFound
	}
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(kv, &v.KVVersions)
	return v, err
}

// ToolNameForStep returns the intent tool name of a tool step (for history rendering).
func (s *Store) IntentForStep(ctx context.Context, tenantID, stepID string) (*domain.ActionIntent, error) {
	return loadIntent(ctx, s.pool, tenantID, stepID)
}

func loadIntent(ctx context.Context, q querier, tenantID, stepID string) (*domain.ActionIntent, error) {
	var in domain.ActionIntent
	err := q.QueryRow(ctx, `SELECT id, environment, target, tool_name, tool_version, input_hash, action_hash,
		risk_class, policy_version, effects FROM action_intents WHERE step_id=$1 AND tenant_id=$2
		ORDER BY created_at DESC LIMIT 1`, stepID, tenantID).
		Scan(&in.ID, &in.Environment, &in.Target, &in.ToolName, &in.ToolVersion, &in.InputHash, &in.ActionHash,
			&in.RiskClass, &in.PolicyVersion, &in.Effects)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &in, err
}

// ListRuns returns the tenant's most recent runs, newest first; an empty
// status lists every status.
func (s *Store) ListRuns(ctx context.Context, tenantID string, status domain.RunStatus, limit int) ([]domain.Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runCols+` FROM agent_runs
		WHERE tenant_id=$1 AND ($2 = '' OR status=$2) ORDER BY created_at DESC, id DESC LIMIT $3`,
		tenantID, string(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}
