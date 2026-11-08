// Package pg implements the durable kernel state on PostgreSQL. PostgreSQL is
// the only source of truth for run state (ADR-002); NATS only carries wake-ups
// derived from the outbox written in the same transaction.
//
// Every state-changing operation follows one pattern: lock the run row
// (SELECT ... FOR UPDATE), validate state / fence, mutate, append run events
// with strictly increasing event_seq, enqueue outbox wake-ups, save the run row,
// commit.
package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/domain"
)

const WakeSubject = "harness.run.wake"

// Contract types are defined in domain so the kernel does not import this package.
type (
	PlannedStep  = domain.PlannedStep
	StepSuccess  = domain.StepSuccess
	StepFailure  = domain.StepFailure
	FailureClass = domain.FailureClass
	EffectState  = domain.EffectState
	KVKey        = domain.KVKey
	KVWrite      = domain.KVWrite
)

const (
	FailRetryable     = domain.FailRetryable
	FailPermanent     = domain.FailPermanent
	FailUnknownEffect = domain.FailUnknownEffect
	FailCancelled     = domain.FailCancelled
	EffectDispatched  = domain.EffectDispatched
	EffectConfirmed   = domain.EffectConfirmed
)

type Config struct {
	// MemoryRetrieval plans a retrieve_memory step before the first reason
	// step of every new run (§11.2).
	MemoryRetrieval   bool
	MaxStepsPerRun    int
	MaxAttempts       int
	DefaultRetryDelay time.Duration
	ApprovalTTL       time.Duration
	// MaxRunningPerTenant caps how many runs of one tenant may hold a live
	// lease at once (0 = unlimited). Over-quota claims are deferred briefly so
	// other tenants' runs get the slots (§14.4 fairness). It is a soft limit:
	// two concurrent claims may both pass the count.
	MaxRunningPerTenant int
	TenantDeferDelay    time.Duration
	// RequireDistinctApprover forbids the run's initiator from approving its
	// own tool actions (separation of duties, §19).
	RequireDistinctApprover bool
	// Kernel transactions (claim, commit, effect ledger, lease recovery) run
	// with these per-transaction limits so one stuck lock holder cannot stall
	// the execution plane (0 = default, <0 = leave the server setting).
	LockTimeout      time.Duration
	StatementTimeout time.Duration
	// TxRetries is how many extra times a kernel transaction is re-run after
	// a failure PostgreSQL guarantees rolled it back (serialization failure,
	// deadlock, lock timeout, error before send). 0 = default, <0 = none.
	TxRetries int
}

const (
	DefaultLockTimeout      = 2 * time.Second
	DefaultStatementTimeout = 10 * time.Second
	DefaultTxRetries        = 3
)

func DefaultConfig() Config {
	return Config{MaxStepsPerRun: 100, MaxAttempts: 3, DefaultRetryDelay: 2 * time.Second, ApprovalTTL: 30 * time.Minute,
		LockTimeout: DefaultLockTimeout, StatementTimeout: DefaultStatementTimeout, TxRetries: DefaultTxRetries}
}

type Store struct {
	pool *pgxpool.Pool
	cfg  Config
	// maxSteps caches spec.max_steps per immutable profile version.
	maxSteps sync.Map
}

func New(pool *pgxpool.Pool, cfg Config) *Store {
	if cfg.MaxStepsPerRun == 0 {
		cfg = DefaultConfig()
	}
	if cfg.LockTimeout == 0 {
		cfg.LockTimeout = DefaultLockTimeout
	}
	if cfg.StatementTimeout == 0 {
		cfg.StatementTimeout = DefaultStatementTimeout
	}
	if cfg.TxRetries == 0 {
		cfg.TxRetries = DefaultTxRetries
	}
	return &Store{pool: pool, cfg: cfg}
}

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func newID() string { return uuid.Must(uuid.NewV7()).String() }

func validID(id string) bool { _, err := uuid.Parse(id); return err == nil }

func hashStrings(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func jsonb(v any) []byte {
	if v == nil {
		return []byte(`{}`)
	}
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return []byte(`{}`)
		}
		return raw
	}
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func ptr[T any](v T) *T { return &v }

func (s *Store) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, fn)
}

// inKernelTx runs a kernel-plane transaction: bounded lock and statement
// waits, and a transparent re-run when PostgreSQL guarantees the attempt was
// rolled back. fn must be safe to re-run: it may only assign (not accumulate)
// captured variables. Ambiguous failures (connection lost during COMMIT) are
// returned to the caller, which settles them against the fenced attempt row.
func (s *Store) inKernelTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	retries := max(s.cfg.TxRetries, 0)
	for i := 0; ; i++ {
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			if err := s.applyTxLimits(ctx, tx); err != nil {
				return err
			}
			return fn(tx)
		})
		if err == nil || i >= retries || ctx.Err() != nil || !RetryableTxError(err) {
			return err
		}
		delay := time.Duration(25<<i) * time.Millisecond
		delay += time.Duration(time.Now().UnixNano() % int64(delay)) // jitter
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

func (s *Store) applyTxLimits(ctx context.Context, tx pgx.Tx) error {
	lock, stmt := pgMillis(s.cfg.LockTimeout), pgMillis(s.cfg.StatementTimeout)
	if lock == "" && stmt == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true) WHERE $1 <> '' UNION ALL
		SELECT set_config('statement_timeout', $2, true) WHERE $2 <> ''`, lock, stmt)
	return err
}

func pgMillis(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return fmt.Sprintf("%dms", max(d.Milliseconds(), 1))
}

// RetryableTxError reports whether err guarantees the transaction did not
// commit, so running it again cannot apply it twice.
func RetryableTxError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization_failure
			"40P01", // deadlock_detected
			"55P03", // lock_not_available (lock_timeout)
			"57014": // query_canceled (statement_timeout; caller cancellation is checked first)
			return true
		}
		return false
	}
	return pgconn.SafeToRetry(err)
}

// ---------- run row locking and mutation ----------

const runCols = `id, tenant_id, incident_id, profile_id, profile_version, status, desired_state, version,
	last_event_seq, next_step_seq, not_before, context_snapshot_id, lease_owner, lease_until, fence_token,
	goal_ref, result_ref, failure_reason, initiator_type, initiator_id, session_id, branch_id, turn_id,
	parent_run_id, parent_step_id, flow_node_id, flow_depth,
	forked_from_checkpoint_id, compaction_epoch, rollout_id, created_at, updated_at`

func scanRun(row pgx.Row, extra ...any) (domain.Run, error) {
	var r domain.Run
	dest := []any{&r.ID, &r.TenantID, &r.IncidentID, &r.ProfileID, &r.ProfileVersion, &r.Status, &r.DesiredState,
		&r.Version, &r.LastEventSeq, &r.NextStepSeq, &r.NotBefore, &r.ContextSnapshotID, &r.LeaseOwner,
		&r.LeaseUntil, &r.FenceToken, &r.GoalRef, &r.ResultRef, &r.FailureReason, &r.InitiatorType, &r.InitiatorID,
		&r.SessionID, &r.BranchID, &r.TurnID, &r.ParentRunID, &r.ParentStepID, &r.FlowNodeID, &r.FlowDepth,
		&r.ForkedFrom, &r.CompactionEpoch, &r.RolloutID, &r.CreatedAt, &r.UpdatedAt}
	err := row.Scan(append(dest, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrNotFound
	}
	return r, err
}

// runTx carries a locked run row through one transaction.
type runTx struct {
	s   *Store
	tx  pgx.Tx
	run domain.Run
	now time.Time // database clock, used for every lease/expiry comparison

	startStatus     domain.RunStatus // status when locked; terminal hooks fire only on a transition
	terminalHandled bool
}

func (s *Store) lockRun(ctx context.Context, tx pgx.Tx, tenantID, runID string) (*runTx, error) {
	if !validID(runID) {
		return nil, domain.ErrNotFound
	}
	var now time.Time
	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runCols+`, now() FROM agent_runs
		WHERE id = $1 AND ($2 = '' OR tenant_id = $2) FOR UPDATE`, runID, tenantID), &now)
	if err != nil {
		return nil, err
	}
	return &runTx{s: s, tx: tx, run: run, now: now, startStatus: run.Status}, nil
}

// lockLeased locks the run and verifies the caller still holds the lease
// identified by lease.FenceToken. A stale worker gets ErrLeaseLost (§14.3).
func (s *Store) lockLeased(ctx context.Context, tx pgx.Tx, lease domain.Lease) (*runTx, error) {
	r, err := s.lockRun(ctx, tx, lease.TenantID, lease.RunID)
	if err != nil {
		return nil, err
	}
	run := r.run
	if run.Status != domain.RunRunning || run.FenceToken != lease.FenceToken ||
		run.LeaseOwner == nil || *run.LeaseOwner != lease.WorkerID ||
		run.LeaseUntil == nil || !run.LeaseUntil.After(r.now) {
		return nil, domain.ErrLeaseLost
	}
	return r, nil
}

// setStatus bumps run.version only on a real, control-plane-visible change.
func (r *runTx) setStatus(st domain.RunStatus) {
	if r.run.Status != st {
		r.run.Status = st
		r.run.Version++
	}
}

func (r *runTx) releaseLease() { r.run.LeaseOwner, r.run.LeaseUntil = nil, nil }

func (r *runTx) event(ctx context.Context, typ, actorType, actorID string, stepID, attemptID *string, payload any) error {
	r.run.LastEventSeq++
	var actor *string
	if actorID != "" {
		actor = &actorID
	}
	_, err := r.tx.Exec(ctx, `INSERT INTO run_events
		(tenant_id, run_id, event_seq, event_id, step_id, attempt_id, event_type, actor_type, actor_id, payload, occurred_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		r.run.TenantID, r.run.ID, r.run.LastEventSeq, newID(), stepID, attemptID, typ, actorType, actor, jsonb(payload), r.now)
	return err
}

// wake enqueues a scheduling notification in the same transaction (§13.3).
func (r *runTx) wake(ctx context.Context, notBefore *time.Time) error {
	at := r.now
	if notBefore != nil && notBefore.After(at) {
		at = *notBefore
	}
	_, err := r.tx.Exec(ctx, `INSERT INTO outbox_messages (id, tenant_id, aggregate_id, subject, payload, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, newID(), r.run.TenantID, r.run.ID, WakeSubject,
		jsonb(map[string]string{"run_id": r.run.ID, "tenant_id": r.run.TenantID}), at)
	return err
}

func (r *runTx) save(ctx context.Context) error {
	run := r.run
	_, err := r.tx.Exec(ctx, `UPDATE agent_runs SET status=$2, desired_state=$3, version=$4, last_event_seq=$5,
		next_step_seq=$6, not_before=$7, context_snapshot_id=$8, lease_owner=$9, lease_until=$10, fence_token=$11,
		result_ref=$12, failure_reason=$13, compaction_epoch=$14, profile_version=$15, updated_at=now() WHERE id=$1`,
		run.ID, run.Status, run.DesiredState, run.Version, run.LastEventSeq, run.NextStepSeq, run.NotBefore,
		run.ContextSnapshotID, run.LeaseOwner, run.LeaseUntil, run.FenceToken, run.ResultRef, run.FailureReason,
		run.CompactionEpoch, run.ProfileVersion)
	if err != nil {
		return err
	}
	return r.afterSave(ctx)
}

// settle is called whenever the run has become QUEUED without a lease. It
// honours desired_state at this safe point, otherwise schedules a wake-up.
func (r *runTx) settle(ctx context.Context) error {
	if r.run.Status != domain.RunQueued {
		return nil
	}
	switch r.run.DesiredState {
	case domain.DesiredPaused:
		r.setStatus(domain.RunPaused)
		return r.event(ctx, "run.paused", "system", "", nil, nil, map[string]any{"after_step_seq": r.run.NextStepSeq - 1})
	case domain.DesiredCancelled:
		return r.cancel(ctx, "system", "", "cancel requested")
	default:
		return r.wake(ctx, r.run.NotBefore)
	}
}

func (r *runTx) cancel(ctx context.Context, actorType, actorID, reason string) error {
	if err := r.cancelChildren(ctx); err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE flow_runs SET status='CANCELLED', updated_at=now()
		WHERE tenant_id=$1 AND run_id=$2 AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		r.run.TenantID, r.run.ID); err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='CANCELLED', updated_at=now()
		WHERE run_id=$1 AND status IN ('PENDING','RETRY_WAIT','WAITING_APPROVAL','WAITING_CHILDREN','WAITING_INPUT')`, r.run.ID); err != nil {
		return err
	}
	if err := r.supersedeApprovals(ctx); err != nil {
		return err
	}
	r.run.DesiredState = domain.DesiredCancelled
	r.run.NotBefore = nil
	r.releaseLease()
	r.setStatus(domain.RunCancelled)
	if err := r.notifyParent(ctx, "CANCELLED", reason); err != nil {
		return err
	}
	return r.event(ctx, "run.cancelled", actorType, actorID, nil, nil, map[string]string{"reason": reason})
}

func (r *runTx) fail(ctx context.Context, reason string) error {
	if err := r.cancelChildren(ctx); err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE flow_runs SET status='FAILED', updated_at=now()
		WHERE tenant_id=$1 AND run_id=$2 AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		r.run.TenantID, r.run.ID); err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `UPDATE agent_steps SET status='CANCELLED', updated_at=now()
		WHERE run_id=$1 AND status IN ('PENDING','RETRY_WAIT','WAITING_APPROVAL','WAITING_CHILDREN','WAITING_INPUT')`, r.run.ID); err != nil {
		return err
	}
	if err := r.supersedeApprovals(ctx); err != nil {
		return err
	}
	r.run.FailureReason = &reason
	r.run.NotBefore = nil
	r.releaseLease()
	r.setStatus(domain.RunFailed)
	if err := r.notifyParent(ctx, "FAILED", reason); err != nil {
		return err
	}
	return r.event(ctx, "run.failed", "system", "", nil, nil, map[string]string{"reason": reason})
}

// notifyParent tells a flow coordinator that this child run reached a
// terminal state outside the normal step-commit path (command cancel, policy
// rejection, step budget, ...).
func (r *runTx) notifyParent(ctx context.Context, status, reason string) error {
	if r.run.ParentRunID == nil {
		return nil
	}
	lease := domain.Lease{TenantID: r.run.TenantID, RunID: r.run.ID, ParentRunID: *r.run.ParentRunID}
	return r.s.flowChildFinished(ctx, r, lease, status, "", reason)
}

func (r *runTx) supersedeApprovals(ctx context.Context) error {
	_, err := r.tx.Exec(ctx, `UPDATE approval_requests a SET status='SUPERSEDED', decided_at=now()
		FROM action_intents i WHERE a.action_intent_id = i.id AND i.run_id=$1 AND a.status='PENDING'`, r.run.ID)
	return err
}

// cancelChildren cancels every descendant run of this coordinator and their
// open steps. The recursive CTE keeps deep subflows consistent.
func (r *runTx) cancelChildren(ctx context.Context) error {
	if _, err := r.tx.Exec(ctx, `WITH RECURSIVE tree AS (
		SELECT id FROM agent_runs WHERE tenant_id=$1 AND parent_run_id=$2
		UNION ALL
		SELECT c.id FROM agent_runs c JOIN tree t ON c.parent_run_id = t.id WHERE c.tenant_id=$1
	)
	UPDATE agent_runs SET status='CANCELLED', desired_state='CANCELLED',
	  failure_reason='parent cancelled', lease_owner=NULL, lease_until=NULL, updated_at=now()
	WHERE id IN (SELECT id FROM tree) AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		r.run.TenantID, r.run.ID); err != nil {
		return err
	}
	_, err := r.tx.Exec(ctx, `WITH RECURSIVE tree AS (
		SELECT id FROM agent_runs WHERE tenant_id=$1 AND parent_run_id=$2
		UNION ALL
		SELECT c.id FROM agent_runs c JOIN tree t ON c.parent_run_id = t.id WHERE c.tenant_id=$1
	)
	UPDATE agent_steps SET status='CANCELLED', updated_at=now()
	WHERE run_id IN (SELECT id FROM tree)
	  AND status IN ('PENDING','RETRY_WAIT','WAITING_APPROVAL','WAITING_CHILDREN','WAITING_INPUT')`,
		r.run.TenantID, r.run.ID)
	if err != nil {
		return err
	}
	if _, err := r.tx.Exec(ctx, `WITH RECURSIVE tree AS (
		SELECT id FROM agent_runs WHERE tenant_id=$1 AND parent_run_id=$2
		UNION ALL
		SELECT c.id FROM agent_runs c JOIN tree t ON c.parent_run_id = t.id WHERE c.tenant_id=$1
	)
	UPDATE flow_runs SET status='CANCELLED', updated_at=now()
	WHERE run_id IN (SELECT id FROM tree) AND status NOT IN ('COMPLETED','FAILED','CANCELLED')`,
		r.run.TenantID, r.run.ID); err != nil {
		return err
	}
	_, err = r.tx.Exec(ctx, `WITH RECURSIVE tree AS (
		SELECT id FROM agent_runs WHERE tenant_id=$1 AND parent_run_id=$2
		UNION ALL
		SELECT c.id FROM agent_runs c JOIN tree t ON c.parent_run_id = t.id WHERE c.tenant_id=$1
	)
	UPDATE flow_node_runs SET status='CANCELLED', updated_at=now()
	WHERE flow_run_id IN (SELECT f.id FROM flow_runs f JOIN tree t ON f.run_id = t.id)
	  AND status IN ('PENDING','RUNNING')`,
		r.run.TenantID, r.run.ID)
	if err != nil {
		return err
	}
	_, err = r.tx.Exec(ctx, `WITH RECURSIVE tree AS (
		SELECT id FROM agent_runs WHERE tenant_id=$1 AND parent_run_id=$2
		UNION ALL
		SELECT c.id FROM agent_runs c JOIN tree t ON c.parent_run_id = t.id WHERE c.tenant_id=$1
	)
	UPDATE approval_requests a SET status='SUPERSEDED', decided_at=now()
	FROM action_intents i JOIN tree t ON i.run_id = t.id
	WHERE a.action_intent_id = i.id AND a.tenant_id=$1 AND a.status='PENDING'`,
		r.run.TenantID, r.run.ID)
	return err
}

// ---------- steps ----------

func (r *runTx) insertStep(ctx context.Context, p PlannedStep) (string, error) {
	id := newID()
	seq := r.run.NextStepSeq
	r.run.NextStepSeq++
	var reason *string
	if p.StatusReason != "" {
		reason = &p.StatusReason
	}
	if _, err := r.tx.Exec(ctx, `INSERT INTO agent_steps
		(id, tenant_id, run_id, seq, kind, executor_name, flow_node_id, status, input_ref, input_hash, status_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		id, r.run.TenantID, r.run.ID, seq, p.Kind, p.Executor, p.FlowNodeID, p.Status, p.InputRef, p.InputHash, reason); err != nil {
		return "", err
	}
	payload := map[string]any{"seq": seq, "kind": p.Kind, "status": p.Status}
	if p.StatusReason != "" {
		payload["status_reason"] = p.StatusReason
	}
	if len(p.PolicyDecision) > 0 {
		payload["policy"] = p.PolicyDecision
	}
	if err := r.event(ctx, "step.planned", "system", "", &id, nil, payload); err != nil {
		return "", err
	}
	if p.Intent != nil {
		intentID := newID()
		p.Intent.ID = intentID
		in := p.Intent
		if _, err := r.tx.Exec(ctx, `INSERT INTO action_intents (id, tenant_id, run_id, step_id, environment, target,
			tool_name, tool_version, input_hash, action_hash, risk_class, policy_version, effects)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			intentID, r.run.TenantID, r.run.ID, id, in.Environment, in.Target, in.ToolName, in.ToolVersion,
			in.InputHash, in.ActionHash, in.RiskClass, in.PolicyVersion, jsonb(in.Effects)); err != nil {
			return "", err
		}
		if p.Status == domain.StepWaitingApproval {
			if err := r.requestApproval(ctx, id, intentID, in, p.RequiredApprovals); err != nil {
				return "", err
			}
		}
	}
	return id, nil
}

func (r *runTx) requestApproval(ctx context.Context, stepID, intentID string, in *domain.ActionIntent, required int) error {
	if required < 1 {
		required = 1
	}
	reqID := newID()
	if err := r.event(ctx, "approval.requested", "system", "", &stepID, nil, map[string]any{
		"approval_request_id": reqID, "action_hash": in.ActionHash, "risk_class": in.RiskClass,
		"tool": in.ToolName, "target": in.Target, "required_approvals": required,
	}); err != nil {
		return err
	}
	_, err := r.tx.Exec(ctx, `INSERT INTO approval_requests
		(id, tenant_id, action_intent_id, required_approvals, expires_at, requested_event_seq)
		VALUES ($1,$2,$3,$4,$5,$6)`, reqID, r.run.TenantID, intentID, required,
		r.now.Add(r.s.cfg.ApprovalTTL), r.run.LastEventSeq)
	return err
}

// reasonStep plans the next reasoning step; every reason step shares the goal input.
func (r *runTx) reasonStep(ctx context.Context) (PlannedStep, error) {
	var inputHash string
	err := r.tx.QueryRow(ctx, `SELECT input_hash FROM agent_steps WHERE run_id=$1 AND input_ref=$2 ORDER BY seq LIMIT 1`,
		r.run.ID, r.run.GoalRef).Scan(&inputHash)
	return PlannedStep{Kind: domain.StepReason, Executor: "reasoner", InputRef: r.run.GoalRef,
		InputHash: inputHash, Status: domain.StepPending}, err
}

// stepBudget is the run's step limit: the profile's max_steps when set,
// never more than the global MaxStepsPerRun.
func (r *runTx) stepBudget(ctx context.Context) int {
	limit := r.s.cfg.MaxStepsPerRun
	if r.run.ProfileID == "" {
		return limit
	}
	key := fmt.Sprintf("%s/%s@%d", r.run.TenantID, r.run.ProfileID, r.run.ProfileVersion)
	v, ok := r.s.maxSteps.Load(key)
	if !ok {
		var n *int
		err := r.tx.QueryRow(ctx, `SELECT (spec->>'max_steps')::int FROM agent_profiles
			WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`, r.run.TenantID, r.run.ProfileID, r.run.ProfileVersion).Scan(&n)
		if err != nil {
			return limit // the profile row is referenced by a FK; a read error keeps the global limit
		}
		m := 0
		if n != nil {
			m = *n
		}
		r.s.maxSteps.Store(key, m)
		v = m
	}
	if m := v.(int); m > 0 && m < limit {
		return m
	}
	return limit
}

// appendPlan inserts planned steps, enforcing the step budget, and moves the
// run to the matching state. The run must not hold a lease when called.
func (r *runTx) appendPlan(ctx context.Context, plan []PlannedStep) error {
	if int(r.run.NextStepSeq-1)+len(plan) > r.stepBudget(ctx) {
		return r.fail(ctx, "step budget exhausted")
	}
	waitingApproval := false
	waitingChildren := false
	waitingInput := false
	for _, p := range plan {
		if _, err := r.insertStep(ctx, p); err != nil {
			return err
		}
		switch p.Status {
		case domain.StepWaitingApproval:
			waitingApproval = true
		case domain.StepWaitingChildren:
			waitingChildren = true
		case domain.StepWaitingInput:
			waitingInput = true
		}
	}
	if waitingApproval {
		if r.run.DesiredState == domain.DesiredCancelled {
			return r.cancel(ctx, "system", "", "cancel requested")
		}
		if r.run.DesiredState == domain.DesiredPaused {
			r.setStatus(domain.RunPaused)
			return r.event(ctx, "run.paused", "system", "", nil, nil, nil)
		}
		r.setStatus(domain.RunWaitingApproval)
		return r.event(ctx, "run.waiting_approval", "system", "", nil, nil, nil)
	}
	if waitingInput {
		if r.run.DesiredState == domain.DesiredCancelled {
			return r.cancel(ctx, "system", "", "cancel requested")
		}
		r.setStatus(domain.RunWaitingInput)
		return r.event(ctx, "run.waiting_input", "system", "", nil, nil, nil)
	}
	if waitingChildren {
		if r.run.DesiredState == domain.DesiredCancelled {
			return r.cancel(ctx, "system", "", "cancel requested")
		}
		r.setStatus(domain.RunWaitingChildren)
		return r.event(ctx, "run.waiting_children", "system", "", nil, nil, nil)
	}
	r.setStatus(domain.RunQueued)
	return r.settle(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func trimErr(err error) string {
	s := err.Error()
	if len(s) > 500 {
		s = s[:500]
	}
	return strings.ToValidUTF8(s, "?")
}
