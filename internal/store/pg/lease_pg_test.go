package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"go-sre-harness/internal/domain"
)

const testTarget = "deploy/checkout"

// seedLeasedToolRun creates a RUNNING run whose tool step attempt holds a live
// lease (fence 1) and whose side effect is in the given ledger state.
func seedLeasedToolRun(t *testing.T, s *Store, tenant, effectState string) domain.Lease {
	t.Helper()
	ctx := context.Background()
	runID, stepID, attemptID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	key := "idem-" + runID
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
			VALUES ($1,'p',3,'{}','h','ACTIVE') ON CONFLICT DO NOTHING`, []any{tenant}},
		{`INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status, create_idempotency_key, goal_ref,
			initiator_type, lease_owner, lease_until, fence_token, next_step_seq)
			VALUES ($1,$2,'p',3,'RUNNING',$3,'g','user','w1',now() + interval '30 seconds',1,2)`, []any{runID, tenant, runID}},
		{`INSERT INTO agent_steps (id, tenant_id, run_id, seq, kind, executor_name, status, input_ref, input_hash,
			current_attempt) VALUES ($1,$2,$3,1,'tool','x','RUNNING','r','h',1)`, []any{stepID, tenant, runID}},
		{`INSERT INTO step_attempts (id, tenant_id, run_id, step_id, attempt_no, worker_id, fence_token, status,
			idempotency_key) VALUES ($1,$2,$3,$4,1,'w1',1,'RUNNING',$5)`, []any{attemptID, tenant, runID, stepID, key}},
		{`INSERT INTO external_tool_effects (id, tenant_id, run_id, step_id, idempotency_key, target, state)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, []any{uuid.NewString(), tenant, runID, stepID, key, testTarget, effectState}},
	} {
		if _, err := s.pool.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	return domain.Lease{TenantID: tenant, RunID: runID, StepID: stepID, StepSeq: 1, AttemptID: attemptID, AttemptNo: 1,
		WorkerID: "w1", FenceToken: 1, Kind: domain.StepTool, IdempotencyKey: key}
}

func expireLease(t *testing.T, s *Store, runID string) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE agent_runs SET lease_until = now() - interval '1 second' WHERE id=$1`, runID); err != nil {
		t.Fatal(err)
	}
}

func runAndStep(t *testing.T, s *Store, l domain.Lease) (run, step, effect string) {
	t.Helper()
	if err := s.pool.QueryRow(context.Background(), `SELECT r.status, s.status, e.state FROM agent_runs r
		JOIN agent_steps s ON s.run_id = r.id JOIN external_tool_effects e ON e.step_id = s.id
		WHERE r.id=$1`, l.RunID).Scan(&run, &step, &effect); err != nil {
		t.Fatal(err)
	}
	return
}

// The effect is confirmed before the step commit; the commit is then lost
// and the lease expires. Recovery must retry the step (which reuses the
// confirmed result), not park the run for a human.
func TestRecordEffectResultSurvivesLostCommitPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-lease-" + uuid.NewString()[:8]

	recorded := seedLeasedToolRun(t, admin, tenant, "DISPATCHED")
	stale := recorded
	stale.FenceToken = 0
	if err := admin.RecordEffectResult(ctx, stale, testTarget, "obs-ref"); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale fence: %v, want ErrLeaseLost", err)
	}
	if err := admin.RecordEffectResult(ctx, recorded, testTarget, "obs-ref"); err != nil {
		t.Fatal(err)
	}
	if err := admin.RecordEffectResult(ctx, recorded, testTarget, "obs-ref"); err == nil {
		t.Fatal("recording twice must be rejected")
	}
	var ref string
	if err := admin.pool.QueryRow(ctx, `SELECT result_ref FROM external_tool_effects WHERE idempotency_key=$1`,
		recorded.IdempotencyKey).Scan(&ref); err != nil || ref != "obs-ref" {
		t.Fatalf("result_ref %q, %v", ref, err)
	}

	unrecorded := seedLeasedToolRun(t, admin, tenant, "DISPATCHED")
	expireLease(t, admin, recorded.RunID)
	expireLease(t, admin, unrecorded.RunID)
	if _, err := admin.RecoverExpiredLeases(ctx, 1000); err != nil {
		t.Fatal(err)
	}
	if run, step, eff := runAndStep(t, admin, recorded); run != "QUEUED" || step != "PENDING" || eff != "CONFIRMED" {
		t.Fatalf("recorded effect: run %s step %s effect %s; want QUEUED PENDING CONFIRMED", run, step, eff)
	}
	if run, step, eff := runAndStep(t, admin, unrecorded); run != "NEEDS_HUMAN" || step != "UNKNOWN" || eff != "UNKNOWN" {
		t.Fatalf("unrecorded effect: run %s step %s effect %s; want NEEDS_HUMAN UNKNOWN UNKNOWN", run, step, eff)
	}
	// The retried attempt finds the confirmed result instead of dispatching.
	retry := recorded
	retry.FenceToken, retry.AttemptID = 2, uuid.NewString()
	if _, err := admin.pool.Exec(ctx, `UPDATE agent_runs SET status='RUNNING', fence_token=2, lease_owner='w1',
		lease_until=now() + interval '30 seconds' WHERE id=$1`, retry.RunID); err != nil {
		t.Fatal(err)
	}
	state, prior, err := admin.PrepareEffect(ctx, retry, testTarget)
	if err != nil || state != EffectConfirmed || prior != "obs-ref" {
		t.Fatalf("prepare on retry: %v %q %v; want CONFIRMED obs-ref", state, prior, err)
	}
}

// AttemptStatus only answers for the fence that owns the attempt.
func TestAttemptStatusPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	l := seedLeasedToolRun(t, admin, "t-att-"+uuid.NewString()[:8], "PREPARED")
	if st, err := admin.AttemptStatus(ctx, l); err != nil || st != domain.AttemptRunning {
		t.Fatalf("running: %v %v", st, err)
	}
	if err := admin.CommitFailure(ctx, l, StepFailure{Class: FailRetryable, Code: "X", Message: "boom"}); err != nil {
		t.Fatal(err)
	}
	if st, err := admin.AttemptStatus(ctx, l); err != nil || st != domain.AttemptFailed {
		t.Fatalf("after commit: %v %v", st, err)
	}
	other := l
	other.FenceToken = 7
	if _, err := admin.AttemptStatus(ctx, other); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("foreign fence: %v, want ErrNotFound", err)
	}
	// A retried CommitFailure must not double-prefix its message: the
	// transaction body only uses locals.
	var msg string
	if err := admin.pool.QueryRow(ctx, `SELECT error_message FROM step_attempts WHERE id=$1`, l.AttemptID).Scan(&msg); err != nil || msg != "boom" {
		t.Fatalf("error_message %q, %v", msg, err)
	}
}

// A kernel transaction blocked on a run lock gives up after LockTimeout
// instead of stalling the worker indefinitely, with a retryable error.
func TestKernelTxLockTimeoutPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	l := seedLeasedToolRun(t, admin, "t-lock-"+uuid.NewString()[:8], "PREPARED")
	cfg := DefaultConfig()
	cfg.LockTimeout, cfg.TxRetries = 200*time.Millisecond, -1
	s := New(admin.pool, cfg)

	holder, err := admin.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	if _, err := holder.Exec(ctx, `SELECT 1 FROM agent_runs WHERE id=$1 FOR UPDATE`, l.RunID); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = s.RecordEffectResult(ctx, l, testTarget, "ref")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("err %v, want lock_not_available", err)
	}
	if !RetryableTxError(err) {
		t.Fatal("lock timeout must be retryable")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("waited %v for a held lock", d)
	}
}

func TestRetryableTxError(t *testing.T) {
	for code, want := range map[string]bool{"40001": true, "40P01": true, "55P03": true, "57014": true,
		"23505": false, "42P01": false} {
		if got := RetryableTxError(&pgconn.PgError{Code: code}); got != want {
			t.Errorf("%s: %v, want %v", code, got, want)
		}
	}
	if RetryableTxError(errors.New("boom")) {
		t.Error("an unknown error is not known to be rolled back")
	}
}
