package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

const tenant = testutil.Tenant

var ctx = context.Background()

func newRun(t *testing.T, s *pg.Store, key string) domain.Run {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"goal": "diagnose service=checkout"})
	arts := artifact.NewMemory()
	ref, h, _ := arts.Put(ctx, tenant, "goal", data)
	run, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: tenant, ProfileID: "sre-diagnosis",
		ProfileVersion: 1, GoalRef: ref, GoalHash: h, IdempotencyKey: key, InitiatorID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func mustRun(t *testing.T, s *pg.Store, id string) domain.Run {
	t.Helper()
	r, err := s.GetRun(ctx, tenant, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func reasonPlan(goalRef string) []domain.PlannedStep {
	return []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: goalRef, InputHash: "h", Status: domain.StepPending}}
}

func success(goalRef string) domain.StepSuccess {
	return domain.StepSuccess{OutputRef: "mem://tenant-a/decision/x", OutputHash: "sha256:x", ArtifactKind: "decision",
		Plan: reasonPlan(goalRef)}
}

// assertEventLog checks invariant 4: event_seq is gap-free and strictly increasing.
func assertEventLog(t *testing.T, s *pg.Store, runID string) []domain.Event {
	t.Helper()
	events, err := s.ListEvents(ctx, tenant, runID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Seq != int64(i+1) {
			t.Fatalf("event %d has seq %d (%s)", i, e.Seq, e.Type)
		}
	}
	return events
}

func countEvents(events []domain.Event, typ string) int {
	n := 0
	for _, e := range events {
		if e.Type == typ {
			n++
		}
	}
	return n
}

func TestCreateRunIsIdempotent(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	a := newRun(t, s, "create-1")
	b := newRun(t, s, "create-1")
	if a.ID != b.ID {
		t.Fatalf("duplicate create produced two runs: %s %s", a.ID, b.ID)
	}
	steps, _ := s.ListSteps(ctx, tenant, a.ID)
	if len(steps) != 1 || steps[0].Kind != domain.StepReason {
		t.Fatalf("expected one reason step, got %+v", steps)
	}
	events := assertEventLog(t, s, a.ID)
	if countEvents(events, "run.created") != 1 {
		t.Fatal("run.created must be written once")
	}
	if _, err := s.GetRun(ctx, "other-tenant", a.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant read must be not found, got %v", err)
	}
}

func TestDuplicateDeliveryYieldsSingleLease(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "dup-1")
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
			if err == nil {
				mu.Lock()
				wins++
				mu.Unlock()
				return
			}
			if !errors.Is(err, domain.ErrLeaseHeld) && !errors.Is(err, domain.ErrNoRunnableStep) {
				t.Errorf("unexpected claim error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("expected exactly one lease, got %d", wins)
	}
	attempts, _ := s.ListAttempts(ctx, tenant, run.ID)
	if len(attempts) != 1 {
		t.Fatalf("expected one attempt, got %d", len(attempts))
	}
	assertEventLog(t, s, run.ID)
}

func TestStaleFenceCannotCommit(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "fence-1")
	leaseA, err := s.ClaimStep(ctx, run.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Worker A stalls past its lease.
	if _, err := s.Pool().Exec(ctx, `UPDATE agent_runs SET lease_until = now() - interval '1 second' WHERE id=$1`, run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RenewLease(ctx, leaseA, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expired lease must not be renewable, got %v", err)
	}
	if n, err := s.RecoverExpiredLeases(ctx, 10); err != nil || n != 1 {
		t.Fatalf("recover: n=%d err=%v", n, err)
	}
	leaseB, err := s.ClaimStep(ctx, run.ID, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if leaseB.FenceToken <= leaseA.FenceToken || leaseB.AttemptNo != 2 {
		t.Fatalf("new lease must have higher fence and attempt 2: %+v", leaseB)
	}
	if leaseB.ContextSnapshotID != leaseA.ContextSnapshotID {
		t.Fatal("retry attempt must reuse the step's context snapshot")
	}
	if err := s.CommitSuccess(ctx, leaseA, success(run.GoalRef)); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale worker commit must be rejected, got %v", err)
	}
	if err := s.CommitSuccess(ctx, leaseB, success(run.GoalRef)); err != nil {
		t.Fatal(err)
	}
	attempts, _ := s.ListAttempts(ctx, tenant, run.ID)
	if attempts[0].Status != domain.AttemptLost || attempts[1].Status != domain.AttemptSucceeded {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}
	assertEventLog(t, s, run.ID)
}

func TestHeartbeatDoesNotBumpVersion(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "hb-1")
	lease, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	v := mustRun(t, s, run.ID).Version
	for i := 0; i < 3; i++ {
		if _, err := s.RenewLease(ctx, lease, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if got := mustRun(t, s, run.ID).Version; got != v {
		t.Fatalf("heartbeat changed version %d -> %d", v, got)
	}
	cmd, err := s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "p",
		ActorID: "alice", ExpectedVersion: &v})
	if err != nil || cmd.Status != domain.CommandApplied {
		t.Fatalf("pause with current version after heartbeats must apply: %v %+v", err, cmd)
	}
}

func TestPauseTakesEffectAtStepBoundary(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "pause-1")
	lease, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "p1", ActorID: "alice"})
	if err != nil || cmd.Status != domain.CommandApplied {
		t.Fatalf("pause: %v %+v", err, cmd)
	}
	r := mustRun(t, s, run.ID)
	if r.Status != domain.RunRunning || r.DisplayStatus() != "PAUSE_REQUESTED" {
		t.Fatalf("accepted is not effective: want RUNNING/PAUSE_REQUESTED, got %s/%s", r.Status, r.DisplayStatus())
	}
	if err := s.CommitSuccess(ctx, lease, success(run.GoalRef)); err != nil {
		t.Fatal(err)
	}
	r = mustRun(t, s, run.ID)
	if r.Status != domain.RunPaused {
		t.Fatalf("expected PAUSED after step commit, got %s", r.Status)
	}
	if _, err := s.ClaimStep(ctx, run.ID, "w", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("paused run must not be claimable: %v", err)
	}
	stale := r.Version - 1
	_, err = s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdResume, IdempotencyKey: "r1",
		ActorID: "alice", ExpectedVersion: &stale})
	if !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("stale resume must conflict, got %v", err)
	}
	if c, _ := s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdResume, IdempotencyKey: "r2",
		ActorID: "alice"}); c.Status != domain.CommandRejected {
		t.Fatalf("resume without expected_version must be rejected, got %+v", c)
	}
	_, err = s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdResume, IdempotencyKey: "r3",
		ActorID: "alice", ExpectedVersion: &r.Version})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimStep(ctx, run.ID, "w", time.Minute); err != nil {
		t.Fatalf("resumed run must be claimable: %v", err)
	}
	events := assertEventLog(t, s, run.ID)
	if countEvents(events, "run.pause_requested") != 1 || countEvents(events, "run.paused") != 1 {
		t.Fatal("pause must be recorded as requested and then effective")
	}
}

func TestCancelQueuedRunAppliesImmediately(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "cancel-1")
	if _, err := s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdCancel, IdempotencyKey: "c", ActorID: "alice"}); err != nil {
		t.Fatal(err)
	}
	if r := mustRun(t, s, run.ID); r.Status != domain.RunCancelled {
		t.Fatalf("queued run must be cancelled directly, got %s", r.Status)
	}
	steps, _ := s.ListSteps(ctx, tenant, run.ID)
	if steps[0].Status != domain.StepCancelled {
		t.Fatalf("pending step must be cancelled, got %s", steps[0].Status)
	}
	if _, err := s.ClaimStep(ctx, run.ID, "w", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatal("cancelled run must not be claimable")
	}
}

func TestCommandIdempotencyAndTenantIsolation(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "idem-1")
	req := domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "same", ActorID: "alice"}
	a, err := s.ApplyCommand(ctx, tenant, run.ID, req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.ApplyCommand(ctx, tenant, run.ID, req)
	if err != nil || a.ID != b.ID {
		t.Fatalf("retried command must return the original: %v %s %s", err, a.ID, b.ID)
	}
	if countEvents(assertEventLog(t, s, run.ID), "command.received") != 1 {
		t.Fatal("retried command must not be applied twice")
	}
	if _, err := s.ApplyCommand(ctx, "other-tenant", run.ID, domain.CommandRequest{Type: domain.CmdCancel,
		IdempotencyKey: "x", ActorID: "mallory"}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("cross-tenant command must be not found, got %v", err)
	}
}

func TestRetryableFailureSchedulesDelayedRetry(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "retry-1")
	l1, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitFailure(ctx, l1, domain.StepFailure{Class: domain.FailRetryable, Code: "RATE_LIMITED",
		RetryAfter: 300 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	r := mustRun(t, s, run.ID)
	if r.Status != domain.RunQueued || r.NotBefore == nil || r.LeaseOwner != nil {
		t.Fatalf("expected QUEUED with not_before and no lease, got %+v", r)
	}
	if _, err := s.ClaimStep(ctx, run.ID, "w", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("retry must wait for not_before, got %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	l2, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if l2.AttemptNo != 2 || l2.IdempotencyKey != l1.IdempotencyKey {
		t.Fatalf("retry must be attempt 2 with the same idempotency key: %+v", l2)
	}
}

func TestPermanentFailureFailsRun(t *testing.T) {
	cfg := pg.DefaultConfig()
	s := testutil.Store(t, cfg)
	run := newRun(t, s, "perm-1")
	l, _ := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err := s.CommitFailure(ctx, l, domain.StepFailure{Class: domain.FailPermanent, Code: "BAD", Message: "boom"}); err != nil {
		t.Fatal(err)
	}
	if r := mustRun(t, s, run.ID); r.Status != domain.RunFailed || r.FailureReason == nil {
		t.Fatalf("expected FAILED with reason, got %+v", r)
	}
}

func TestStepBudgetExhaustionFailsRun(t *testing.T) {
	cfg := pg.DefaultConfig()
	cfg.MaxStepsPerRun = 2
	s := testutil.Store(t, cfg)
	run := newRun(t, s, "budget-1")
	for i := 0; i < 2; i++ {
		l, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CommitSuccess(ctx, l, success(run.GoalRef)); err != nil {
			t.Fatal(err)
		}
	}
	if r := mustRun(t, s, run.ID); r.Status != domain.RunFailed {
		t.Fatalf("expected FAILED after budget, got %s", r.Status)
	}
}

func TestLostWorkerAfterDispatchNeedsHuman(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	run := newRun(t, s, "effect-1")
	// Plan a side-effecting tool step directly (approval-free R1 intent for this test).
	l, _ := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	intent := &domain.ActionIntent{Environment: "staging", Target: "checkout", ToolName: "t.write", ToolVersion: "1",
		InputHash: "in", ActionHash: "sha256:act", RiskClass: domain.RiskR1, PolicyVersion: "p", Effects: json.RawMessage(`{}`)}
	if err := s.CommitSuccess(ctx, l, domain.StepSuccess{OutputRef: "mem://tenant-a/decision/x", OutputHash: "h",
		Plan: []domain.PlannedStep{{Kind: domain.StepTool, Executor: "tool", InputRef: "mem://tenant-a/tool-input/x",
			InputHash: "in", Status: domain.StepPending, Intent: intent}}}); err != nil {
		t.Fatal(err)
	}
	tl, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if st, _, err := s.PrepareEffect(ctx, tl, "checkout"); err != nil || st != domain.EffectDispatched {
		t.Fatalf("prepare: %v %v", st, err)
	}
	// Worker dies after dispatching; lease expires.
	s.Pool().Exec(ctx, `UPDATE agent_runs SET lease_until = now() - interval '1 second' WHERE id=$1`, run.ID)
	if _, err := s.RecoverExpiredLeases(ctx, 10); err != nil {
		t.Fatal(err)
	}
	r := mustRun(t, s, run.ID)
	if r.Status != domain.RunNeedsHuman {
		t.Fatalf("a possibly-applied side effect must not be retried automatically, got %s", r.Status)
	}
	if _, err := s.ClaimStep(ctx, run.ID, "w", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatal("NEEDS_HUMAN run must not be claimable")
	}
	steps, _ := s.ListSteps(ctx, tenant, run.ID)
	_, err = s.ApplyCommand(ctx, tenant, run.ID, domain.CommandRequest{Type: domain.CmdResolveUnknown, IdempotencyKey: "res",
		ActorID: "alice", ExpectedVersion: &r.Version,
		Payload: json.RawMessage(`{"step_id":"` + steps[1].ID + `","resolution":"retry","note":"checked: not applied"}`)})
	if err != nil {
		t.Fatal(err)
	}
	l3, err := s.ClaimStep(ctx, run.ID, "w", time.Minute)
	if err != nil {
		t.Fatalf("resolved run must be claimable: %v", err)
	}
	if st, _, err := s.PrepareEffect(ctx, l3, "checkout"); err != nil || st != domain.EffectDispatched {
		t.Fatalf("operator-confirmed retry must be dispatchable: %v %v", st, err)
	}
}

func TestKVSemantics(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	k := domain.KVKey{TenantID: tenant, Scope: "service", OwnerID: "checkout", Name: "owner"}
	v1, err := s.KVPutIfAbsent(ctx, domain.KVWrite{Key: k, Data: []byte(`"team-a"`)})
	if err != nil || v1 != 1 {
		t.Fatalf("put: %d %v", v1, err)
	}
	if _, err := s.KVPutIfAbsent(ctx, domain.KVWrite{Key: k, Data: []byte(`"x"`)}); !errors.Is(err, pg.ErrKVAlreadyExists) {
		t.Fatalf("second put must fail: %v", err)
	}
	if _, err := s.KVCompareAndSwap(ctx, domain.KVWrite{Key: k, Data: []byte(`"x"`)}, 7); !errors.Is(err, pg.ErrKVConflict) {
		t.Fatalf("stale CAS must conflict: %v", err)
	}
	v2, err := s.KVCompareAndSwap(ctx, domain.KVWrite{Key: k, Data: []byte(`"team-b"`)}, v1)
	if err != nil || v2 != 2 {
		t.Fatalf("cas: %d %v", v2, err)
	}
	if err := s.KVDeleteIfVersion(ctx, k, v2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.KVGet(ctx, k); !errors.Is(err, pg.ErrKVNotFound) {
		t.Fatal("deleted key must be not found")
	}
	if _, err := s.KVCompareAndSwap(ctx, domain.KVWrite{Key: k, Data: []byte(`"y"`)}, 3); !errors.Is(err, pg.ErrKVConflict) {
		t.Fatal("CAS on a tombstone must fail")
	}
	v4, err := s.KVPutIfAbsent(ctx, domain.KVWrite{Key: k, Data: []byte(`"team-c"`)})
	if err != nil || v4 != 4 {
		t.Fatalf("revive must continue the version sequence: %d %v", v4, err)
	}
	old, err := s.KVGetAtVersion(ctx, k, 2)
	if err != nil || string(old.Data) != `"team-b"` {
		t.Fatalf("history read: %s %v", old.Data, err)
	}
	// Expired keys behave as absent.
	e := domain.KVKey{TenantID: tenant, Scope: "service", OwnerID: "checkout", Name: "ttl"}
	if _, err := s.KVPutIfAbsent(ctx, domain.KVWrite{Key: e, Data: []byte(`1`), TTL: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if _, err := s.KVGet(ctx, e); !errors.Is(err, pg.ErrKVNotFound) {
		t.Fatal("expired key must be not found")
	}
	if _, err := s.KVCompareAndSwap(ctx, domain.KVWrite{Key: e, Data: []byte(`2`)}, 1); !errors.Is(err, pg.ErrKVConflict) {
		t.Fatal("CAS on an expired key must fail")
	}
	if v, err := s.KVPutIfAbsent(ctx, domain.KVWrite{Key: e, Data: []byte(`3`)}); err != nil || v != 2 {
		t.Fatalf("expired key revive: %d %v", v, err)
	}
	other := domain.KVKey{TenantID: "other-tenant", Scope: "service", OwnerID: "checkout", Name: "owner"}
	if _, err := s.KVGet(ctx, other); !errors.Is(err, pg.ErrKVNotFound) {
		t.Fatal("KV must be tenant-isolated")
	}
}

func TestOutboxClaimIsExclusive(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	for i := 0; i < 5; i++ {
		newRun(t, s, "outbox-"+string(rune('a'+i)))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs, err := s.ClaimOutbox(ctx, 10, time.Minute)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			for _, m := range msgs {
				seen[m.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 5 {
		t.Fatalf("expected 5 messages claimed, got %d", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s claimed %d times", id, n)
		}
	}
}
