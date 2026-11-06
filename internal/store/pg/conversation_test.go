package pg_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

func newSession(t *testing.T, s *pg.Store) domain.Session {
	t.Helper()
	sess, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: tenant, ActorID: "alice",
		ProfileID: "sre-diagnosis", IdempotencyKey: fmt.Sprint("sess-", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	return sess
}

func submit(t *testing.T, s *pg.Store, sess domain.Session, clientID string, policy domain.BranchPolicy) (domain.SubmittedTurn, error) {
	t.Helper()
	return s.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: tenant, ActorID: "alice", SessionID: sess.ID,
		ClientRequestID: clientID, InputRef: "mem://tenant-a/goal/" + clientID, InputHash: "h-" + clientID,
		ProfileVersion: 1}, policy, 20)
}

// completeRun drives the run's current step to a final answer.
func completeRun(t *testing.T, s *pg.Store, runID string) {
	t.Helper()
	l, err := s.ClaimStep(ctx, runID, "w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitSuccess(ctx, l, domain.StepSuccess{OutputRef: "mem://tenant-a/decision/x", OutputHash: "h",
		Complete: true, ResultRef: "mem://tenant-a/result/" + runID}); err != nil {
		t.Fatal(err)
	}
}

func TestTurnsOnOneBranchAreSerialized(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	t1, err := submit(t, s, sess, "c1", domain.BranchQueue)
	if err != nil || t1.Queued || t1.Status != domain.TurnRunning {
		t.Fatalf("first turn must run immediately: %+v %v", t1, err)
	}
	t2, err := submit(t, s, sess, "c2", domain.BranchQueue)
	if err != nil || !t2.Queued || t2.Seq != 2 {
		t.Fatalf("second turn must queue: %+v %v", t2, err)
	}
	if r := mustRun(t, s, t2.RunID); r.Status != domain.RunPendingActivation {
		t.Fatalf("queued turn's run must be PENDING_ACTIVATION, got %s", r.Status)
	}
	if _, err := s.ClaimStep(ctx, t2.RunID, "w", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatal("a queued turn's run must not be schedulable")
	}
	var outbox int
	s.Pool().QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE aggregate_id=$1`, t2.RunID).Scan(&outbox)
	if outbox != 0 {
		t.Fatal("no wake-up may be written for a pending-activation run")
	}
	msgs, _ := s.BranchMessages(ctx, tenant, sess.DefaultBranchID, 1<<62)
	if len(msgs) != 1 {
		t.Fatalf("queued input must not become a message yet: %d messages", len(msgs))
	}

	completeRun(t, s, t1.RunID)
	if r := mustRun(t, s, t2.RunID); r.Status != domain.RunQueued {
		t.Fatalf("finishing turn 1 must activate turn 2, got %s", r.Status)
	}
	completeRun(t, s, t2.RunID)
	msgs, _ = s.BranchMessages(ctx, tenant, sess.DefaultBranchID, 1<<62)
	var order []string
	for _, m := range msgs {
		order = append(order, fmt.Sprintf("%d:%s", m.Seq, m.Role))
	}
	if fmt.Sprint(order) != "[1:user 2:assistant 3:user 4:assistant]" {
		t.Fatalf("messages must alternate in turn order: %v", order)
	}
	turns, _ := s.ListTurns(ctx, tenant, sess.ID, "")
	for _, tr := range turns {
		if tr.Status != domain.TurnCompleted {
			t.Fatalf("turn %d: %s", tr.Seq, tr.Status)
		}
	}
	if *turns[1].InputMessageCursor != 3 {
		t.Fatalf("turn 2's input cursor must be pinned at activation: %v", *turns[1].InputMessageCursor)
	}
}

func TestDuplicateTurnRequestsReturnSameTurn(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	var wg sync.WaitGroup
	ids := make(chan string, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr, err := submit(t, s, sess, "same", domain.BranchQueue)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- tr.TurnID + "/" + tr.RunID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("duplicate submissions created %d turns", len(seen))
	}
	var runs int
	s.Pool().QueryRow(ctx, `SELECT count(*) FROM agent_runs WHERE session_id=$1`, sess.ID).Scan(&runs)
	if runs != 1 {
		t.Fatalf("expected 1 run, got %d", runs)
	}
}

func TestConcurrentTurnsGetMonotonicSeq(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := submit(t, s, sess, fmt.Sprint("c", i), domain.BranchQueue); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	turns, _ := s.ListTurns(ctx, tenant, sess.ID, "")
	running := 0
	for i, tr := range turns {
		if tr.Seq != int64(i+1) {
			t.Fatalf("turn seqs must be gap-free: %d at %d", tr.Seq, i)
		}
		if tr.Status == domain.TurnRunning {
			running++
		}
	}
	if running != 1 {
		t.Fatalf("exactly one turn may be active per branch, got %d", running)
	}
}

func TestRejectPolicyAndQueueLimit(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	submit(t, s, sess, "a", domain.BranchReject)
	if _, err := submit(t, s, sess, "b", domain.BranchReject); !errors.Is(err, domain.ErrBranchBusy) {
		t.Fatalf("reject policy must refuse while a turn is active: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, err := submit(t, s, sess, fmt.Sprint("q", i), domain.BranchQueue); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := submit(t, s, sess, "overflow", domain.BranchQueue); !errors.Is(err, domain.ErrTurnQueueFull) {
		t.Fatalf("queue limit must apply: %v", err)
	}
}

func TestCancelQueuedTurnAndActivateNext(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	t1, _ := submit(t, s, sess, "1", domain.BranchQueue)
	t2, _ := submit(t, s, sess, "2", domain.BranchQueue)
	t3, _ := submit(t, s, sess, "3", domain.BranchQueue)
	if _, err := s.ApplyCommand(ctx, tenant, t2.RunID, domain.CommandRequest{Type: domain.CmdCancel, IdempotencyKey: "x", ActorID: "alice"}); err != nil {
		t.Fatal(err)
	}
	if r := mustRun(t, s, t2.RunID); r.Status != domain.RunCancelled {
		t.Fatalf("queued turn's run must cancel directly: %s", r.Status)
	}
	completeRun(t, s, t1.RunID)
	if r := mustRun(t, s, t3.RunID); r.Status != domain.RunQueued {
		t.Fatalf("turn 3 must be activated after turn 1, skipping cancelled turn 2: %s", r.Status)
	}
	tr2, _ := s.GetTurn(ctx, tenant, t2.TurnID)
	if tr2.Status != domain.TurnCancelled {
		t.Fatalf("turn 2: %s", tr2.Status)
	}
}

func TestReconcilerActivatesStalledBranch(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	t1, _ := submit(t, s, sess, "1", domain.BranchQueue)
	t2, _ := submit(t, s, sess, "2", domain.BranchQueue)
	// Simulate a lost activation: turn 1 finished but the branch was not advanced.
	s.Pool().Exec(ctx, `UPDATE conversation_branches SET active_turn_id=NULL`)
	s.Pool().Exec(ctx, `UPDATE conversation_turns SET status='COMPLETED' WHERE id=$1`, t1.TurnID)
	if n, err := s.ActivateStalledBranches(ctx, 10); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if r := mustRun(t, s, t2.RunID); r.Status != domain.RunQueued {
		t.Fatalf("stalled branch must be activated: %s", r.Status)
	}
}

func TestForkBranchHistory(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	sess := newSession(t, s)
	t1, _ := submit(t, s, sess, "1", domain.BranchQueue)
	completeRun(t, s, t1.RunID)
	t2, _ := submit(t, s, sess, "2", domain.BranchQueue)
	completeRun(t, s, t2.RunID)

	mid := int64(1)
	if _, err := s.ForkBranch(ctx, domain.ForkBranchRequest{TenantID: tenant, ActorID: "alice", SessionID: sess.ID,
		ParentBranchID: sess.DefaultBranchID, MessageCursor: &mid}); !errors.Is(err, domain.ErrInvalidCursor) {
		t.Fatalf("forking after a user message (mid-turn) must be refused: %v", err)
	}
	cur := int64(2)
	br, err := s.ForkBranch(ctx, domain.ForkBranchRequest{TenantID: tenant, ActorID: "alice", SessionID: sess.ID,
		ParentBranchID: sess.DefaultBranchID, MessageCursor: &cur})
	if err != nil {
		t.Fatal(err)
	}
	f1, err := s.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: tenant, ActorID: "alice", SessionID: sess.ID,
		BranchID: br.ID, ClientRequestID: "f1", InputRef: "mem://x", InputHash: "h", ProfileVersion: 1}, domain.BranchQueue, 20)
	if err != nil || f1.Queued {
		t.Fatalf("fork must run independently of the parent: %+v %v", f1, err)
	}
	completeRun(t, s, f1.RunID)
	hist, _ := s.BranchMessages(ctx, tenant, br.ID, 1<<62)
	var seqs []string
	for _, m := range hist {
		seqs = append(seqs, fmt.Sprintf("%d:%s:%v", m.Seq, m.Role, m.BranchID == br.ID))
	}
	if len(hist) != 4 || hist[1].Seq != 2 || hist[2].Seq != 3 || hist[2].BranchID != br.ID {
		t.Fatalf("fork history must be parent[..2] + own messages: %v", seqs)
	}
	parent, _ := s.BranchMessages(ctx, tenant, sess.DefaultBranchID, 1<<62)
	if len(parent) != 4 || parent[2].BranchID == br.ID {
		t.Fatal("parent history must be unaffected by the fork")
	}
	view, err := s.ResumeSession(ctx, tenant, sess.ID)
	if err != nil || len(view.Branches) != 2 || view.ActiveTurn != nil {
		t.Fatalf("resume: %+v %v", view, err)
	}
}

func TestRunQuota(t *testing.T) {
	s := testutil.Store(t, pg.DefaultConfig())
	s.Pool().Exec(ctx, `INSERT INTO tenant_quotas (tenant_id, max_active_runs, max_queued_runs) VALUES ($1, 10, 2)`, tenant)
	newRun(t, s, "q1")
	newRun(t, s, "q2")
	_, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: tenant, ProfileID: "sre-diagnosis", ProfileVersion: 1,
		GoalRef: "mem://x", GoalHash: "h", IdempotencyKey: "q3"})
	if !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("queued-run quota must apply: %v", err)
	}
}
