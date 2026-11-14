package orchestration

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
)

// blockingRuns starts runs instantly, except for Works marked slow, whose
// ReserveRun blocks until release is closed (a stuck downstream).
type blockingRuns struct {
	*fakeRuns
	mu      sync.Mutex
	slow    map[string]bool
	release chan struct{}
	started map[string]time.Time
}

func (b *blockingRuns) ReserveRun(ctx context.Context, req RunRequest) (string, error) {
	b.mu.Lock()
	slow := b.slow[req.Labels["work_id"]]
	b.started[req.Labels["work_id"]] = time.Now()
	b.mu.Unlock()
	if slow {
		select {
		case <-b.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return b.fakeRuns.ReserveRun(ctx, req)
}

func (b *blockingRuns) startedAt(workID string) (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.started[workID]
	return t, ok
}

// A flood of background Works stuck on a slow downstream must not delay a
// critical mitigation: every queue has its own claim loop and slots.
func TestQueuesIsolateCriticalFromFlood(t *testing.T) {
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	runs := &blockingRuns{fakeRuns: newFakeRuns(), slow: map[string]bool{}, release: make(chan struct{}),
		started: map[string]time.Time{}}
	tpl := work.Template{ID: "investigate", Version: 1, Stages: []work.Stage{
		{ID: "investigate", Type: work.StageRun, Phase: "investigate", Run: &work.RunSpec{Goal: "Investigate ${subjects}"}},
		{ID: "confirm", Type: work.StageConfirm}}}
	ctx := context.Background()
	mk := func(q work.Queue) work.Work {
		w, _, err := s.CreateWork(ctx, work.NewWork{TenantID: "t1", Kind: work.KindIncident, Title: "x", Template: tpl,
			Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{"deployment://prod/checkout"}}, Queue: q,
			ActorType: "user", ActorID: "op", IdempotencyKey: cpkit.NewID()})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	for i := 0; i < 12; i++ {
		w := mk(work.QueueBackground)
		runs.mu.Lock()
		runs.slow[w.ID] = true
		runs.mu.Unlock()
	}
	o := &Orchestrator{Store: s, Runs: runs, Clock: time.Now, Owner: "o1", Poll: 20 * time.Millisecond,
		Lease: time.Minute, Log: quiet}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = o.Run(rctx); close(done) }()
	defer func() {
		cancel()
		close(runs.release)
		<-done
	}()

	// Let the background queue fill its slot with a stuck Work, then raise
	// an incident that needs mitigation.
	time.Sleep(100 * time.Millisecond)
	crit := mk(work.QueueCritical)
	created := time.Now()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if at, ok := runs.startedAt(crit.ID); ok {
			if lag := at.Sub(created); lag > time.Second {
				t.Fatalf("critical Work waited %s behind background Works", lag)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("critical Work never started while the background queue was stuck")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The stuck background queue holds exactly its own slots.
	n := 0
	for id := range runs.slow {
		if _, ok := runs.startedAt(id); ok {
			n++
		}
	}
	if lim := DefaultQueueLimits()[work.QueueBackground].Concurrency; n != lim {
		t.Fatalf("background Works in flight: %d, want its concurrency %d", n, lim)
	}
}

// The model-run budget defers (never fails) a stage while the tenant's
// queue budget is used up, records why once, and resumes when runs finish.
// critical-mitigation is exempt from the tenant total.
func TestModelRunBudgetDefers(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.orch.Queues = DefaultQueueLimits()
	lim := h.orch.Queues[work.QueueRoutine]
	lim.Concurrency, lim.ModelRunsPerTenant = 1, 1
	h.orch.Queues[work.QueueRoutine] = lim
	h.orch.Pools = NewPools(PoolLimits{ModelRunsPerTenant: 1})
	mk := func(q work.Queue) work.Work {
		w, _, err := h.svc.Create(h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
			Kind: work.KindRoutine, TemplateID: "mitigate", Title: "check", Queue: q,
			Goal: work.GoalSpec{Intent: "check", SubjectRefs: []string{"deployment://prod/checkout"}}})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	a, b := mk(""), mk("")
	if a.Queue != work.QueueRoutine {
		t.Fatalf("queue: %s", a.Queue)
	}
	for i := 0; i < 3; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.clock.Add(time.Second)
	}
	if n := h.runs.started(); n != 1 {
		t.Fatalf("runs started under a budget of 1: %d", n)
	}
	var waiting work.Work
	for _, id := range []string{a.ID, b.ID} {
		w, _ := h.store.GetWork(h.ctx, "t1", id)
		if strings.HasPrefix(w.Reason, "queued: ") {
			waiting = w
		}
	}
	if waiting.ID == "" || !strings.Contains(waiting.Reason, "model run budget") || waiting.Status.Terminal() || waiting.Status == work.StatusBlocked {
		t.Fatalf("deferred Work: %+v", waiting)
	}

	// Mitigation is exempt from the tenant total.
	c := mk(work.QueueCritical)
	for i := 0; i < 2; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.clock.Add(time.Second)
	}
	if n := h.runs.started(); n != 2 {
		t.Fatalf("critical Work should start despite the tenant budget: %d runs", n)
	}
	if cw, _ := h.store.GetWork(h.ctx, "t1", c.ID); strings.HasPrefix(cw.Reason, "queued: ") {
		t.Fatalf("critical Work deferred: %s", cw.Reason)
	}

	// Finishing the runs frees the budget; the deferred Work starts.
	h.runs.finishAll(map[string]any{"answer": "nothing to do"}, true)
	for i := 0; i < 20 && h.runs.started() < 3; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.clock.Add(11 * time.Second)
	}
	if n := h.runs.started(); n != 3 {
		t.Fatalf("deferred Work never started: %d runs", n)
	}
}

// A full production-action pool postpones dispatch; nothing is executed
// until a slot frees up, then the action runs exactly once.
func TestActionPoolDefersDispatch(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.orch.Pools = NewPools(PoolLimits{Actions: 1})
	hold, ok := h.orch.Pools.Actions.TryAcquire("other-tenant")
	if !ok {
		t.Fatal("acquire")
	}
	spec := &outcome.Spec{AllOf: []outcome.Objective{{ID: "image", Kind: outcome.KindState,
		State: &outcome.StateExpr{Predicate: "image", Equals: "v1"}, Freshness: cpkit.Duration(time.Hour)}}}
	w := h.create("mitigate", spec)
	h.settle(w.ID, work.StatusActive)
	h.runs.finishAll(rcaResult, true)
	for i := 0; i < 10; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.clock.Add(6 * time.Second)
	}
	cur, _ := h.store.GetWork(h.ctx, "t1", w.ID)
	if h.exec.calls != 0 || !strings.Contains(cur.Reason, "action pool is full") {
		t.Fatalf("dispatch with a full pool: calls=%d reason=%q", h.exec.calls, cur.Reason)
	}
	hold()
	w = h.settle(w.ID, work.StatusSucceeded, work.StatusBlocked, work.StatusFailed)
	if w.Status != work.StatusSucceeded || h.exec.calls != 1 {
		t.Fatalf("after release: %s (%s) calls=%d", w.Status, w.Reason, h.exec.calls)
	}
}

// Correlation rules can route authenticated events to critical-mitigation;
// operators can move a Work between queues, with a timeline event.
func TestRuleQueueAndSetQueueCommand(t *testing.T) {
	h := newHarness(t, domain.RiskR1, eventing.Rule{Name: "sev1", Match: eventing.Predicate{EventTypes: []string{"alert.firing"}},
		Key: "subject", Template: "mitigate", WorkKind: "incident", Queue: string(work.QueueCritical)})
	rec, _, err := h.ing.Ingest(h.ctx, eventing.Envelope{ID: "e1", TenantID: "t1", Source: "alertmanager", Type: "alert.firing",
		SubjectRef: "deployment://prod/checkout", OccurredAt: h.clock.Now()})
	if err != nil || rec.WorkID == "" {
		t.Fatalf("ingest: %v %+v", err, rec)
	}
	w, _ := h.store.GetWork(h.ctx, "t1", rec.WorkID)
	if w.Queue != work.QueueCritical {
		t.Fatalf("rule queue: %s", w.Queue)
	}
	w, err = h.command(w.ID, work.CmdSetQueue, map[string]any{"queue": "incident-investigate"})
	if err != nil || w.Queue != work.QueueIncident {
		t.Fatalf("set_queue: %v %s", err, w.Queue)
	}
	if count(h.events(w.ID), "work.queue_changed") != 1 {
		t.Fatalf("events: %v", h.events(w.ID))
	}
	if _, err := h.command(w.ID, work.CmdSetQueue, map[string]any{"queue": "asap"}); err == nil {
		t.Fatal("unknown queue accepted")
	}
}

func TestLimiter(t *testing.T) {
	l := NewLimiter("x", 3, 2)
	r1, ok1 := l.TryAcquire("a")
	r2, ok2 := l.TryAcquire("a")
	if _, ok := l.TryAcquire("a"); !ok1 || !ok2 || ok {
		t.Fatal("per-key cap")
	}
	r3, ok3 := l.TryAcquire("b")
	if _, ok := l.TryAcquire("c"); !ok3 || ok {
		t.Fatal("total cap")
	}
	if l.InFlight() != 3 {
		t.Fatalf("in flight: %d", l.InFlight())
	}
	r1()
	r1() // idempotent release
	if l.InFlight() != 2 {
		t.Fatalf("after release: %d", l.InFlight())
	}
	if _, err := l.Acquire(context.Background(), "a", 30*time.Millisecond); err != nil {
		t.Fatalf("slot free: %v", err)
	}
	if _, err := l.Acquire(context.Background(), "a", 30*time.Millisecond); err == nil {
		t.Fatal("expected timeout")
	}
	r2()
	r3()
	var nilL *Limiter
	if rel, ok := nilL.TryAcquire("z"); !ok {
		t.Fatal("nil limiter is unlimited")
	} else {
		rel()
	}
	m := MergePoolLimits(DefaultPoolLimits(), &PoolLimits{Actions: 2, Snapshots: -1})
	if m.Actions != 2 || m.Snapshots != 0 || m.ActionsPerTenant != DefaultPoolLimits().ActionsPerTenant {
		t.Fatalf("merge: %+v", m)
	}
	if _, err := ValidateQueueLimits(map[string]QueueLimit{"urgent": {}}); err == nil {
		t.Fatal("unknown queue name accepted")
	}
	q, err := ValidateQueueLimits(map[string]QueueLimit{"routine-work": {Concurrency: 7}})
	if err != nil || q[work.QueueRoutine].Concurrency != 7 || q[work.QueueRoutine].Batch != DefaultQueueLimits()[work.QueueRoutine].Batch {
		t.Fatalf("override: %v %+v", err, q[work.QueueRoutine])
	}
}
