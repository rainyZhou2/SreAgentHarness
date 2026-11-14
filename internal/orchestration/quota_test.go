package orchestration

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/migrations"
)

// countingRuns tracks how many runs are in flight at once.
type countingRuns struct {
	*fakeRuns
	mu       sync.Mutex
	inflight int
	peak     int
}

func (c *countingRuns) ReserveRun(ctx context.Context, req RunRequest) (string, error) {
	before := c.fakeRuns.started()
	id, err := c.fakeRuns.ReserveRun(ctx, req)
	if err == nil && c.fakeRuns.started() > before {
		c.mu.Lock()
		c.inflight++
		c.peak = max(c.peak, c.inflight)
		c.mu.Unlock()
	}
	return id, err
}

// finishSome completes up to n running runs.
func (c *countingRuns) finishSome(n int) {
	c.fakeRuns.mu.Lock()
	done := 0
	for _, id := range c.fakeRuns.order {
		r := c.fakeRuns.runs[id]
		if r.state.Terminal || done >= n {
			continue
		}
		r.state = RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: []byte(`{"answer":"ok"}`)}
		done++
	}
	c.fakeRuns.mu.Unlock()
	c.mu.Lock()
	c.inflight -= done
	c.mu.Unlock()
}

func quotaStores(t *testing.T, fn func(t *testing.T, s *cpsql.Store)) {
	t.Run("sqlite", func(t *testing.T) {
		s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fn(t, s)
	})
	t.Run("postgres", func(t *testing.T) {
		raw := os.Getenv("HARNESS_PG_TEST_URL")
		if raw == "" {
			t.Skip("HARNESS_PG_TEST_URL not set")
		}
		// Orchestrators claim Works of every tenant, so the shared test
		// database (full of other tests' Works) would be scheduled too:
		// use a scratch database for the duration of the test.
		ctx := context.Background()
		admin, err := pgxpool.New(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		defer admin.Close()
		name := "harness_quota_" + strings.ReplaceAll(cpkit.NewID()[24:], "-", "")
		if _, err := admin.Exec(ctx, `CREATE DATABASE `+name+` TEMPLATE template0 ENCODING 'UTF8'`); err != nil {
			t.Fatal(err)
		}
		defer admin.Exec(context.Background(), `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`) //nolint:errcheck
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		pool, err := pgxpool.New(ctx, u.String())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if err := migrations.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}
		s := cpsql.OpenPostgres(pool)
		defer s.Close()
		fn(t, s)
	})
}

// F07: three orchestrator replicas start the agents stages of 100 Works at
// once. The tenant's model-run budget is a transactional reservation, so
// the runs in flight never exceed it — not even by the stages in flight,
// which the former read-then-check allowed.
func TestGlobalModelRunBudgetAcrossReplicas(t *testing.T) {
	quotaStores(t, func(t *testing.T, s *cpsql.Store) {
		const capacity, works, replicas = 6, 100, 3
		ctx := context.Background()
		tenant := "quota-" + cpkit.NewID()[24:]
		runs := &countingRuns{fakeRuns: newFakeRuns()}
		tpl := work.Template{ID: "triage", Version: 1, Stages: []work.Stage{
			{ID: "triage", Type: work.StageAgents, Phase: "assess", Agents: []work.AgentSpec{
				{Role: "metrics", RunSpec: work.RunSpec{Goal: "Check ${subjects}"}, Required: true},
				{Role: "changes", RunSpec: work.RunSpec{Goal: "Changes of ${subjects}"}}}},
			{ID: "confirm", Type: work.StageConfirm}}}
		for i := 0; i < works; i++ {
			if _, _, err := s.CreateWork(ctx, work.NewWork{TenantID: tenant, Kind: work.KindRoutine, Title: "x", Template: tpl,
				Goal:      work.GoalSpec{Intent: "check", SubjectRefs: []string{"service://prod/search-api"}},
				ActorType: "user", ActorID: "op", IdempotencyKey: cpkit.NewID()}); err != nil {
				t.Fatal(err)
			}
		}
		limits := DefaultQueueLimits()
		l := limits[work.QueueRoutine]
		l.Concurrency, l.Batch, l.ModelRunsPerTenant = 16, 32, 0
		limits[work.QueueRoutine] = l
		rctx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for r := 0; r < replicas; r++ {
			o := &Orchestrator{Store: s, Runs: runs, Clock: time.Now, Owner: "replica-" + string(rune('a'+r)),
				Poll: 5 * time.Millisecond, RunPoll: 10 * time.Millisecond, Lease: time.Minute, Log: quiet, Queues: limits,
				Pools: NewPools(PoolLimits{ModelRunsPerTenant: capacity})}
			wg.Add(1)
			go func() { defer wg.Done(); _ = o.Run(rctx) }()
		}
		deadline := time.Now().Add(60 * time.Second)
		for runs.started() < works*2 && time.Now().Before(deadline) {
			runs.mu.Lock()
			peak := runs.peak
			runs.mu.Unlock()
			if peak > capacity {
				break
			}
			runs.finishSome(2)
			time.Sleep(2 * time.Millisecond)
		}
		cancel()
		wg.Wait()
		runs.mu.Lock()
		peak := runs.peak
		runs.mu.Unlock()
		if peak > capacity {
			t.Fatalf("%d model runs in flight under a budget of %d", peak, capacity)
		}
		if n := runs.started(); n != works*2 {
			t.Fatalf("started %d of %d runs: the budget starved Works", n, works*2)
		}
	})
}

// A cancelled Work returns its reservation at once.
func TestCancelReleasesModelRunBudget(t *testing.T) {
	h := newHarness(t, "R1")
	h.orch.Pools = NewPools(PoolLimits{ModelRunsPerTenant: 1})
	w, _, err := h.svc.Create(h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
		Kind: work.KindRoutine, TemplateID: "mitigate", Title: "check",
		Goal: work.GoalSpec{Intent: "check", SubjectRefs: []string{"deployment://prod/checkout"}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			t.Fatal(err)
		}
		h.clock.Add(time.Second)
	}
	held, _ := h.store.ActiveReservations(h.ctx, "t1", 10)
	if len(held) != 2 || held[0].WorkID != w.ID { // queue budget + tenant total
		t.Fatalf("a running stage must hold its reservation: %+v", held)
	}
	if _, err := h.svc.Command(h.ctx, CommandRequest{TenantID: "t1", WorkID: w.ID, ActorID: "alice",
		IdempotencyKey: cpkit.NewID(), Type: work.CmdCancel, Reason: "not needed"}); err != nil {
		t.Fatal(err)
	}
	if held, _ := h.store.ActiveReservations(h.ctx, "t1", 10); len(held) != 0 {
		t.Fatalf("cancel must release the budget: %+v", held)
	}
}
