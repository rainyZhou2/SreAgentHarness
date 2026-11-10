//go:build integration

// Fault-injection suite (RFC §24.3): several replicas process the same
// Works while replicas crash mid-pass, commits are lost after they
// succeeded, executors time out before or after applying, and leases
// expire. Run with:
//
//	go test -tags=integration ./internal/orchestration -run Fault -count=3
//	HARNESS_PG_TEST_URL=... go test -tags=integration ./internal/orchestration -run Fault
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
	"go-sre-harness/migrations"
)

// chaosDownstream is the target system: it applies each effect key at most
// once by itself (native idempotency) and counts every dispatch, so a
// replayed dispatch is visible even though it changes nothing.
type chaosDownstream struct {
	mu         sync.Mutex
	rnd        *rand.Rand
	world      *world.Service
	applied    map[string]int // effect key -> times applied
	dispatches map[string]int // effect key -> Execute calls
}

func (c *chaosDownstream) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "deployment.rollback", Version: "1", ResourceKinds: []string{"deployment"},
		Risk: domain.RiskR1, IdempotencyMode: action.IdemNative, BlastRadius: "single_resource"}
}

func (c *chaosDownstream) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}

func (c *chaosDownstream) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	c.mu.Lock()
	c.dispatches[key]++
	mode := c.rnd.Intn(10)
	c.mu.Unlock()
	apply := func() error {
		c.mu.Lock()
		first := c.applied[key] == 0
		if first {
			c.applied[key]++
		}
		c.mu.Unlock()
		if !first {
			return nil
		}
		now := c.world.Clock()
		_, _, err := c.world.Store.AssertFact(ctx, world.Fact{TenantID: p.TenantID, SubjectRef: p.TargetRef, Predicate: "image",
			Value: "v1", Source: "deployer", TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now})
		return err
	}
	switch {
	case mode < 2: // timeout before the downstream saw it
		return nil, errors.New("dial tcp: i/o timeout")
	case mode < 4: // applied, but the answer was lost
		if err := apply(); err != nil {
			return nil, err
		}
		return nil, errors.New("read tcp: connection reset")
	}
	if err := apply(); err != nil {
		return nil, err
	}
	return json.RawMessage(`{"ok":true}`), nil
}

func (c *chaosDownstream) Inspect(_ context.Context, p action.Plan) (action.InspectResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ok := c.applied[p.EffectKey] > 0
	return action.InspectResult{Applied: &ok, Detail: json.RawMessage(`"effect key lookup"`)}, nil
}

// lossyActions loses commits: the write succeeds, the caller sees an error.
type lossyActions struct {
	action.Store
	mu  sync.Mutex
	rnd *rand.Rand
}

func (l *lossyActions) UpdateAction(ctx context.Context, r action.Record, prev int64) (action.Record, error) {
	out, err := l.Store.UpdateAction(ctx, r, prev)
	l.mu.Lock()
	lose := l.rnd.Intn(8) == 0
	l.mu.Unlock()
	if err == nil && lose {
		return out, errors.New("connection reset while committing")
	}
	return out, err
}

func chaosStores(t *testing.T) map[string]*cpsql.Store {
	out := map[string]*cpsql.Store{}
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	out["sqlite"] = s
	if url := os.Getenv("HARNESS_PG_TEST_URL"); url != "" {
		pool, err := pgxpool.New(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		if err := migrations.Apply(context.Background(), pool); err != nil {
			t.Fatal(err)
		}
		out["postgres"] = cpsql.OpenPostgres(pool)
	}
	return out
}

func TestFaultInjectionExactlyOnce(t *testing.T) {
	for name, s := range chaosStores(t) {
		t.Run(name, func(t *testing.T) {
			seed := time.Now().UnixNano()
			if v := os.Getenv("HARNESS_CHAOS_SEED"); v != "" {
				fmt.Sscan(v, &seed)
			}
			t.Logf("seed %d", seed)
			runChaos(t, s, seed)
		})
	}
}

func runChaos(t *testing.T, s *cpsql.Store, seed int64) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(seed))
	var rmu sync.Mutex
	roll := func(n int) int { rmu.Lock(); defer rmu.Unlock(); return rnd.Intn(n) }
	clock := &fakeClock{t: time.Now().UTC().Truncate(time.Second)}
	s.Clock = clock.Now
	ws := &world.Service{Store: s, Clock: clock.Now}
	down := &chaosDownstream{rnd: rand.New(rand.NewSource(seed + 1)), world: ws, applied: map[string]int{}, dispatches: map[string]int{}}
	reg := action.NewRegistry()
	if err := reg.Register(down); err != nil {
		t.Fatal(err)
	}
	ev := &outcome.Evaluator{States: WorldFacts{World: ws}, Events: s}
	acts := &action.Runtime{Store: &lossyActions{Store: s, rnd: rand.New(rand.NewSource(seed + 2))}, Registry: reg, World: ws,
		Outcomes: ev, Clock: clock.Now, Log: quiet,
		Policy: policy.Engine{Version: "chaos", Environment: "staging", AllowR1WithoutApproval: true}}
	tpl := work.Template{ID: "chaos", Version: 1, Kind: work.KindIncident, Stages: []work.Stage{
		{ID: "mitigate", Type: work.StageAction, Phase: "mitigate", OnFailure: work.OnFailBlock,
			Action: &work.ActionSpec{Name: "deployment.rollback", Target: "${subject}", Args: json.RawMessage(`{"to":"v1"}`)}},
		{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: work.OnFailBlock,
			Verify: &work.VerifySpec{RecheckEvery: cpkit.Duration(10 * time.Second), Timeout: cpkit.Duration(30 * time.Minute)}},
	}}
	tenant := "chaos-" + cpkit.NewID()[24:]
	const works = 12
	var ids []string
	for i := 0; i < works; i++ {
		target := fmt.Sprintf("deployment://prod/svc-%d", i)
		now := clock.Now()
		if _, _, err := s.AssertFact(ctx, world.Fact{TenantID: tenant, SubjectRef: target, Predicate: "image", Value: "v2",
			Source: "deployer", TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
			t.Fatal(err)
		}
		w, _, err := s.CreateWork(ctx, work.NewWork{TenantID: tenant, Kind: work.KindIncident, Title: target, Template: tpl,
			Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{target}, Success: &outcome.Spec{AllOf: []outcome.Objective{{
				ID: "image", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "image", Equals: "v1"},
				Freshness: cpkit.Duration(24 * time.Hour)}}}},
			ActorType: "user", ActorID: "op", IdempotencyKey: cpkit.NewID(), Now: now,
			Subjects: []work.SubjectLink{{Kind: "subject", ID: target}}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, w.ID)
	}
	// The rollback lands strictly after the observed v2 image: facts of one
	// instant with different values are a conflict, never a winner.
	clock.Add(time.Second)
	replicas := make([]*Orchestrator, 3)
	for i := range replicas {
		replicas[i] = &Orchestrator{Store: s, Runs: newFakeRuns(), Actions: acts, Evaluator: ev, Clock: clock.Now,
			Owner: fmt.Sprintf("replica-%d", i), Lease: 30 * time.Second, Log: quiet}
	}
	rec := &Reconciler{Store: s, Actions: acts, Clock: clock.Now}

	settled := func() bool {
		for _, id := range ids {
			w, err := s.GetWork(ctx, tenant, id)
			if err != nil {
				t.Fatal(err)
			}
			if !w.Status.Terminal() && w.Status != work.StatusBlocked {
				return false
			}
		}
		return true
	}
	for round := 0; round < 400 && !settled(); round++ {
		var wg sync.WaitGroup
		for _, o := range replicas {
			wg.Add(1)
			go func(o *Orchestrator) {
				defer wg.Done()
				cctx, cancel := context.WithCancel(ctx)
				defer cancel()
				if roll(5) == 0 { // crash this replica somewhere in its pass
					go func() { time.Sleep(time.Duration(roll(3000)) * time.Microsecond); cancel() }()
				}
				_, _ = o.Tick(cctx)
			}(o)
		}
		wg.Wait()
		if _, err := rec.FireDue(ctx); err != nil {
			t.Fatal(err)
		}
		// Operators reconcile unknown effects read-only from time to time.
		if roll(3) == 0 {
			for _, id := range ids {
				as, _ := s.ListActions(ctx, tenant, id)
				for _, a := range as {
					if a.State == action.StateEffectUnknown || a.State == action.StateDispatched {
						_, _ = acts.Reconcile(ctx, tenant, a.ID)
						_ = s.WakeWork(ctx, tenant, id, "reconciled", "user", "op", nil)
					}
				}
			}
		}
		clock.Add(time.Duration(5+roll(40)) * time.Second)
	}

	// Invariants (RFC §24.5 hard gates).
	down.mu.Lock()
	defer down.mu.Unlock()
	for key, n := range down.applied {
		if n != 1 {
			t.Errorf("effect %s applied %d times", key, n)
		}
	}
	for key, n := range down.dispatches {
		if n != 1 {
			t.Errorf("effect %s dispatched %d times: a dispatched action must never be sent again", key, n)
		}
	}
	// No action that needed an approval reached the downstream without one
	// (the global gauge is Backlog.UnapprovedDispatches; this is per tenant).
	for _, id := range ids {
		as, _ := s.ListActions(ctx, tenant, id)
		for _, a := range as {
			if a.MayHaveDispatched && a.Approvals > 0 && len(a.ApprovalLog) == 0 && a.BreakGlass == nil {
				t.Errorf("action %s dispatched without its approval", a.ID)
			}
		}
	}
	succeeded, blocked := 0, 0
	for _, id := range ids {
		w, _ := s.GetWork(ctx, tenant, id)
		switch w.Status {
		case work.StatusSucceeded:
			succeeded++
			outs, _ := s.ListOutcomes(ctx, tenant, id, 1)
			if len(outs) == 0 || outs[0].Verdict != outcome.Satisfied {
				t.Errorf("work %s SUCCEEDED without a SATISFIED outcome", id)
			}
		case work.StatusBlocked:
			// The only acceptable stop: an effect nobody could confirm,
			// handed to a human instead of being sent again.
			blocked++
			if !strings.Contains(w.Reason, "effect is unknown") {
				t.Errorf("work %s blocked for an unexpected reason: %s", id, w.Reason)
			}
		default:
			t.Errorf("work %s did not settle: %s (%s)", id, w.Status, w.Reason)
		}
		if acts, _ := s.ListActions(ctx, tenant, id); len(acts) > w.Template.Limits.MaxActions {
			t.Errorf("work %s has %d actions", id, len(acts))
		}
	}
	t.Logf("succeeded=%d blocked=%d (blocked Works wait for a human, e.g. an effect nobody could verify)", succeeded, blocked)
	if succeeded == 0 {
		t.Error("no Work survived the faults")
	}
}
