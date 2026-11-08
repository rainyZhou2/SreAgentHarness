package action_test

// Action-runtime safety tests from the RFC test matrix (§24.1):
//
//	T09 stale or missing required facts → preflight refuses
//	T10 an approval binds exactly the plan; a changed target invalidates it
//	T11 the same plan is one ledger entry and one downstream effect
//	T12 a dispatch whose result is lost is never replayed automatically
//	T14 effect applied but the result commit failed → never reported success
//	T24 two Works acting on one deployment cannot both write
//	    (plus: sources that disagree on the target version block dispatch)
//
// They run on SQLite and, with HARNESS_PG_TEST_URL, on PostgreSQL.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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

const target = "deployment://prod/checkout"

var t0 = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeDeploy is a deployment executor with native idempotency: the
// downstream applies each effect key once and bumps the generation fact.
type fakeDeploy struct {
	name  string
	risk  domain.RiskClass
	idem  string
	world *world.Service
	// fail, when set, is returned by Execute after (applied=true) or
	// instead of (applied=false) the effect.
	fail        error
	failApplied bool
	noInspect   bool

	mu      sync.Mutex
	calls   int
	effects map[string]bool // effect keys the downstream applied
	gen     int
}

func (f *fakeDeploy) Descriptor() action.Descriptor {
	return action.Descriptor{Name: f.name, Version: "1", ResourceKinds: []string{"deployment"}, Risk: f.risk,
		IdempotencyMode: f.idem, RequiredFacts: []string{"image"}, VersionFact: "generation",
		BlastRadius: "single_resource"}
}

func (f *fakeDeploy) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}

func (f *fakeDeploy) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	f.mu.Lock()
	f.calls++
	if f.fail != nil && !f.failApplied {
		f.mu.Unlock()
		return nil, f.fail
	}
	if !f.effects[key] {
		f.effects[key] = true
		f.gen++
	}
	gen := f.gen
	f.mu.Unlock()
	now := f.world.Clock.Now()
	var a struct {
		To string `json:"to_image"`
	}
	_ = json.Unmarshal(p.Args, &a)
	for pred, val := range map[string]string{"image": a.To, "generation": itoa(gen)} {
		if _, _, err := f.world.Store.AssertFact(ctx, world.Fact{TenantID: p.TenantID, SubjectRef: p.TargetRef,
			Predicate: pred, Value: val, Source: "k8s", TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
			return nil, err
		}
	}
	if f.fail != nil {
		return nil, f.fail
	}
	return json.RawMessage(`{"ok":true}`), nil
}

func (f *fakeDeploy) Inspect(_ context.Context, p action.Plan) (action.InspectResult, error) {
	if f.noInspect {
		return action.InspectResult{}, action.ErrUnsupported
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	applied := f.effects[p.EffectKey]
	return action.InspectResult{Applied: &applied}, nil
}

func (f *fakeDeploy) callCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

// failingStore fails the commit of chosen action states (lost commit).
type failingStore struct {
	action.Store
	mu     sync.Mutex
	failOn map[action.State]bool
}

var errLostCommit = errors.New("connection reset while committing")

func (s *failingStore) UpdateAction(ctx context.Context, r action.Record, prev int64) (action.Record, error) {
	s.mu.Lock()
	fail := s.failOn[r.State]
	s.mu.Unlock()
	if fail {
		return r, errLostCommit
	}
	return s.Store.UpdateAction(ctx, r, prev)
}

type env struct {
	t     *testing.T
	ctx   context.Context
	clock *clock
	store *cpsql.Store
	world *world.Service
	tn    string
}

func forEachStore(t *testing.T, fn func(t *testing.T, e *env)) {
	run := func(t *testing.T, s *cpsql.Store) {
		c := &clock{t: t0}
		s.Clock = c.now
		e := &env{t: t, ctx: context.Background(), clock: c, store: s, tn: "t-" + cpkit.NewID()[24:],
			world: &world.Service{Store: s, Clock: c.now}}
		fn(t, e)
	}
	t.Run("sqlite", func(t *testing.T) {
		s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		run(t, s)
	})
	t.Run("postgres", func(t *testing.T) {
		url := os.Getenv("HARNESS_PG_TEST_URL")
		if url == "" {
			t.Skip("HARNESS_PG_TEST_URL not set")
		}
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if err := migrations.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}
		s := cpsql.OpenPostgres(pool)
		defer s.Close()
		run(t, s)
	})
}

// work creates a Work the actions belong to.
func (e *env) work() string {
	e.t.Helper()
	var tpl work.Template
	for _, x := range work.Builtin() {
		if x.ID == "incident" {
			tpl = x
		}
	}
	w, _, err := e.store.CreateWork(e.ctx, work.NewWork{TenantID: e.tn, Kind: work.KindIncident, Title: "checkout 5xx",
		Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{target}}, Template: tpl,
		IdempotencyKey: cpkit.NewID(), Now: e.clock.now(), Subjects: []work.SubjectLink{{Kind: "subject", ID: target}}})
	if err != nil {
		e.t.Fatal(err)
	}
	return w.ID
}

// observe asserts the current deployment facts as the cluster would.
func (e *env) observe(image string, gen int, at time.Time) {
	e.t.Helper()
	for pred, val := range map[string]string{"image": image, "generation": itoa(gen)} {
		if _, _, err := e.store.AssertFact(e.ctx, world.Fact{TenantID: e.tn, SubjectRef: target, Predicate: pred, Value: val,
			Source: "k8s", TrustTier: world.TrustFirstParty, ValidFrom: at, ObservedAt: at}); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) deploy(name string, risk domain.RiskClass) *fakeDeploy {
	return &fakeDeploy{name: name, risk: risk, idem: action.IdemNative, world: e.world, effects: map[string]bool{}, gen: 1}
}

func (e *env) runtime(execs ...action.Executor) *action.Runtime {
	e.t.Helper()
	reg := action.NewRegistry()
	for _, x := range execs {
		if err := reg.Register(x); err != nil {
			e.t.Fatal(err)
		}
	}
	return &action.Runtime{Store: e.store, Registry: reg, World: e.world, Clock: e.clock.now,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Outcomes: &outcome.Evaluator{Events: e.store},
		Policy:   policy.Engine{Version: "test", Environment: "staging", AllowR1WithoutApproval: true}}
}

func (e *env) propose(rt *action.Runtime, workID, name, image string) action.Record {
	e.t.Helper()
	r, err := rt.Propose(e.ctx, action.ProposeRequest{TenantID: e.tn, WorkID: workID, Generation: 1, Name: name,
		Target: target, Args: json.RawMessage(`{"to_image":"` + image + `"}`), ProposedBy: "work-owner",
		AllowedTargets: []string{target}})
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// T09: an action is refused when a required fact is missing or stale.
func TestPreflightRefusesMissingOrStaleFacts(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		w := e.work()

		r := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if r.State != action.StateRejected || !strings.Contains(r.Reason, "required fact missing") {
			t.Fatalf("missing facts: %s %q", r.State, r.Reason)
		}

		e.observe("checkout:v2", 1, e.clock.now())
		e.clock.add(10 * time.Minute) // older than the 5m default freshness
		r = e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if r.State != action.StateRejected || !strings.Contains(r.Reason, "stale") {
			t.Fatalf("stale facts: %s %q", r.State, r.Reason)
		}
		if ex.callCount() != 0 {
			t.Fatal("a refused plan must never reach the executor")
		}
		if _, err := rt.Execute(e.ctx, e.tn, r.ID); !errors.Is(err, action.ErrInvalidState) {
			t.Fatalf("executing a rejected plan: %v", err)
		}

		// Re-observed (unchanged value): fresh again, the plan is allowed.
		// (One second later: an identical plan in the same second is the
		// same ledger entry and would return the rejection above.)
		e.clock.add(time.Second)
		e.observe("checkout:v2", 1, e.clock.now())
		r = e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if r.State != action.StateReady || r.TargetVersion != "1" {
			t.Fatalf("fresh facts: %s %q version=%q", r.State, r.Reason, r.TargetVersion)
		}

		// Facts can also go stale between approval and dispatch.
		e.clock.add(10 * time.Minute)
		out, err := rt.Execute(e.ctx, e.tn, r.ID)
		if !errors.Is(err, action.ErrPrecondition) || out.State != action.StateRejected || ex.callCount() != 0 {
			t.Fatalf("stale at dispatch: %v %s calls=%d", err, out.State, ex.callCount())
		}
	})
}

// T10: an approval approves exactly the plan the approver saw.
func TestApprovalBindsThePlan(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR2)
		rt := e.runtime(ex)
		w := e.work()
		e.observe("checkout:v2", 1, e.clock.now())

		a := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if a.State != action.StateWaitingApproval || a.Approvals < 1 {
			t.Fatalf("R2 must wait for approval: %s approvals=%d", a.State, a.Approvals)
		}
		// Different arguments are a different intent.
		b := e.propose(rt, w, "deployment.rollback", "checkout:v0")
		if b.IntentHash == a.IntentHash || b.ID == a.ID {
			t.Fatal("different args must produce a different intent and plan")
		}

		// The approval of b cannot be replayed onto a.
		if _, err := rt.Decide(e.ctx, e.tn, a.ID, "oncall", "approve", b.IntentHash, "", ""); !errors.Is(err, action.ErrApprovalMismatch) {
			t.Fatalf("foreign intent hash: %v", err)
		}
		if _, err := rt.Execute(e.ctx, e.tn, a.ID); !errors.Is(err, action.ErrInvalidState) {
			t.Fatalf("unapproved dispatch: %v", err)
		}

		// Distinct approver: the owner cannot approve its own action.
		rt.RequireDistinctApprover = true
		if _, err := rt.Decide(e.ctx, e.tn, a.ID, "work-owner", "approve", a.IntentHash, "", "work-owner"); !errors.Is(err, action.ErrApprovalMismatch) {
			t.Fatalf("self approval: %v", err)
		}
		ok, err := rt.Decide(e.ctx, e.tn, a.ID, "oncall", "approve", a.IntentHash, "ok", "work-owner")
		if err != nil || ok.State != action.StateReady {
			t.Fatalf("approve: %v %s", err, ok.State)
		}

		// The target moved on after the approval: the approval no longer
		// covers it, and the plan is refused at dispatch.
		e.clock.add(time.Minute)
		e.observe("checkout:v3", 2, e.clock.now())
		out, err := rt.Execute(e.ctx, e.tn, a.ID)
		if !errors.Is(err, action.ErrPrecondition) || out.State != action.StateRejected ||
			!strings.Contains(out.Reason, "target version") || ex.callCount() != 0 {
			t.Fatalf("moved target: %v %s %q calls=%d", err, out.State, out.Reason, ex.callCount())
		}

		// A stricter policy after the approval also wins.
		c := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if _, err := rt.Decide(e.ctx, e.tn, c.ID, "oncall", "approve", c.IntentHash, "", "work-owner"); err != nil {
			t.Fatal(err)
		}
		rt.Policy = policy.Engine{Version: "test-2", Environment: "staging", AllowR1WithoutApproval: true}
		if _, err := rt.Execute(e.ctx, e.tn, c.ID); !errors.Is(err, action.ErrPrecondition) || ex.callCount() != 0 {
			t.Fatalf("policy change: %v calls=%d", err, ex.callCount())
		}

		// An approval after the window is refused and closes the plan.
		rt.Policy = policy.Engine{Version: "test", Environment: "staging", AllowR1WithoutApproval: true}
		e.clock.add(time.Second) // not the same plan as c
		d := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		e.clock.add(31 * time.Minute)
		late, err := rt.Decide(e.ctx, e.tn, d.ID, "oncall", "approve", d.IntentHash, "", "work-owner")
		if err != nil || late.State != action.StateRejected {
			t.Fatalf("expired approval: %v %s", err, late.State)
		}
	})
}

// T11: the same plan is one ledger entry; concurrent and repeated dispatch
// reach the downstream once.
func TestDispatchOnce(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		w := e.work()
		e.observe("checkout:v2", 1, e.clock.now())

		r := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		again := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if again.ID != r.ID || r.State != action.StateReady {
			t.Fatalf("re-proposal must return the same record: %s vs %s (%s)", again.ID, r.ID, r.State)
		}

		var wg sync.WaitGroup
		results := make(chan action.State, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, err := rt.Execute(e.ctx, e.tn, r.ID)
				if err == nil {
					results <- out.State
				}
			}()
		}
		wg.Wait()
		close(results)
		applied := 0
		for s := range results {
			if s == action.StateApplied {
				applied++
			}
		}
		if applied != 1 || ex.callCount() != 1 {
			t.Fatalf("concurrent dispatch: applied=%d calls=%d", applied, ex.callCount())
		}
		if _, err := rt.Execute(e.ctx, e.tn, r.ID); !errors.Is(err, action.ErrInvalidState) || ex.callCount() != 1 {
			t.Fatalf("repeated dispatch: %v calls=%d", err, ex.callCount())
		}
		got, _ := e.store.GetAction(e.ctx, e.tn, r.ID)
		if got.State != action.StateApplied || got.DispatchedAt == nil || !got.MayHaveDispatched {
			t.Fatalf("ledger: %+v", got)
		}
		acts, _ := e.store.ListActions(e.ctx, e.tn, w)
		if len(acts) != 1 {
			t.Fatalf("ledger entries: %d", len(acts))
		}
	})
}

// T12: an executor error that is not a proven no-op leaves the effect
// unknown; it is reconciled read-only or handed to a human, never re-sent.
func TestUnknownEffectIsNeverReplayed(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		e.observe("checkout:v2", 1, e.clock.now())
		w := e.work()

		// Native idempotency with Inspect: the downstream applied it.
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		ex.fail, ex.failApplied = errors.New("read tcp: i/o timeout"), true
		rt := e.runtime(ex)
		r := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		out, err := rt.Execute(e.ctx, e.tn, r.ID)
		if err != nil || out.State != action.StateEffectUnknown {
			t.Fatalf("timeout: %v %s", err, out.State)
		}
		if _, err := rt.Execute(e.ctx, e.tn, r.ID); !errors.Is(err, action.ErrInvalidState) {
			t.Fatalf("an unknown effect must not be dispatched again: %v", err)
		}
		rec, err := rt.Reconcile(e.ctx, e.tn, r.ID)
		if err != nil || rec.State != action.StateApplied || ex.callCount() != 1 {
			t.Fatalf("reconcile: %v %s calls=%d", err, rec.State, ex.callCount())
		}
		if v, err := rt.VerifyEffect(e.ctx, e.tn, r.ID, e.clock.now()); err != nil || v.State != action.StateVerified {
			t.Fatalf("verify reconciled effect: %v %s", err, v.State)
		}

		// No idempotency: nothing can be inspected; a human decides.
		e2 := e.deploy("traffic.drain", domain.RiskR1)
		e2.idem, e2.fail = action.IdemNone, errors.New("connection reset")
		rt2 := e.runtime(e2)
		e.clock.add(time.Second)
		e.observe("checkout:v2", 1, e.clock.now())
		r2 := e.propose(rt2, w, "traffic.drain", "checkout:v1")
		if out, err := rt2.Execute(e.ctx, e.tn, r2.ID); err != nil || out.State != action.StateEffectUnknown {
			t.Fatalf("unknown: %v %s", err, out.State)
		}
		rec, err = rt2.Reconcile(e.ctx, e.tn, r2.ID)
		if err != nil || rec.State != action.StateManualRecovery || e2.callCount() != 1 {
			t.Fatalf("manual recovery: %v %s calls=%d", err, rec.State, e2.callCount())
		}
		if _, err := rt2.Execute(e.ctx, e.tn, r2.ID); !errors.Is(err, action.ErrInvalidState) || e2.callCount() != 1 {
			t.Fatalf("manual recovery must not re-dispatch: %v calls=%d", err, e2.callCount())
		}
		done, err := rt2.Resolve(e.ctx, e.tn, r2.ID, "oncall", false, "verified by hand: not drained")
		if err != nil || done.State != action.StateFailed {
			t.Fatalf("resolve: %v %s", err, done.State)
		}

		// A proven no-op (ErrNotApplied) fails cleanly and frees the target.
		e3 := e.deploy("deployment.restart", domain.RiskR1)
		e3.fail = action.ErrNotApplied
		rt3 := e.runtime(e3)
		r3 := e.propose(rt3, w, "deployment.restart", "checkout:v2")
		if out, err := rt3.Execute(e.ctx, e.tn, r3.ID); err != nil || out.State != action.StateFailed {
			t.Fatalf("not applied: %v %s", err, out.State)
		}
		// Every action above settled, so none of them still holds the target.
		ok, holder, err := e.store.LockTarget(e.ctx, e.tn, target, cpkit.NewID(), e.clock.now().Add(time.Minute), e.clock.now())
		if err != nil || !ok {
			t.Fatalf("target still locked by %s: %v", holder, err)
		}
	})
}

// T14: the downstream applied the effect but the result commit was lost.
// The action is never reported as applied from that path; crash recovery
// reconciles it read-only and does not call Execute again.
func TestLostResultCommit(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		e.observe("checkout:v2", 1, e.clock.now())
		w := e.work()
		for _, inspectable := range []bool{true, false} {
			ex := e.deploy("deployment.rollback", domain.RiskR1)
			ex.noInspect = !inspectable
			rt := e.runtime(ex)
			fs := &failingStore{Store: e.store, failOn: map[action.State]bool{action.StateApplied: true}}
			rt.Store = fs
			e.clock.add(time.Second)
			e.observe("checkout:v2", 1, e.clock.now())
			r := e.propose(rt, w, "deployment.rollback", "checkout:v1")
			if r.State != action.StateReady {
				t.Fatalf("propose: %s %q", r.State, r.Reason)
			}
			out, err := rt.Execute(e.ctx, e.tn, r.ID)
			if !errors.Is(err, errLostCommit) {
				t.Fatalf("lost commit must surface as an error, got %v %s", err, out.State)
			}
			got, _ := e.store.GetAction(e.ctx, e.tn, r.ID)
			if got.State != action.StateDispatched {
				t.Fatalf("durable state after a lost commit: %s", got.State)
			}
			// The database is back; the recovery sweep runs later.
			fs.mu.Lock()
			fs.failOn = nil
			fs.mu.Unlock()
			e.clock.add(5 * time.Minute)
			if _, err := rt.RecoverStale(e.ctx, time.Minute, 1000); err != nil {
				t.Fatal(err)
			}
			got, _ = e.store.GetAction(e.ctx, e.tn, r.ID)
			want := action.StateApplied
			if !inspectable {
				want = action.StateManualRecovery
			}
			if got.State != want || ex.callCount() != 1 {
				t.Fatalf("inspectable=%v: recovered to %s (want %s), calls=%d", inspectable, got.State, want, ex.callCount())
			}
			// Settle so the next round can lock the target.
			if got.State == action.StateManualRecovery {
				if _, err := rt.Resolve(e.ctx, e.tn, r.ID, "oncall", false, ""); err != nil {
					t.Fatal(err)
				}
			} else if _, err := rt.VerifyEffect(e.ctx, e.tn, r.ID, e.clock.now()); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// T24: two Works plan opposite changes to one deployment. The target lock
// keeps the second out while the first is unsettled, and the version pin
// rejects it once the first has changed the deployment.
func TestConflictingActionsOnOneTarget(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.set_image", domain.RiskR1)
		rt := e.runtime(ex)
		e.observe("checkout:v2", 1, e.clock.now())
		wa, wb := e.work(), e.work()

		a := e.propose(rt, wa, "deployment.set_image", "checkout:v1")
		b := e.propose(rt, wb, "deployment.set_image", "checkout:v3")
		if a.State != action.StateReady || b.State != action.StateReady || a.TargetVersion != "1" || b.TargetVersion != "1" {
			t.Fatalf("plans: %s %s", a.State, b.State)
		}

		e.clock.add(time.Second)
		// Concurrent dispatch: at most one reaches the deployment.
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, id := range []string{a.ID, b.ID} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = rt.Execute(e.ctx, e.tn, id)
			}()
		}
		wg.Wait()
		if ex.callCount() != 1 {
			t.Fatalf("both plans reached the deployment: calls=%d errs=%v", ex.callCount(), errs)
		}
		winner, loser, lerr := a, b, errs[1]
		if errs[0] != nil {
			winner, loser, lerr = b, a, errs[0]
		}
		// The loser was either kept out by the target lock (and stays READY,
		// to be retried, never forced) or, if it ran after the winner had
		// changed the deployment, refused by its version pin.
		got, _ := e.store.GetAction(e.ctx, e.tn, loser.ID)
		switch {
		case errors.Is(lerr, action.ErrTargetBusy) && got.State == action.StateReady:
		case errors.Is(lerr, action.ErrPrecondition) && got.State == action.StateRejected:
		default:
			t.Fatalf("loser: %v %s", lerr, got.State)
		}

		// The winner settles and frees the target; the deployment is now at
		// generation 2, so the loser's plan (pinned to 1) can never apply.
		if v, err := rt.VerifyEffect(e.ctx, e.tn, winner.ID, e.clock.now()); err != nil || v.State != action.StateVerified {
			t.Fatalf("verify: %v %s", err, v.State)
		}
		out, err := rt.Execute(e.ctx, e.tn, loser.ID)
		if err == nil || out.State == action.StateApplied || ex.callCount() != 1 {
			t.Fatalf("stale plan after the other Work's change: %v %s calls=%d", err, out.State, ex.callCount())
		}
		if got, _ := e.store.GetAction(e.ctx, e.tn, loser.ID); got.State != action.StateRejected {
			t.Fatalf("loser final state: %s", got.State)
		}
	})
}

// Sources that disagree on the version a plan is pinned to block the plan
// at proposal and at dispatch.
func TestDisagreeingVersionSources(t *testing.T) {
	forEachStore(t, func(t *testing.T, e *env) {
		ex := e.deploy("deployment.rollback", domain.RiskR1)
		rt := e.runtime(ex)
		w := e.work()
		e.observe("checkout:v2", 1, e.clock.now())
		r := e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if r.State != action.StateReady {
			t.Fatalf("propose: %s %q", r.State, r.Reason)
		}
		// Another first-party source reports a different generation now.
		now := e.clock.now()
		if _, _, err := e.store.AssertFact(e.ctx, world.Fact{TenantID: e.tn, SubjectRef: target, Predicate: "generation",
			Value: "7", Source: "argocd", TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
			t.Fatal(err)
		}
		out, err := rt.Execute(e.ctx, e.tn, r.ID)
		if !errors.Is(err, action.ErrPrecondition) || out.State != action.StateRejected || ex.callCount() != 0 {
			t.Fatalf("dispatch with disagreeing sources: %v %s calls=%d", err, out.State, ex.callCount())
		}
		e.clock.add(time.Second)
		e.observe("checkout:v2", 1, e.clock.now())
		r = e.propose(rt, w, "deployment.rollback", "checkout:v1")
		if r.State != action.StateRejected || !strings.Contains(r.Reason, "sources disagree") {
			t.Fatalf("propose with disagreeing sources: %s %q", r.State, r.Reason)
		}
	})
}
