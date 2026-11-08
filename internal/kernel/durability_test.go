package kernel_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

var errTransient = errors.New("connection reset by peer")

// faultStore injects failures into the kernel's store calls for tool steps.
type faultStore struct {
	*localstore.Store
	mu sync.Mutex
	// failCommits: tool-step commits rejected without being applied.
	failCommits int
	// landThenFail: tool-step commits applied, then reported as failed
	// (connection lost during COMMIT).
	landThenFail int
	// claimDelay delays the return of tool-step claims (a slow claim or a
	// paused process), consuming the worker's view of its lease.
	claimDelay time.Duration
	failRenew  bool
	commits    int
}

func (f *faultStore) take(n *int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if *n > 0 {
		*n--
		return true
	}
	return false
}

func (f *faultStore) ClaimStep(ctx context.Context, runID, workerID string, ttl time.Duration) (domain.Lease, error) {
	l, err := f.Store.ClaimStep(ctx, runID, workerID, ttl)
	f.mu.Lock()
	d := f.claimDelay
	f.mu.Unlock()
	if err == nil && l.Kind == domain.StepTool && d > 0 {
		time.Sleep(d)
	}
	return l, err
}

func (f *faultStore) RenewLease(ctx context.Context, l domain.Lease, ttl time.Duration) (domain.DesiredState, error) {
	f.mu.Lock()
	fail := f.failRenew
	f.mu.Unlock()
	if fail {
		return "", errTransient
	}
	return f.Store.RenewLease(ctx, l, ttl)
}

func (f *faultStore) CommitSuccess(ctx context.Context, l domain.Lease, res domain.StepSuccess) error {
	if l.Kind == domain.StepTool {
		f.mu.Lock()
		f.commits++
		f.mu.Unlock()
		if f.take(&f.failCommits) {
			return errTransient
		}
		if f.take(&f.landThenFail) {
			if err := f.Store.CommitSuccess(ctx, l, res); err != nil {
				return err
			}
			return errTransient
		}
	}
	return f.Store.CommitSuccess(ctx, l, res)
}

// plainStore hides the optional EffectRecorder / AttemptInspector methods,
// i.e. a store without the early effect confirmation.
type plainStore struct{ kernel.StateStore }

type durEnv struct {
	*loopEnv
	fs    *faultStore
	calls *int32
}

// newDurEnv: reason → side-effecting tool (R1, no approval) → final answer.
func newDurEnv(t *testing.T, cfg kernel.Config, hideOptional bool) *durEnv {
	t.Helper()
	var calls int32
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("deploy.restart", `{"service":"checkout"}`), nil
		}
		return finalDecision("restarted checkout"), nil
	}}
	tool := fixedTool{name: "deploy.restart", risk: domain.RiskR1, out: `{"restarted":true}`, calls: &calls}
	env := newLoopEnvWith(t, cfg, agent, []tools.Tool{tool}, false)
	fs := &faultStore{Store: env.st}
	var store kernel.StateStore = fs
	if hideOptional {
		store = plainStore{fs}
	}
	if cfg.ID == "" {
		cfg.ID = "w1"
	}
	reg := tools.NewRegistry(tool, kernel.BatchTool{})
	// R1 without approval outside production: these tests exercise the
	// effect ledger, not the approval path.
	pol := policy.Engine{AllowR1WithoutApproval: true, Environment: "staging"}
	env.w = kernel.NewWorker(withRules(cfg, env.st), store, env.arts, agent, reg, pol, nil)
	return &durEnv{loopEnv: env, fs: fs, calls: &calls}
}

// runUntil drives the run, sleeping through retry back-off and lease expiry.
func (e *durEnv) runUntil(t *testing.T, stop func(domain.Run) bool) domain.Run {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := e.w.ExecuteOne(ctx, e.run.ID); err != nil && !errors.Is(err, domain.ErrNoRunnableStep) {
			t.Logf("execute: %v", err)
		}
		r, _ := e.st.GetRun(ctx, "t1", e.run.ID)
		if stop(r) {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	r, _ := e.st.GetRun(ctx, "t1", e.run.ID)
	t.Fatalf("run did not settle: status %s", r.Status)
	return r
}

func settled(r domain.Run) bool { return r.Status.Terminal() || r.Status == domain.RunNeedsHuman }

// A commit that fails transiently after the effect is applied is retried
// within the same lease: no second dispatch, no human.
func TestCommitRetriedAfterTransientFailure(t *testing.T) {
	e := newDurEnv(t, kernel.Config{CommitRetryBudget: 5 * time.Second}, false)
	e.fs.failCommits = 2
	r := e.runUntil(t, settled)
	if r.Status != domain.RunCompleted {
		t.Fatalf("status %s, want COMPLETED", r.Status)
	}
	if *e.calls != 1 {
		t.Fatalf("tool dispatched %d times, want 1", *e.calls)
	}
	if e.fs.commits != 3 {
		t.Fatalf("tool commit tries %d, want 3", e.fs.commits)
	}
}

// A commit that landed but reported an error (lost connection during COMMIT)
// is recognised from the fenced attempt row instead of being treated as lost.
func TestAmbiguousCommitSettledByAttemptRow(t *testing.T) {
	e := newDurEnv(t, kernel.Config{CommitRetryBudget: 5 * time.Second}, false)
	e.fs.landThenFail = 1
	ctx := context.Background()
	for i := 0; i < 2; i++ { // reason step, then the tool step
		if err := e.w.ExecuteOne(ctx, e.run.ID); err != nil {
			t.Fatalf("step %d: %v", i+1, err)
		}
	}
	r := e.runUntil(t, settled)
	if r.Status != domain.RunCompleted || *e.calls != 1 {
		t.Fatalf("status %s, dispatches %d; want COMPLETED, 1", r.Status, *e.calls)
	}
}

// The commit never lands and the lease expires. With the effect confirmed
// ahead of the commit, the retried step reuses the result. Without it (the
// previous behaviour, kept for stores lacking EffectRecorder) the run needs a
// human even though the effect's outcome was known.
func TestLostCommitAfterEffectReusesConfirmedResult(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hide   bool
		status domain.RunStatus
	}{
		{"recorded", false, domain.RunCompleted},
		{"not recorded", true, domain.RunNeedsHuman},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newDurEnv(t, kernel.Config{LeaseTTL: 300 * time.Millisecond, CommitRetryBudget: -1}, tc.hide)
			e.fs.failCommits = 1 << 20 // the database stays unreachable for commits...
			ctx := context.Background()
			if err := e.w.ExecuteOne(ctx, e.run.ID); err != nil {
				t.Fatalf("reason step: %v", err)
			}
			if err := e.w.ExecuteOne(ctx, e.run.ID); err == nil {
				t.Fatal("tool step commit should have failed")
			}
			e.fs.mu.Lock()
			e.fs.failCommits = 0 // ...until after the lease expired
			e.fs.mu.Unlock()
			time.Sleep(400 * time.Millisecond)
			r := e.runUntil(t, settled)
			if r.Status != tc.status {
				t.Fatalf("status %s, want %s", r.Status, tc.status)
			}
			if *e.calls != 1 {
				t.Fatalf("tool dispatched %d times, want exactly 1", *e.calls)
			}
		})
	}
}

// A worker that cannot confirm its lease does not dispatch a side effect;
// the step is retried once the database is reachable again.
func TestNoDispatchWithoutConfirmedLease(t *testing.T) {
	e := newDurEnv(t, kernel.Config{LeaseTTL: 300 * time.Millisecond}, false)
	ctx := context.Background()
	if err := e.w.ExecuteOne(ctx, e.run.ID); err != nil {
		t.Fatalf("reason step: %v", err)
	}
	e.fs.claimDelay = 200 * time.Millisecond // past the usable lease (150ms)
	e.fs.failRenew = true
	_ = e.w.ExecuteOne(ctx, e.run.ID)
	if *e.calls != 0 {
		t.Fatalf("tool dispatched %d times without a confirmed lease", *e.calls)
	}
	e.fs.mu.Lock()
	e.fs.claimDelay, e.fs.failRenew = 0, false
	e.fs.mu.Unlock()
	r := e.runUntil(t, settled)
	if r.Status != domain.RunCompleted || *e.calls != 1 {
		t.Fatalf("status %s, dispatches %d; want COMPLETED, 1", r.Status, *e.calls)
	}
}

func withRules(cfg kernel.Config, st *localstore.Store) kernel.Config {
	cfg.Rules = &profile.RulesFor{Loader: st}
	return cfg
}
