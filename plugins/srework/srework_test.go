package srework

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

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

// runs completes every run immediately with an RCA that proposes a rollback.
type runs struct {
	mu    sync.Mutex
	goals []string
}

func (r *runs) StartRun(_ context.Context, req orchestration.RunRequest) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.goals = append(r.goals, req.Goal)
	return cpkit.NewID(), nil
}

func (r *runs) RunState(context.Context, string, string) (orchestration.RunState, error) {
	b, _ := json.Marshal(map[string]any{
		"answer": "checkout:v2 raised the error rate; roll back to checkout:v1.",
		"hypotheses": []map[string]any{{"statement": "deploy of checkout:v2 caused the 5xx spike", "status": "VERIFIED",
			"supports": []string{"obs:metrics"}}},
		"proposed_action": map[string]any{"action": "deployment.rollback", "target": "deployment://prod/checkout",
			"args": map[string]any{"to_image": "checkout:v1"}},
	})
	return orchestration.RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: b}, nil
}

func (r *runs) CancelRun(context.Context, string, string, string, string, string) error { return nil }

// The MVP loop of the RFC on the simulator: alert → incident Work →
// investigation → proposed rollback (R2) → approval bound to the intent
// hash → dispatch (ledger first) → effect verified by metrics → Work
// SUCCEEDED with a SATISFIED outcome.
func TestIncidentLoopOnSimulator(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	store.Clock = c.now
	sim, err := New(Config{Tenant: "local", Deployments: []DeploymentConfig{{Ref: "deployment://prod/checkout",
		Image: "checkout:v2", GoodImage: "checkout:v1", BadImages: []string{"checkout:v2"}}}})
	if err != nil {
		t.Fatal(err)
	}
	sim.SetClock(c.now)
	ws := &world.Service{Store: store, Clock: c.now}
	ev := &outcome.Evaluator{Metrics: outcome.MetricSourceMap(sim.MetricSources()), States: orchestration.WorldFacts{World: ws},
		Events: store}
	reg := action.NewRegistry()
	for _, x := range sim.Actions() {
		if err := reg.Register(x); err != nil {
			t.Fatal(err)
		}
	}
	rt := &action.Runtime{Store: store, Registry: reg, World: ws, Outcomes: ev, Clock: c.now, Log: quiet,
		Policy: policy.Default(), RequireKnownTarget: false}
	tpl := work.Template{ID: "incident-mitigate", Version: 1, Kind: work.KindIncident, Stages: []work.Stage{
		{ID: "investigate", Type: work.StageRun, Phase: "investigate", Run: &work.RunSpec{Goal: "Investigate ${title} on ${subjects}"}},
		{ID: "mitigate", Type: work.StageAction, Phase: "mitigate", Action: &work.ActionSpec{FromProposal: "investigate",
			Allowed: []string{"deployment.rollback", "traffic.shift"}}},
		{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: "investigate", Verify: &work.VerifySpec{
			RecheckEvery: cpkit.Duration(30 * time.Second), Timeout: cpkit.Duration(15 * time.Minute),
			Spec: &outcome.Spec{StableFor: cpkit.Duration(time.Minute), AllOf: []outcome.Objective{{ID: "errors", Kind: outcome.KindMetric,
				Metric: &outcome.MetricExpr{Source: "sim", Query: "error_rate", Aggregate: "max", Operator: "<", Threshold: 0.01},
				Window: cpkit.Duration(2 * time.Minute), MinSamples: 3}}}}},
	}}
	cat, err := orchestration.NewCatalog([]work.Template{tpl})
	if err != nil {
		t.Fatal(err)
	}
	router, err := eventing.NewRouter([]eventing.Rule{{Name: "high-errors", Match: eventing.Predicate{EventTypes: []string{"alert.firing"}},
		Key: "dedup_key", Template: "incident-mitigate", WorkKind: "incident", Title: "${label.alertname} on ${subject}"}})
	if err != nil {
		t.Fatal(err)
	}
	ing := &orchestration.Ingestor{Store: store, Router: router, Catalog: cat, Clock: c.now, Log: quiet}
	kr := &runs{}
	orch := &orchestration.Orchestrator{Store: store, Runs: kr, Actions: rt, Evaluator: ev, Clock: c.now, Log: quiet, Owner: "o"}

	env := plugin.WorkEnv{AssertFact: func(ctx context.Context, f world.Fact) error { _, _, err := store.AssertFact(ctx, f); return err },
		Ingest: func(ctx context.Context, e eventing.Envelope) error { _, _, err := ing.Ingest(ctx, e); return err }}
	sim.world = env.AssertFact
	sim.publish(ctx)
	sim.alert(ctx, env.Ingest)
	sim.alert(ctx, env.Ingest) // the same episode: attaches, no second Work

	works, _ := store.ListWorks(ctx, work.Filter{TenantID: "local"})
	if len(works) != 1 {
		t.Fatalf("works: %d", len(works))
	}
	id := works[0].ID
	tick := func(n int) work.Work {
		var w work.Work
		for i := 0; i < n; i++ {
			if _, err := orch.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			w, _ = store.GetWork(ctx, "local", id)
			if w.Status.Terminal() {
				return w
			}
			c.add(15 * time.Second)
			sim.publish(ctx)
		}
		return w
	}
	w := tick(10)
	if w.Status != work.StatusWaiting {
		t.Fatalf("want WAITING for approval, got %s (%s)", w.Status, w.Reason)
	}
	acts, _ := store.ListActions(ctx, "local", id)
	if len(acts) != 1 || acts[0].State != action.StateWaitingApproval || acts[0].TargetVersion != "1" {
		t.Fatalf("action: %+v", acts)
	}
	a := acts[0]
	if _, err := rt.Decide(ctx, "local", a.ID, "oncall-bob", "approve", a.IntentHash, "looks right", ""); err != nil {
		t.Fatal(err)
	}
	if err := store.WakeWork(ctx, "local", id, "approval", "user", "oncall-bob", nil); err != nil {
		t.Fatal(err)
	}
	w = tick(60)
	if w.Status != work.StatusSucceeded {
		evs, _ := store.ListWorkEvents(ctx, "local", id, 0, 500)
		var types []string
		for _, e := range evs {
			types = append(types, e.Type)
		}
		t.Fatalf("status %s (%s) events=%v", w.Status, w.Reason, types)
	}
	if got := sim.deps["deployment://prod/checkout"].Image; got != "checkout:v1" {
		t.Fatalf("image: %s", got)
	}
	outs, _ := store.ListOutcomes(ctx, "local", id, 5)
	if len(outs) == 0 || outs[0].Verdict != outcome.Satisfied {
		t.Fatalf("outcome: %+v", outs)
	}
	fs, _ := store.FactsAt(ctx, "local", []string{"deployment://prod/checkout"}, []string{"image"}, c.now(), c.now())
	if len(fs) != 1 || fs[0].Value != "checkout:v1" {
		t.Fatalf("world: %+v", fs)
	}
	// History: as known before the rollback, the image was v2.
	fs, _ = store.FactsAt(ctx, "local", []string{"deployment://prod/checkout"}, []string{"image"}, w.CreatedAt, w.CreatedAt)
	if len(fs) != 1 || fs[0].Value != "checkout:v2" {
		t.Fatalf("world as of creation: %+v", fs)
	}
	// Executing the same plan again is answered from the effect key.
	again, err := rt.Execute(ctx, "local", a.ID)
	if err == nil && again.State == action.StateDispatched {
		t.Fatal("a settled action must not be dispatched twice")
	}
}

// fakeTopo records facts and relations the sim publishes.
type fakeTopo struct {
	mu    sync.Mutex
	facts map[string]string // subject|predicate -> value
	rels  map[string]bool   // from|type|to -> live
}

func (f *fakeTopo) env() plugin.WorkEnv {
	f.facts, f.rels = map[string]string{}, map[string]bool{}
	return plugin.WorkEnv{
		AssertFact: func(_ context.Context, x world.Fact) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.facts[x.SubjectRef+"|"+x.Predicate] = x.Value
			return nil
		},
		AssertRelation: func(_ context.Context, r world.Relation) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.rels[r.FromRef+"|"+r.Type+"|"+r.ToRef] = true
			return nil
		},
		RetractRelation: func(_ context.Context, _, from, typ, to, _ string, _ time.Time) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.rels[from+"|"+typ+"|"+to] = false
			return nil
		},
	}
}

func (f *fakeTopo) live(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k, v := range f.rels {
		if v && len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out
}

func TestTopologyAndPersistentState(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)}
	state := filepath.Join(t.TempDir(), "sim.json")
	cfg := Config{Tenant: "local", StateFile: state, Deployments: []DeploymentConfig{{Ref: "deployment://prod/checkout",
		Image: "checkout:v2", GoodImage: "checkout:v1", BadImages: []string{"checkout:v2"}, Replicas: 2}}}
	sim, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sim.SetClock(c.now)
	topo := &fakeTopo{}
	env := topo.env()
	sim.world, sim.env = env.AssertFact, env
	sim.publish(ctx)

	if got := topo.live("service://prod/checkout|deployed_as|deployment://prod/checkout"); len(got) != 1 {
		t.Fatalf("deployed_as: %v", got)
	}
	before := topo.live("deployment://prod/checkout|owns|")
	if len(before) != 2 {
		t.Fatalf("owns: %v", before)
	}
	if got := topo.live("pod://"); len(got) != 2 {
		t.Fatalf("runs_on: %v", got)
	}

	var rb action.Executor
	for _, x := range sim.Actions() {
		if x.Descriptor().Name == "deployment.rollback" {
			rb = x
		}
	}
	if rb == nil {
		t.Fatal("no rollback action")
	}
	plan := action.Plan{TenantID: "local", TargetRef: "deployment://prod/checkout", TargetVersion: "1", Args: json.RawMessage(`{}`), EffectKey: "key-1"}
	if _, err := rb.Execute(ctx, plan, "key-1"); err != nil {
		t.Fatal(err)
	}
	sim.publish(ctx)
	after := topo.live("deployment://prod/checkout|owns|")
	if len(after) != 2 || after[0] == before[0] || after[0] == before[1] {
		t.Fatalf("rollout did not replace pods: before=%v after=%v", before, after)
	}
	oldPod := before[0][len("deployment://prod/checkout|owns|"):]
	if topo.facts[oldPod+"|phase"] != "Terminated" {
		t.Fatalf("old pod phase: %q", topo.facts[oldPod+"|phase"])
	}
	if topo.facts["deployment://prod/checkout|changes_applied"] != "1" {
		t.Fatalf("changes: %q", topo.facts["deployment://prod/checkout|changes_applied"])
	}

	// A restarted harness sees the downstream as it really is, and a retry
	// of the same effect key is answered without a second change.
	sim2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sim2.SetClock(c.now)
	d := sim2.deps["deployment://prod/checkout"]
	if d.Image != "checkout:v1" || d.generation != 2 || d.changes != 1 {
		t.Fatalf("restored: image=%s gen=%d changes=%d", d.Image, d.generation, d.changes)
	}
	var rb2 action.Executor
	for _, x := range sim2.Actions() {
		if x.Descriptor().Name == "deployment.rollback" {
			rb2 = x
		}
	}
	res, err := rb2.Execute(ctx, plan, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	_ = json.Unmarshal(res, &out)
	if out["to_image"] != "checkout:v1" || sim2.deps["deployment://prod/checkout"].changes != 1 {
		t.Fatalf("retry was not deduplicated: %s changes=%d", res, sim2.deps["deployment://prod/checkout"].changes)
	}
	ins, err := rb2.Inspect(ctx, plan)
	if err != nil || ins.Applied == nil || !*ins.Applied {
		t.Fatalf("inspect after restart: %+v %v", ins, err)
	}
}
