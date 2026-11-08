package kernel_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
)

// failingStrategy makes every flow child fail with a permanent error.
type failingStrategy struct{}

func (failingStrategy) Name() string { return "failing" }
func (failingStrategy) Decide(context.Context, cognitive.ReasoningInput) (domain.Decision, error) {
	return domain.Decision{}, errors.New("upstream API returned 500")
}

// scriptedPlanner returns one flow per call and records its inputs.
type scriptedPlanner struct {
	flows  []string
	inputs []planner.Input
}

func (p *scriptedPlanner) Plan(_ context.Context, _ gw.Scope, in planner.Input) (planner.Result, error) {
	p.inputs = append(p.inputs, in)
	raw := p.flows[0]
	p.flows = p.flows[1:]
	fs, canon, hash, err := flow.Compile([]byte(raw), flow.CompileOptions{AllowTool: func(string) bool { return true }})
	if err != nil {
		return planner.Result{}, err
	}
	gen := 0
	if in.Replan != nil {
		gen = in.Replan.Generation
	}
	a := planner.Analyze(fs, planner.AnalysisOptions{})
	return planner.Result{Mode: "flow", Flow: &fs, Canonical: canon, Hash: hash, Rationale: "test plan",
		Analysis: &a, Generation: gen, PlanHash: planner.PlanHash("flow", hash, nil, gen)}, nil
}

const oneAgentFlow = `{"entry":"probe","nodes":[
	{"id":"probe","type":"agent","role":"prober","tools":["prom.query"],"max_steps":2,"next":[{"to":"done"}]},
	{"id":"done","type":"end","output":"${nodes.probe.output}"}]}`

func TestFailedGeneratedFlowIsReplanned(t *testing.T) {
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	arts := artifact.NewMemory()
	pl := &scriptedPlanner{flows: []string{oneAgentFlow, oneAgentFlow}}
	reg := tools.NewRegistry()
	w := kernel.NewWorker(kernel.Config{ID: "w1", Planner: pl, PlanApprovalMode: "never"}, st, arts, failingStrategy{}, reg,
		policy.Engine{}, nil)

	goal, _ := json.Marshal(map[string]string{"goal": "check checkout"})
	ref, hash, err := arts.Put(ctx, "t1", "goal", goal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterProfile(ctx, "t1", "auto", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":10}`)); err != nil {
		t.Fatal(err)
	}
	run, _, err := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "auto", ProfileVersion: 1,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: "r1", InitiatorType: "user",
		InitialSteps: []domain.PlannedStep{{Kind: domain.StepPlan, Executor: "planner", InputRef: ref,
			InputHash: hash, Status: domain.StepPending}}})
	if err != nil {
		t.Fatal(err)
	}

	step := func(runID string) {
		t.Helper()
		if err := w.ExecuteOne(ctx, runID); err != nil {
			t.Logf("execute %s: %v", runID, err)
		}
	}
	child := func(gen int) domain.Run {
		t.Helper()
		snap, err := st.LoadFlow(ctx, "t1", run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Generation != gen || len(snap.Nodes) != 1 {
			t.Fatalf("generation %d snapshot: %+v", gen, snap)
		}
		c, err := st.GetRun(ctx, "t1", snap.Nodes[0].ChildRunID)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	step(run.ID) // plan -> flow step
	step(run.ID) // flow -> child run for "probe"
	first := child(0)
	for i := 0; i < 5 && !first.Status.Terminal(); i++ {
		step(first.ID)
		first, _ = st.GetRun(ctx, "t1", first.ID)
	}
	if first.Status != domain.RunFailed {
		t.Fatalf("child should fail, got %s", first.Status)
	}

	step(run.ID) // flow sees the failed node -> replan step instead of failing the run
	parent, _ := st.GetRun(ctx, "t1", run.ID)
	if parent.Status.Terminal() {
		t.Fatalf("run must survive the first flow failure, status %s", parent.Status)
	}
	snap, _ := st.LoadFlow(ctx, "t1", run.ID)
	if snap.Status != "FAILED" || snap.Generation != 0 {
		t.Fatalf("failed generation not recorded: %+v", snap)
	}

	step(run.ID) // replan step -> planner sees the failure
	if len(pl.inputs) != 2 || pl.inputs[1].Replan == nil {
		t.Fatalf("planner was not asked to replan: %+v", pl.inputs)
	}
	rp := pl.inputs[1].Replan
	if rp.Generation != 1 || rp.Failure == "" || len(rp.Nodes) != 1 || rp.Nodes[0].ID != "probe" ||
		rp.Nodes[0].Status != string(flow.NodeFailed) || rp.PreviousPlanHash == "" {
		t.Fatalf("replan context: %+v", rp)
	}

	step(run.ID) // generation 1 flow -> a new child for the same node id
	second := child(1)
	if second.ID == first.ID {
		t.Fatal("generation 1 reused the generation 0 child run")
	}

	for i := 0; i < 5 && !second.Status.Terminal(); i++ {
		step(second.ID)
		second, _ = st.GetRun(ctx, "t1", second.ID)
	}
	step(run.ID) // second failure: budget (1 replan) used up -> run fails
	parent, _ = st.GetRun(ctx, "t1", run.ID)
	if parent.Status != domain.RunFailed {
		t.Fatalf("run must fail once the replan budget is used, status %s", parent.Status)
	}
	if len(pl.inputs) != 2 {
		t.Fatalf("planner called %d times", len(pl.inputs))
	}

	events, err := st.ListEvents(ctx, "t1", run.ID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, e := range events {
		seen[e.Type]++
	}
	if seen["plan.replan_requested"] != 1 || seen["plan.replanned"] != 1 {
		t.Fatalf("events: %v", seen)
	}
}
