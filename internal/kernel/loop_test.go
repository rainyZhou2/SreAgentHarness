package kernel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

// fixedTool returns a fixed output (or error) and counts its calls.
type fixedTool struct {
	name  string
	risk  domain.RiskClass
	out   string
	err   error
	calls *int32
}

func (f fixedTool) Descriptor() tools.Descriptor {
	r := f.risk
	if r == "" {
		r = domain.RiskR0
	}
	return tools.Descriptor{Name: f.name, Version: "1", Risk: r, Source: "native", InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (f fixedTool) Invoke(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	if f.calls != nil {
		atomic.AddInt32(f.calls, 1)
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.out == "echo" {
		return args, nil
	}
	return json.RawMessage(f.out), nil
}

// scripted runs decide on every step and records the inputs.
type scripted struct {
	inputs []cognitive.ReasoningInput
	decide func(n int, in cognitive.ReasoningInput) (domain.Decision, error)
}

func (s *scripted) Name() string { return "scripted" }
func (s *scripted) Decide(_ context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	s.inputs = append(s.inputs, in)
	return s.decide(len(s.inputs)-1, in)
}

func toolDecision(name, args string, deferred ...domain.ToolCall) domain.Decision {
	return domain.Decision{Kind: domain.DecisionTool, Intent: domain.IntentTask, ReasonSummary: "check",
		Tool: &domain.ToolCall{Name: name, Args: json.RawMessage(args)}, DeferredToolCalls: deferred}
}

func finalDecision(answer string, refs ...string) domain.Decision {
	return domain.Decision{Kind: domain.DecisionFinal, Intent: domain.IntentTask, Answer: answer, ReasonSummary: "done",
		EvidenceRefs: refs}
}

// usageStore reports a fixed run usage.
type usageStore struct {
	*localstore.Store
	usage domain.RunUsage
}

func (u usageStore) RunUsage(context.Context, string, string) (domain.RunUsage, error) {
	return u.usage, nil
}

type loopEnv struct {
	st   *localstore.Store
	arts *artifact.Memory
	run  domain.Run
	w    *kernel.Worker
}

func newLoopEnvWith(t *testing.T, cfg kernel.Config, agent cognitive.Strategy, ts []tools.Tool, loader bool) *loopEnv {
	t.Helper()
	return newLoopEnvFull(t, cfg, agent, "", nil, loader, ts...)
}

func newLoopEnv(t *testing.T, cfg kernel.Config, agent cognitive.Strategy, profileJSON string, usage *domain.RunUsage, ts ...tools.Tool) *loopEnv {
	t.Helper()
	return newLoopEnvFull(t, cfg, agent, profileJSON, usage, false, ts...)
}

func newLoopEnvFull(t *testing.T, cfg kernel.Config, agent cognitive.Strategy, profileJSON string, usage *domain.RunUsage, loader bool, ts ...tools.Tool) *loopEnv {
	t.Helper()
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	arts := artifact.NewMemory()
	reg := tools.NewRegistry(append(ts, kernel.BatchTool{})...)
	if loader {
		reg.Register(kernel.LoadToolsTool{Registry: reg})
	}
	if cfg.ID == "" {
		cfg.ID = "w1"
	}
	cfg.Rules = &profile.RulesFor{Loader: st}
	var store kernel.StateStore = st
	if usage != nil {
		store = usageStore{Store: st, usage: *usage}
	}
	w := kernel.NewWorker(cfg, store, arts, agent, reg, policy.Engine{}, nil)
	if profileJSON == "" {
		profileJSON = `{"strategy":"llm","tools":["*"],"max_steps":40}`
	}
	if _, err := st.RegisterProfile(ctx, "t1", "p", 1, json.RawMessage(profileJSON)); err != nil {
		t.Fatal(err)
	}
	goal, _ := json.Marshal(map[string]string{"goal": "checkout is slow"})
	ref, hash, _ := arts.Put(ctx, "t1", "goal", goal)
	run, _, err := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: "r1", InitiatorType: "user",
		InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash,
			Status: domain.StepPending}}})
	if err != nil {
		t.Fatal(err)
	}
	return &loopEnv{st: st, arts: arts, run: run, w: w}
}

func (e *loopEnv) drive(t *testing.T, n int) domain.Run {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := e.w.ExecuteOne(ctx, e.run.ID); err != nil {
			t.Logf("execute: %v", err)
		}
		if r, _ := e.st.GetRun(ctx, "t1", e.run.ID); r.Status.Terminal() {
			return r
		}
	}
	r, _ := e.st.GetRun(ctx, "t1", e.run.ID)
	return r
}

func (e *loopEnv) result(t *testing.T, r domain.Run) map[string]any {
	t.Helper()
	if r.ResultRef == nil {
		t.Fatalf("run %s has no result", r.Status)
	}
	var res map[string]any
	data, _ := e.arts.Get(context.Background(), "t1", *r.ResultRef)
	_ = json.Unmarshal(data, &res)
	return res
}

func (e *loopEnv) summaries(t *testing.T) []map[string]any {
	t.Helper()
	evs, _ := e.st.ListEvents(context.Background(), "t1", e.run.ID, 0, 1000)
	var out []map[string]any
	for _, ev := range evs {
		if ev.Type != "step.succeeded" {
			continue
		}
		m := map[string]any{}
		_ = json.Unmarshal(ev.Payload, &m)
		out = append(out, m)
	}
	return out
}

func TestParallelReadOnlyBatch(t *testing.T) {
	var a, b, c int32
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("metrics.a", `{"q":1}`,
				domain.ToolCall{Name: "logs.b", Args: json.RawMessage(`{"q":2}`)},
				domain.ToolCall{Name: "deploy.c", Args: json.RawMessage(`{}`)},
				domain.ToolCall{Name: "metrics.a", Args: json.RawMessage(`{"q":1}`)}), nil
		}
		return finalDecision("pool exhausted", "obs:step1.2"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil,
		fixedTool{name: "metrics.a", out: `{"cpu":0.9}`, calls: &a},
		fixedTool{name: "logs.b", out: `{"errors":12}`, calls: &b},
		fixedTool{name: "deploy.c", risk: domain.RiskR2, out: `{}`, calls: &c})
	r := env.drive(t, 10)
	if r.Status != domain.RunCompleted {
		t.Fatalf("status %s: %v", r.Status, env.summaries(t))
	}
	if a != 1 || b != 1 || c != 0 {
		t.Fatalf("calls a=%d b=%d c=%d", a, b, c)
	}
	in := agent.inputs[1]
	if len(in.Observations) != 2 || in.Observations[0].Part != 1 || in.Observations[1].Part != 2 ||
		in.Observations[1].Tool != "logs.b" || !strings.Contains(string(in.Observations[1].Content), "errors") {
		t.Fatalf("observations: %+v", in.Observations)
	}
	for _, d := range in.Tools {
		if d.Name == kernel.BatchToolName {
			t.Fatal("batch tool visible to the model")
		}
	}
	if ref, ok := cognitive.NewRefSet(in).Resolve("obs:step1.2"); !ok || ref != "obs:step1.2" {
		t.Fatalf("part ref: %q %v", ref, ok)
	}
	if ref, ok := cognitive.NewRefSet(in).Resolve("obs:logs.b"); !ok || ref != "obs:step1.2" {
		t.Fatalf("tool ref: %q %v", ref, ok)
	}
	sums := env.summaries(t)
	notes := fmt.Sprint(sums[0]["deferred_not_run"])
	if sums[0]["batch"] != float64(2) || !strings.Contains(notes, "deploy.c: has side effects") ||
		!strings.Contains(notes, "duplicate") {
		t.Fatalf("reason summary: %v", sums[0])
	}
}

func TestBatchDisabledRunsFirstCallOnly(t *testing.T) {
	var a, b int32
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("metrics.a", `{}`, domain.ToolCall{Name: "logs.b", Args: json.RawMessage(`{}`)}), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, `{"strategy":"llm","tools":["*"],"max_steps":40,"run_limits":{"max_parallel_tools":1}}`, nil,
		fixedTool{name: "metrics.a", out: `{}`, calls: &a}, fixedTool{name: "logs.b", out: `{}`, calls: &b})
	if r := env.drive(t, 10); r.Status != domain.RunCompleted || a != 1 || b != 0 {
		t.Fatalf("status %s a=%d b=%d", r.Status, a, b)
	}
}

func TestModelCannotCallBatchTool(t *testing.T) {
	var a int32
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision(kernel.BatchToolName, `{"calls":[{"name":"metrics.a","args":{},"tool_version":"1","risk":"R0"}]}`), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: `{}`, calls: &a})
	env.drive(t, 10)
	if a != 0 {
		t.Fatal("a model-forged batch ran")
	}
}

func TestReadOnlyToolErrorBecomesObservation(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("metrics.a", `{"svc":"nope"}`), nil
		}
		return finalDecision("service not found"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", err: errors.New("unknown service nope")})
	r := env.drive(t, 10)
	if r.Status != domain.RunCompleted || len(agent.inputs) != 2 {
		t.Fatalf("status %s after %d decisions", r.Status, len(agent.inputs))
	}
	o := agent.inputs[1].Observations
	if len(o) != 1 || o[0].Outcome != "error" || !strings.Contains(string(o[0].Content), "unknown service") {
		t.Fatalf("observation: %+v", o)
	}
}

func TestRetryableToolErrorStillRetries(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("metrics.a", `{}`), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", err: domain.Retryable(errors.New("503"), 0)})
	env.drive(t, 3)
	if len(agent.inputs) != 1 {
		t.Fatalf("a transient error was handed to the model: %d decisions", len(agent.inputs))
	}
}

func TestInvalidOutputIsRepaired(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		switch n {
		case 0:
			return domain.Decision{Kind: domain.DecisionFinal, Intent: domain.IntentTask}, nil // no answer
		case 1:
			return toolDecision("metrics.zzz", `{}`), nil // unknown tool
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: `{}`})
	r := env.drive(t, 5)
	if r.Status != domain.RunCompleted || len(agent.inputs) != 3 {
		t.Fatalf("status %s after %d decisions", r.Status, len(agent.inputs))
	}
	if !strings.Contains(agent.inputs[1].Repair, "without answer") {
		t.Fatalf("repair 1: %q", agent.inputs[1].Repair)
	}
	if !strings.Contains(agent.inputs[2].Repair, `"metrics.zzz" does not exist`) || !strings.Contains(agent.inputs[2].Repair, "metrics.a") {
		t.Fatalf("repair 2: %q", agent.inputs[2].Repair)
	}
	if reps, _ := env.summaries(t)[0]["repairs"].([]any); len(reps) != 2 {
		t.Fatalf("summary: %v", env.summaries(t)[0])
	}
}

func TestRepairsAreBounded(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		return domain.Decision{}, domain.Retryable(&cognitive.InvalidOutputError{Err: errors.New("no tool call")}, 0)
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: `{}`})
	env.drive(t, 1)
	// One attempt: the first try and two repairs, then the step retries.
	if len(agent.inputs) != 3 || !strings.Contains(agent.inputs[2].Repair, "unusable response") {
		t.Fatalf("%d decisions, repair %q", len(agent.inputs), agent.inputs[len(agent.inputs)-1].Repair)
	}
}

func TestRepeatedCallIsRefused(t *testing.T) {
	var a int32
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n < 3 {
			return toolDecision("metrics.a", `{"q": 1, "w": "5m"}`), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: "echo", calls: &a})
	r := env.drive(t, 12)
	if r.Status != domain.RunCompleted || a != 2 {
		t.Fatalf("status %s, %d calls", r.Status, a)
	}
	last := agent.inputs[3].Observations
	if len(last) != 3 || last[2].Outcome != "blocked" || !strings.Contains(last[2].Note, "loop guard") ||
		!strings.Contains(last[2].Note, "obs:step1") {
		t.Fatalf("observations: %+v", last)
	}
}

func TestStallForcesConclusion(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		return toolDecision("metrics.a", fmt.Sprintf(`{"page":%d}`, n)), nil
	}}
	env := newLoopEnv(t, kernel.Config{StallSteps: 2}, agent, "", nil, fixedTool{name: "metrics.a", out: `{"same":true}`})
	r := env.drive(t, 30)
	if r.Status != domain.RunCompleted {
		t.Fatalf("status %s", r.Status)
	}
	var advised, forced bool
	for _, in := range agent.inputs {
		if strings.Contains(strings.Join(in.Advisories, " "), "brought nothing new") {
			advised = true
		}
		forced = forced || in.ForceConclude
	}
	res := env.result(t, r)
	if !advised || !forced || !strings.Contains(fmt.Sprint(res["stopped"]), "no new information") {
		t.Fatalf("advised=%v forced=%v result=%v", advised, forced, res)
	}
	// 1 new result, then 4 stalled ones force the stop at the 6th decision.
	if len(agent.inputs) != 6 {
		t.Fatalf("%d decisions", len(agent.inputs))
	}
}

func TestBudgetSoftAndHardStop(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		return finalDecision("done"), nil
	}}
	usage := domain.RunUsage{Requests: 3, InputTokens: 800, OutputTokens: 50}
	env := newLoopEnv(t, kernel.Config{RunMaxTokens: 1000}, agent, "", &usage, fixedTool{name: "metrics.a", out: `{}`})
	if r := env.drive(t, 3); r.Status != domain.RunCompleted {
		t.Fatalf("status %s", r.Status)
	}
	if in := agent.inputs[0]; !in.ForceConclude || !strings.Contains(strings.Join(in.Advisories, " "), "85%") {
		t.Fatalf("soft limit: %+v %v", in.ForceConclude, in.Advisories)
	}

	// Over the profile's (tighter) cost limit: no model call at all.
	agent2 := &scripted{decide: func(int, cognitive.ReasoningInput) (domain.Decision, error) {
		t.Fatal("model called after the budget was exhausted")
		return domain.Decision{}, nil
	}}
	usage2 := domain.RunUsage{Requests: 9, CostMicros: 600_000}
	env2 := newLoopEnv(t, kernel.Config{RunMaxCostUSD: 5}, agent2,
		`{"strategy":"llm","tools":["*"],"max_steps":40,"run_limits":{"max_cost_usd":0.5}}`, &usage2, fixedTool{name: "metrics.a", out: `{}`})
	r := env2.drive(t, 3)
	if r.Status != domain.RunCompleted {
		t.Fatalf("status %s", r.Status)
	}
	res := env2.result(t, r)
	if !strings.Contains(fmt.Sprint(res["stopped"]), "run budget exhausted") || !strings.Contains(fmt.Sprint(res["answer"]), "stopped before a conclusion") {
		t.Fatalf("result: %v", res)
	}
}
