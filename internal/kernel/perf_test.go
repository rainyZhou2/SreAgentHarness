package kernel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/tools"
)

// streamingAgent reports its tool call through OnToolCall (as the model
// stream does) and waits a little before returning, so a speculative call
// can finish first.
type streamingAgent struct{ scripted }

func (s *streamingAgent) Decide(ctx context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	d, err := s.scripted.Decide(ctx, in)
	if d.Kind == domain.DecisionTool && in.OnToolCall != nil {
		in.OnToolCall(d.Tool.Name, d.Tool.Args)
		time.Sleep(20 * time.Millisecond)
	}
	return d, err
}

func TestSpeculativeReadOnlyCallIsReused(t *testing.T) {
	var a, w int32
	agent := &streamingAgent{scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		switch n {
		case 0:
			return toolDecision("metrics.a", `{"q":1}`), nil
		case 1:
			return toolDecision("deploy.w", `{}`), nil // side effect: never speculated
		}
		return finalDecision("done"), nil
	}}}
	env := newLoopEnv(t, kernel.Config{SpeculativeTools: true}, agent,
		"", nil,
		fixedTool{name: "metrics.a", out: `{"cpu":1}`, calls: &a}, fixedTool{name: "deploy.w", risk: domain.RiskR2, out: `{}`, calls: &w})
	env.drive(t, 10)
	if a != 1 {
		t.Fatalf("read-only tool called %d times (speculation not reused)", a)
	}
	if !strings.Contains(string(agent.inputs[1].Observations[0].Content), "cpu") {
		t.Fatalf("observation: %+v", agent.inputs[1].Observations)
	}
}

// manyTools returns n read-only tools with distinct descriptions.
func manyTools(n int) []tools.Tool {
	var out []tools.Tool
	for i := 0; i < n; i++ {
		out = append(out, describedTool{fixedTool: fixedTool{name: fmt.Sprintf("misc.tool%02d", i), out: `{}`},
			desc: fmt.Sprintf("Generic probe number %d.", i)})
	}
	return out
}

type describedTool struct {
	fixedTool
	desc string
}

func (d describedTool) Descriptor() tools.Descriptor {
	x := d.fixedTool.Descriptor()
	x.Description = d.desc
	return x
}

func TestToolSelectionAndLoading(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		switch n {
		case 0:
			return toolDecision(kernel.LoadToolsName, `{"names":["misc.tool07"]}`), nil
		case 1:
			return toolDecision("misc.tool07", `{}`), nil
		}
		return finalDecision("done"), nil
	}}
	ts := append(manyTools(40),
		describedTool{fixedTool: fixedTool{name: "kafka.consumer_lag", out: `{}`}, desc: "Consumer lag per kafka partition."})
	env := newLoopEnvWith(t, kernel.Config{ToolTopK: 5}, agent, ts, true)
	if r := env.drive(t, 10); r.Status != domain.RunCompleted {
		t.Fatalf("status %s", r.Status)
	}
	first := agent.inputs[0]
	names := map[string]bool{}
	for _, d := range first.Tools {
		names[d.Name] = true
	}
	if !names["kafka.consumer_lag"] || !names[kernel.LoadToolsName] || len(first.MoreTools) == 0 || len(first.Tools) > 6 {
		t.Fatalf("shown %v, more %d", names, len(first.MoreTools))
	}
	if names["misc.tool07"] {
		t.Fatal("unrelated tool shown")
	}
	second := map[string]bool{}
	for _, d := range agent.inputs[1].Tools {
		second[d.Name] = true
	}
	if !second["misc.tool07"] {
		t.Fatal("loaded tool not shown on the next step")
	}
	// Stable: the third step shows the same set as the second.
	third := map[string]bool{}
	for _, d := range agent.inputs[2].Tools {
		third[d.Name] = true
	}
	if fmt.Sprint(second) != fmt.Sprint(third) {
		t.Fatalf("selection changed between steps:\n%v\n%v", second, third)
	}
}

func TestFewToolsHideLoader(t *testing.T) {
	agent := &scripted{decide: func(int, cognitive.ReasoningInput) (domain.Decision, error) { return finalDecision("done"), nil }}
	env := newLoopEnvWith(t, kernel.Config{}, agent, []tools.Tool{fixedTool{name: "metrics.a", out: `{}`}}, true)
	env.drive(t, 3)
	for _, d := range agent.inputs[0].Tools {
		if d.Name == kernel.LoadToolsName {
			t.Fatal("load_tools shown without hidden tools")
		}
	}
}

func TestConvergenceAdvisory(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			d := toolDecision("metrics.a", `{}`)
			d.Hypotheses = []domain.Hypothesis{{ID: "h1", Statement: "pool exhausted", Status: "VERIFIED", ConfidenceEstimate: 0.9},
				{ID: "h2", Statement: "bad deploy", Status: "REFUTED", ConfidenceEstimate: 0.05}}
			return d, nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{}, agent, "", nil, fixedTool{name: "metrics.a", out: `{}`})
	env.drive(t, 5)
	if !strings.Contains(strings.Join(agent.inputs[1].Advisories, " "), "Hypothesis h1 is VERIFIED") {
		t.Fatalf("advisories: %v", agent.inputs[1].Advisories)
	}
	if agent.inputs[1].ForceConclude {
		t.Fatal("convergence must only advise")
	}
}

func TestTokenBudgetCompaction(t *testing.T) {
	big := `{"rows":"` + strings.Repeat("x", 30000) + `"}`
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n < 2 {
			return toolDecision("metrics.a", fmt.Sprintf(`{"page":%d}`, n)), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnv(t, kernel.Config{CompactTokens: 15000, OutputOffloadBytes: -1}, agent, "", nil, fixedTool{name: "metrics.a", out: big})
	env.drive(t, 12)
	steps, _ := env.st.History(context.Background(), "t1", env.run.ID, 1<<62)
	compacted := false
	for _, s := range steps {
		if s.Kind == domain.StepCompact {
			compacted = true
		}
	}
	if !compacted {
		t.Fatal("no compaction although the results exceed the token budget")
	}
	if in := agent.inputs[2]; in.Summary == "" || !strings.Contains(in.Summary, "obs:step") {
		t.Fatalf("summary: %q", in.Summary)
	}
}

func TestStepTimingsInSummary(t *testing.T) {
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("metrics.a", `{}`), nil
		}
		return finalDecision("done"), nil
	}}
	obs := &phaseObs{}
	env := newLoopEnv(t, kernel.Config{Observer: obs}, agent, "", nil, fixedTool{name: "metrics.a", out: `{}`})
	r := env.drive(t, 5)
	sums := env.summaries(t)
	tm, _ := sums[0]["timings_ms"].(map[string]any)
	if _, ok := tm["model"]; !ok {
		t.Fatalf("reason timings: %v", sums[0])
	}
	if tm, _ := sums[1]["timings_ms"].(map[string]any); tm["tool"] == nil {
		t.Fatalf("tool timings: %v", sums[1])
	}
	if obs.phases.Load() == 0 || obs.cacheHits.Load() == 0 {
		t.Fatalf("phases %d, cache hits %d", obs.phases.Load(), obs.cacheHits.Load())
	}
	loop, _ := env.result(t, r)["loop"].(map[string]any)
	if loop["steps"] == nil {
		t.Fatalf("result loop stats: %v", env.result(t, r))
	}
}

type phaseObs struct {
	phases, cacheHits atomic.Int32
}

func (*phaseObs) ObserveStep(string, string, time.Duration) {}
func (*phaseObs) ObserveClaim(string)                       {}
func (*phaseObs) ObserveTool(string, string, string)        {}
func (p *phaseObs) ObservePhase(string, string, time.Duration) {
	p.phases.Add(1)
}
func (p *phaseObs) ObserveLoop(e string) {
	if e == "artifact_cache_hit" {
		p.cacheHits.Add(1)
	}
}

var _ = json.Valid
