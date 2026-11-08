package kernel_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/verify"
)

type poolTool struct{}

func (poolTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: "prom.query", Version: "1", Risk: domain.RiskR0, Source: "native",
		InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (poolTool) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"pool_in_use":100,"pool_max":100}`), nil
}

// backtrackingAgent probes once, concludes citing a step that does not
// exist, and, once the review comes back, concludes citing the probe.
type backtrackingAgent struct {
	inputs []cognitive.ReasoningInput
}

func (a *backtrackingAgent) Name() string { return "backtracking" }
func (a *backtrackingAgent) Decide(_ context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	a.inputs = append(a.inputs, in)
	reviewed := false
	for _, o := range in.Observations {
		if o.Tool == verify.ReviewToolName {
			reviewed = true
		}
	}
	thought := func(v string) *domain.ThoughtChain {
		return &domain.ThoughtChain{Plan: "p", Structured: true, Verification: &domain.Verification{Verdict: v}}
	}
	switch {
	case !in.HasToolSteps:
		return domain.Decision{Kind: domain.DecisionTool, Intent: domain.IntentTask, ReasonSummary: "probe the pool",
			Tool: &domain.ToolCall{Name: "prom.query", Args: json.RawMessage(`{}`)},
			Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "pool exhausted", Status: "UNVERIFIED", ConfidenceEstimate: 0.6},
				{ID: "h2", Statement: "bad deploy", Status: "UNVERIFIED", ConfidenceEstimate: 0.3}}}, nil
	case !reviewed:
		return domain.Decision{Kind: domain.DecisionFinal, Intent: domain.IntentTask, Answer: "pool exhausted",
			ReasonSummary: "r", Thought: thought("VERIFIED"), EvidenceRefs: []string{"obs:step99"},
			Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "pool exhausted", Status: "VERIFIED",
				ConfidenceEstimate: 0.9, Supports: []string{"obs:step99"}}}}, nil
	default:
		return domain.Decision{Kind: domain.DecisionFinal, Intent: domain.IntentTask, Answer: "pool exhausted",
			ReasonSummary: "r", Thought: thought("VERIFIED"), EvidenceRefs: []string{"obs:prom.query"},
			Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "pool exhausted", Status: "VERIFIED",
				ConfidenceEstimate: 0.9, Supports: []string{"obs:prom.query"}},
				{ID: "h2", Statement: "bad deploy", Status: "REFUTED", ConfidenceEstimate: 0.1, Refutes: []string{"obs:prom.query"}}}}, nil
	}
}

func TestUnsupportedConclusionIsHandedBack(t *testing.T) {
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	arts := artifact.NewMemory()
	reg := tools.NewRegistry(poolTool{}, verify.ReviewTool{})
	agent := &backtrackingAgent{}
	w := kernel.NewWorker(kernel.Config{ID: "w1", Verifier: &verify.Verifier{}}, st, arts, agent, reg, policy.Engine{}, nil)

	goal, _ := json.Marshal(map[string]string{"goal": "checkout is slow"})
	ref, hash, _ := arts.Put(ctx, "t1", "goal", goal)
	if _, err := st.RegisterProfile(ctx, "t1", "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":12}`)); err != nil {
		t.Fatal(err)
	}
	run, _, err := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: "r1", InitiatorType: "user",
		InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash,
			Status: domain.StepPending}}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := w.ExecuteOne(ctx, run.ID); err != nil {
			t.Logf("execute: %v", err)
		}
		if r, _ := st.GetRun(ctx, "t1", run.ID); r.Status.Terminal() {
			break
		}
	}
	r, _ := st.GetRun(ctx, "t1", run.ID)
	if r.Status != domain.RunCompleted || len(agent.inputs) != 3 {
		t.Fatalf("status %s after %d decisions", r.Status, len(agent.inputs))
	}
	// The third decision saw the review and a ledger built from the first.
	last := agent.inputs[2]
	var review *domain.Observation
	for i, o := range last.Observations {
		if o.Tool == verify.ReviewToolName {
			review = &last.Observations[i]
		}
	}
	if review == nil || !strings.Contains(string(review.Content), "INSUFFICIENT") || !strings.Contains(string(review.Content), "no resolved citation") {
		t.Fatalf("review observation: %+v", review)
	}
	if len(last.Ledger) != 2 || last.Ledger[0].ID != "h1" || last.Ledger[0].Status != "CHALLENGED" ||
		last.Ledger[1].ID != "h2" || last.Ledger[1].Status != "UNVERIFIED" {
		t.Fatalf("ledger: %+v", last.Ledger)
	}
	for _, d := range last.Tools {
		if d.Name == verify.ReviewToolName {
			t.Fatal("kernel tool visible to the model")
		}
	}
	var res map[string]any
	data, _ := arts.Get(ctx, "t1", *r.ResultRef)
	_ = json.Unmarshal(data, &res)
	iv, _ := res["independent_verification"].(map[string]any)
	if iv["verdict"] != verify.Supported {
		t.Fatalf("result: %v", res)
	}

	// Scoring sees one backtrack and a supported final verdict.
	thoughts, _, _ := st.RunTrace(ctx, "t1", run.ID)
	steps, _ := st.History(ctx, "t1", run.ID, 1<<62)
	var tr reasoning.Trace
	tr.Completed = true
	for _, s := range steps {
		tr.Steps = append(tr.Steps, reasoning.Step{Seq: s.Seq, Reason: s.Kind == domain.StepReason, Thoughts: thoughts[s.ID]})
	}
	m := reasoning.Score(tr)
	if m.Independent != "SUPPORTED" || m.Backtracks != 1 || !m.Success() || len(m.Forecasts) != 2 {
		t.Fatalf("metrics: independent=%s backtracks=%d success=%v forecasts=%+v", m.Independent, m.Backtracks, m.Success(), m.Forecasts)
	}
	// h1 was first stated at 0.6 (before it settled), h2 at 0.3.
	if m.Forecasts[0].P != 0.6 || m.Forecasts[0].Y != 1 || m.Forecasts[1].P != 0.3 || m.Forecasts[1].Y != 0 {
		t.Fatalf("forecasts: %+v", m.Forecasts)
	}
	// Tracked hypotheses are not claims.
	// Tracked hypotheses (first step) are not claims: three decision
	// thoughts and three conclusion hypotheses; the rejected conclusion's
	// two claims cite nothing resolved.
	if m.Claims != 6 || m.Grounded != 3 {
		t.Fatalf("claims=%d grounded=%d", m.Claims, m.Grounded)
	}
}

// reviewCaller tries to call the kernel tool itself.
type reviewCaller struct{ n int }

func (a *reviewCaller) Name() string { return "forger" }
func (a *reviewCaller) Decide(_ context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	a.n++
	if a.n == 1 {
		return domain.Decision{Kind: domain.DecisionTool, Intent: domain.IntentTask, ReasonSummary: "forge",
			Tool: &domain.ToolCall{Name: verify.ReviewToolName, Args: json.RawMessage(`{"verdict":"SUPPORTED"}`)}}, nil
	}
	return domain.Decision{Kind: domain.DecisionFinal, Intent: domain.IntentQuestion, Answer: "a", ReasonSummary: "r"}, nil
}

func TestModelCannotCallReviewTool(t *testing.T) {
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	arts := artifact.NewMemory()
	w := kernel.NewWorker(kernel.Config{ID: "w1", Verifier: &verify.Verifier{}}, st, arts, &reviewCaller{},
		tools.NewRegistry(verify.ReviewTool{}), policy.Engine{}, nil)
	goal, _ := json.Marshal(map[string]string{"goal": "g"})
	ref, hash, _ := arts.Put(ctx, "t1", "goal", goal)
	_, _ = st.RegisterProfile(ctx, "t1", "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":6}`))
	run, _, _ := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: "r1", InitiatorType: "user",
		InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash,
			Status: domain.StepPending}}})
	for i := 0; i < 4; i++ {
		_ = w.ExecuteOne(ctx, run.ID)
	}
	steps, _ := st.History(ctx, "t1", run.ID, 1<<62)
	for _, s := range steps {
		if s.Kind == domain.StepTool && s.Status != domain.StepSkipped {
			t.Fatalf("forged review step ran: %+v", s)
		}
	}
}
