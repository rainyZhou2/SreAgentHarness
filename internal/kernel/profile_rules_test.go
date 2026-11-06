package kernel_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/profile"
)

func (h *harness) createWith(t *testing.T, profileID, spec, goal string) domain.Run {
	t.Helper()
	ctx := context.Background()
	if err := h.store.EnsureProfile(ctx, tenant, profileID, 1, json.RawMessage(spec)); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]string{"goal": goal})
	ref, hash, _ := h.arts.Put(ctx, tenant, "goal", data)
	run, _, err := h.store.CreateRun(ctx, domain.CreateRunRequest{TenantID: tenant, ProfileID: profileID,
		ProfileVersion: 1, GoalRef: ref, GoalHash: hash, IdempotencyKey: goal + time.Now().String(), InitiatorID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func withRules(h *harness, c *kernel.Config) { c.Rules = &profile.RulesFor{Loader: h.store} }

func restartCall() *domain.ToolCall {
	return &domain.ToolCall{Name: "demo.restart", Args: json.RawMessage(`{"service":"checkout"}`)}
}

// The profile's tool allowlist limits what the model sees and what can be
// planned; a disallowed call is skipped, never executed or sent for approval.
func TestProfileToolAllowlistIsEnforced(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = seen[:0]
		for _, d := range in.Tools {
			seen = append(seen, d.Name)
		}
		if len(in.Observations) == 0 {
			return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "restart", Tool: restartCall()}, nil
		}
		return domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", Answer: "ok"}, nil
	}}
	h := start(t, opts{strategy: strat, workerFn: withRules})
	run := h.createWith(t, "readonly", `{"tools":["demo.metrics"]}`, "restart checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)
	mu.Lock()
	if strings.Join(seen, ",") != "demo.metrics" {
		t.Fatalf("model saw tools %v", seen)
	}
	mu.Unlock()
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	for _, s := range steps {
		if s.Kind == domain.StepTool {
			if s.Status != domain.StepSkipped || s.StatusReason == nil || !strings.Contains(*s.StatusReason, "not allowed by profile readonly@1") {
				t.Fatalf("tool step = %s %v", s.Status, s.StatusReason)
			}
		}
	}
}

// Risk overrides and approval rules tighten the policy for this profile.
func TestProfileRiskOverrideAndApprovalRules(t *testing.T) {
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "look", Tool: metricsCall("checkout")}, nil
	}}
	h := start(t, opts{strategy: strat, workerFn: withRules})
	run := h.createWith(t, "strict", `{"tools":["demo.*"],"risk_overrides":{"demo.metrics":"R1"},
		"approval_rules":{"min_approvals":{"R1":2}}}`, "check checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	var required int
	var risk string
	err := h.store.Pool().QueryRow(context.Background(), `SELECT ar.required_approvals, ai.risk_class
		FROM approval_requests ar JOIN action_intents ai ON ai.id = ar.action_intent_id
		WHERE ai.run_id=$1`, run.ID).Scan(&required, &risk)
	if err != nil || required != 2 || risk != "R1" {
		t.Fatalf("approval request: required=%d risk=%s err=%v", required, risk, err)
	}
}

// max_steps in the profile lowers the global step budget.
func TestProfileMaxSteps(t *testing.T) {
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "again", Tool: metricsCall("checkout")}, nil
	}}
	h := start(t, opts{strategy: strat, workerFn: withRules, worker: kernel.Config{CompactEvery: -1}})
	run := h.createWith(t, "short", `{"tools":["demo.metrics"],"max_steps":4}`, "loop")
	r := h.waitStatus(t, run.ID, domain.RunFailed)
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	if len(steps) > 4 {
		t.Fatalf("run %s has %d steps, profile allows 4", r.ID, len(steps))
	}
}
