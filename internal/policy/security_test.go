package policy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

func TestUnifiedEvaluatorPreservesLegacyFloor(t *testing.T) {
	evaluator := UnifiedEvaluator{Gate: RuleEngine{}}
	request := security.Request{Operation: security.ToolExecute, TenantID: "t", Subject: "run:r", Risk: "R3", ResourceRef: "api", InputHash: "sha256:" + strings.Repeat("a", 64), OccurredAt: time.Now(), Attributes: map[string]string{"tool_name": "svc.delete", "tool_version": "1"}, Input: json.RawMessage(`{"service":"api"}`)}
	decision, err := evaluator.Evaluate(context.Background(), request)
	if err != nil || decision.Effect != security.Deny {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	request.Risk = string(domain.RiskR2)
	decision, err = evaluator.Evaluate(context.Background(), request)
	if err != nil || decision.Effect != security.RequireApproval || decision.RequiredApprovals < 1 {
		t.Fatalf("approval floor=%+v err=%v", decision, err)
	}
}

func TestUnifiedEvaluatorMergesConstraints(t *testing.T) {
	zero := 0
	constraints, err := security.CompileConstraints([]security.ConstraintRule{{Name: "rollout", Version: "v1", CanaryPercent: &zero}})
	if err != nil {
		t.Fatal(err)
	}
	evaluator := UnifiedEvaluator{Gate: Engine{Version: "legacy-v1"}, Constraints: constraints}
	decision, err := evaluator.Evaluate(context.Background(), security.Request{Operation: security.ToolExecute, TenantID: "t", Risk: "R0", ResourceRef: "*", OccurredAt: time.Now(), Attributes: map[string]string{"tool_name": "metrics"}})
	if err != nil || decision.Effect != security.Deny || decision.PolicyVersion == "legacy-v1" {
		t.Fatalf("merged=%+v err=%v", decision, err)
	}
}
