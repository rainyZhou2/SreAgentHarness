package action_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type inspectionTool struct {
	risk  domain.RiskClass
	calls int
}

func (t *inspectionTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: "inspection-test", Version: "1", Risk: t.risk}
}

func (t *inspectionTool) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	t.calls++
	return json.RawMessage(`{"applied":true}`), nil
}

func TestToolExecutorInspectRequiresExplicitReadOnly(t *testing.T) {
	for _, risk := range []domain.RiskClass{domain.RiskR0, domain.RiskR1, domain.RiskR2, domain.RiskR3, "", "UNKNOWN"} {
		t.Run(string(risk), func(t *testing.T) {
			writeTool := &inspectionTool{risk: domain.RiskR2}
			inspectTool := &inspectionTool{risk: risk}
			executor := &action.ToolExecutor{Tool: writeTool, InspectTool: inspectTool}
			plan := action.Plan{TenantID: "tenant", WorkID: "work", TargetRef: "deployment://prod/api", EffectKey: "effect"}
			result, err := executor.Inspect(context.Background(), plan)
			if risk == domain.RiskR0 {
				if err != nil || result.Applied == nil || !*result.Applied || inspectTool.calls != 1 {
					t.Fatalf("read-only inspection result=%+v calls=%d err=%v", result, inspectTool.calls, err)
				}
			} else if !errors.Is(err, action.ErrUnsupported) || inspectTool.calls != 0 || result.Applied != nil {
				t.Fatalf("unsafe inspection result=%+v calls=%d err=%v", result, inspectTool.calls, err)
			}
			if writeTool.calls != 0 {
				t.Fatal("inspection called the action tool")
			}
			// The runtime remains responsible for authorization before Execute;
			// constraining Inspect must not change the write adapter's dispatch.
			if _, err := executor.Execute(context.Background(), plan, plan.EffectKey); err != nil || writeTool.calls != 1 {
				t.Fatalf("action dispatch changed: calls=%d err=%v", writeTool.calls, err)
			}
		})
	}
}

func TestToolExecutorInspectRechecksDescriptor(t *testing.T) {
	probe := &inspectionTool{risk: domain.RiskR0}
	executor := &action.ToolExecutor{Tool: &inspectionTool{risk: domain.RiskR2}, InspectTool: probe}
	if _, err := executor.Inspect(context.Background(), action.Plan{}); err != nil {
		t.Fatal(err)
	}
	probe.risk = domain.RiskR2
	if _, err := executor.Inspect(context.Background(), action.Plan{}); !errors.Is(err, action.ErrUnsupported) || probe.calls != 1 {
		t.Fatalf("changed inspection descriptor was invoked: calls=%d err=%v", probe.calls, err)
	}
}
