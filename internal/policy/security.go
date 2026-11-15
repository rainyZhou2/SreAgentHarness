package policy

import (
	"context"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/tools"
)

type UnifiedEvaluator struct {
	Gate        Gate
	Constraints *security.CompiledConstraints
}

func (e UnifiedEvaluator) Evaluate(ctx context.Context, request security.Request) (security.PolicyDecision, error) {
	descriptor := tools.Descriptor{
		Name: request.Attributes["tool_name"], Version: request.Attributes["tool_version"],
		Risk: domain.RiskClass(request.Risk), TargetArg: request.Attributes["tool_target_arg"], Source: request.Attributes["tool_source"],
		Labels: map[string]string{},
	}
	for key, value := range request.Attributes {
		if name, ok := strings.CutPrefix(key, "tool_label_"); ok {
			descriptor.Labels[name] = value
		}
	}
	legacy := EvaluateRequest(ctx, Guarded(e.Gate), Request{TenantID: request.TenantID, RunID: request.RunID, Tool: &descriptor,
		Target: request.ResourceRef, Args: request.Input, Now: request.OccurredAt})
	decision := securityDecision(legacy)
	if e.Constraints == nil {
		return decision, nil
	}
	constraint, err := e.Constraints.Evaluate(request, request.OccurredAt)
	if err != nil {
		return security.PolicyDecision{}, err
	}
	return security.Merge([]security.PolicyDecision{decision, constraint}, request.OccurredAt)
}

func securityDecision(decision Decision) security.PolicyDecision {
	effect := security.Deny
	switch decision.Verdict {
	case Allow:
		effect = security.Allow
	case RequireApproval:
		effect = security.RequireApproval
	}
	return security.PolicyDecision{Effect: effect, RequiredApprovals: decision.RequiredApprovals, PolicyVersion: decision.PolicyVersion,
		Reasons: []security.Reason{{Code: "policy_" + string(decision.Verdict), Message: decision.Reason}}}
}
