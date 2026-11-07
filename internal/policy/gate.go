package policy

import (
	"context"
	"encoding/json"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// Gate is the policy extension point. Implementations decide whether a tool
// intent is allowed, needs approval or is denied, and compute the
// ActionIntent an approval binds to. Engine is the built-in implementation.
type Gate interface {
	Evaluate(desc *tools.Descriptor, target string) Decision
	BuildIntent(desc tools.Descriptor, call domain.ToolCall, inputHash string, d Decision) domain.ActionIntent
}

var _ Gate = Engine{}

// Guarded wraps any Gate with framework invariants no plugin can relax:
// unregistered tools and missing targets are denied, R3 is denied, R2 always
// needs at least one approval, and the risk class always comes from the
// registry descriptor. A plugin may only be stricter than this floor.
func Guarded(g Gate) Gate {
	if gg, ok := g.(guarded); ok {
		return gg
	}
	return guarded{g}
}

type guarded struct{ inner Gate }

func (g guarded) Evaluate(desc *tools.Descriptor, target string) Decision {
	return floor(g.inner.Evaluate(desc, target), desc, target)
}

// EvaluateRequest forwards the full request to gates that understand it and
// applies the same framework floor.
func (g guarded) EvaluateRequest(ctx context.Context, req Request) Decision {
	req = req.withTarget()
	return floor(EvaluateRequest(ctx, g.inner, req), req.Tool, req.Target)
}

// EnvironmentFor delegates to the inner gate when it resolves environments.
func (g guarded) EnvironmentFor(desc tools.Descriptor, args json.RawMessage) string {
	if er, ok := g.inner.(EnvironmentResolver); ok {
		return er.EnvironmentFor(desc, args)
	}
	return ""
}

// floor applies the invariants no gate can relax.
func floor(d Decision, desc *tools.Descriptor, target string) Decision {
	switch {
	case desc == nil:
		return Decision{Verdict: Deny, Reason: "tool is not registered", PolicyVersion: d.PolicyVersion}
	case desc.TargetArg != "" && target == "":
		return Decision{Verdict: Deny, Reason: "tool target is missing", Risk: desc.Risk, PolicyVersion: d.PolicyVersion}
	}
	d.Risk = desc.Risk
	switch desc.Risk {
	case domain.RiskR0, domain.RiskR1:
	case domain.RiskR2:
		if d.Verdict == Allow {
			d.Verdict, d.Reason = RequireApproval, "framework floor: R2 always requires approval"
		}
		if d.Verdict == RequireApproval && d.RequiredApprovals < 1 {
			d.RequiredApprovals = 1
		}
	default:
		d.Verdict, d.Reason = Deny, "framework floor: destructive (R3) or unclassified actions are disabled"
	}
	switch d.Verdict {
	case Allow, RequireApproval, Deny:
	default:
		// An unknown verdict from a plugin is not an allow.
		d.Verdict, d.Reason = Deny, "policy returned no valid verdict"
	}
	if d.Verdict == RequireApproval && d.RequiredApprovals < 1 {
		d.RequiredApprovals = 1
	}
	return d
}

func (g guarded) BuildIntent(desc tools.Descriptor, call domain.ToolCall, inputHash string, d Decision) domain.ActionIntent {
	return g.inner.BuildIntent(desc, call, inputHash, d)
}
