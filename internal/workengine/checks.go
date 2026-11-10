package workengine

import (
	"context"
	"fmt"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/evidenceplan"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

// KernelChecks gives evidence planning the kernel's own view of a run: the
// tenant's registered tools, the run profile's allowlist and risk
// overrides, and the policy gate the kernel will enforce. A plan therefore
// never names a tool the run could not call without approval.
type KernelChecks struct {
	Server *api.Server
}

// Checks resolves the profile version the run will get (same routing key
// as the run start) and returns the constraints of that version.
func (k KernelChecks) Checks(ctx context.Context, tenantID, profileID string, version int64, routingKey string) ([]tools.Descriptor, evidenceplan.Constraints, error) {
	s := k.Server
	if s == nil || s.Tools == nil {
		return nil, evidenceplan.Constraints{}, fmt.Errorf("no tool registry")
	}
	if profileID == "" {
		profileID, version = s.DefaultProfile, s.DefaultVersion
	}
	if version == 0 && s.ResolveProfile != nil {
		version, _ = s.ResolveProfile(ctx, tenantID, profileID, routingKey)
	}
	if version == 0 {
		version = s.DefaultVersion
	}
	cons := evidenceplan.Constraints{}
	if s.Policy != nil {
		cons.Gate = policy.Guarded(s.Policy)
	}
	if s.Rules != nil {
		rules, err := s.Rules(ctx, tenantID, profileID, version)
		if err != nil {
			return nil, cons, fmt.Errorf("profile %s@%d: %w", profileID, version, err)
		}
		cons.Visible = visibleUnder(rules)
	}
	return s.Tools.DescriptorsFor(tenantID), cons, nil
}

func visibleUnder(r profile.Rules) func(tools.Descriptor) (tools.Descriptor, bool) {
	return func(d tools.Descriptor) (tools.Descriptor, bool) {
		if !r.AllowsTool(d.Name) {
			return d, false
		}
		return r.Effective(d), true
	}
}
