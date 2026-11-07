package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
)

// Rules is the enforced, frozen part of a profile version (§12.4): the tool
// allowlist, risk overrides, approval rules, policy version and step limits.
// The kernel (tool exposure, planning, execution), the store (step budget)
// and the command API (replace_pending_input) all decide through Rules, so a
// profile restricts what a run can do no matter which path plans a tool step.
type Rules struct {
	Ref string // profile@version, for messages

	restricted    bool     // false: the spec has no "tools" field (legacy: every registered tool)
	tools         []string // exact names or "prefix.*" / "*"
	riskOverrides map[string]domain.RiskClass
	minApprovals  map[string]int // tool name or risk class → minimum approvals
	policyVersion string

	MaxSteps      int
	CompactEvery  int
	MemoryTopK    int
	CapabilityMap bool

	// Flow is the compiled orchestration plan when strategy is "flow".
	Flow     *flow.Spec
	FlowHash string
}

// ApprovalRules is the format of spec.approval_rules. Rules only tighten:
// they can require approval for tools the policy allows and raise the number
// of approvals, never lower them or turn a deny into an allow.
//
//	{"min_approvals": {"R1": 1, "R2": 2, "k8s.rollout_restart": 2}}
type ApprovalRules struct {
	MinApprovals map[string]int `json:"min_approvals"`
}

// Unrestricted is used for runs without a profile (tests, legacy rows).
func Unrestricted() Rules { return Rules{Ref: "none"} }

var riskOrder = map[domain.RiskClass]int{domain.RiskR0: 0, domain.RiskR1: 1, domain.RiskR2: 2, domain.RiskR3: 3}

// Compile validates a spec's enforced fields.
func Compile(ref string, s Spec) (Rules, error) {
	r := Rules{Ref: ref, restricted: s.Tools != nil, tools: s.Tools, policyVersion: s.PolicyVersion,
		MaxSteps: s.MaxSteps, CompactEvery: s.CompactEvery, MemoryTopK: s.MemoryTopK,
		CapabilityMap: s.CapabilityMap}
	for _, t := range s.Tools {
		if t == "" || (strings.Contains(t, "*") && t != "*" && !strings.HasSuffix(t, ".*")) || strings.Count(t, "*") > 1 {
			return r, fmt.Errorf("profile %s: invalid tool pattern %q (use a name, \"prefix.*\" or \"*\")", ref, t)
		}
	}
	if len(s.RiskOverrides) > 0 {
		r.riskOverrides = map[string]domain.RiskClass{}
		for tool, rc := range s.RiskOverrides {
			c := domain.RiskClass(rc)
			if _, ok := riskOrder[c]; !ok {
				return r, fmt.Errorf("profile %s: risk_overrides[%s]: unknown risk class %q", ref, tool, rc)
			}
			r.riskOverrides[tool] = c
		}
	}
	if len(s.ApprovalRules) > 0 && string(s.ApprovalRules) != "null" {
		var ar ApprovalRules
		dec := json.NewDecoder(strings.NewReader(string(s.ApprovalRules)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&ar); err != nil {
			return r, fmt.Errorf("profile %s: approval_rules: %w", ref, err)
		}
		for k, n := range ar.MinApprovals {
			if n < 0 || n > 5 {
				return r, fmt.Errorf("profile %s: approval_rules.min_approvals[%s] must be 0..5", ref, k)
			}
		}
		r.minApprovals = ar.MinApprovals
	}
	if s.MaxSteps < 0 || s.CompactEvery < 0 || s.MemoryTopK < 0 {
		return r, fmt.Errorf("profile %s: max_steps, compact_every and memory_top_k must be >= 0", ref)
	}
	if s.ModelAlias != "" && len(s.AllowedAliases) > 0 && !contains(s.AllowedAliases, s.ModelAlias) {
		return r, fmt.Errorf("profile %s: model_alias %q is not in allowed_aliases %v", ref, s.ModelAlias, s.AllowedAliases)
	}
	if len(s.Flow) > 0 {
		fs, _, hash, err := flow.Compile(s.Flow, flow.CompileOptions{AllowTool: r.AllowsTool})
		if err != nil {
			return r, fmt.Errorf("profile %s: %w", ref, err)
		}
		r.Flow, r.FlowHash = &fs, hash
	}
	if s.Strategy == "flow" && r.Flow == nil {
		return r, fmt.Errorf("profile %s: strategy flow requires a flow spec", ref)
	}
	if s.Strategy != "flow" && r.Flow != nil {
		return r, fmt.Errorf("profile %s: flow is only valid with strategy \"flow\"", ref)
	}
	return r, nil
}

// Restricted reports whether the profile declares a tool allowlist.
func (r Rules) Restricted() bool { return r.restricted }

// AllowsTool reports whether the profile lets the run use the tool.
func (r Rules) AllowsTool(name string) bool {
	if !r.restricted || tools.IsBuiltin(name) {
		// Framework tools (harness.*) only read the run's own artifacts.
		return true
	}
	for _, p := range r.tools {
		if p == name || p == "*" || (strings.HasSuffix(p, ".*") && strings.HasPrefix(name, strings.TrimSuffix(p, "*"))) {
			return true
		}
	}
	return false
}

// Effective applies risk overrides. Overrides only raise the risk class; a
// lower override is ignored.
func (r Rules) Effective(d tools.Descriptor) tools.Descriptor {
	if o, ok := r.riskOverrides[d.Name]; ok && riskOrder[o] > riskOrder[d.Risk] {
		d.Risk = o
	}
	return d
}

// Visible returns the descriptors the model may see, sorted by name, with
// overridden risk classes.
func (r Rules) Visible(ds []tools.Descriptor) []tools.Descriptor {
	var out []tools.Descriptor
	for _, d := range ds {
		if r.AllowsTool(d.Name) {
			out = append(out, r.Effective(d))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Call identifies a tool intent for DecideCall.
type Call struct {
	TenantID string
	RunID    string
	Name     string
	Args     json.RawMessage
	// Now is the decision time (zero = time.Now()).
	Now time.Time
}

// Decide evaluates a tool intent without request context (no tenant, so
// rate-based policy rules fail closed). Prefer DecideCall.
func (r Rules) Decide(g policy.Gate, name string, desc *tools.Descriptor, args json.RawMessage) (policy.Decision, *tools.Descriptor) {
	return r.DecideCall(context.Background(), g, Call{Name: name, Args: args}, desc)
}

// DecideCall evaluates a tool intent: allowlist → risk override → policy gate
// → policy version pin → approval rules. desc is the registry descriptor
// (nil for an unregistered tool); the returned descriptor carries the
// effective risk and must be used to build and verify the ActionIntent.
func (r Rules) DecideCall(ctx context.Context, g policy.Gate, c Call, desc *tools.Descriptor) (policy.Decision, *tools.Descriptor) {
	if !r.AllowsTool(c.Name) {
		pd := g.Evaluate(nil, "")
		return policy.Decision{Verdict: policy.Deny, Reason: fmt.Sprintf("tool %q is not allowed by profile %s", c.Name, r.Ref),
			PolicyVersion: pd.PolicyVersion}, nil
	}
	if desc == nil {
		return g.Evaluate(nil, ""), nil
	}
	eff := r.Effective(*desc)
	pd := policy.EvaluateRequest(ctx, g, policy.Request{TenantID: c.TenantID, RunID: c.RunID, Tool: &eff,
		Target: tools.Target(eff, c.Args), Args: c.Args, Now: c.Now})
	if r.policyVersion != "" && pd.PolicyVersion != r.policyVersion {
		return policy.Decision{Verdict: policy.Deny, Risk: eff.Risk, PolicyVersion: pd.PolicyVersion,
			Reason: fmt.Sprintf("profile %s requires policy %s, active policy is %s", r.Ref, r.policyVersion, pd.PolicyVersion)}, &eff
	}
	if pd.Verdict == policy.Deny {
		return pd, &eff
	}
	need := max(r.minApprovals[string(eff.Risk)], r.minApprovals[eff.Name])
	if need > pd.RequiredApprovals {
		pd.RequiredApprovals = need
		pd.Verdict = policy.RequireApproval
		pd.Reason = strings.TrimSpace(pd.Reason + fmt.Sprintf("; profile %s requires %d approval(s)", r.Ref, need))
	}
	return pd, &eff
}

// RulesFor caches compiled rules per immutable profile version.
type RulesFor struct {
	Loader Loader
	mu     sync.Mutex
	cache  map[string]Rules
}

func (c *RulesFor) Rules(ctx context.Context, tenantID, profileID string, version int64) (Rules, error) {
	if profileID == "" {
		return Unrestricted(), nil
	}
	ref := fmt.Sprintf("%s@%d", profileID, version)
	key := tenantID + "/" + ref
	c.mu.Lock()
	if r, ok := c.cache[key]; ok {
		c.mu.Unlock()
		return r, nil
	}
	c.mu.Unlock()
	raw, err := c.Loader.ProfileSpec(ctx, tenantID, profileID, version)
	if err != nil {
		return Rules{}, err
	}
	spec, err := Parse(raw)
	if err != nil {
		return Rules{}, err
	}
	r, err := Compile(ref, spec)
	if err != nil {
		return Rules{}, err
	}
	c.mu.Lock()
	if c.cache == nil {
		c.cache = map[string]Rules{}
	}
	c.cache[key] = r
	c.mu.Unlock()
	return r, nil
}
