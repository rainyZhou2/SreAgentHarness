// Package profile defines the versioned agent profile spec (§12, §32) and the
// rules for evolving it. Profiles are immutable per version; a run pins the
// version it started with, and canary rollouts route a share of new runs to a
// candidate version.
package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"

	"go-sre-harness/internal/cognitive"
)

// Spec is the operator-owned, versioned behavior of an agent. Fields marked
// "frozen" are safety-critical and can never be changed by an evolution
// candidate (§12.4): only humans editing a new profile version may.
type Spec struct {
	Strategy            string `json:"strategy"`              // llm | rule_based
	ModelAlias          string `json:"model_alias,omitempty"` // evolvable within AllowedAliases
	Effort              string `json:"effort,omitempty"`      // evolvable
	PromptPrefixVersion string `json:"prompt_prefix_version,omitempty"`
	MaxOutputTokens     int64  `json:"max_output_tokens,omitempty"`
	MaxContextTokens    int64  `json:"max_context_tokens,omitempty"`
	MemoryTopK          int    `json:"memory_top_k,omitempty"`
	CompactEvery        int    `json:"compact_every,omitempty"`

	// frozen
	Tools          []string          `json:"tools"`
	PolicyVersion  string            `json:"policy_version,omitempty"`
	RiskOverrides  map[string]string `json:"risk_overrides,omitempty"`
	ApprovalRules  json.RawMessage   `json:"approval_rules,omitempty"`
	AllowedAliases []string          `json:"allowed_aliases,omitempty"`
	MaxSteps       int               `json:"max_steps,omitempty"`
}

// Frozen lists the JSON fields an evolution candidate may not change.
var Frozen = []string{"tools", "policy_version", "risk_overrides", "approval_rules", "allowed_aliases", "max_steps", "strategy"}

func Parse(raw json.RawMessage) (Spec, error) {
	var s Spec
	if len(raw) == 0 {
		return s, nil
	}
	err := json.Unmarshal(raw, &s)
	return s, err
}

// ApplyPatch applies a candidate's patch to a base spec after checking that
// no frozen field is touched and evolvable values stay within bounds.
func ApplyPatch(base Spec, patch json.RawMessage) (Spec, []string, error) {
	var p map[string]json.RawMessage
	if err := json.Unmarshal(patch, &p); err != nil {
		return base, nil, fmt.Errorf("patch must be a JSON object: %w", err)
	}
	frozen := map[string]bool{}
	for _, f := range Frozen {
		frozen[f] = true
	}
	var changed []string
	for k := range p {
		if frozen[k] {
			return base, nil, fmt.Errorf("field %q is frozen and cannot be changed by an evolution candidate", k)
		}
		changed = append(changed, k)
	}
	sort.Strings(changed)
	bj, _ := json.Marshal(base)
	var merged map[string]json.RawMessage
	_ = json.Unmarshal(bj, &merged)
	for k, v := range p {
		merged[k] = v
	}
	mj, _ := json.Marshal(merged)
	var out Spec
	dec := json.NewDecoder(bytesReader(mj))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return base, nil, fmt.Errorf("invalid patch: %w", err)
	}
	if !reflect.DeepEqual(out.Tools, base.Tools) || out.Strategy != base.Strategy {
		return base, nil, fmt.Errorf("frozen fields changed")
	}
	if out.ModelAlias != base.ModelAlias && len(base.AllowedAliases) > 0 && !contains(base.AllowedAliases, out.ModelAlias) {
		return base, nil, fmt.Errorf("model_alias %q is not in allowed_aliases", out.ModelAlias)
	}
	switch out.Effort {
	case "", "low", "medium", "high", "xhigh", "max":
	default:
		return base, nil, fmt.Errorf("invalid effort %q", out.Effort)
	}
	if out.MemoryTopK < 0 || out.MemoryTopK > 20 || out.CompactEvery < 0 || out.CompactEvery > 50 ||
		out.MaxOutputTokens < 0 || out.MaxOutputTokens > 128_000 {
		return base, nil, fmt.Errorf("evolvable value out of bounds")
	}
	return out, changed, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// ---------- strategy resolution per run ----------

// Loader reads an immutable profile version.
type Loader interface {
	ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error)
}

// Factory builds the strategy for a spec.
type Factory func(spec Spec) (cognitive.Strategy, error)

// Resolver caches one strategy per (tenant, profile, version); versions are
// immutable, so the cache never needs invalidation.
type Resolver struct {
	Loader  Loader
	Factory Factory
	mu      sync.Mutex
	cache   map[string]cognitive.Strategy
}

func (r *Resolver) StrategyFor(ctx context.Context, tenantID, profileID string, version int64) (cognitive.Strategy, error) {
	key := fmt.Sprintf("%s/%s@%d", tenantID, profileID, version)
	r.mu.Lock()
	if r.cache == nil {
		r.cache = map[string]cognitive.Strategy{}
	}
	if s, ok := r.cache[key]; ok {
		r.mu.Unlock()
		return s, nil
	}
	r.mu.Unlock()
	raw, err := r.Loader.ProfileSpec(ctx, tenantID, profileID, version)
	if err != nil {
		return nil, err
	}
	spec, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	s, err := r.Factory(spec)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cache[key] = s
	r.mu.Unlock()
	return s, nil
}
