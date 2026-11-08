package planner

import (
	"fmt"
	"regexp"
	"strings"
)

// FallbackRule picks the deterministic fallback flow for a class of goals.
// Rules are tried in order; the first whose services and goal pattern match
// wins. An empty Services list or GoalPattern matches anything.
type FallbackRule struct {
	Name        string   `json:"name,omitempty"`
	Services    []string `json:"services,omitempty"`
	GoalPattern string   `json:"goal_pattern,omitempty"`
	ProfileID   string   `json:"profile_id"`
	Version     int64    `json:"version"`
}

// Validate checks a rule.
func (r FallbackRule) Validate() error {
	if r.ProfileID == "" || r.Version <= 0 {
		return fmt.Errorf("fallback rule %q: profile_id and version are required", r.Name)
	}
	if r.GoalPattern != "" {
		if _, err := regexp.Compile("(?i)" + r.GoalPattern); err != nil {
			return fmt.Errorf("fallback rule %q: goal_pattern: %w", r.Name, err)
		}
	}
	return nil
}

func (r FallbackRule) matches(goal string, services []string) bool {
	if len(r.Services) > 0 {
		hit := false
		for _, want := range r.Services {
			for _, s := range services {
				if strings.EqualFold(want, s) {
					hit = true
				}
			}
		}
		if !hit {
			return false
		}
	}
	if r.GoalPattern != "" {
		re, err := regexp.Compile("(?i)" + r.GoalPattern)
		if err != nil || !re.MatchString(goal) {
			return false
		}
	}
	return true
}

// Override is the per-tenant planner configuration carried by the run
// profile (spec.planner). It can choose models and fallbacks, but budgets
// and the approval mode only ever tighten the process configuration.
type Override struct {
	ModelAlias         string         `json:"model_alias,omitempty"`
	ModelLadder        []string       `json:"model_ladder,omitempty"`
	ApprovalMode       string         `json:"approval_mode,omitempty"`
	MaxNodes           int            `json:"max_nodes,omitempty"`
	MaxModelCalls      int            `json:"max_model_calls,omitempty"`
	MaxEstimatedTokens int64          `json:"max_estimated_tokens,omitempty"`
	MaxRepairs         *int           `json:"max_repairs,omitempty"`
	Fallbacks          []FallbackRule `json:"fallbacks,omitempty"`
}

// approvalRank orders approval modes by strictness.
func approvalRank(m string) int {
	switch strings.ToLower(m) {
	case "always":
		return 2
	case "never":
		return 0
	default:
		return 1
	}
}

// StricterApproval returns the stricter of two approval modes.
func StricterApproval(a, b string) string {
	if b == "" {
		return a
	}
	if a == "" || approvalRank(b) > approvalRank(a) {
		return strings.ToLower(b)
	}
	return a
}

func minPositive(a, b int) int {
	if b > 0 && (a <= 0 || b < a) {
		return b
	}
	return a
}

// effective merges a tenant override into the process configuration.
func (c Config) effective(o *Override) Config {
	if o == nil {
		return c
	}
	if o.ModelAlias != "" {
		c.ModelAlias = o.ModelAlias
		c.ModelLadder = nil
	}
	if len(o.ModelLadder) > 0 {
		c.ModelLadder = append([]string(nil), o.ModelLadder...)
	}
	c.ApprovalMode = StricterApproval(c.ApprovalMode, o.ApprovalMode)
	c.MaxNodes = minPositive(c.MaxNodes, o.MaxNodes)
	c.MaxModelCalls = minPositive(c.MaxModelCalls, o.MaxModelCalls)
	if o.MaxEstimatedTokens > 0 && (c.MaxEstimatedTokens <= 0 || o.MaxEstimatedTokens < c.MaxEstimatedTokens) {
		c.MaxEstimatedTokens = o.MaxEstimatedTokens
	}
	if o.MaxRepairs != nil && *o.MaxRepairs >= 0 && *o.MaxRepairs < c.MaxRepairs {
		c.MaxRepairs = *o.MaxRepairs
	}
	if len(o.Fallbacks) > 0 {
		c.Fallbacks = append(append([]FallbackRule(nil), o.Fallbacks...), c.Fallbacks...)
	}
	return c
}

// aliasFor returns the model alias of a planning attempt: the ladder starts
// with the cheapest model and escalates after each invalid plan.
func (c Config) aliasFor(attempt int) string {
	if len(c.ModelLadder) == 0 {
		return c.ModelAlias
	}
	if attempt >= len(c.ModelLadder) {
		attempt = len(c.ModelLadder) - 1
	}
	return c.ModelLadder[attempt]
}

// fallbackFor returns the fallback flow profile for a goal: the first
// matching rule, else the default FallbackProfileID/Version.
func (c Config) fallbackFor(goal string, services []string) (string, int64, string) {
	for _, r := range c.Fallbacks {
		if r.matches(goal, services) {
			name := r.Name
			if name == "" {
				name = r.ProfileID
			}
			return r.ProfileID, r.Version, "rule " + name
		}
	}
	return c.FallbackProfileID, c.FallbackVersion, "default"
}
