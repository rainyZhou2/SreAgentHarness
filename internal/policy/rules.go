package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/tools"
)

// RuleConfig is the operator-owned configuration of the rule-based policy.
//
// Without a matching rule the built-in risk-class engine decides. With one or
// more matching rules the most restrictive rule decides (deny, then
// require_approval with the highest approval count, then allow). The
// framework floor (Guarded) applies on top of either result.
type RuleConfig struct {
	Version                string            `json:"version,omitempty"`
	DefaultEnvironment     string            `json:"default_environment,omitempty"`
	AllowR1WithoutApproval bool              `json:"allow_r1_without_approval,omitempty"`
	ApprovalTTL            Duration          `json:"approval_ttl,omitempty"`
	Environments           []EnvironmentRule `json:"environments,omitempty"`
	Rules                  []Rule            `json:"rules,omitempty"`
}

// EnvironmentRule assigns an environment name to calls whose target (and
// optionally tool or labels) match. The first matching rule wins.
type EnvironmentRule struct {
	Name    string            `json:"name"`
	Targets []string          `json:"targets,omitempty"`
	Tools   []string          `json:"tools,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// Rule matches tool intents and assigns a verdict.
type Rule struct {
	Name      string  `json:"name"`
	Match     Match   `json:"match"`
	Effect    Verdict `json:"effect"`
	Approvals int     `json:"approvals,omitempty"`
	Reason    string  `json:"reason,omitempty"`
}

// Match is a conjunction: every non-empty field must match. Lists match when
// any element matches. Glob patterns use "*" for any sequence (including
// "/") and "?" for one character.
type Match struct {
	Tools        []string           `json:"tools,omitempty"`
	Sources      []string           `json:"sources,omitempty"`
	Labels       map[string]string  `json:"labels,omitempty"`
	Risks        []domain.RiskClass `json:"risks,omitempty"`
	Environments []string           `json:"environments,omitempty"`
	Targets      []string           `json:"targets,omitempty"`
	// Args matches argument values (stringified) by glob.
	Args        map[string]string `json:"args,omitempty"`
	Windows     []Window          `json:"windows,omitempty"`
	BlastRadius *BlastRadius      `json:"blast_radius,omitempty"`
	Rate        *Rate             `json:"rate,omitempty"`
}

type Window = security.Window

// BlastRadius matches calls whose argument names more than Over targets
// (array length, or comma-separated values).
type BlastRadius struct {
	Arg  string `json:"arg"`
	Over int    `json:"over"`
}

// Rate matches when at least Over actions of the same tool (Per "tool", the
// default) or of the same tool on the same target (Per "target") were planned
// for the tenant within Window.
type Rate struct {
	Window Duration `json:"window"`
	Over   int      `json:"over"`
	Per    string   `json:"per,omitempty"`
}

// ActionCounter counts planned action intents (the rate-rule history).
type ActionCounter interface {
	// CountActions counts intents for tool in the tenant since the given
	// time; an empty target counts every target.
	CountActions(ctx context.Context, tenantID, tool, target string, since time.Time) (int, error)
}

// RuleEngine is the rule-based Gate.
type RuleEngine struct {
	Base         Engine
	Environments []EnvironmentRule
	Rules        []Rule
	Counter      ActionCounter
}

var (
	_ Gate                = RuleEngine{}
	_ ContextGate         = RuleEngine{}
	_ EnvironmentResolver = RuleEngine{}
)

// NewRuleEngine validates a configuration and compiles it.
func NewRuleEngine(cfg RuleConfig, counter ActionCounter) (RuleEngine, error) {
	base := Default()
	if cfg.Version != "" {
		base.Version = cfg.Version
	}
	if cfg.DefaultEnvironment != "" {
		base.Environment = cfg.DefaultEnvironment
	}
	if cfg.ApprovalTTL > 0 {
		base.ApprovalTTL = time.Duration(cfg.ApprovalTTL)
	}
	base.AllowR1WithoutApproval = cfg.AllowR1WithoutApproval
	e := RuleEngine{Base: base, Counter: counter}
	for i, env := range cfg.Environments {
		if strings.TrimSpace(env.Name) == "" {
			return e, fmt.Errorf("policy environments[%d]: name is required", i)
		}
		if len(env.Targets) == 0 && len(env.Tools) == 0 && len(env.Labels) == 0 {
			return e, fmt.Errorf("policy environment %q: needs targets, tools or labels", env.Name)
		}
		e.Environments = append(e.Environments, env)
	}
	seen := map[string]bool{}
	for i, r := range cfg.Rules {
		if strings.TrimSpace(r.Name) == "" {
			return e, fmt.Errorf("policy rules[%d]: name is required", i)
		}
		if seen[r.Name] {
			return e, fmt.Errorf("policy rule %q is defined twice", r.Name)
		}
		seen[r.Name] = true
		switch r.Effect {
		case Allow, Deny:
			if r.Approvals != 0 {
				return e, fmt.Errorf("policy rule %q: approvals only apply to require_approval", r.Name)
			}
		case RequireApproval:
			if r.Approvals == 0 {
				r.Approvals = 1
			}
			if r.Approvals < 1 || r.Approvals > 5 {
				return e, fmt.Errorf("policy rule %q: approvals must be 1..5", r.Name)
			}
		default:
			return e, fmt.Errorf("policy rule %q: effect must be allow, require_approval or deny", r.Name)
		}
		for _, rc := range r.Match.Risks {
			if _, ok := riskRank[rc]; !ok {
				return e, fmt.Errorf("policy rule %q: unknown risk class %q", r.Name, rc)
			}
		}
		for j := range r.Match.Windows {
			if err := r.Match.Windows[j].Compile(); err != nil {
				return e, fmt.Errorf("policy rule %q windows[%d]: %w", r.Name, j, err)
			}
		}
		if b := r.Match.BlastRadius; b != nil && (b.Arg == "" || b.Over < 1) {
			return e, fmt.Errorf("policy rule %q: blast_radius needs arg and over >= 1", r.Name)
		}
		if rt := r.Match.Rate; rt != nil {
			if rt.Window <= 0 || rt.Over < 1 {
				return e, fmt.Errorf("policy rule %q: rate needs window > 0 and over >= 1", r.Name)
			}
			switch rt.Per {
			case "", "tool", "target":
			default:
				return e, fmt.Errorf("policy rule %q: rate.per must be tool or target", r.Name)
			}
		}
		e.Rules = append(e.Rules, r)
	}
	return e, nil
}

var riskRank = map[domain.RiskClass]int{domain.RiskR0: 0, domain.RiskR1: 1, domain.RiskR2: 2, domain.RiskR3: 3}

// Evaluate implements Gate for callers without request context. Rate rules
// cannot count without a tenant and fail closed.
func (e RuleEngine) Evaluate(desc *tools.Descriptor, target string) Decision {
	return e.EvaluateRequest(context.Background(), Request{Tool: desc, Target: target})
}

// EnvironmentFor resolves the environment of a call.
func (e RuleEngine) EnvironmentFor(desc tools.Descriptor, args json.RawMessage) string {
	return e.environment(desc, tools.Target(desc, args))
}

func (e RuleEngine) environment(desc tools.Descriptor, target string) string {
	for _, env := range e.Environments {
		if len(env.Targets) > 0 && !anyGlob(env.Targets, target) {
			continue
		}
		if len(env.Tools) > 0 && !anyGlob(env.Tools, desc.Name) {
			continue
		}
		if !labelsMatch(env.Labels, desc.Labels) {
			continue
		}
		return env.Name
	}
	return e.Base.Environment
}

// EvaluateRequest decides a tool intent.
func (e RuleEngine) EvaluateRequest(ctx context.Context, req Request) Decision {
	if req.Tool == nil {
		return Decision{Verdict: Deny, Reason: "tool is not registered", PolicyVersion: e.Base.Version}
	}
	desc := *req.Tool
	target := req.Target
	if target == "" && len(req.Args) > 0 {
		target = tools.Target(desc, req.Args)
	}
	env := e.environment(desc, target)
	base := e.Base
	base.Environment = env
	d := base.Evaluate(&desc, target)
	d.Environment = env

	var args map[string]any
	if len(req.Args) > 0 {
		_ = json.Unmarshal(req.Args, &args)
	}
	now := req.now()
	var best *Rule
	var notes []string
	for i := range e.Rules {
		r := &e.Rules[i]
		ok, note := e.matches(ctx, r.Match, req, desc, target, env, args, now)
		if !ok {
			continue
		}
		d.MatchedRules = append(d.MatchedRules, r.Name)
		if note != "" {
			notes = append(notes, note)
		}
		if best == nil || stricter(*r, *best) {
			best = r
		}
	}
	if best == nil {
		return d
	}
	reason := best.Reason
	if reason == "" {
		reason = fmt.Sprintf("matched policy rule %q", best.Name)
	} else {
		reason = fmt.Sprintf("policy rule %q: %s", best.Name, reason)
	}
	if len(notes) > 0 {
		reason += " (" + strings.Join(notes, "; ") + ")"
	}
	d.Verdict, d.Reason, d.RequiredApprovals = best.Effect, reason, 0
	if best.Effect == RequireApproval {
		d.RequiredApprovals = best.Approvals
	}
	return d
}

func severity(r Rule) int {
	switch r.Effect {
	case Deny:
		return 100
	case RequireApproval:
		return 10 + r.Approvals
	default:
		return 1
	}
}

func stricter(a, b Rule) bool { return severity(a) > severity(b) }

func (e RuleEngine) matches(ctx context.Context, m Match, req Request, desc tools.Descriptor, target, env string,
	args map[string]any, now time.Time) (bool, string) {
	if len(m.Tools) > 0 && !anyGlob(m.Tools, desc.Name) {
		return false, ""
	}
	if len(m.Sources) > 0 && !anyGlob(m.Sources, desc.Source) {
		return false, ""
	}
	if !labelsMatch(m.Labels, desc.Labels) {
		return false, ""
	}
	if len(m.Risks) > 0 {
		found := false
		for _, rc := range m.Risks {
			if rc == desc.Risk {
				found = true
				break
			}
		}
		if !found {
			return false, ""
		}
	}
	if len(m.Environments) > 0 && !anyGlob(m.Environments, env) {
		return false, ""
	}
	if len(m.Targets) > 0 && !anyGlob(m.Targets, target) {
		return false, ""
	}
	for k, pattern := range m.Args {
		v, ok := args[k]
		if !ok || !glob(pattern, stringValue(v)) {
			return false, ""
		}
	}
	if len(m.Windows) > 0 {
		in := false
		for _, w := range m.Windows {
			if w.Contains(now) {
				in = true
				break
			}
		}
		if !in {
			return false, ""
		}
	}
	if b := m.BlastRadius; b != nil {
		if countValues(args[b.Arg]) <= b.Over {
			return false, ""
		}
	}
	if rt := m.Rate; rt != nil {
		if e.Counter == nil || req.TenantID == "" {
			return true, "action history unavailable; rate rule applied"
		}
		scope := ""
		if rt.Per == "target" {
			scope = target
		}
		n, err := e.Counter.CountActions(ctx, req.TenantID, desc.Name, scope, now.Add(-time.Duration(rt.Window)))
		if err != nil {
			return true, "action history unavailable; rate rule applied"
		}
		if n < rt.Over {
			return false, ""
		}
		return true, fmt.Sprintf("%d action(s) in the last %s", n, time.Duration(rt.Window))
	}
	return true, ""
}

// BuildIntent binds the per-call environment into the intent.
func (e RuleEngine) BuildIntent(desc tools.Descriptor, call domain.ToolCall, inputHash string, d Decision) domain.ActionIntent {
	base := e.Base
	base.Environment = e.environment(desc, tools.Target(desc, call.Args))
	return base.BuildIntent(desc, call, inputHash, d)
}

func labelsMatch(want, have map[string]string) bool {
	for k, pattern := range want {
		v, ok := have[k]
		if !ok || !glob(pattern, v) {
			return false
		}
	}
	return true
}

func anyGlob(patterns []string, s string) bool {
	for _, p := range patterns {
		if glob(p, s) {
			return true
		}
	}
	return false
}

// glob matches s against a pattern where "*" matches any sequence and "?"
// one rune. Matching is case-sensitive.
func glob(pattern, s string) bool {
	p, str := []rune(pattern), []rune(s)
	pi, si := 0, 0
	star, mark := -1, 0
	for si < len(str) {
		switch {
		case pi < len(p) && (p[pi] == '?' || p[pi] == str[si]):
			pi++
			si++
		case pi < len(p) && p[pi] == '*':
			star, mark = pi, si
			pi++
		case star >= 0:
			pi = star + 1
			mark++
			si = mark
		default:
			return false
		}
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

func stringValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func countValues(v any) int {
	switch x := v.(type) {
	case nil:
		return 0
	case []any:
		return len(x)
	case map[string]any:
		return len(x)
	case string:
		n := 0
		for _, part := range strings.Split(x, ",") {
			if strings.TrimSpace(part) != "" {
				n++
			}
		}
		return n
	default:
		return 1
	}
}

// Duration decodes "30s"/"1h" or a number of seconds.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		*d = Duration(v)
		return nil
	}
	var n float64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\" or a number of seconds")
	}
	*d = Duration(time.Duration(n * float64(time.Second)))
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// RuleNames lists the configured rule names (for diagnostics).
func (e RuleEngine) RuleNames() []string {
	out := make([]string, 0, len(e.Rules))
	for _, r := range e.Rules {
		out = append(out, r.Name)
	}
	sort.Strings(out)
	return out
}
