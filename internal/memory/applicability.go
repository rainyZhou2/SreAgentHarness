package memory

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Applicability bounds where and when a memory is valid experience (RFC
// §14.2). SRE knowledge has an environment and a time: a runbook for an old
// Kubernetes version, or a GPU fault signature of an old driver, must not
// be recalled as if it applied today. A memory is a candidate experience
// and a retrieval hint; it never grants permission to act (§14.3 ①).
type Applicability struct {
	// Kind marks the memory's role: "" (plain), "playbook" (procedural,
	// promoted from a verified Work) or "rca" (a root-cause finding).
	Kind string `json:"kind,omitempty"`
	// Environments, Services and SubjectKinds restrict reuse; empty = any.
	Environments []string `json:"environments,omitempty"`
	Services     []string `json:"services,omitempty"`
	Teams        []string `json:"teams,omitempty"`
	SubjectKinds []string `json:"subject_kinds,omitempty"`
	// VersionConstraints are "<component><op><version>" with op one of
	// = != >= <= > < ~ (prefix), e.g. "kubernetes>=1.28", "driver~550.".
	VersionConstraints []string   `json:"version_constraints,omitempty"`
	ValidFrom          *time.Time `json:"valid_from,omitempty"`
	EvidenceRefs       []string   `json:"evidence_refs,omitempty"`
	SourceRunIDs       []string   `json:"source_run_ids,omitempty"`
	SourceWorkIDs      []string   `json:"source_work_ids,omitempty"`
	Reviewer           string     `json:"reviewer,omitempty"`
	// RiskOfReuse is low | medium | high: high-risk playbooks require the
	// current environment to satisfy every precondition (versions known).
	RiskOfReuse string `json:"risk_of_reuse,omitempty"`
}

// Verification is the outcome record of a memory (machine-verified Work
// outcomes only; a model's opinion never moves these counters, §14.3 ④).
type Verification struct {
	SuccessCount   int        `json:"verified_success_count"`
	FailureCount   int        `json:"verified_failure_count"`
	LastVerifiedAt *time.Time `json:"last_verified_at,omitempty"`
}

// Env is the environment a retrieval runs in. Unknown fields do not
// exclude a memory, except for high-risk memories whose constraints must
// be positively satisfied.
type Env struct {
	Environment  string
	Services     []string
	Teams        []string
	SubjectKinds []string
	// Versions maps component -> current version (from world facts).
	Versions map[string]string
}

type envKey struct{}

// WithEnv attaches the retrieval environment to ctx.
func WithEnv(ctx context.Context, e Env) context.Context { return context.WithValue(ctx, envKey{}, e) }

// EnvFrom returns the retrieval environment, if any.
func EnvFrom(ctx context.Context) (Env, bool) {
	e, ok := ctx.Value(envKey{}).(Env)
	return e, ok
}

var constraintRe = regexp.MustCompile(`^([a-z0-9][a-z0-9_.-]{0,63})(>=|<=|!=|=|>|<|~)([A-Za-z0-9_.+-]{1,64})$`)

// Normalize validates the metadata.
func (a *Applicability) Normalize() error {
	switch a.Kind {
	case "", "playbook", "rca":
	default:
		return fmt.Errorf("applicability kind %q (want playbook or rca)", a.Kind)
	}
	switch a.RiskOfReuse {
	case "":
		a.RiskOfReuse = "medium"
	case "low", "medium", "high":
	default:
		return fmt.Errorf("risk_of_reuse %q (want low, medium or high)", a.RiskOfReuse)
	}
	for _, c := range a.VersionConstraints {
		if !constraintRe.MatchString(c) {
			return fmt.Errorf("version constraint %q (want <component><op><version>)", c)
		}
	}
	for _, l := range [][]string{a.Environments, a.Services, a.Teams, a.SubjectKinds, a.EvidenceRefs, a.SourceRunIDs, a.SourceWorkIDs} {
		if len(l) > 64 {
			return fmt.Errorf("applicability lists are limited to 64 entries")
		}
	}
	return nil
}

// Applies reports whether the memory may be recalled in env at now; the
// reason names the first failed condition.
func (m Memory) Applies(env Env, now time.Time) (bool, string) {
	if m.SupersededBy != nil && *m.SupersededBy != "" {
		return false, "superseded"
	}
	if m.ExpiresAt != nil && !m.ExpiresAt.After(now) {
		return false, "expired"
	}
	a := m.Applicability
	if a == nil {
		return true, ""
	}
	if a.ValidFrom != nil && a.ValidFrom.After(now) {
		return false, "not yet valid"
	}
	strict := a.RiskOfReuse == "high"
	if !matchAny(a.Environments, []string{env.Environment}, strict) {
		return false, "environment " + env.Environment
	}
	if !matchAny(a.Services, env.Services, strict) {
		return false, "service"
	}
	if !matchAny(a.Teams, env.Teams, strict) {
		return false, "team"
	}
	if !matchAny(a.SubjectKinds, env.SubjectKinds, strict) {
		return false, "subject kind"
	}
	for _, c := range a.VersionConstraints {
		ok, known := satisfies(c, env.Versions)
		if !known && strict {
			return false, "version unknown for " + c
		}
		if known && !ok {
			return false, "version " + c
		}
	}
	return true, ""
}

// matchAny: an empty restriction matches; an unknown env value matches
// unless strict.
func matchAny(allowed, have []string, strict bool) bool {
	if len(allowed) == 0 {
		return true
	}
	known := false
	for _, h := range have {
		if h == "" {
			continue
		}
		known = true
		for _, a := range allowed {
			if strings.EqualFold(a, h) {
				return true
			}
		}
	}
	return !known && !strict
}

// satisfies evaluates one constraint against the known versions.
func satisfies(c string, versions map[string]string) (ok, known bool) {
	m := constraintRe.FindStringSubmatch(c)
	if m == nil {
		return false, true
	}
	cur, has := versions[m[1]]
	if !has || cur == "" {
		return false, false
	}
	cur = strings.TrimPrefix(cur, "v")
	want := strings.TrimPrefix(m[3], "v")
	if m[2] == "~" {
		return strings.HasPrefix(cur, want), true
	}
	cmp := compareVersions(cur, want)
	switch m[2] {
	case "=":
		return cmp == 0, true
	case "!=":
		return cmp != 0, true
	case ">=":
		return cmp >= 0, true
	case "<=":
		return cmp <= 0, true
	case ">":
		return cmp > 0, true
	case "<":
		return cmp < 0, true
	}
	return false, true
}

// compareVersions compares dotted versions numerically where possible
// ("1.28.3" > "1.9"), falling back to string order per segment.
func compareVersions(a, b string) int {
	as, bs := strings.FieldsFunc(a, sep), strings.FieldsFunc(b, sep)
	for i := 0; i < max(len(as), len(bs)); i++ {
		var x, y string
		if i < len(as) {
			x = as[i]
		}
		if i < len(bs) {
			y = bs[i]
		}
		xi, ex := strconv.Atoi(x)
		yi, ey := strconv.Atoi(y)
		switch {
		case ex == nil && ey == nil:
			if xi != yi {
				if xi < yi {
					return -1
				}
				return 1
			}
		case x != y:
			if x == "" {
				return -1
			}
			if y == "" {
				return 1
			}
			return strings.Compare(x, y)
		}
	}
	return 0
}

func sep(r rune) bool { return r == '.' || r == '-' || r == '+' }

// OutcomeBoost ranks memories by their verified track record: unverified
// memories are neutral, verified successes lift up to +30%, a failure
// majority sinks a memory (Laplace-smoothed success rate).
func OutcomeBoost(v Verification) float64 {
	n := v.SuccessCount + v.FailureCount
	if n == 0 {
		return 1
	}
	rate := (float64(v.SuccessCount) + 1) / (float64(n) + 2)
	conf := 1 - math.Exp(-float64(n)/3)
	return 1 + conf*(rate-0.5)*0.6
}

// CanPromote checks the promotion rules of a procedural memory sourced
// from Works (§14.3 ⑤ ⑥): a playbook becomes ACTIVE only after at least
// one machine-verified success, with no failure majority, approved by a
// reviewer who did not create it.
func CanPromote(m Memory, actor string) error {
	if m.Applicability == nil || m.Applicability.Kind != "playbook" {
		return nil
	}
	if actor == "" || actor == m.CreatedBy {
		return fmt.Errorf("a playbook must be approved by a reviewer other than its creator")
	}
	if m.Verification.SuccessCount < 1 {
		return fmt.Errorf("a playbook needs at least one verified successful outcome before promotion")
	}
	if m.Verification.FailureCount > m.Verification.SuccessCount {
		return fmt.Errorf("a playbook with more verified failures (%d) than successes (%d) cannot be promoted",
			m.Verification.FailureCount, m.Verification.SuccessCount)
	}
	return nil
}
