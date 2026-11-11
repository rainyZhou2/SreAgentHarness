// Package governance decides what a deployment is allowed to turn on
// (design §4). The rollout switches of the work engine stay what they are;
// this package validates them against the deployment tier before the
// engine starts, derives the rollout stage (G0..G5) they amount to, and
// produces a machine-readable launch attestation.
//
// The gate never widens anything. When a production deployment asks for
// production writes without the evidence that makes them safe (a policy,
// an audit sink, authenticated callers, an action allow-list, healthy
// required sources), the result is investigation mode: Works, evidence and
// recommendations keep working, actions are not dispatched. One
// environment variable cannot open production writes.
package governance

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Tier is the operator-declared deployment tier (HARNESS_DEPLOYMENT_TIER).
type Tier string

const (
	TierLocal      Tier = "local"
	TierStaging    Tier = "staging"
	TierProduction Tier = "production"
)

// ParseTier reads a tier; empty is local.
func ParseTier(s string) (Tier, error) {
	switch t := Tier(strings.ToLower(strings.TrimSpace(s))); t {
	case "":
		return TierLocal, nil
	case TierLocal, TierStaging, TierProduction:
		return t, nil
	}
	return "", fmt.Errorf("unknown deployment tier %q (local|staging|production)", s)
}

// Stage is the rollout stage the flags amount to.
type Stage string

const (
	G0 Stage = "G0" // legacy: v1 only
	G1 Stage = "G1" // shadow: events mirrored and compared, no Works from events
	G2 Stage = "G2" // read-only: Works, watches, RCA, drafts; no actions
	G3 Stage = "G3" // dry-run: preflight and approval, dispatch simulated
	G4 Stage = "G4" // bounded production: allow-listed actions with human approval
	G5 Stage = "G5" // scoped automation: pre-authorized idempotent actions
)

// Flags are the rollout switches the gate looks at (a copy of the work
// engine's, so this package does not depend on it).
type Flags struct {
	Engine                  bool `json:"engine"`
	Shadow                  bool `json:"shadow"`
	ActionV2                bool `json:"action_v2"`
	ActionDryRun            bool `json:"action_dry_run"`
	RequireKnownTarget      bool `json:"require_known_target"`
	RequireDistinctApprover bool `json:"require_distinct_approver"`
	InlineTemplates         bool `json:"inline_templates"`
	// ActionAllowlist are the actions that may be dispatched (empty: all
	// registered). ActionScope are the resources they may target.
	ActionAllowlist []string `json:"action_allowlist,omitempty"`
	ActionScope     []string `json:"action_scope,omitempty"`
	// Preauthorized are actions that may run without a per-action approval.
	Preauthorized     []string `json:"preauthorized,omitempty"`
	ModelMaxDataClass string   `json:"model_max_data_class,omitempty"`
}

// SourceStatus is one data source as the gate sees it.
type SourceStatus struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	State    string `json:"state"`
	Healthy  bool   `json:"healthy"`
	Probed   bool   `json:"probed"`
	Required bool   `json:"required,omitempty"`
}

// ActionInfo is one registered action.
type ActionInfo struct {
	Name            string `json:"name"`
	Risk            string `json:"risk"`
	IdempotencyMode string `json:"idempotency_mode"`
	HasVersionFact  bool   `json:"has_version_fact"`
}

// Input is everything the gate judges.
type Input struct {
	Tier          Tier
	Flags         Flags
	BuildSHA      string
	SchemaVersion string
	// ExpectedSchema is the newest migration compiled into the binary.
	ExpectedSchema string
	Store          string // postgres | sqlite
	AuthMode       string // dev | token | oidc
	ArtifactStore  string // fs | memory | s3
	// PolicyVersion and PolicyCustom describe the policy gate: Custom is
	// false for the built-in default.
	PolicyVersion string
	PolicyCustom  bool
	PolicyDigest  string
	AuditSink     bool
	Capabilities  []string
	Actions       []ActionInfo
	MetricSources []string
	DataSources   []SourceStatus
	// ConfigDigest identifies the effective configuration (no secrets).
	ConfigDigest string
	Now          time.Time
}

// Check statuses.
const (
	Pass = "pass"
	Fail = "fail"
	Warn = "warn"
	Skip = "skip"
)

// Check is one gate result with a repair hint. It never contains a secret,
// a token or an endpoint URL.
type Check struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Fix    string `json:"fix,omitempty"`
	// Blocks names what a failed check takes away: "start" (refuse to
	// boot), "actions" (investigation mode), or "" (advisory).
	Blocks string `json:"blocks,omitempty"`
}

// Attestation is the launch attestation of a deployment.
type Attestation struct {
	BuildSHA      string            `json:"build_sha"`
	SchemaVersion string            `json:"schema_version"`
	Tier          string            `json:"tier"`
	Stage         Stage             `json:"stage"`
	Capabilities  []string          `json:"capabilities"`
	DataSources   []string          `json:"data_sources"`
	PolicyDigest  string            `json:"policy_digest"`
	ConfigDigest  string            `json:"config_digest"`
	Gates         map[string]string `json:"gates"`
	GeneratedAt   time.Time         `json:"generated_at"`

	// RequestedStage is what the flags asked for; Stage is what the gate
	// allows. They differ when the deployment was degraded.
	RequestedStage Stage `json:"requested_stage"`
	// Mode: normal | investigation (actions withheld) | refused.
	Mode    string   `json:"mode"`
	Checks  []Check  `json:"checks"`
	Reasons []string `json:"reasons,omitempty"`
	// Effective are the flags the engine must run with.
	Effective Flags `json:"effective_flags"`
}

// Modes.
const (
	ModeNormal        = "normal"
	ModeInvestigation = "investigation"
	ModeRefused       = "refused"
)

// OK reports whether the deployment may start.
func (a Attestation) OK() bool { return a.Mode != ModeRefused }

// StageOf derives the rollout stage from the flags.
func StageOf(f Flags) Stage {
	switch {
	case !f.Engine:
		return G0
	case f.Shadow:
		return G1
	case !f.ActionV2:
		return G2
	case f.ActionDryRun:
		return G3
	case len(f.Preauthorized) > 0:
		return G5
	}
	return G4
}

// Evaluate judges a deployment. It has no side effects and calls nothing:
// the caller collects the input (including source probes).
func Evaluate(in Input) Attestation {
	a := Attestation{BuildSHA: in.BuildSHA, SchemaVersion: in.SchemaVersion, Tier: string(in.Tier),
		Capabilities: append([]string(nil), in.Capabilities...), PolicyDigest: in.PolicyDigest, ConfigDigest: in.ConfigDigest,
		Gates: map[string]string{}, GeneratedAt: in.Now, RequestedStage: StageOf(in.Flags), Effective: in.Flags, Mode: ModeNormal}
	sort.Strings(a.Capabilities)
	for _, s := range in.DataSources {
		a.DataSources = append(a.DataSources, s.ID)
	}
	sort.Strings(a.DataSources)
	add := func(c Check) {
		a.Checks = append(a.Checks, c)
		a.Gates[c.ID] = c.Status
	}
	check := func(id string, ok bool, blocks, detail, fix string) bool {
		c := Check{ID: id, Status: Pass}
		if !ok {
			c.Status, c.Detail, c.Fix, c.Blocks = Fail, detail, fix, blocks
			if blocks == "" {
				c.Status = Warn
			}
		}
		add(c)
		return ok
	}
	prod := in.Tier == TierProduction
	team := in.Tier != TierLocal
	f := in.Flags

	// ---- always ----
	check("schema.current", in.ExpectedSchema == "" || in.SchemaVersion == in.ExpectedSchema, "start",
		fmt.Sprintf("database schema is %q, this build needs %q", in.SchemaVersion, in.ExpectedSchema), "run: harness migrate")
	if team {
		check("store.postgres", in.Store == "postgres", "start",
			"team deployments need PostgreSQL; SQLite is single-writer and local only", "set HARNESS_DATABASE_URL to a PostgreSQL database")
		check("auth.not_dev", in.AuthMode != "dev" && in.AuthMode != "", "start",
			"development authentication trusts caller-supplied tenant headers", "configure token or OIDC authentication")
		check("templates.no_inline", !f.InlineTemplates || !prod, "start",
			"inline Work templates let API callers define their own stages", "unset HARNESS_WORK_INLINE_TEMPLATES in production")
	}
	if prod {
		check("artifact.durable", in.ArtifactStore != "memory" && in.ArtifactStore != "", "start",
			"the in-memory artifact store loses evidence on restart", "configure the fs or s3 artifact store")
		check("model.data_class", f.ModelMaxDataClass != "restricted", "",
			"restricted data may be sent to the model provider", "set HARNESS_MODEL_MAX_DATA_CLASS to confidential or lower")
	}

	// ---- observability (G2+) ----
	if f.Engine && !f.Shadow {
		missing, unhealthy := []string{}, []string{}
		for _, s := range in.DataSources {
			if !s.Required {
				continue
			}
			switch {
			case s.State == "DISABLED":
				missing = append(missing, s.ID)
			case s.Probed && !s.Healthy:
				unhealthy = append(unhealthy, s.ID)
			}
		}
		block := ""
		if prod && f.ActionV2 {
			// Outcome verification reads these sources: without them an
			// action's effect could not be judged.
			block = "actions"
		}
		check("sources.required", len(missing) == 0 && len(unhealthy) == 0, block,
			fmt.Sprintf("required sources disabled %v, unhealthy %v", missing, unhealthy),
			"run: harness doctor --checks sources; fix or re-enable the listed sources")
	} else {
		add(Check{ID: "sources.required", Status: Skip})
	}

	// ---- actions (G3+) ----
	if f.ActionV2 {
		block := ""
		if prod {
			block = "actions"
		}
		check("policy.custom", in.PolicyCustom || !prod, block,
			"production writes run under the built-in default policy", "configure a policy plugin with rules for this environment")
		check("audit.sink", in.AuditSink || !team, block,
			"approvals and break-glass releases would not be audited", "enable the audit log (API role with a database)")
		check("action.known_target", f.RequireKnownTarget || !prod, block,
			"actions may target resources the world state does not know", "set HARNESS_ACTION_REQUIRE_KNOWN_TARGET=true")
		check("action.distinct_approver", f.RequireDistinctApprover || !prod, block,
			"a Work owner may approve their own action", "set HARNESS_REQUIRE_DISTINCT_APPROVER=true")
		if !f.ActionDryRun {
			check("action.allowlist", len(f.ActionAllowlist) > 0 && len(f.ActionScope) > 0 || !prod, block,
				"every registered action may be dispatched on every resource",
				"set HARNESS_ACTION_ALLOWLIST and HARNESS_ACTION_SCOPE to the piloted actions and resources")
			var unversioned, unknownAllowed []string
			known := map[string]ActionInfo{}
			for _, x := range in.Actions {
				known[x.Name] = x
			}
			for _, name := range f.ActionAllowlist {
				x, ok := known[name]
				switch {
				case !ok:
					unknownAllowed = append(unknownAllowed, name)
				case x.Risk == "R2" && !x.HasVersionFact:
					unversioned = append(unversioned, name)
				}
			}
			check("action.allowlist_known", len(unknownAllowed) == 0, block,
				fmt.Sprintf("allow-listed actions are not registered: %v", unknownAllowed), "fix HARNESS_ACTION_ALLOWLIST or enable the capability")
			check("action.target_version", len(unversioned) == 0 || !prod, block,
				fmt.Sprintf("R2 actions without a pinned target version: %v", unversioned),
				"only allow-list R2 actions whose descriptor declares a version fact")
		}
		if len(f.Preauthorized) > 0 {
			var bad []string
			known := map[string]ActionInfo{}
			for _, x := range in.Actions {
				known[x.Name] = x
			}
			for _, name := range f.Preauthorized {
				x, ok := known[name]
				if !ok || x.IdempotencyMode == "NONE" || x.IdempotencyMode == "" || x.Risk == "R3" {
					bad = append(bad, name)
				}
			}
			check("action.preauthorized_idempotent", len(bad) == 0, block,
				fmt.Sprintf("pre-authorized actions must be registered, idempotent or inspectable, and below R3: %v", bad),
				"remove them from HARNESS_ACTION_PREAUTHORIZED")
			check("action.preauthorized_scoped", len(f.ActionScope) > 0, block,
				"pre-authorization without a resource scope", "set HARNESS_ACTION_SCOPE")
		}
	}

	// ---- verdict ----
	for _, c := range a.Checks {
		if c.Status != Fail {
			continue
		}
		a.Reasons = append(a.Reasons, c.ID+": "+c.Detail)
		switch c.Blocks {
		case "start":
			a.Mode = ModeRefused
		case "actions":
			if a.Mode != ModeRefused {
				a.Mode = ModeInvestigation
			}
		}
	}
	if a.Mode == ModeInvestigation {
		// Degrade, never widen: keep the read path, withhold dispatch.
		a.Effective.ActionV2, a.Effective.ActionDryRun, a.Effective.Preauthorized = false, false, nil
	}
	a.Stage = StageOf(a.Effective)
	if a.Mode == ModeRefused {
		a.Stage = G0
	}
	return a
}

// Digest hashes a configuration value for the attestation. Callers pass
// structures without secrets (credentials are references in this system).
func Digest(v any) string {
	h, err := cpkit.Hash(v)
	if err != nil {
		return ""
	}
	return h
}

// ReleaseGate compares a candidate against a baseline (design §4.3
// `harness release-gate`): the candidate may not fail a gate the baseline
// passed, may not run at a higher stage than the baseline unless every gate
// passes, and may not lose a capability or a data source.
type GateDiff struct {
	OK          bool     `json:"ok"`
	Regressions []string `json:"regressions,omitempty"`
	Notes       []string `json:"notes,omitempty"`
}

// ReleaseGate judges a candidate attestation against a baseline.
func ReleaseGate(baseline, candidate Attestation) GateDiff {
	d := GateDiff{}
	if !candidate.OK() {
		d.Regressions = append(d.Regressions, "candidate is refused: "+strings.Join(candidate.Reasons, "; "))
	}
	for id, st := range baseline.Gates {
		if st == Pass && candidate.Gates[id] == Fail {
			d.Regressions = append(d.Regressions, "gate "+id+" passed in the baseline and fails in the candidate")
		}
	}
	if candidate.Stage > baseline.Stage {
		failed := false
		for _, st := range candidate.Gates {
			failed = failed || st == Fail
		}
		if failed {
			d.Regressions = append(d.Regressions, fmt.Sprintf("candidate advances from %s to %s with failing gates", baseline.Stage, candidate.Stage))
		} else {
			d.Notes = append(d.Notes, fmt.Sprintf("stage advances from %s to %s", baseline.Stage, candidate.Stage))
		}
	}
	if candidate.Mode == ModeInvestigation && baseline.Mode == ModeNormal {
		d.Regressions = append(d.Regressions, "candidate is degraded to investigation mode")
	}
	lost := func(kind string, base, cand []string) {
		have := map[string]bool{}
		for _, c := range cand {
			have[c] = true
		}
		for _, b := range base {
			if !have[b] {
				d.Regressions = append(d.Regressions, kind+" "+b+" is missing in the candidate")
			}
		}
	}
	lost("capability", baseline.Capabilities, candidate.Capabilities)
	lost("data source", baseline.DataSources, candidate.DataSources)
	if baseline.SchemaVersion > candidate.SchemaVersion {
		d.Regressions = append(d.Regressions, "candidate schema is older than the baseline")
	}
	sort.Strings(d.Regressions)
	d.OK = len(d.Regressions) == 0
	return d
}
