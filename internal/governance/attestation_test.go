package governance

import (
	"strings"
	"testing"
	"time"
)

func prodInput() Input {
	return Input{Tier: TierProduction, Store: "postgres", AuthMode: "oidc", ArtifactStore: "s3", SchemaVersion: "0031", ExpectedSchema: "0031",
		PolicyVersion: "prod-v3", PolicyCustom: true, AuditSink: true, Now: time.Unix(0, 0),
		Actions: []ActionInfo{{Name: "deployment.rollback", Risk: "R2", IdempotencyMode: "QUERY_BEFORE_RETRY", HasVersionFact: true},
			{Name: "ticket.comment", Risk: "R1", IdempotencyMode: "NATIVE"}, {Name: "legacy.script", Risk: "R2", IdempotencyMode: "NONE"}},
		DataSources: []SourceStatus{{ID: "prom", Kind: "metrics", State: "ACTIVE", Healthy: true, Probed: true, Required: true}},
		Flags: Flags{Engine: true, ActionV2: true, RequireKnownTarget: true, RequireDistinctApprover: true,
			ActionAllowlist: []string{"deployment.rollback"}, ActionScope: []string{"deployment://prod/search-api"}}}
}

func TestStages(t *testing.T) {
	for _, tc := range []struct {
		f    Flags
		want Stage
	}{
		{Flags{}, G0}, {Flags{Engine: true, Shadow: true}, G1}, {Flags{Engine: true}, G2},
		{Flags{Engine: true, ActionV2: true, ActionDryRun: true}, G3}, {Flags{Engine: true, ActionV2: true}, G4},
		{Flags{Engine: true, ActionV2: true, Preauthorized: []string{"x"}}, G5},
	} {
		if got := StageOf(tc.f); got != tc.want {
			t.Errorf("%+v: %s, want %s", tc.f, got, tc.want)
		}
	}
}

func TestProductionBoundedActionsPass(t *testing.T) {
	a := Evaluate(prodInput())
	if a.Mode != ModeNormal || a.Stage != G4 || !a.Effective.ActionV2 {
		t.Fatalf("%s %s %v", a.Mode, a.Stage, a.Reasons)
	}
}

// One environment variable must not open production writes: every missing
// safeguard degrades to investigation mode with actions off.
func TestProductionDegradesToInvestigation(t *testing.T) {
	for name, mut := range map[string]func(*Input){
		"default policy":     func(in *Input) { in.PolicyCustom = false },
		"no audit":           func(in *Input) { in.AuditSink = false },
		"no allowlist":       func(in *Input) { in.Flags.ActionAllowlist = nil },
		"no scope":           func(in *Input) { in.Flags.ActionScope = nil },
		"unknown target":     func(in *Input) { in.Flags.RequireKnownTarget = false },
		"self approval":      func(in *Input) { in.Flags.RequireDistinctApprover = false },
		"source unhealthy":   func(in *Input) { in.DataSources[0].Healthy = false },
		"source disabled":    func(in *Input) { in.DataSources[0].State = "DISABLED" },
		"unregistered":       func(in *Input) { in.Flags.ActionAllowlist = []string{"nope"} },
		"non-idempotent pre": func(in *Input) { in.Flags.Preauthorized = []string{"legacy.script"} },
	} {
		in := prodInput()
		mut(&in)
		a := Evaluate(in)
		if a.Mode != ModeInvestigation || a.Effective.ActionV2 || a.Stage != G2 || !a.OK() || len(a.Reasons) == 0 {
			t.Errorf("%s: mode=%s stage=%s action_v2=%v reasons=%v", name, a.Mode, a.Stage, a.Effective.ActionV2, a.Reasons)
		}
		if a.RequestedStage == a.Stage {
			t.Errorf("%s: the attestation must show the requested stage", name)
		}
	}
}

func TestProductionRefusals(t *testing.T) {
	for name, mut := range map[string]func(*Input){
		"dev auth":        func(in *Input) { in.AuthMode = "dev" },
		"sqlite":          func(in *Input) { in.Store = "sqlite" },
		"old schema":      func(in *Input) { in.SchemaVersion = "0027" },
		"memory artifact": func(in *Input) { in.ArtifactStore = "memory" },
		"inline":          func(in *Input) { in.Flags.InlineTemplates = true },
	} {
		in := prodInput()
		mut(&in)
		if a := Evaluate(in); a.OK() || a.Stage != G0 {
			t.Errorf("%s: must refuse to start, got %s %s", name, a.Mode, a.Stage)
		}
	}
}

// Local and staging keep the existing behaviour: the gate only reports.
func TestLocalIsPermissive(t *testing.T) {
	a := Evaluate(Input{Tier: TierLocal, Store: "sqlite", AuthMode: "dev", ArtifactStore: "memory",
		Flags: Flags{Engine: true, ActionV2: true}})
	if a.Mode != ModeNormal || !a.Effective.ActionV2 {
		t.Fatalf("local: %s %v", a.Mode, a.Reasons)
	}
}

func TestReleaseGate(t *testing.T) {
	base := Evaluate(prodInput())
	if d := ReleaseGate(base, base); !d.OK {
		t.Fatalf("identical: %v", d.Regressions)
	}
	in := prodInput()
	in.AuditSink = false
	in.DataSources = nil
	d := ReleaseGate(base, Evaluate(in))
	joined := strings.Join(d.Regressions, "\n")
	if d.OK || !strings.Contains(joined, "gate audit.sink") || !strings.Contains(joined, "data source prom is missing") ||
		!strings.Contains(joined, "investigation mode") {
		t.Fatalf("regressions: %v", d.Regressions)
	}
}
