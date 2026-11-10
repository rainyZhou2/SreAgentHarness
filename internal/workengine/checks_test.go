package workengine

import (
	"context"
	"testing"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidenceplan"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

// The planner sees what the kernel would enforce for the run: the profile
// allowlist, its risk overrides and the policy gate.
func TestKernelChecksUseRunConstraints(t *testing.T) {
	tool := func(name string, risk domain.RiskClass) tools.Tool {
		return &tools.Func{Desc: tools.Descriptor{Name: name, Version: "1", Risk: risk, Trust: "T3"}}
	}
	reg := tools.NewRegistry(tool("prom.query", domain.RiskR0), tool("k8s.events", domain.RiskR0),
		tool("logs.raw", domain.RiskR0), tool("deployment.restart", domain.RiskR1))
	var asked struct {
		profile string
		version int64
	}
	srv := &api.Server{Tools: reg, Policy: policy.Engine{Version: "p", Environment: "production"},
		DefaultProfile: "sre-diagnosis", DefaultVersion: 3,
		Rules: func(_ context.Context, _, profileID string, version int64) (profile.Rules, error) {
			asked.profile, asked.version = profileID, version
			return profile.Compile("sre@3", profile.Spec{Tools: []string{"prom.*", "k8s.events", "deployment.restart"},
				RiskOverrides: map[string]string{"k8s.events": "R1"}})
		}}
	ds, cons, err := KernelChecks{Server: srv}.Checks(context.Background(), "t1", "", 0, "key")
	if err != nil {
		t.Fatal(err)
	}
	if asked.profile != "sre-diagnosis" || asked.version != 3 {
		t.Fatalf("resolved %s@%d", asked.profile, asked.version)
	}
	cands, ex := evidenceplan.Filter(ds, cons)
	if len(cands) != 1 || cands[0].Name != "prom.query" {
		t.Fatalf("candidates: %v", cands)
	}
	reasons := map[string]string{}
	for _, e := range ex {
		reasons[e.Tool] = e.Reason
	}
	if reasons["logs.raw"] != evidenceplan.ExclNotVisible || reasons["k8s.events"] != evidenceplan.ExclNotReadOnly ||
		reasons["deployment.restart"] != evidenceplan.ExclNotReadOnly {
		t.Fatalf("exclusions: %v", reasons)
	}
}
