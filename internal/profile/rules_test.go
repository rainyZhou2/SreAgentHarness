package profile

import (
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
)

func compile(t *testing.T, spec string) Rules {
	t.Helper()
	s, err := Parse(json.RawMessage(spec))
	if err != nil {
		t.Fatal(err)
	}
	r, err := Compile("p@1", s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRulesAllowlistOverridesAndApprovals(t *testing.T) {
	gate := policy.Guarded(policy.Default())
	read := tools.Descriptor{Name: "prom.query", Risk: domain.RiskR0}
	write := tools.Descriptor{Name: "k8s.rollout_restart", Risk: domain.RiskR2}

	legacy := compile(t, `{"strategy":"llm"}`)
	if legacy.Restricted() || !legacy.AllowsTool("anything") {
		t.Fatal("a spec without tools stays unrestricted (legacy)")
	}
	none := compile(t, `{"tools":[]}`)
	if !none.Restricted() || none.AllowsTool("prom.query") {
		t.Fatal("tools: [] must allow nothing")
	}

	r := compile(t, `{"tools":["prom.*","k8s.rollout_restart"],
		"risk_overrides":{"prom.query":"R1","k8s.rollout_restart":"R0"},
		"approval_rules":{"min_approvals":{"R2":2}}}`)
	if !r.AllowsTool("prom.alerts") || r.AllowsTool("k8s.deployment_status") || r.AllowsTool("prometheus") {
		t.Fatal("pattern matching")
	}
	if v := r.Visible([]tools.Descriptor{write, read, {Name: "demo.metrics"}}); len(v) != 2 || v[1].Name != "prom.query" || v[1].Risk != domain.RiskR1 {
		t.Fatalf("visible = %+v", v)
	}
	// Raised risk: R0 read now needs approval.
	pd, eff := r.Decide(gate, "prom.query", &read, nil)
	if pd.Verdict != policy.RequireApproval || eff.Risk != domain.RiskR1 {
		t.Fatalf("raised override: %+v %+v", pd, eff)
	}
	// Lowering is ignored; the approval rule raises R2 to two approvals.
	pd, eff = r.Decide(gate, write.Name, &write, nil)
	if eff.Risk != domain.RiskR2 || pd.Verdict != policy.RequireApproval || pd.RequiredApprovals != 2 {
		t.Fatalf("lowered override / approval rule: %+v %+v", pd, eff)
	}
	pd, _ = r.Decide(gate, "demo.restart", &tools.Descriptor{Name: "demo.restart", Risk: domain.RiskR2}, nil)
	if pd.Verdict != policy.Deny || !strings.Contains(pd.Reason, "not allowed by profile") {
		t.Fatalf("allowlist: %+v", pd)
	}
	// Approval rules never turn a deny into anything else.
	r3 := compile(t, `{"tools":["*"],"approval_rules":{"min_approvals":{"R3":1}}}`)
	if pd, _ := r3.Decide(gate, "x", &tools.Descriptor{Name: "x", Risk: domain.RiskR3}, nil); pd.Verdict != policy.Deny {
		t.Fatalf("R3 must stay denied: %+v", pd)
	}
	pinned := compile(t, `{"tools":["*"],"policy_version":"policy-v9"}`)
	if pd, _ := pinned.Decide(gate, read.Name, &read, nil); pd.Verdict != policy.Deny || !strings.Contains(pd.Reason, "policy-v9") {
		t.Fatalf("policy pin: %+v", pd)
	}
}

func TestCompileRejectsInvalidRules(t *testing.T) {
	for _, spec := range []string{
		`{"tools":["pro*m"]}`,
		`{"risk_overrides":{"x":"R7"}}`,
		`{"approval_rules":{"min_approvals":{"R2":9}}}`,
		`{"approval_rules":{"typo":{}}}`,
		`{"model_alias":"big","allowed_aliases":["small"]}`,
		`{"max_steps":-1}`,
	} {
		s, err := Parse(json.RawMessage(spec))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Compile("p@1", s); err == nil {
			t.Fatalf("%s must be rejected", spec)
		}
	}
}
