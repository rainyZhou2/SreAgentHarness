package app

import (
	"strings"
	"testing"

	"go-sre-harness/internal/security"
)

func TestSecurityModeConfig(t *testing.T) {
	for input, want := range map[string]security.Mode{
		"": security.ModeLegacy, "legacy": security.ModeLegacy,
		"shadow": security.ModeShadow, "enforce": security.ModeEnforce,
	} {
		got, err := securityMode(func(string) string { return input })
		if err != nil || got != want {
			t.Fatalf("%q: got %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := securityMode(func(string) string { return "off" }); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

func TestEnforceRequiresSecurityDependencies(t *testing.T) {
	err := validateSecurity(security.ModeEnforce, security.Dependencies{})
	if err == nil || !strings.Contains(err.Error(), "recorder") {
		t.Fatalf("missing recorder must fail startup: %v", err)
	}
}

func TestSecurityConfigPreservesRules(t *testing.T) {
	zero := 0
	base := File{Security: SecurityConfig{Constraints: []security.ConstraintRule{{Name: "base", Version: "v1"}}}}
	merged, err := merge(base, File{Security: SecurityConfig{Constraints: []security.ConstraintRule{{Name: "rollout", Version: "v2", CanaryPercent: &zero}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Security.Constraints) != 1 || merged.Security.Constraints[0].Name != "rollout" {
		t.Fatalf("security constraints were not merged: %+v", merged.Security)
	}
	if _, err := security.CompileConstraints(merged.Security.Constraints); err != nil {
		t.Fatal(err)
	}
}
