package workengine

import "testing"

func TestFlagsEnableCurrentCapabilitiesByDefault(t *testing.T) {
	f := FlagsFromEnv(func(string) string { return "" })
	if !f.Engine || !f.Actions || f.Shadow || !f.InlineTemplates ||
		!f.RequireKnownTarget || !f.RequireDistinctApprover || !f.EvidencePlanning {
		t.Fatalf("unexpected defaults: %+v", f)
	}
	if f.ActionDryRun || len(f.Preauthorized) != 0 || f.TriggerMirror {
		t.Fatalf("unsafe defaults: %+v", f)
	}
}

func TestActionsEnvironmentCurrentOnly(t *testing.T) {
	tests := []struct {
		name    string
		current string
		want    bool
	}{
		{name: "current enables", current: "true", want: true},
		{name: "current disables", current: "false", want: false},
		{name: "invalid current fails closed", current: "invalid", want: false},
		{name: "unset enables", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := actionEnabled(func(key string) string {
				if key == "HARNESS_ACTIONS_ENABLED" {
					return tc.current
				}
				return ""
			})
			if got != tc.want {
				t.Fatalf("actionEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRemovedLegacyActionVariableIsIgnored(t *testing.T) {
	got := actionEnabled(func(key string) string {
		if key == "HARNESS_ACTION_V2_ENABLED" {
			return "false"
		}
		return ""
	})
	if !got {
		t.Fatal("removed legacy variable must not disable current Actions")
	}
}

func TestFlagsKeepExplicitRollbackControls(t *testing.T) {
	env := map[string]string{
		"HARNESS_WORK_ENGINE_ENABLED": "false",
		"HARNESS_EVENT_SHADOW_MODE":   "true",
		"HARNESS_ACTIONS_ENABLED":     "false",
	}
	f := FlagsFromEnv(func(key string) string { return env[key] })
	if f.Engine || f.Actions || !f.Shadow || !f.TriggerMirror {
		t.Fatalf("rollback controls were ignored: %+v", f)
	}
}
