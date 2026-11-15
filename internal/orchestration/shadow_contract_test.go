package orchestration

import (
	"encoding/json"
	"testing"

	"go-sre-harness/internal/trigger"
)

func TestShadowRowJSONContract(t *testing.T) {
	row := ShadowRow{
		LegacyAction:    trigger.ActionCreated,
		LegacyRunID:     "run-1",
		CurrentDecision: "created",
		CurrentWouldDo:  "create",
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"legacy_action": trigger.ActionCreated,
		"legacy_run_id": "run-1",
		"decision":      "created",
		"would_do":      "create",
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %q; JSON: %s", key, got[key], want, raw)
		}
	}
	for _, key := range []string{"v1_action", "v1_run_id", "v2_decision", "v2_would"} {
		if _, ok := got[key]; ok {
			t.Errorf("removed generation-versioned key %q remains in JSON: %s", key, raw)
		}
	}
}
