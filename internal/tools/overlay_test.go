package tools

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

type stubTool struct{ d Descriptor }

func (s stubTool) Descriptor() Descriptor { return s.d }
func (s stubTool) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"ok":true}`), nil
}

func TestOverlayAppliesToLookupAndDescriptors(t *testing.T) {
	r := NewRegistry(stubTool{Descriptor{Name: "svc.restart", Risk: domain.RiskR1, Labels: map[string]string{"team": "a"}}})
	r.SetOverlays(map[string]Overlay{"svc.restart": {
		Labels:         map[string]string{"kind": "restart"},
		Lifecycle:      &Lifecycle{Verify: &FollowUp{Tool: "svc.health", Args: map[string]any{"service": "${args.service}"}}, Expect: "output.healthy == true"},
		TimeArgs:       &TimeArgs{Start: "from"},
		MaxInlineBytes: 1024,
	}})
	tool, ok := r.Lookup("svc.restart")
	if !ok {
		t.Fatal("lookup")
	}
	d := tool.Descriptor()
	if d.Labels["team"] != "a" || d.Labels["kind"] != "restart" || d.Lifecycle == nil || d.TimeArgs == nil || d.MaxInlineBytes != 1024 {
		t.Fatalf("descriptor %+v", d)
	}
	if out, err := tool.Invoke(context.Background(), "", nil); err != nil || string(out) != `{"ok":true}` {
		t.Fatalf("invoke passthrough: %s %v", out, err)
	}
	found := false
	for _, x := range r.Descriptors() {
		if x.Name == "svc.restart" && x.Lifecycle != nil {
			found = true
		}
	}
	if !found {
		t.Fatal("Descriptors must apply overlays")
	}
}

func TestOverlayValidation(t *testing.T) {
	bad := map[string]Overlay{
		"comp-without-verify": {Lifecycle: &Lifecycle{Compensate: &FollowUp{Tool: "x"}}},
		"empty-verify":        {Lifecycle: &Lifecycle{Verify: &FollowUp{}}},
		"self":                {Lifecycle: &Lifecycle{Verify: &FollowUp{Tool: "self"}}},
		"bad-expect":          {Lifecycle: &Lifecycle{Verify: &FollowUp{Tool: "v"}, Expect: "output.(("}},
		"attempts":            {Lifecycle: &Lifecycle{Verify: &FollowUp{Tool: "v"}, Attempts: 50}},
		"negative":            {Lifecycle: &Lifecycle{Verify: &FollowUp{Tool: "v"}, Interval: Duration(-time.Second)}},
		"time-empty":          {TimeArgs: &TimeArgs{}},
		"time-format":         {TimeArgs: &TimeArgs{Start: "s", Format: "iso"}},
	}
	for name, o := range bad {
		if err := o.Validate(name); err == nil {
			t.Errorf("%s should be rejected", name)
		}
	}
	ok := Overlay{Lifecycle: &Lifecycle{Verify: &FollowUp{Tool: "v"}, Compensate: &FollowUp{Tool: "c"}, Attempts: 5}}
	if err := ok.Validate("a"); err != nil {
		t.Fatalf("valid overlay rejected: %v", err)
	}
}

func TestDurationJSON(t *testing.T) {
	var v struct{ D Duration }
	for raw, want := range map[string]time.Duration{`{"D":"90s"}`: 90 * time.Second, `{"D":30}`: 30 * time.Second} {
		if err := json.Unmarshal([]byte(raw), &v); err != nil || v.D.D() != want {
			t.Errorf("%s -> %v %v", raw, v.D.D(), err)
		}
	}
}
