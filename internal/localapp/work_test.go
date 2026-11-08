package localapp

import (
	"testing"

	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/orchestration"
)

// The shipped v2 demo config must load and validate.
func TestV2DemoConfig(t *testing.T) {
	f, err := loadLocalFile("../../scripts/v2/harness.local.json", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.WorkExtensions) != 1 || f.WorkExtensions[0].Name != "sre-sim" {
		t.Fatalf("work extensions: %+v", f.WorkExtensions)
	}
	cat, err := orchestration.NewCatalog(f.Work.Templates)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"auto-rollback", "investigate-and-rollback"} {
		if _, ok := cat.Get(id); !ok {
			t.Fatalf("template %s missing", id)
		}
	}
	r, err := eventing.NewRouter(f.Work.CorrelationRules)
	if err != nil {
		t.Fatal(err)
	}
	e := eventing.Envelope{Type: "alert.firing", Labels: map[string]string{"alertname": "HighErrorRate"}}
	if rule, ok := r.Route(&e); !ok || rule.Template != "auto-rollback" {
		t.Fatalf("route: %+v %v", rule, ok)
	}
}
