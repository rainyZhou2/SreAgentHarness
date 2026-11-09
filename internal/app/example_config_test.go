package app

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/trigger"
)

func TestExampleConfigLoads(t *testing.T) {
	f, err := LoadFile("../../deploy/harness.example.yaml", File{})
	if err != nil {
		t.Fatalf("example config: %v", err)
	}
	if f.Triggers == nil && f.ToolOverlays == nil {
		t.Log("example sections are empty (commented)")
	}
}

// TestExampleCommentedSectionsAreValid uncomments the documented examples
// and loads them, so the docs cannot drift from the schema.
func TestExampleCommentedSectionsAreValid(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/harness.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// Uncomment the documented blocks that start with one of these keys and
	// drop their empty placeholders and the default policy line.
	heads := []string{"policy:", "context_provider:", "triggers:", "tool_overlays:"}
	skip := map[string]bool{"triggers: []": true, "tool_overlays: {}": true, "context_provider: []": true}
	commented := regexp.MustCompile(`^(\s*)# (.*)$`)
	var out []string
	in := false
	for _, l := range strings.Split(string(raw), "\n") {
		trim := strings.TrimSpace(l)
		if skip[trim] || strings.HasPrefix(trim, "policy: {name: default") {
			continue
		}
		m := commented.FindStringSubmatch(l)
		if m == nil {
			in = false
			out = append(out, l)
			continue
		}
		for _, h := range heads {
			if strings.HasPrefix(m[2], h) {
				in = true
			}
		}
		if in && !strings.HasPrefix(m[2], " ") && !startsWithAny(m[2], heads) {
			in = false
		}
		if in {
			out = append(out, m[1]+m[2])
		} else {
			out = append(out, l)
		}
	}
	p := t.TempDir() + "/full.yaml"
	if err := os.WriteFile(p, []byte(strings.Join(out, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := LoadFile(p, File{})
	if err != nil {
		t.Fatalf("uncommented example: %v\n%s", err, strings.Join(out, "\n"))
	}
	if len(f.Triggers) != 2 || len(f.ToolOverlays) != 2 {
		t.Fatalf("triggers=%d overlays=%d", len(f.Triggers), len(f.ToolOverlays))
	}
	if _, err := trigger.New(f.Triggers, nil, nil, nil); err != nil {
		t.Fatalf("example triggers: %v", err)
	}
	for name, o := range f.ToolOverlays {
		if err := o.Validate(name); err != nil {
			t.Fatalf("example overlay: %v", err)
		}
	}
	pol := f.Plugins["policy"].Enabled()
	if len(pol) != 1 || pol[0].Name != "rules" {
		t.Fatalf("policy spec %+v", pol)
	}
	var rc policy.RuleConfig
	if err := strictJSON(pol[0].Config, &rc); err != nil {
		t.Fatalf("rules config: %v", err)
	}
	if _, err := policy.NewRuleEngine(rc, nil); err != nil {
		t.Fatalf("rules engine: %v", err)
	}
	for _, s := range f.Plugins["context_provider"].Enabled() {
		var c envctx.Config
		if err := strictJSON(s.Config, &c); err != nil {
			t.Fatalf("context provider %s: %v", s.Name, err)
		}
		c.Type = s.Name
		if _, err := envctx.Build(c, envctx.Deps{Registry: tools.NewRegistry(), Dependencies: nopDeps{}}); err != nil {
			t.Fatalf("context provider %s: %v", s.Name, err)
		}
	}
}

func startsWithAny(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

type nopDeps struct{}

func (nopDeps) ServiceDependencies(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func strictJSON(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}

// TestLoadFileKeepsWorkSection guards the v2 section: templates and
// correlation rules in HARNESS_CONFIG must reach the work engine.
func TestLoadFileKeepsWorkSection(t *testing.T) {
	path := t.TempDir() + "/harness.json"
	body := `{"work":{"templates":[{"id":"t","version":1,"kind":"incident","stages":[{"id":"c","type":"confirm"}]}],
	  "correlation_rules":[{"name":"r","match":{"event_types":["alert.firing"]},"key":"dedup_key","template":"t","work_kind":"incident","title":"${type} on ${subject} (${HARNESS_TEST_UNSET_VAR})"}]}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := LoadFile(path, File{})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Work.Templates) != 1 || f.Work.Templates[0].ID != "t" || len(f.Work.CorrelationRules) != 1 {
		t.Fatalf("work section dropped: %+v", f.Work)
	}
	// Template placeholders survive environment expansion; unset upper-case
	// variables still expand to empty.
	if got := f.Work.CorrelationRules[0].Title; got != "${type} on ${subject} ()" {
		t.Fatalf("title: %q", got)
	}
}
