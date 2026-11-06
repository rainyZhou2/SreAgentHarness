package app

import "testing"

// The shipped example config must stay loadable (strict parsing).
func TestExampleConfigParses(t *testing.T) {
	base, err := FromEnv(envMap(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	f, err := LoadFile("../../deploy/harness.example.yaml", base)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"profile_source", "route_policy", "context_assembler", "compactor"} {
		if len(f.Plugins[k].Enabled()) != 1 {
			t.Fatalf("%s: %+v", k, f.Plugins[k])
		}
	}
}
