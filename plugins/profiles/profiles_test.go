package profiles

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPathListsAndTenants(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(`
- {id: triage, version: 1, spec: {strategy: llm, tools: [demo.metrics]}}
- {id: triage, version: 2, tenant: tenant-x, spec: {strategy: rule_based, tools: []}}
`), 0o600)
	os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{"id":"remediate","version":1,"spec":{"strategy":"llm","tools":[]}}`), 0o600)
	os.WriteFile(filepath.Join(dir, "notes.txt"), []byte(`ignored`), 0o600)
	defs, err := LoadPath(dir, []string{"t1", "t2"}, "default")
	if err != nil {
		t.Fatal(err)
	}
	// triage@1 ×2 tenants, triage@2 for tenant-x only, remediate@1 ×2 tenants
	if len(defs) != 5 {
		t.Fatalf("defs = %+v", defs)
	}
	for _, d := range defs {
		if _, _, err := d.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte(`{id: x, version: 1, spec: {strategy: llm, toolz: []}}`), 0o600)
	defs, err = LoadPath(dir, nil, "default")
	if err != nil {
		t.Fatal(err)
	}
	bad := 0
	for _, d := range defs {
		if _, _, err := d.Validate(); err != nil {
			bad++
		}
	}
	if bad != 1 {
		t.Fatalf("unknown spec field must fail validation (bad=%d)", bad)
	}
}
