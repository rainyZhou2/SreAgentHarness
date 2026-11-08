package localapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadLocalFile(t *testing.T) {
	dir := t.TempDir()
	if f, err := loadLocalFile(filepath.Join(dir, "missing.json"), false); err != nil || f.Policy != nil {
		t.Fatalf("missing default file: %+v %v", f, err)
	}
	if _, err := loadLocalFile(filepath.Join(dir, "missing.json"), true); err == nil {
		t.Fatal("missing explicit file must fail")
	}
	write := func(body string) string {
		p := filepath.Join(dir, "c.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	f, err := loadLocalFile(write(`{
		"policy":{"rules":[{"name":"freeze","match":{"tools":["*"]},"effect":"deny"}]},
		"triggers":[{"name":"patrol","goal":"review","schedule":{"interval":"1h"}}],
		"context_providers":[{"type":"static","facts":[{"kind":"owner","summary":"team a"}]}],
		"tool_overlays":{"svc.restart":{"labels":{"kind":"restart"}}}
	}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if f.Triggers[0].Tenant != tenant || len(f.ContextProviders) != 1 || f.Policy == nil {
		t.Fatalf("loaded %+v", f)
	}
	for _, body := range []string{
		`{"unknown":1}`,
		`{"triggers":[{"name":"x","goal":"g","tenant":"other"}]}`,
		`{"tool_overlays":{"a":{"time_args":{}}}}`,
	} {
		if _, err := loadLocalFile(write(body), true); err == nil {
			t.Errorf("%s should be rejected", body)
		}
	}
}
