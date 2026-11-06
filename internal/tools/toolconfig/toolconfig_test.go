package toolconfig

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndBuild(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "tools.json")
	_ = os.WriteFile(p, []byte(`{"prometheus":{"url":"http://prom:9090"},
		"kubernetes":{"api_server":"https://k8s:6443","namespaces":["prod"]}}`), 0o600)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(context.Background(), c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	names := map[string]bool{}
	for _, d := range b.Registry.Descriptors() {
		names[d.Name] = true
	}
	for _, n := range []string{"demo.metrics", "demo.restart", "prom.query", "prom.alerts", "k8s.rollout_restart", "k8s.deployment_status"} {
		if !names[n] {
			t.Fatalf("missing %s in %v", n, names)
		}
	}
	_ = os.WriteFile(p, []byte(`{"prometheus":{"url":"http://prom:9090","risk":"R0"}}`), 0o600)
	if _, err := Load(p); err == nil {
		t.Fatal("unknown fields must be rejected")
	}
}
