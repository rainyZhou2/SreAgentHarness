package localapp

import (
	"io"
	"log/slog"
	"testing"

	"go-sre-harness/internal/tools"
)

func TestRegisterLocalTools(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	has := func(r *tools.Registry, n string) bool { _, ok := r.Lookup(n); return ok }

	r := tools.NewRegistry()
	registerLocalTools(r, env(nil), log)
	if !has(r, "web.fetch") || has(r, "prom.query") {
		t.Fatalf("default: web.fetch only, got fetch=%v prom=%v", has(r, "web.fetch"), has(r, "prom.query"))
	}
	r = tools.NewRegistry()
	registerLocalTools(r, env(map[string]string{"HARNESS_LOCAL_WEB": "off"}), log)
	if has(r, "web.fetch") {
		t.Fatal("HARNESS_LOCAL_WEB=off must remove web.fetch")
	}
	r = tools.NewRegistry()
	registerLocalTools(r, env(map[string]string{"HARNESS_LOCAL_PROMETHEUS_URL": "http://prom:9090"}), log)
	if !has(r, "prom.query") {
		t.Fatal("prometheus url must register prom tools")
	}
	r = tools.NewRegistry()
	registerLocalTools(r, env(map[string]string{"HARNESS_LOCAL_PROMETHEUS_URL": "not a url"}), log)
	if has(r, "prom.query") {
		t.Fatal("invalid prometheus url must not register tools")
	}
}
