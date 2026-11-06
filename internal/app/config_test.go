package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "go-sre-harness/plugins/all"
)

func envMap(m map[string]string) Env { return func(k string) string { return m[k] } }

func names(f File, kind string) string {
	var out []string
	for _, s := range f.Plugins[kind].Enabled() {
		out = append(out, s.Name)
	}
	return strings.Join(out, ",")
}

func TestFromEnvDefaults(t *testing.T) {
	f, err := FromEnv(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"artifact_store": "fs", "broker": "local", "stream_bus": "local", "model_provider": "scripted",
		"tools": "demo", "authenticator": "dev", "strategy": "llm,rule_based", "job": "consolidator,rollout_monitor,retention",
		"api": "memory,eval,extensions", "policy": "default", "grader": "substring", "embedder": "hashing", "memory_retriever": "hybrid"}
	for k, v := range want {
		if got := names(f, k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	f, err = FromEnv(envMap(map[string]string{"ANTHROPIC_API_KEY": "k", "HARNESS_OPENAI_BASE_URL": "http://x", "HARNESS_OPENAI_MODEL": "m",
		"HARNESS_NATS_URL": "nats://n", "HARNESS_AUTH_MODE": "token,oidc", "HARNESS_STRATEGY": "rule"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := names(f, "model_provider"); got != "anthropic,openai,scripted" {
		t.Fatalf("providers %q", got)
	}
	if names(f, "broker") != "nats" || names(f, "authenticator") != "token,oidc" || f.Strategy.Default != "rule_based" ||
		f.Strategy.Aliases["llm"] != "rule_based" {
		t.Fatalf("%+v", f)
	}
	if _, err := FromEnv(envMap(map[string]string{"HARNESS_AUTH_MODE": "magic"})); err == nil {
		t.Fatal("unknown auth mode must fail")
	}
}

func TestLoadFileMergesYAMLWithEnvExpansion(t *testing.T) {
	t.Setenv("TEST_PROM_URL", `http://prom:9090/"q"`)
	dir := t.TempDir()
	p := filepath.Join(dir, "h.yaml")
	os.WriteFile(p, []byte(`
settings: {model_alias: sre-fast}
plugins:
  tools:
    - demo
    - {name: prometheus, config: {url: "${TEST_PROM_URL}"}}
  artifact_store: {name: fs, config: {root: "${UNSET_VAR:-/tmp/x}"}}
`), 0o600)
	base, _ := FromEnv(envMap(nil))
	f, err := LoadFile(p, base)
	if err != nil {
		t.Fatal(err)
	}
	if f.Settings.ModelAlias != "sre-fast" || names(f, "tools") != "demo,prometheus" || names(f, "broker") != "local" {
		t.Fatalf("%+v", f)
	}
	if got := string(f.Plugins["tools"][1].Config); got != `{"url":"http://prom:9090/\"q\""}` {
		t.Fatalf("expanded config = %s", got)
	}
	if got := string(f.Plugins["artifact_store"][0].Config); got != `{"root":"/tmp/x"}` {
		t.Fatalf("default expansion = %s", got)
	}

	os.WriteFile(p, []byte("plugins:\n  toolz: [demo]\n"), 0o600)
	if _, err := LoadFile(p, base); err == nil || !strings.Contains(err.Error(), "unknown plugin kinds") {
		t.Fatalf("unknown kind: %v", err)
	}
	os.WriteFile(p, []byte(`{"plugin": {}}`), 0o600)
	if _, err := LoadFile(p, base); err == nil {
		t.Fatal("unknown top-level field must fail")
	}
}
