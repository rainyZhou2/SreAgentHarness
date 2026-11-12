package localapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseEnv(t *testing.T) {
	v, err := ParseEnv("# c\nANTHROPIC_API_KEY=\"sk-1\"\n\nHARNESS_OPENAI_MODEL = gpt-x\n")
	if err != nil || v["ANTHROPIC_API_KEY"] != "sk-1" || v["HARNESS_OPENAI_MODEL"] != "gpt-x" {
		t.Fatalf("%v %v", v, err)
	}
	for _, bad := range []string{"PATH=/evil", "LD_PRELOAD=x", "nonsense", "=x"} {
		if _, err := ParseEnv(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestSaveAndLoadEnv(t *testing.T) {
	dir := t.TempDir()
	if err := SaveEnv(dir, map[string]string{"ANTHROPIC_API_KEY": "sk-a", "HARNESS_LOCAL_WEB": "off"}); err != nil {
		t.Fatal(err)
	}
	if err := SaveEnv(dir, map[string]string{"HARNESS_LOCAL_WEB": ""}); err != nil { // remove
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, EnvFileName))
	if !strings.Contains(string(b), "ANTHROPIC_API_KEY=sk-a") || strings.Contains(string(b), "HARNESS_LOCAL_WEB") {
		t.Fatalf("merge: %s", b)
	}
	if runtime.GOOS != "windows" {
		if st, _ := os.Stat(filepath.Join(dir, EnvFileName)); st.Mode().Perm() != 0o600 {
			t.Fatalf("mode %v", st.Mode().Perm())
		}
	}
	if SaveEnv(dir, map[string]string{"PATH": "x"}) == nil || SaveEnv(dir, map[string]string{"HARNESS_X": "a\nb"}) == nil {
		t.Fatal("disallowed key / multi-line value must be rejected")
	}
	// The real environment wins over the file.
	t.Setenv("ANTHROPIC_API_KEY", "from-env")
	set, err := LoadEnvFile(dir)
	if err != nil || len(set) != 0 || os.Getenv("ANTHROPIC_API_KEY") != "from-env" {
		t.Fatalf("env must win: %v %v", set, err)
	}
	if set, err := LoadEnvFile(t.TempDir()); err != nil || set != nil {
		t.Fatalf("missing file is fine: %v %v", set, err)
	}
}

func TestProbeProvider(t *testing.T) {
	var gotPath, gotKey, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotKey, gotAuth = r.URL.Path, r.Header.Get("x-api-key")+r.Header.Get("x-goog-api-key"), r.Header.Get("Authorization")
		if r.Header.Get("x-api-key") == "bad" || r.Header.Get("Authorization") == "Bearer bad" {
			w.WriteHeader(401)
			return
		}
		if r.URL.RawQuery != "" {
			t.Errorf("the key must not travel in the URL: %q", r.URL.RawQuery)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	if err := ProbeProvider(ctx, nil, ProviderAnthropic, srv.URL, "good"); err != nil || gotPath != "/v1/models" || gotKey != "good" {
		t.Fatalf("anthropic: %v %s %s", err, gotPath, gotKey)
	}
	if err := ProbeProvider(ctx, nil, ProviderAnthropic, srv.URL, "bad"); err == nil || !strings.Contains(err.Error(), "rejected the key") {
		t.Fatalf("bad key: %v", err)
	}
	if err := ProbeProvider(ctx, nil, ProviderOpenAI, srv.URL+"/v1", "good"); err != nil || gotPath != "/v1/models" || gotAuth != "Bearer good" {
		t.Fatalf("openai: %v %s %s", err, gotPath, gotAuth)
	}
	if err := ProbeProvider(ctx, nil, ProviderGemini, srv.URL, "g"); err != nil || gotKey != "g" {
		t.Fatalf("gemini: %v %s", err, gotKey)
	}
	if ProbeProvider(ctx, nil, ProviderOpenAI, "", "k") == nil || ProbeProvider(ctx, nil, "x", "", "k") == nil {
		t.Fatal("missing base URL / unknown provider must fail")
	}
	srv.Close()
	if err := ProbeProvider(ctx, nil, ProviderAnthropic, srv.URL, "k"); err == nil || !strings.Contains(err.Error(), "cannot reach") {
		t.Fatalf("unreachable: %v", err)
	}
}

func TestDoctor(t *testing.T) {
	dir := seedDir(t)
	env := func(string) string { return "" }
	checks := Doctor(context.Background(), DoctorOptions{DataDir: dir, Addr: "127.0.0.1:0", Getenv: env, SkipModels: true})
	by := map[string]Check{}
	for _, c := range checks {
		by[c.Name] = c
	}
	for _, n := range []string{"data directory", "api token", "database", "listen address", "model"} {
		if _, ok := by[n]; !ok {
			t.Errorf("missing check %q in %+v", n, checks)
		}
	}
	if by["database"].Level != OK || by["api token"].Level != OK {
		t.Fatalf("a fresh install should be healthy: %+v", checks)
	}
	// A corrupted token pair is reported.
	_ = os.WriteFile(filepath.Join(dir, "token"), []byte("tampered"), 0o600)
	for _, c := range Doctor(context.Background(), DoctorOptions{DataDir: dir, Getenv: env, SkipModels: true}) {
		if c.Name == "api token" && c.Level != Warn {
			t.Fatalf("tampered token must warn: %+v", c)
		}
	}
	// A key the provider rejects is a failure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	genv := func(k string) string {
		return map[string]string{"ANTHROPIC_API_KEY": "k", "HARNESS_ANTHROPIC_BASE_URL": srv.URL}[k]
	}
	failed := false
	for _, c := range Doctor(context.Background(), DoctorOptions{DataDir: dir, Getenv: genv}) {
		failed = failed || (c.Name == "model anthropic" && c.Level == Fail)
	}
	if !failed {
		t.Fatal("a rejected key must be a FAIL")
	}
}
