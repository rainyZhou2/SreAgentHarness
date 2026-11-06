package app

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/testutil"
)

// The first-batch extension points work together in an assembled process:
// a file profile source supplies the default profile, the run completes
// through the configured routing/context/compaction plugins, and a webhook
// event sink receives the signed run.completed event.
func TestProfileSourceAndEventSinkEndToEnd(t *testing.T) {
	pool := testutil.Pool(t)
	dir := t.TempDir()

	var mu sync.Mutex
	var got []domain.Event
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m := hmac.New(sha256.New, []byte("s3cret"))
		m.Write(body)
		if r.Header.Get("X-Harness-Signature") != "sha256="+hex.EncodeToString(m.Sum(nil)) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		var e domain.Event
		_ = json.Unmarshal(body, &e)
		mu.Lock()
		got = append(got, e)
		mu.Unlock()
	}))
	defer hook.Close()

	profiles := filepath.Join(dir, "profiles")
	os.Mkdir(profiles, 0o700)
	os.WriteFile(filepath.Join(profiles, "triage.yaml"), []byte(`
id: ops-triage
version: 3
spec:
  strategy: rule_based
  tools: [demo.metrics]
  max_steps: 8
`), 0o600)
	cfg := filepath.Join(dir, "harness.yaml")
	os.WriteFile(cfg, []byte(`
settings:
  default_profile: ops-triage
  default_profile_version: 3
models:
  circuit_failure_threshold: 5
  circuit_cooldown: 10s
plugins:
  artifact_store: memory
  tools: [demo]
  job: []
  profile_source: [builtin, {name: file, config: {path: `+profiles+`}}]
  route_policy: {name: weighted, config: {cost_usd: 100}}
  context_assembler: {name: sectioned, config: {instructions: "You triage incidents."}}
  compactor: {name: deterministic, config: {keep: 3}}
  event_sink:
    - {name: webhook, config: {url: "`+hook.URL+`", secret: s3cret, types: [run.completed], start_from: earliest}}
    - {name: log, instance: audit-log, config: {types: ["approval.*"]}}
`), 0o600)
	env := envMap(map[string]string{"HARNESS_CONFIG": cfg, "HARNESS_HTTP_ADDR": "127.0.0.1:0", "HARNESS_STRATEGY": "rule"})
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := Build(ctx, "all", env, pool, log)
	if err != nil {
		t.Fatal(err)
	}
	active := map[string]bool{}
	for _, x := range a.PluginReport().Active {
		active[x.Kind+"/"+x.Instance] = true
	}
	for _, want := range []string{"profile_source/file", "profile_source/builtin", "route_policy/weighted",
		"context_assembler/sectioned", "compactor/deterministic", "event_sink/webhook", "event_sink/audit-log"} {
		if !active[want] {
			t.Fatalf("%s not active: %v", want, a.PluginReport().Active)
		}
	}
	if len(a.sinks) != 2 {
		t.Fatalf("dispatchers = %d", len(a.sinks))
	}

	srv := httptest.NewServer(a.Handler)
	defer srv.Close()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	req, _ := http.NewRequest("POST", srv.URL+"/v1/runs", strings.NewReader(`{"goal":"checkout latency is high"}`))
	req.Header.Set("X-Tenant-ID", testutil.Tenant)
	req.Header.Set("X-Actor-ID", "alice")
	req.Header.Set("X-Roles", "operator")
	req.Header.Set("Idempotency-Key", "ext-points-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		RunID string `json:"run_id"`
	}
	json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if resp.StatusCode >= 300 || created.RunID == "" {
		t.Fatalf("create run: %d %+v", resp.StatusCode, created)
	}
	run, err := a.Store.GetRun(ctx, testutil.Tenant, created.RunID)
	if err != nil || run.ProfileID != "ops-triage" || run.ProfileVersion != 3 {
		t.Fatalf("run must use the file-sourced default profile: %v %+v", err, run)
	}

	testutil.Eventually(t, 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range got {
			if e.RunID == run.ID && e.Type == "run.completed" {
				return true
			}
		}
		return false
	}, "webhook never received run.completed for %s", run.ID)
	mu.Lock()
	for _, e := range got {
		if e.Type != "run.completed" {
			t.Errorf("type filter let %s through", e.Type)
		}
	}
	mu.Unlock()
	st, err := a.Store.EventSinkStatuses(ctx)
	if err != nil || len(st) != 2 {
		t.Fatalf("sink statuses: %v %+v", err, st)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop")
	}

	// Profile versions are immutable: changing version 3 fails startup.
	os.WriteFile(filepath.Join(profiles, "triage.yaml"), []byte(`
id: ops-triage
version: 3
spec: {strategy: rule_based, tools: [demo.metrics, demo.restart]}
`), 0o600)
	if _, err := Build(context.Background(), "api", env, pool, log); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("changed profile version must fail startup, got %v", err)
	}
}
