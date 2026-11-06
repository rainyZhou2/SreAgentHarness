package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/testutil"
)

// replace_pending_input goes through the same profile rules as the kernel:
// the fresh approval keeps the profile's two-approval requirement.
func TestReplacePendingInputKeepsProfileApprovalRules(t *testing.T) {
	pool := testutil.Pool(t)
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "gated.yaml"), []byte(`
id: gated
version: 1
spec:
  strategy: rule_based
  tools: ["demo.*"]
  approval_rules: {min_approvals: {R2: 2}}
`), 0o600)
	cfg := filepath.Join(dir, "harness.yaml")
	os.WriteFile(cfg, []byte(`
settings: {default_profile: gated, default_profile_version: 1}
plugins:
  artifact_store: memory
  tools: [demo]
  job: []
  profile_source: {name: file, config: {path: `+dir+`/gated.yaml}}
`), 0o600)
	env := envMap(map[string]string{"HARNESS_CONFIG": cfg, "HARNESS_HTTP_ADDR": "127.0.0.1:0", "HARNESS_STRATEGY": "rule"})
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := Build(ctx, "all", env, pool, log)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler)
	defer srv.Close()
	go a.Run(ctx)

	call := func(method, path, roles, key, body string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", testutil.Tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", roles)
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	code, body := call("POST", "/v1/runs", "operator", "gated-1", `{"goal":"remediate latency service=checkout"}`)
	if code != 202 {
		t.Fatalf("create: %d %v", code, body)
	}
	runID := body["run_id"].(string)
	required := func() (int, string) {
		var n int
		var step string
		_ = pool.QueryRow(ctx, `SELECT ar.required_approvals, ai.step_id::text FROM approval_requests ar
			JOIN action_intents ai ON ai.id = ar.action_intent_id
			WHERE ai.run_id=$1 ORDER BY ar.created_at DESC LIMIT 1`, runID).Scan(&n, &step)
		return n, step
	}
	testutil.Eventually(t, 20*time.Second, func() bool {
		r, _ := a.Store.GetRun(ctx, testutil.Tenant, runID)
		return r.Status == domain.RunWaitingApproval
	}, "run never waited for approval")
	n, stepID := required()
	if n != 2 {
		t.Fatalf("profile approval rule: required %d", n)
	}
	r, _ := a.Store.GetRun(ctx, testutil.Tenant, runID)
	cmd := fmt.Sprintf(`{"type":"replace_pending_input","expected_version":%d,"reason":"other service",
		"payload":{"step_id":%q,"args":{"service":"payments"}}}`, r.Version, stepID)
	if code, body := call("POST", "/v1/runs/"+runID+"/commands", "operator", "replace-1", cmd); code != 202 {
		t.Fatalf("replace: %d %v", code, body)
	}
	if n, _ := required(); n != 2 {
		t.Fatalf("replaced input lost the profile's approval rule: required %d", n)
	}
}
