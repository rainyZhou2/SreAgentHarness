package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"go-sre-harness/internal/tools"

	_ "go-sre-harness/examples/plugins/hello"
)

func TestAppBuildsFromConfigAndRunsWithPlugins(t *testing.T) {
	pool := testutil.Pool(t)
	dir := t.TempDir()
	cfg := filepath.Join(dir, "harness.yaml")
	os.WriteFile(cfg, []byte(`
plugins:
  artifact_store: memory
  tools: [demo, {name: hello, config: {greeting: Hi}}]
  policy: {name: freeze_window, config: {deny: [demo.restart]}}
  api: [memory, eval, hello, extensions]
  job: []
`), 0o600)
	env := envMap(map[string]string{"HARNESS_CONFIG": cfg, "HARNESS_HTTP_ADDR": "127.0.0.1:0", "HARNESS_STRATEGY": "rule"})
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := Build(ctx, "all", env, pool, log)
	if err != nil {
		t.Fatal(err)
	}

	rep := a.PluginReport()
	if !strings.Contains(strings.Join(rep.Tools, ","), "example.hello") {
		t.Fatalf("tools = %v", rep.Tools)
	}
	active := map[string]bool{}
	for _, x := range rep.Active {
		active[x.Kind+"/"+x.Instance] = true
	}
	for _, want := range []string{"policy/freeze_window", "api/hello", "artifact_store/memory", "authenticator/dev", "job/consolidator"} {
		if want == "job/consolidator" {
			if active[want] {
				t.Fatalf("job list was emptied in config, but %s is active", want)
			}
			continue
		}
		if !active[want] {
			t.Fatalf("%s not active: %v", want, rep.Active)
		}
	}

	// Plugin policy denies demo.restart; the framework floor still applies to R2.
	restart, _ := a.Host.Tools.Lookup("demo.restart")
	d := restart.Descriptor()
	if v := a.Host.Policy.Evaluate(&d, "checkout"); v.Verdict != "deny" {
		t.Fatalf("freeze_window must deny, got %+v", v)
	}
	r2 := tools.Descriptor{Name: "x", Risk: domain.RiskR2}
	if v := a.Host.Policy.Evaluate(&r2, ""); v.Verdict != "require_approval" {
		t.Fatalf("R2 floor: %+v", v)
	}

	srv := httptest.NewServer(a.Handler)
	defer srv.Close()
	var sessionID string
	get := func(path, roles string) (int, map[string]any) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.Header.Set("X-Tenant-ID", testutil.Tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", roles)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body
	}
	if code, body := get("/v1/example/hello", "viewer"); code != 200 || body["message"] != "hello from a plugin" {
		t.Fatalf("plugin route: %d %v", code, body)
	}
	if code, _ := get("/v1/admin/plugins", "viewer"); code != 403 {
		t.Fatalf("plugins listing must need admin, got %d", code)
	}
	if code, body := get("/v1/admin/plugins", "admin"); code != 200 || body["active"] == nil {
		t.Fatalf("admin plugins: %d %v", code, body)
	}

	// The assembled process executes a run end to end.
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	req, _ := http.NewRequest("POST", srv.URL+"/v1/runs", strings.NewReader(`{"goal":"checkout latency is high"}`))
	req.Header.Set("X-Tenant-ID", testutil.Tenant)
	req.Header.Set("X-Actor-ID", "alice")
	req.Header.Set("X-Roles", "operator")
	req.Header.Set("Idempotency-Key", "app-test-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var run struct {
		ID     string `json:"id"`
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	json.NewDecoder(resp.Body).Decode(&run)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("create run: %d", resp.StatusCode)
	}
	id := run.ID
	if id == "" {
		id = run.RunID
	}
	deadline := time.Now().Add(20 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		_, body := get("/v1/runs/"+id, "viewer")
		if r, ok := body["run"].(map[string]any); ok {
			status, _ = r["status"].(string)
		}
		if status == string(domain.RunCompleted) || status == string(domain.RunWaitingApproval) || status == string(domain.RunFailed) ||
			status == string(domain.RunNeedsHuman) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if status != string(domain.RunCompleted) {
		t.Fatalf("run %s ended in %q", id, status)
	}

	// GET /v1/runs lists the run (with its goal) for viewers, filters by
	// status, validates limit and is tenant scoped.
	listed := func(path, tenant string) (int, []map[string]any) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		req.Header.Set("X-Tenant-ID", tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", "viewer")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body struct {
			Runs []map[string]any `json:"runs"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, body.Runs
	}
	code, runs := listed("/v1/runs?limit=200", testutil.Tenant)
	found := false
	for _, r := range runs {
		if r["id"] == id {
			found = r["goal"] == "checkout latency is high" && r["display_status"] == "COMPLETED"
		}
	}
	if code != 200 || !found {
		t.Fatalf("list runs: %d, run %s not listed with goal: %v", code, id, runs)
	}
	if _, runs := listed("/v1/runs?status=FAILED&limit=200", testutil.Tenant); len(runs) > 0 {
		for _, r := range runs {
			if r["status"] != "FAILED" {
				t.Fatalf("status filter leaked %v", r["status"])
			}
		}
	}
	if code, _ := listed("/v1/runs?limit=0", testutil.Tenant); code != 400 {
		t.Fatalf("limit=0 must be rejected, got %d", code)
	}
	if _, runs := listed("/v1/runs", "other-tenant-"+id[:8]); len(runs) != 0 {
		t.Fatalf("other tenant sees %d runs", len(runs))
	}

	// steps?expand=io inlines step inputs and outputs (tool args, observations).
	_, body := get("/v1/runs/"+id+"/steps?expand=io", "viewer")
	steps, _ := body["steps"].([]any)
	sawToolIO := false
	for _, x := range steps {
		st := x.(map[string]any)
		if st["kind"] == "tool" && st["status"] == "SUCCEEDED" {
			in, _ := st["input"].(map[string]any)
			out, _ := st["output"].(map[string]any)
			sawToolIO = in["name"] != nil && out["content"] != nil
		}
	}
	if !sawToolIO {
		t.Fatalf("expand=io: no tool step with inlined input/output: %v", steps)
	}
	if _, body := get("/v1/runs/"+id+"/steps", "viewer"); strings.Contains(fmt.Sprint(body), "output:map") {
		t.Fatalf("steps without expand must not inline outputs")
	}

	// GET /v1/sessions lists a session with its turn count.
	post := func(path, body, key string) int {
		req, _ := http.NewRequest("POST", srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", testutil.Tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", "operator")
		req.Header.Set("Idempotency-Key", key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if s, ok := out["session"].(map[string]any); ok {
			sessionID, _ = s["id"].(string)
		}
		return resp.StatusCode
	}
	if code := post("/v1/sessions", `{"title":"desktop chat"}`, "sess-1-"+id); code != 201 {
		t.Fatalf("create session: %d", code)
	}
	if code := post("/v1/sessions/"+sessionID+"/turns", `{"text":"checkout latency is high"}`, "turn-1-"+id); code != 202 {
		t.Fatalf("submit turn: %d", code)
	}
	_, body = get("/v1/sessions?limit=200", "viewer")
	sessions, _ := body["sessions"].([]any)
	listedSession := false
	for _, x := range sessions {
		s := x.(map[string]any)
		if s["id"] == sessionID {
			listedSession = s["title"] == "desktop chat" && s["turns"] == float64(1)
		}
	}
	if !listedSession {
		t.Fatalf("session %s not listed with title and 1 turn: %v", sessionID, sessions)
	}
	if code, _ := get("/v1/sessions?limit=999", "viewer"); code != 400 {
		t.Fatalf("sessions limit=999 must be rejected, got %d", code)
	}

	// Rename, search, archive, restore and delete a session.
	patch := func(path, body, roles string) int {
		req, _ := http.NewRequest("PATCH", srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", testutil.Tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", roles)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	sessionIDs := func(path string) string {
		_, body := get(path, "viewer")
		var ids []string
		for _, x := range body["sessions"].([]any) {
			ids = append(ids, x.(map[string]any)["id"].(string))
		}
		return strings.Join(ids, ",")
	}
	if code := patch("/v1/sessions/"+sessionID, `{"title":"renamed"}`, "viewer"); code != 403 {
		t.Fatalf("viewer rename: %d", code)
	}
	if code := patch("/v1/sessions/"+sessionID, `{"title":"Checkout pool incident `+id[:8]+`"}`, "operator"); code != 200 {
		t.Fatalf("rename: %d", code)
	}
	if !strings.Contains(sessionIDs("/v1/sessions?q=pool+INCIDENT+"+id[:8]), sessionID) || strings.Contains(sessionIDs("/v1/sessions?q=nomatch-"+id[:8]), sessionID) {
		t.Fatal("title search")
	}
	if code := patch("/v1/sessions/"+sessionID, `{"status":"archived"}`, "operator"); code != 200 {
		t.Fatalf("archive: %d", code)
	}
	if strings.Contains(sessionIDs("/v1/sessions?limit=200"), sessionID) || !strings.Contains(sessionIDs("/v1/sessions?status=archived&limit=200"), sessionID) {
		t.Fatal("archived session must move to the archived list")
	}
	if code := patch("/v1/sessions/"+sessionID, `{"status":"bogus"}`, "operator"); code != 400 {
		t.Fatalf("bad status: %d", code)
	}
	if code := patch("/v1/sessions/"+sessionID, `{"status":"deleted"}`, "operator"); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if strings.Contains(sessionIDs("/v1/sessions?status=archived&limit=200"), sessionID) {
		t.Fatal("deleted session still listed")
	}
	if code := patch("/v1/sessions/"+sessionID, `{"status":"active"}`, "operator"); code != 404 {
		t.Fatalf("a deleted session cannot be restored: %d", code)
	}
	if code, body := get("/v1/approvals", "viewer"); code != 200 || body["approvals"] == nil {
		t.Fatalf("approvals: %d %v", code, body)
	}

	// Extensions: admins manage them, viewers read them, other tenants
	// never see them, and the default profile (v2) allows runtime tools.
	put := func(path, body, roles, tenant string) int {
		req, _ := http.NewRequest("PUT", srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", tenant)
		req.Header.Set("X-Actor-ID", "alice")
		req.Header.Set("X-Roles", roles)
		req.Header.Set("Idempotency-Key", "ext-"+roles+"-"+id)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	skill := `{"spec":{"description":"triage","instructions":"Always cite a runbook."}}`
	if code := put("/v1/extensions/skills/app-test", skill, "operator", testutil.Tenant); code != 403 {
		t.Fatalf("operator must not change extensions, got %d", code)
	}
	if code := put("/v1/extensions/skills/app-test", skill, "admin", testutil.Tenant); code != 200 {
		t.Fatalf("admin put skill: %d", code)
	}
	_, body = get("/v1/extensions", "viewer")
	if !strings.Contains(fmt.Sprint(body["skills"]), "app-test") {
		t.Fatalf("skill not listed: %v", body)
	}
	req, _ = http.NewRequest("GET", srv.URL+"/v1/extensions", nil)
	req.Header.Set("X-Tenant-ID", "other-tenant-"+id[:8])
	req.Header.Set("X-Actor-ID", "mallory")
	req.Header.Set("X-Roles", "viewer")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(b), "app-test") {
			t.Fatalf("other tenant sees the skill: %s", b)
		}
	}
	_, body = get("/v1/tools", "viewer")
	if body["profile"] != "sre-diagnosis@2" {
		t.Fatalf("default profile = %v", body["profile"])
	}
	// (Skill rendering into prompts is covered by the cognitive and
	// extensions tests; this config uses the rule strategy, which makes no
	// model call and so writes no prompt manifest.)
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop")
	}
}
