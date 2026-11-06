package kernel_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/retention"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/stream"
)

func TestAuthRBACAuditAndSeparationOfDuties(t *testing.T) {
	cfg := pg.DefaultConfig()
	cfg.RequireDistinctApprover = true
	h := start(t, opts{cfg: cfg})
	toks, err := auth.NewStaticTokens([]auth.StaticToken{
		{SHA256: auth.HashToken("view"), Tenant: tenant, Subject: "vera", Roles: []auth.Role{auth.Viewer}},
		{SHA256: auth.HashToken("op"), Tenant: tenant, Subject: "otto", Roles: []auth.Role{auth.Operator}},
		{SHA256: auth.HashToken("appr"), Tenant: tenant, Subject: "ada", Roles: []auth.Role{auth.Approver}},
		{SHA256: auth.HashToken("opappr"), Tenant: tenant, Subject: "sam", Roles: []auth.Role{auth.Operator, auth.Approver}},
		{SHA256: auth.HashToken("admin"), Tenant: tenant, Subject: "root", Roles: []auth.Role{auth.Admin}},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := obs.NewMetrics()
	srv := httptest.NewServer((&api.Server{Store: h.store, Artifacts: h.arts, DefaultProfile: "sre-diagnosis",
		DefaultVersion: 1, PollInterval: 20 * time.Millisecond, Log: quiet, Audit: h.store, Admin: h.store, Metrics: m,
		Auth: &auth.Chain{Authenticators: []auth.Authenticator{toks}, Roles: h.store}}).Handler())
	defer srv.Close()

	do := func(tok, method, path, key, body string, hdr ...string) (int, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
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

	if code, _ := do("", "GET", "/v1/whoami", "", ""); code != 401 {
		t.Fatalf("no credentials: %d", code)
	}
	if code, _ := do("", "GET", "/v1/whoami", "", "", "X-Tenant-ID", tenant, "X-Actor-ID", "mallory"); code != 401 {
		t.Fatalf("dev headers must be ignored when tokens are configured: %d", code)
	}
	if code, _ := do("op", "GET", "/v1/whoami", "", "", "X-Tenant-ID", "tenant-b"); code != 403 {
		t.Fatalf("token used for another tenant: %d", code)
	}
	if code, who := do("view", "GET", "/v1/whoami", "", ""); code != 200 || who["subject"] != "vera" {
		t.Fatalf("whoami: %d %v", code, who)
	}
	if code, _ := do("view", "POST", "/v1/runs", "k1", `{"goal":"remediate service=checkout"}`); code != 403 {
		t.Fatalf("viewer created a run: %d", code)
	}
	code, body := do("sam", "POST", "/v1/runs", "k1", `{"goal":"remediate service=checkout"}`)
	if code != 401 {
		t.Fatalf("unknown token: %d", code)
	}
	code, body = do("opappr", "POST", "/v1/runs", "k1", `{"goal":"remediate service=checkout"}`)
	if code != 202 {
		t.Fatalf("operator create: %d %v", code, body)
	}
	runID := body["run_id"].(string)
	h.waitStatus(t, runID, domain.RunWaitingApproval)

	_, list := do("view", "GET", "/v1/runs/"+runID+"/approvals", "", "")
	appr := list["approvals"].([]any)[0].(map[string]any)
	approve := func(tok, key string) (int, map[string]any) {
		v := h.ver(t, runID)
		b, _ := json.Marshal(map[string]any{"type": "approve_tool", "expected_version": *v, "payload": map[string]any{
			"approval_request_id": appr["id"], "action_hash": appr["action_hash"]}})
		return do(tok, "POST", "/v1/runs/"+runID+"/commands", key, string(b))
	}
	if code, _ := approve("op", "a1"); code != 403 {
		t.Fatalf("operator approved: %d", code)
	}
	if code, out := approve("opappr", "a2"); code != 422 || out["error"] != "approver_is_initiator" {
		t.Fatalf("initiator approved own action: %d %v", code, out)
	}
	if code, out := approve("appr", "a3"); code != 202 {
		t.Fatalf("distinct approver: %d %v", code, out)
	}
	h.waitStatus(t, runID, domain.RunCompleted)

	// Role bindings from the DB extend token roles.
	if code, _ := do("op", "POST", "/v1/admin/role-bindings", "", `{"principal_id":"vera","role":"operator"}`); code != 403 {
		t.Fatalf("operator managed roles: %d", code)
	}
	if code, out := do("admin", "POST", "/v1/admin/role-bindings", "", `{"principal_id":"vera","role":"operator"}`); code != 200 {
		t.Fatalf("bind: %d %v", code, out)
	}
	if code, _ := do("view", "POST", "/v1/runs", "k2", `{"goal":"diagnose service=checkout"}`); code != 202 {
		t.Fatalf("viewer with DB operator binding: %d", code)
	}

	_, audit := do("admin", "GET", "/v1/admin/audit?limit=100", "", "")
	seen := map[string]int{}
	for _, e := range audit["entries"].([]any) {
		x := e.(map[string]any)
		seen[x["action"].(string)+"/"+x["outcome"].(string)]++
	}
	for _, want := range []string{"authenticate/denied", "authorize/denied", "command.approve_tool/denied",
		"command.approve_tool/rejected", "command.approve_tool/applied", "role.bind/applied"} {
		if seen[want] == 0 {
			t.Fatalf("missing audit %s in %v", want, seen)
		}
	}

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	ms := string(mb)
	for _, want := range []string{`harness_http_requests_total{method="POST",route="/v1/runs",code="403"}`,
		`harness_http_requests_total{method="POST",route="/v1/runs/{run_id}/commands",code="202"}`} {
		if !strings.Contains(ms, want) {
			t.Fatalf("metrics missing %s:\n%s", want, ms)
		}
	}
	if strings.Contains(ms, runID) {
		t.Fatal("metrics must not contain run ids")
	}
}

func TestRetentionCleaner(t *testing.T) {
	h := start(t, opts{strategyFn: llmStrategy(gw.DefaultPolicy(false, false, ""), "sre-reasoning",
		cognitive.ScriptedModel(cognitive.RuleBased{}))})
	ctx := context.Background()
	run := h.create(t, "diagnose latency service=checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)

	// A memory that expired an hour ago, and a stream segment past its TTL.
	exp := time.Now().Add(-time.Hour)
	mem := seed(t, h, memory.NewMemory{Scope: "service", ScopeID: "checkout", Type: "semantic", Status: "ACTIVE",
		Subject: "checkout:ttl", Title: "temporary note", Content: "feature flag X is on during the migration window",
		ExpiresAt: &exp})
	reqs, err := h.store.ListModelRequests(ctx, tenant, run.ID)
	if err != nil || len(reqs) == 0 {
		t.Fatalf("model requests: %v %d", err, len(reqs))
	}
	ref, hash, _ := h.arts.Put(ctx, tenant, "stream-segment", []byte("deltas"))
	if err := h.store.InsertStreamSegment(ctx, stream.Segment{TenantID: tenant, RunID: run.ID, ModelRequestID: reqs[0].ID,
		No: 99, FirstSeq: 1, LastSeq: 2, Ref: ref, Hash: hash, ExpiresAt: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}

	c := &retention.Cleaner{Store: h.store, Artifacts: h.arts, Cfg: retention.Config{OutboxRetention: time.Nanosecond},
		Log: quiet, Now: func() time.Time { return time.Now().Add(time.Second) }}
	res, err := c.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outbox == 0 || res.Segments < 1 || res.SegmentObjects != res.Segments || res.Memories != 1 {
		t.Fatalf("retention: %+v", res)
	}
	if _, err := h.arts.Get(ctx, tenant, ref); err != artifact.ErrNotFound {
		t.Fatalf("segment object must be deleted: %v", err)
	}
	got, _ := h.store.GetMemory(ctx, tenant, mem.ID)
	if got.Status != "STALE" {
		t.Fatalf("expired memory status %s", got.Status)
	}
}
