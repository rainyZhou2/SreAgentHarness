package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/curate"
	curatewire "go-sre-harness/internal/curate/wire"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
)

func TestMemoryCurationAPI(t *testing.T) {
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := memory.Writer{Embedder: memory.HashingEmbedder{Dims: 16}, Artifacts: artifact.NewMemory()}
	seed := func(subject, content string) string {
		p, err := w.Prepare(ctx, memory.NewMemory{TenantID: "t", Scope: "service", ScopeID: "checkout", Type: "semantic", Status: "ACTIVE",
			Subject: subject, Title: "pool size", Content: content, Trust: "T3", Sensitivity: "internal", CreatedBy: "seed"})
		if err != nil {
			t.Fatal(err)
		}
		m, _, err := st.CreateMemory(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond) // distinct update times
		return m.ID
	}
	oldID := seed("note:pool-old", "checkout database connection pool maximum size is 100 connections")
	seed("note:pool-new", "checkout database connection pool maximum size is 150 connections")

	cur := &curate.Curator{Memories: curatewire.Memories(st), Store: st}
	m := &api.MemoryAPI{Store: st, Writer: w, Server: &api.Server{}, Curator: cur}
	mux := http.NewServeMux()
	m.Register(mux)
	do := func(roles []auth.Role, method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t", Subject: "alice", Roles: roles}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		out := map[string]any{}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	viewer, approver := []auth.Role{auth.Viewer}, []auth.Role{auth.Viewer, auth.Operator, auth.Approver}

	code, out := do(viewer, "GET", "/v1/memory/curation?status=PENDING", "")
	if code != 200 || len(out["proposals"].([]any)) != 0 {
		t.Fatalf("an empty list is [] not null: %d %v", code, out)
	}
	code, out = do(approver, "POST", "/v1/memory/curation/run", "{}")
	if code != 200 || out["new_proposals"] != float64(1) {
		t.Fatalf("%d %v", code, out)
	}
	code, out = do(viewer, "GET", "/v1/memory/curation?status=PENDING", "")
	props := out["proposals"].([]any)
	if code != 200 || len(props) != 1 {
		t.Fatalf("%d %v", code, out)
	}
	p := props[0].(map[string]any)
	if p["signal"] != "superseded_fact" || len(p["evidence"].([]any)) == 0 {
		t.Fatalf("%v", p)
	}
	id := p["id"].(string)

	if code, _ := do(approver, "POST", "/v1/memory/curation/"+id+"/decide", `{"decision":"nope"}`); code != 400 {
		t.Fatalf("an unknown decision: %d", code)
	}
	if code, _ := do(approver, "POST", "/v1/memory/curation/"+id+"/decide", `{"decision":"apply","reason":"ok"}`); code != 200 {
		t.Fatalf("apply: %d", code)
	}
	got, _ := st.GetMemory(ctx, "t", oldID)
	if got.Status != "STALE" {
		t.Fatalf("applying sets the memory aside as STALE, got %s", got.Status)
	}
	if code, _ := do(approver, "POST", "/v1/memory/curation/"+id+"/decide", `{"decision":"apply"}`); code != 422 {
		t.Fatalf("a decided proposal cannot be applied again: %d", code)
	}
	if code, _ := do(approver, "POST", "/v1/memory/curation/missing/decide", `{"decision":"dismiss"}`); code != 404 {
		t.Fatalf("%d", code)
	}

	// Roles: running is an operator action, deciding an approver action.
	roles := m.RouteRoles()
	if roles["POST /v1/memory/curation/run"] != auth.Operator || roles["POST /v1/memory/curation/{id}/decide"] != auth.Approver {
		t.Fatalf("%v", roles)
	}

	// Without a curator the routes are not served.
	plain := &api.MemoryAPI{Store: st, Writer: w, Server: &api.Server{}}
	mux2 := http.NewServeMux()
	plain.Register(mux2)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/memory/curation", nil)
	req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t", Subject: "a", Roles: viewer}))
	mux2.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("a server without the curator must not serve its routes: %d", rec.Code)
	}
	if _, ok := plain.RouteRoles()["POST /v1/memory/curation/run"]; ok {
		t.Fatal("no role entries for routes that are not served")
	}
}
