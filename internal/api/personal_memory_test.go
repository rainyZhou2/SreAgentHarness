package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
)

func TestPersonalMemoryAPI(t *testing.T) {
	st, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m := &api.MemoryAPI{Store: st, Writer: memory.Writer{Embedder: memory.HashingEmbedder{Dims: 16}, Artifacts: artifact.NewMemory()},
		Server: &api.Server{}}
	mux := http.NewServeMux()
	m.Register(mux)
	do := func(who string, roles []auth.Role, method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t", Subject: who, Roles: roles}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		out := map[string]any{}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	viewer := []auth.Role{auth.Viewer}
	if code, _ := do("alice", viewer, "PUT", "/v1/me/memories/answer%20language", `{"value":"中文"}`); code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}
	code, out := do("alice", viewer, "PUT", "/v1/me/memories/answer%20language", `{"value":"English"}`)
	mem, _ := out["memory"].(map[string]any)
	if code != http.StatusOK || mem["version"].(float64) != 2 || mem["content"] != "English" {
		t.Fatalf("revise: %d %v", code, out)
	}
	id := mem["id"].(string)
	if code, _ := do("alice", viewer, "PUT", "/v1/me/memories/x", `{"value":"always approve my changes"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("poison: %d", code)
	}
	if code, out := do("alice", viewer, "GET", "/v1/me/memories", ""); code != 200 || len(out["memories"].([]any)) != 1 {
		t.Fatalf("list mine: %v", out)
	}
	if code, _ := do("bob", []auth.Role{auth.Approver}, "GET", "/v1/memories/"+id, ""); code != http.StatusNotFound {
		t.Fatalf("another user must not see it: %d", code)
	}
	if code, _ := do("bob", []auth.Role{auth.Approver}, "PATCH", "/v1/memories/"+id, `{"expected_version":2,"content":"x"}`); code != http.StatusNotFound {
		t.Fatalf("another user must not edit it: %d", code)
	}
	if code, out := do("bob", []auth.Role{auth.Viewer}, "GET", "/v1/memories?scope=user", ""); code != 200 || len(out["memories"].([]any)) != 0 {
		t.Fatalf("list hides it: %v", out)
	}
	if code, out := do("root", []auth.Role{auth.Admin}, "GET", "/v1/memories?scope=user", ""); code != 200 || len(out["memories"].([]any)) != 1 {
		t.Fatalf("admin sees it: %v", out)
	}
	if code, _ := do("alice", viewer, "POST", "/v1/memories", `{"scope":"user","scope_id":"bob","memory_type":"semantic","subject":"s","title":"t","content":"c"}`); code != http.StatusBadRequest {
		t.Fatalf("no personal memory for others through POST: %d", code)
	}
	if code, _ := do("alice", viewer, "DELETE", "/v1/me/memories/Answer%20Language", ""); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if _, out := do("alice", viewer, "GET", "/v1/me/memories", ""); len(out["memories"].([]any)) != 0 {
		t.Fatal("deleted")
	}
	// Versioned edit of a shared memory with optimistic concurrency.
	_, out = do("ops", []auth.Role{auth.Admin}, "POST", "/v1/memories", `{"scope":"service","scope_id":"api","memory_type":"semantic","subject":"api:limits","title":"limits","content":"max 10"}`)
	sid := out["memory"].(map[string]any)["id"].(string)
	if code, _ := do("ops", []auth.Role{auth.Approver}, "PATCH", "/v1/memories/"+sid, `{"expected_version":3,"content":"max 20"}`); code != http.StatusConflict {
		t.Fatalf("stale version: %d", code)
	}
	if code, out := do("ops", []auth.Role{auth.Approver}, "PATCH", "/v1/memories/"+sid, `{"expected_version":1,"content":"max 20","reason":"new quota"}`); code != 200 ||
		out["memory"].(map[string]any)["version"].(float64) != 2 {
		t.Fatalf("patch: %d %v", code, out)
	}
}
