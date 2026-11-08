package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
)

type runOnlyStore struct {
	Store
	run domain.Run
}

func (s runOnlyStore) GetRun(_ context.Context, tenant, id string) (domain.Run, error) {
	if tenant != "t1" || id != s.run.ID {
		return domain.Run{}, domain.ErrNotFound
	}
	return s.run, nil
}

func TestGetAttachment(t *testing.T) {
	ctx := context.Background()
	arts := artifact.NewMemory()
	outRef, _, _ := arts.Put(ctx, "t1", "tool-output", []byte(`{"lines":["a"]}`))
	pngRef, _, _ := arts.Put(ctx, "t1", "tool-output", []byte("\x89PNG...."))
	svgRef, _, _ := arts.Put(ctx, "t1", "tool-output", []byte(`<svg onload="alert(1)"/>`))
	res, _ := json.Marshal(map[string]any{"answer": "x", "attachments": []domain.Attachment{
		{Type: "file", Title: "logs/../../evil", Ref: outRef, MimeType: "application/json"},
		{Type: "image", Title: "chart", Ref: pngRef, MimeType: "image/png"},
		{Type: "image", Title: "vector", Ref: svgRef, MimeType: "image/svg+xml"},
		{Type: "link", URL: "https://example.com"},
	}})
	resRef, _, _ := arts.Put(ctx, "t1", "result", res)
	s := &Server{Store: runOnlyStore{run: domain.Run{ID: "r1", ResultRef: &resRef}}, Artifacts: arts}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/runs/{run_id}/attachments/{index}", s.getAttachment)
	get := func(path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t1", Subject: "u"}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	rec := get("/v1/runs/r1/attachments/0")
	if rec.Code != 200 || rec.Body.String() != `{"lines":["a"]}` {
		t.Fatalf("file: %d %s", rec.Code, rec.Body)
	}
	if cd := rec.Header().Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment") || strings.Contains(cd, "/") {
		t.Fatalf("disposition %q", cd)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(rec.Header().Get("Content-Security-Policy"), "sandbox") {
		t.Fatalf("headers: %v", rec.Header())
	}
	if rec = get("/v1/runs/r1/attachments/1"); rec.Header().Get("Content-Type") != "image/png" || !strings.HasPrefix(rec.Header().Get("Content-Disposition"), "inline") {
		t.Fatalf("png: %v", rec.Header())
	}
	if rec = get("/v1/runs/r1/attachments/2"); rec.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(rec.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("svg must download as bytes: %v", rec.Header())
	}
	for _, p := range []string{"/v1/runs/r1/attachments/3", "/v1/runs/r1/attachments/9", "/v1/runs/r1/attachments/-1", "/v1/runs/other/attachments/0"} {
		if rec = get(p); rec.Code/100 == 2 {
			t.Fatalf("%s: want error, got %d", p, rec.Code)
		}
	}
}
