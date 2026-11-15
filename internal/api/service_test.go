package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-sre-harness/internal/api"
)

func TestServiceReportsWholeReleaseMetadata(t *testing.T) {
	h := (&api.Server{ReleaseVersion: "1.2.3", BuildRevision: "abc123"}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/service", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	req.Header.Set("X-Actor-ID", "viewer")
	req.Header.Set("X-Roles", "viewer")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/service returned %d: %s", rec.Code, rec.Body.String())
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"name":            "sre-agent-harness",
		"release_version": "1.2.3",
		"build_revision":  "abc123",
		"api_prefix":      "/api",
	}
	if len(got) != len(want) {
		t.Fatalf("service metadata has unexpected fields: %v", got)
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %q, want %q", key, got[key], value)
		}
	}
}

func TestServiceRequiresAuthentication(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/service", nil)
	rec := httptest.NewRecorder()
	(&api.Server{}).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /api/service returned %d, want 401", rec.Code)
	}
}
