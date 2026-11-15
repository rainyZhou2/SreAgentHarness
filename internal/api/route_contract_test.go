package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCoreRouteCatalogUsesOnlyCurrentAPIRoot(t *testing.T) {
	for pattern := range routeRoles {
		if strings.Contains(pattern, " /v1/") {
			t.Errorf("generation-versioned route remains: %s", pattern)
		}
		if !strings.Contains(pattern, " /api/") {
			t.Errorf("route is outside the current API root: %s", pattern)
		}
	}
}

func TestRemovedVersionedWhoamiRouteIsNotRegistered(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/whoami", nil)
	rec := httptest.NewRecorder()
	(&Server{}).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/whoami returned %d, want 404", rec.Code)
	}
}
