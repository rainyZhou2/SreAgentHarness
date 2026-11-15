package api

import (
	"testing"

	"go-sre-harness/internal/auth"
)

func TestLegacyAuthorizationContract(t *testing.T) {
	for pattern, want := range map[string]auth.Role{
		"GET /api/service":                  auth.Viewer,
		"POST /api/runs":                    auth.Operator,
		"POST /api/runs/{run_id}/commands":  auth.Viewer,
		"POST /api/runs/{run_id}/fork":      auth.Operator,
		"POST /api/admin/role-bindings":     auth.Admin,
		"DELETE /api/admin/role-bindings":   auth.Admin,
		"GET /api/admin/audit":              auth.Admin,
		"POST /api/graph/relations":         auth.Admin,
		"DELETE /api/graph/relations":       auth.Admin,
		"GET /api/memories":                 auth.Viewer,
		"GET /api/admin/security/decisions": auth.Viewer,
	} {
		got, known := RequiredRole(pattern)
		if !known || got != want {
			t.Fatalf("%s: got %q, %v; want %q", pattern, got, known, want)
		}
	}
	if _, known := RequiredRole("POST /api/unknown"); known {
		t.Fatal("unknown write route must remain denied")
	}
}
