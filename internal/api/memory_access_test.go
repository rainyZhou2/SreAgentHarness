package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/memory"
)

func TestMemoryAccessUsesAuthenticatedPrincipal(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/memories", nil)
	req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t", Subject: "alice", Roles: []auth.Role{auth.Operator}}))
	withMemoryAccess(func(_ http.ResponseWriter, r *http.Request) {
		access, ok := memory.AccessFrom(r.Context())
		if !ok || access.TenantID != "t" {
			t.Fatalf("access=%+v", access)
		}
		for _, subject := range []string{"user:alice", "role:operator", "role:viewer"} {
			if !access.Allows("m", "t", []memory.ACLGrant{{TenantID: "t", MemoryID: "m", Subject: subject}}) {
				t.Fatalf("missing %s", subject)
			}
		}
	}).ServeHTTP(httptest.NewRecorder(), req)
}
