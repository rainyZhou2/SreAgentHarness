package workapi

import (
	"strings"
	"testing"
)

func TestRouteCatalogHasUniquePatternsAndRoles(t *testing.T) {
	a := &API{}
	routes := a.routes()
	seen := make(map[string]bool, len(routes))
	for _, rt := range routes {
		if seen[rt.pattern] {
			t.Fatalf("duplicate route %q", rt.pattern)
		}
		seen[rt.pattern] = true
		if rt.handler == nil {
			t.Fatalf("nil handler for %q", rt.pattern)
		}
		if strings.Contains(rt.pattern, " /v1/") {
			t.Errorf("generation-versioned route remains: %s", rt.pattern)
		}
		if !strings.Contains(rt.pattern, " /api/") {
			t.Errorf("route is outside the current API root: %s", rt.pattern)
		}
		if got := a.RouteRoles()[rt.pattern]; got != rt.role {
			t.Fatalf("role mismatch for %q: %v != %v", rt.pattern, got, rt.role)
		}
	}
	if len(seen) != 60 {
		t.Fatalf("route count = %d, want 60", len(seen))
	}
}
