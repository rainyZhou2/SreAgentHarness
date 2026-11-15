package main

import "fmt"

// ListAudit returns the tenant's newest audit entries (admin).
func (a *App) ListAudit(limit int) (map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return a.get(fmt.Sprintf("/api/admin/audit?limit=%d", limit))
}

// ListRoleBindings returns the tenant's role grants (admin).
func (a *App) ListRoleBindings() (map[string]any, error) {
	return a.get("/api/admin/role-bindings")
}

// ListRevocations returns the tenant's newest revocation events (admin).
func (a *App) ListRevocations(limit int) (map[string]any, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return a.get(fmt.Sprintf("/api/admin/security/revocations?limit=%d", limit))
}
