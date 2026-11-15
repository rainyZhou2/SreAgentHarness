package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/store/pg"
)

type fakeAdmin struct {
	bindings    []pg.RoleBinding
	revocations []pg.RevocationEvent
}

func (f fakeAdmin) BindRole(context.Context, string, string, auth.Role, string) error { return nil }
func (f fakeAdmin) UnbindRole(context.Context, string, string, auth.Role) error       { return nil }
func (f fakeAdmin) ListAudit(context.Context, string, int) ([]pg.AuditEntry, error)   { return nil, nil }
func (f fakeAdmin) ListRoleBindings(context.Context, string) ([]pg.RoleBinding, error) {
	return f.bindings, nil
}
func (f fakeAdmin) ListRevocations(context.Context, string, int) ([]pg.RevocationEvent, error) {
	return f.revocations, nil
}

func adminRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	return req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{
		Tenant: "tenant-1", Subject: "admin", Roles: []auth.Role{auth.Admin}, Method: "token",
	}))
}

func TestListRoleBindings(t *testing.T) {
	now := time.Now().UTC()
	s := &Server{Admin: fakeAdmin{bindings: []pg.RoleBinding{
		{PrincipalID: "alice", Role: auth.Operator, CreatedBy: "admin", CreatedAt: now},
	}}}
	rec := httptest.NewRecorder()
	s.listRoleBindings(rec, adminRequest(http.MethodGet, "/api/admin/role-bindings"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Bindings []pg.RoleBinding `json:"bindings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Bindings) != 1 || out.Bindings[0].PrincipalID != "alice" || out.Bindings[0].Role != auth.Operator {
		t.Fatalf("bindings = %+v", out.Bindings)
	}
}

func TestListRevocations(t *testing.T) {
	s := &Server{Admin: fakeAdmin{revocations: []pg.RevocationEvent{
		{ID: "rev-1", Scope: "subject", ScopeID: "alice", Version: 3, ActorID: "admin", Reason: "offboarding", CreatedAt: time.Now().UTC()},
	}}}
	rec := httptest.NewRecorder()
	s.listRevocations(rec, adminRequest(http.MethodGet, "/api/admin/security/revocations"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out struct {
		Revocations []pg.RevocationEvent `json:"revocations"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Revocations) != 1 || out.Revocations[0].ScopeID != "alice" || out.Revocations[0].Version != 3 {
		t.Fatalf("revocations = %+v", out.Revocations)
	}
}
