package auth

import (
	"strings"
	"testing"
)

func TestRolesForServiceAndBreakGlass(t *testing.T) {
	svc := Principal{Roles: []Role{Service}}
	if !svc.Has(Service) || svc.Has(Viewer) || svc.Has(Operator) {
		t.Fatal("service may only submit events")
	}
	if !(Principal{Roles: []Role{Operator}}).Has(Service) {
		t.Fatal("operators keep event ingestion")
	}
	admin := Principal{Roles: []Role{Admin}}
	if admin.Has(BreakGlass) {
		t.Fatal("break-glass is never implied, not even by admin")
	}
	bg := Principal{Roles: []Role{BreakGlass}}
	if !bg.Has(BreakGlass) || !bg.Has(Viewer) || bg.Has(Approver) {
		t.Fatal("break-glass grants itself and read access only")
	}
}

func TestScope(t *testing.T) {
	p := Principal{Scope: &Scope{EventSources: []string{"alertmanager"}, EventTypes: []string{"alert.*"},
		Resources: []string{"service://prod/*", "deployment://prod/checkout"}, MaxRisk: "R1"}}
	cases := []struct {
		src, typ, subj string
		ok             bool
	}{
		{"alertmanager", "alert.firing", "service://prod/search", true},
		{"alertmanager", "alert.firing", "", true},
		{"alertmanager", "deploy.done", "service://prod/search", false},
		{"grafana", "alert.firing", "service://prod/search", false},
		{"alertmanager", "alert.firing", "service://staging/search", false},
	}
	for _, c := range cases {
		if got := p.AllowsEvent(c.src, c.typ, c.subj); got != c.ok {
			t.Fatalf("AllowsEvent(%s,%s,%s)=%v", c.src, c.typ, c.subj, got)
		}
	}
	if !p.AllowsResource("deployment://prod/checkout") || p.AllowsResource("deployment://prod/checkout-v2") {
		t.Fatal("exact resource patterns must not match prefixes")
	}
	if !p.AllowsRisk("R0") || !p.AllowsRisk("R1") || p.AllowsRisk("R2") {
		t.Fatal("risk ceiling")
	}
	open := Principal{}
	if !open.AllowsEvent("x", "y", "z") || !open.AllowsResource("any://x") || !open.AllowsRisk("R2") || open.AllowsRisk("R3") {
		t.Fatal("an unscoped principal is limited only by the R3 floor")
	}
	for _, bad := range []*Scope{{MaxRisk: "R3"}, {Resources: []string{"a*b"}}, {EventTypes: []string{""}},
		{Resources: []string{"**"}}} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("invalid scope accepted: %+v", bad)
		}
	}
	if _, err := NewStaticTokens([]StaticToken{{SHA256: strings.Repeat("a", 64), Tenant: "t", Subject: "s",
		Roles: []Role{Service}, Scope: &Scope{MaxRisk: "R9"}}}); err == nil {
		t.Fatal("token file with a bad scope must be refused")
	}
}
