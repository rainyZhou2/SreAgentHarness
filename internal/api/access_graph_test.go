package api

import (
	"testing"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
)

func TestGraphHidesOtherUsersWork(t *testing.T) {
	p1 := visibleGraphNode
	alice := authPrincipal("alice", "operator")
	admin := authPrincipal("root", "admin")
	mem := func(scope, owner string) domain.GraphNode {
		return domain.GraphNode{Kind: "memory", Attrs: map[string]any{"scope": scope, "scope_id": owner}}
	}
	if !p1(alice, mem("service", "checkout")) || !p1(alice, mem("user", "alice")) {
		t.Error("shared and own memory are visible")
	}
	if p1(alice, mem("user", "bob")) {
		t.Error("another user's personal memory title must not be visible")
	}
	if p1(alice, domain.GraphNode{Kind: "thought"}) || p1(alice, domain.GraphNode{Kind: "step"}) {
		t.Error("thought and step labels are reasoning text with no owner")
	}
	if !p1(alice, domain.GraphNode{Kind: "tool"}) || !p1(admin, mem("user", "bob")) || !p1(admin, domain.GraphNode{Kind: "thought"}) {
		t.Error("tools are shared; admins see everything")
	}
}

func authPrincipal(subject string, roles ...string) auth.Principal {
	p := auth.Principal{Tenant: "t1", Subject: subject}
	for _, r := range roles {
		p.Roles = append(p.Roles, auth.Role(r))
	}
	return p
}
