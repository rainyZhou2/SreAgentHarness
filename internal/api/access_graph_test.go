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
	owned := func(kind, typ, who string) domain.GraphNode {
		return domain.GraphNode{Kind: kind, Attrs: map[string]any{"initiator_type": typ, "initiator_id": who}}
	}
	for _, kind := range []string{"run", "step", "thought"} {
		if !p1(alice, owned(kind, "user", "alice")) {
			t.Errorf("a user sees their own %s nodes", kind)
		}
		if p1(alice, owned(kind, "user", "bob")) {
			t.Errorf("another user's %s node must be hidden", kind)
		}
		if !p1(alice, owned(kind, "system", "")) || !p1(alice, owned(kind, "service_principal", "")) {
			t.Errorf("%s nodes of shared operational runs are visible", kind)
		}
		if p1(alice, domain.GraphNode{Kind: kind}) || p1(alice, owned(kind, "unknown", "")) {
			t.Errorf("a %s node with no recognizable owner fails closed", kind)
		}
		if !p1(admin, owned(kind, "user", "bob")) {
			t.Errorf("admins see %s nodes of every user", kind)
		}
	}
	if p1(alice, domain.GraphNode{Kind: "shard", Ref: "user:bob"}) || !p1(alice, domain.GraphNode{Kind: "shard", Ref: "user:alice"}) ||
		!p1(alice, domain.GraphNode{Kind: "shard", Ref: "service:checkout"}) {
		t.Error("a user's memory shard is visible to that user only")
	}
	if !p1(alice, domain.GraphNode{Kind: "tool"}) || !p1(admin, mem("user", "bob")) {
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
