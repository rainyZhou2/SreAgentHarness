package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/internal/memory"
)

// Applicability round-trips, outcomes count once per Work under RLS, and
// a playbook is promoted only after a verified success by another reviewer.
func TestMemoryApplicabilityAndPlaybookPromotionPG(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "pb-" + uuid.NewString()[:8]
	tctx := WithTenant(ctx, tenant)
	m, _, err := app.CreateMemory(tctx, memory.NewMemory{TenantID: tenant, Scope: "service", ScopeID: "checkout",
		Type: "procedural", Status: "CANDIDATE", Subject: "playbook:incident:deployment", Title: "Roll back bad deploy",
		Content: "1. deployment.rollback", ContentHash: "h-" + tenant, ContentRef: "ref", Trust: "T3", Sensitivity: "internal",
		CreatedBy: "alice", Applicability: &memory.Applicability{Kind: "playbook", Services: []string{"checkout"},
			VersionConstraints: []string{"kubernetes>=1.28"}, RiskOfReuse: "high", SourceWorkIDs: []string{"w1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if m.Applicability == nil || m.Applicability.Kind != "playbook" || m.Applicability.VersionConstraints[0] != "kubernetes>=1.28" {
		t.Fatalf("applicability: %+v", m.Applicability)
	}
	if _, err := app.TransitionMemory(tctx, tenant, m.ID, "CANDIDATE", "ACTIVE", "bob", "lgtm"); err == nil {
		t.Fatal("promoted without a verified success")
	}
	for i, w := range []string{"w1", "w1", "w2"} {
		counted, err := app.RecordMemoryOutcome(tctx, tenant, m.ID, w, true, "ev")
		if err != nil || counted != (i != 1) {
			t.Fatalf("record %s: %v %v", w, counted, err)
		}
	}
	// Another tenant cannot record against it (RLS hides the memory).
	if counted, err := app.RecordMemoryOutcome(WithTenant(ctx, "x-"+tenant), "x-"+tenant, m.ID, "w9", false, ""); err != nil || counted {
		t.Fatalf("cross-tenant record: %v %v", counted, err)
	}
	if _, err := app.TransitionMemory(tctx, tenant, m.ID, "CANDIDATE", "ACTIVE", "alice", "self"); err == nil {
		t.Fatal("self-promotion accepted")
	}
	got, err := app.TransitionMemory(tctx, tenant, m.ID, "CANDIDATE", "ACTIVE", "bob", "lgtm")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "ACTIVE" || got.Verification.SuccessCount != 2 || got.Verification.LastVerifiedAt == nil {
		t.Fatalf("promoted: %+v", got)
	}
	upd, err := admin.SetMemoryApplicability(ctx, tenant, m.ID, "carol", memory.Applicability{Kind: "playbook", Environments: []string{"prod"}})
	if err != nil || upd.Applicability.Environments[0] != "prod" || upd.Applicability.RiskOfReuse != "medium" {
		t.Fatalf("set applicability: %+v %v", upd.Applicability, err)
	}
	if _, err := admin.SetMemoryApplicability(ctx, tenant, m.ID, "carol", memory.Applicability{Kind: "bogus"}); err == nil {
		t.Fatal("invalid applicability accepted")
	}
}
