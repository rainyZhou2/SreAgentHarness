package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func TestCurationProposalsPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	other := "t-" + uuid.NewString()[:8]
	tctx, octx := WithTenant(ctx, tenant), WithTenant(ctx, other)
	_ = admin

	mk := func(sig string) curate.Proposal {
		return curate.Proposal{ID: uuid.NewString(), TenantID: tenant, Signal: curate.Duplicate, Status: curate.Pending, Signature: sig,
			Targets: []curate.Target{{MemoryID: uuid.NewString(), Version: 1, Title: "t"}}, Reason: "r", CreatedAt: time.Now().UTC()}
	}
	p := mk("sig-1")
	if _, created, err := app.CreateProposal(tctx, p); err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	dup := mk("sig-1")
	got, created, err := app.CreateProposal(tctx, dup)
	if err != nil || created || got.ID != p.ID {
		t.Fatalf("the same signature must return the existing proposal: %+v %v %v", got, created, err)
	}
	// Tenant isolation: row-level security hides it, and the signature is per tenant.
	if _, err := app.GetProposal(octx, other, p.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("another tenant must not read it: %v", err)
	}
	if l, _ := app.ListProposals(octx, other, "", 10); len(l) != 0 {
		t.Fatalf("%+v", l)
	}
	o := mk("sig-1")
	o.TenantID = other
	if _, created, err := app.CreateProposal(octx, o); err != nil || !created {
		t.Fatalf("signatures are per tenant: %v %v", created, err)
	}

	l, err := app.ListProposals(tctx, tenant, curate.Pending, 10)
	if err != nil || len(l) != 1 || l[0].Targets[0].Title != "t" {
		t.Fatalf("%+v %v", l, err)
	}
	d, err := app.DecideProposal(tctx, tenant, p.ID, curate.Dismissed, "alice", "keep it", time.Now().UTC())
	if err != nil || d.Status != curate.Dismissed || d.DecidedBy != "alice" {
		t.Fatalf("%+v %v", d, err)
	}
	if _, err := app.DecideProposal(tctx, tenant, p.ID, curate.Applied, "bob", "", time.Now().UTC()); !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("a decided proposal cannot be decided again: %v", err)
	}
	if _, created, _ := app.CreateProposal(tctx, mk("sig-1")); created {
		t.Fatal("a dismissed finding must not be raised again")
	}
	if l, _ := app.ListProposals(tctx, tenant, curate.Pending, 10); len(l) != 0 {
		t.Fatalf("%+v", l)
	}
}

func TestMemoryLastHitsPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	_, stepID := seedRun(t, admin, tenant)
	m, _, err := admin.CreateMemory(ctx, memory.NewMemory{TenantID: tenant, Scope: "service", ScopeID: "api", Type: "semantic",
		Status: "ACTIVE", Subject: "s", Title: "p", Content: "c", ContentHash: uuid.NewString(), Trust: "T3", Sensitivity: "internal", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(ctx, `INSERT INTO step_memory_hits (tenant_id, step_id, memory_id, memory_version, score, rank)
		VALUES ($1,$2,$3,1,0.5,1)`, tenant, stepID, m.ID); err != nil {
		t.Fatal(err)
	}
	got, err := app.MemoryLastHits(WithTenant(ctx, tenant), tenant, []string{m.ID, uuid.NewString()})
	if err != nil || len(got) != 1 || got[m.ID].IsZero() {
		t.Fatalf("%v %v", got, err)
	}
	if got, err := app.MemoryLastHits(WithTenant(ctx, tenant), tenant, nil); err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
}
