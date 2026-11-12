package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
)

func TestCurationProposalsPersistAndDedupe(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "harness.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	p := curate.Proposal{ID: "p1", TenantID: "t", Signal: curate.Duplicate, Status: curate.Pending, Signature: "sig1",
		Targets: []curate.Target{{MemoryID: "m1", Version: 1}}, CreatedAt: time.Now().UTC()}
	if _, created, err := s.CreateProposal(ctx, p); err != nil || !created {
		t.Fatalf("%v %v", created, err)
	}
	dup := p
	dup.ID = "p2"
	if got, created, _ := s.CreateProposal(ctx, dup); created || got.ID != "p1" {
		t.Fatalf("the same signature must not create a second proposal: %+v %v", got, created)
	}
	if _, created, _ := s.CreateProposal(ctx, curate.Proposal{ID: "p3", TenantID: "other", Signature: "sig1", Status: curate.Pending}); !created {
		t.Fatal("signatures are per tenant")
	}
	if _, err := s.DecideProposal(ctx, "t", "p1", curate.Dismissed, "alice", "keep", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecideProposal(ctx, "t", "p1", curate.Applied, "alice", "", time.Now()); !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("a decided proposal cannot be decided again: %v", err)
	}
	if _, err := s.GetProposal(ctx, "other", "p1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
	s.Close()

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.GetProposal(ctx, "t", "p1")
	if err != nil || got.Status != curate.Dismissed || got.DecidedBy != "alice" || got.DecisionReason != "keep" {
		t.Fatalf("a decision must survive a restart: %+v %v", got, err)
	}
	if _, created, _ := s.CreateProposal(ctx, dup); created {
		t.Fatal("a dismissed finding must not be raised again after a restart")
	}
	if l, _ := s.ListProposals(ctx, "t", curate.Pending, 10); len(l) != 0 {
		t.Fatalf("%+v", l)
	}
}

func TestMemoryLastHits(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	at := time.Now().UTC().Add(-48 * time.Hour)
	s.steps["s1"] = domain.Step{ID: "s1", TenantID: "t", UpdatedAt: at.Add(-time.Hour)}
	s.steps["s2"] = domain.Step{ID: "s2", TenantID: "t", UpdatedAt: at}
	s.steps["s3"] = domain.Step{ID: "s3", TenantID: "other", UpdatedAt: time.Now()}
	s.memHits["s1"] = []memoryHitRec{{MemoryID: "m1"}}
	s.memHits["s2"] = []memoryHitRec{{MemoryID: "m1"}, {MemoryID: "m2"}}
	s.memHits["s3"] = []memoryHitRec{{MemoryID: "m1"}}
	got, err := s.MemoryLastHits(context.Background(), "t", []string{"m1", "m3"})
	if err != nil || !got["m1"].Equal(at) || len(got) != 1 {
		t.Fatalf("the latest hit of this tenant only: %v %v", got, err)
	}
}
