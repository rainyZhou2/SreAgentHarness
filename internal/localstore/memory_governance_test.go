package localstore

import (
	"context"
	"path/filepath"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

func memoryAccess(tenant string, subjects ...string) context.Context {
	return memory.WithAccessContext(context.Background(), memory.AccessContext{TenantID: tenant, Subjects: subjects})
}

func createGovernedMemory(t *testing.T, s *Store, id, title, owner string) memory.Memory {
	t.Helper()
	m, created, err := s.CreateMemory(context.Background(), memory.NewMemory{
		TenantID: "t", Scope: "service", ScopeID: "api", Type: "semantic", Status: memory.Verified,
		Subject: "subject:" + id, Title: title, Content: title, ContentHash: "hash:" + id,
		Sensitivity: "internal", Trust: "T2", CreatedBy: owner,
	})
	if err != nil || !created {
		t.Fatalf("create memory: created=%v err=%v", created, err)
	}
	return m
}

func TestMemoryACLFiltersBeforeLimit(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	high := createGovernedMemory(t, s, "high", "shared runbook", "alice")
	low := createGovernedMemory(t, s, "low", "shared preference", "bob")

	ctx := memoryAccess("t", "user:bob", "role:viewer")
	candidates, err := s.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: "t", Services: []string{"api"},
		Text: "shared", MaxSensitivity: "internal", Limit: 1})
	if err != nil || len(candidates) != 1 || candidates[0].ID != low.ID {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	if _, err := s.GetMemory(memoryAccess("t", "user:alice", "role:viewer"), "t", low.ID); err != nil {
		t.Fatalf("shared viewer grant: %v", err)
	}
	if _, err := s.GetMemory(memoryAccess("t", "user:nobody"), "t", low.ID); err != domain.ErrNotFound {
		t.Fatalf("unauthorized get: %v", err)
	}
	list, err := s.ListMemories(memoryAccess("t", "user:nobody"), "t", pg.MemoryFilter{Limit: 10})
	if err != nil || len(list) != 0 {
		t.Fatalf("unauthorized list: %+v, %v", list, err)
	}
	admin, err := s.GetMemory(memoryAccess("t", "role:admin"), "t", high.ID)
	if err != nil || admin.ID != high.ID {
		t.Fatalf("admin get: %+v, %v", admin, err)
	}
}

func TestVerifyAndApproveMemory(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, _, err := s.CreateMemory(context.Background(), memory.NewMemory{TenantID: "t", Scope: "service", ScopeID: "api",
		Type: "procedural", Status: memory.StatusCandidate, Subject: "api:runbook", Title: "Runbook", Content: "verify",
		ContentHash: "content", EvidenceRefs: []string{"evidence:1"}, Trust: "T2", Sensitivity: "internal", CreatedBy: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	input := memory.VerificationInputFor(m, "decision:1", "outcome:1")
	if _, err := s.TransitionMemory(context.Background(), "t", m.ID, memory.StatusCandidate, memory.Verified, "machine", "direct"); err == nil {
		t.Fatal("generic transition must not verify")
	}
	record, verified, err := s.VerifyMemory(context.Background(), input, "machine", "request-1")
	if err != nil || verified.Status != memory.Verified || record.MemoryVersion != m.Version {
		t.Fatalf("verify: record=%+v memory=%+v err=%v", record, verified, err)
	}
	again, _, err := s.VerifyMemory(context.Background(), input, "machine", "request-1")
	if err != nil || again.ID != record.ID {
		t.Fatalf("idempotent verify: %+v, %v", again, err)
	}
	changed := input
	changed.DecisionRef = "decision:2"
	if _, _, err := s.VerifyMemory(context.Background(), changed, "machine", "request-1"); err != domain.ErrVersionConflict {
		t.Fatalf("idempotency conflict: %v", err)
	}
	approved, err := s.ApproveMemory(context.Background(), "t", m.ID, m.Version, "approver")
	if err != nil || approved.Status != memory.Approved || approved.ApprovedBy == nil || *approved.ApprovedBy != "approver" {
		t.Fatalf("approve: %+v, %v", approved, err)
	}
	stale := memory.VerificationInputFor(approved, "decision:2", "outcome:2")
	stale.ExpectedVersion++
	if _, _, err := s.VerifyMemory(context.Background(), stale, "machine", "request-2"); err != domain.ErrVersionConflict {
		t.Fatalf("stale version: %v", err)
	}
}
