package localstore

import (
	"context"
	"path/filepath"
	"testing"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func TestConflictResolutionRequiresFreshVerification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a := createGovernedMemory(t, s, "a", "first", "alice")
	b := createGovernedMemory(t, s, "b", "second", "alice")
	record, err := s.DeclareMemoryConflict(context.Background(), memory.ConflictRecord{TenantID: "t",
		Pair: memory.ConflictPair{FirstID: b.ID, SecondID: a.ID}, Reason: "contradictory evidence", CreatedBy: "reviewer"})
	pair, pairErr := memory.NewConflictPair(a, b)
	if err != nil || pairErr != nil || record.Pair != pair {
		t.Fatalf("a=%s b=%s record=%+v err=%v", a.ID, b.ID, record, err)
	}
	if s.memories[a.ID].Mem.Status != memory.Contested || s.memories[b.ID].Mem.Status != memory.Contested {
		t.Fatalf("statuses=%s/%s", s.memories[a.ID].Mem.Status, s.memories[b.ID].Mem.Status)
	}
	resolved, err := s.ResolveMemoryConflict(context.Background(), "t", record.Pair, "reviewer", "evidence reconciled")
	if err != nil || resolved.ResolvedAt == nil || s.memories[a.ID].Mem.Status != memory.Contested {
		t.Fatalf("resolved=%+v status=%s err=%v", resolved, s.memories[a.ID].Mem.Status, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if len(s.memories[a.ID].Mem.ConflictsWith) != 1 || s.memories[a.ID].Mem.ConflictsWith[0] != b.ID {
		t.Fatalf("conflict projection=%+v", s.memories[a.ID].Mem.ConflictsWith)
	}
}

func TestSupersedeMemoryIsExplicitAndAtomic(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old, _, err := s.CreateMemory(context.Background(), memory.NewMemory{TenantID: "t", Scope: "service", ScopeID: "api",
		Type: "semantic", Status: memory.Verified, Subject: "cause", Title: "old", Content: "old", ContentHash: "old", Trust: "T2"})
	if err != nil {
		t.Fatal(err)
	}
	next, _, err := s.CreateMemory(context.Background(), memory.NewMemory{TenantID: "t", Scope: "service", ScopeID: "api",
		Type: "semantic", Status: memory.StatusCandidate, Subject: "cause", Title: "new", Content: "new", ContentHash: "new", Trust: "T2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SupersedeMemory(context.Background(), "t", old.ID, next.ID, old.Version, "reviewer"); err == nil {
		t.Fatal("unpublished replacement accepted")
	}
	next.Status = memory.Verified
	s.memories[next.ID] = memoryRec{Mem: next}
	if _, err := s.SupersedeMemory(context.Background(), "t", old.ID, next.ID, old.Version+1, "reviewer"); err != domain.ErrVersionConflict {
		t.Fatalf("stale CAS: %v", err)
	}
	superseded, err := s.SupersedeMemory(context.Background(), "t", old.ID, next.ID, old.Version, "reviewer")
	if err != nil || superseded.Status != memory.Superseded || superseded.SupersededBy == nil || *superseded.SupersededBy != next.ID {
		t.Fatalf("superseded=%+v err=%v", superseded, err)
	}
	if _, err := s.SupersedeMemory(context.Background(), "t", next.ID, old.ID, next.Version, "reviewer"); err == nil {
		t.Fatal("supersession cycle accepted")
	}
}
