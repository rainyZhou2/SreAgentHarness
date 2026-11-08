package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func TestConsolidationQueue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.runs["r1"] = domain.Run{ID: "r1", TenantID: "t", Status: domain.RunCompleted, CreatedAt: now()}
	s.runs["r2"] = domain.Run{ID: "r2", TenantID: "t", Status: domain.RunRunning, CreatedAt: now()}
	s.mu.Unlock()

	jobs, err := s.ClaimConsolidation(ctx, 10, time.Minute)
	if err != nil || len(jobs) != 1 || jobs[0].RunID != "r1" {
		t.Fatalf("claim: %v %+v", err, jobs)
	}
	if again, _ := s.ClaimConsolidation(ctx, 10, time.Minute); len(again) != 0 {
		t.Fatal("a claimed item must not be claimed twice")
	}
	if err := s.CompleteConsolidation(ctx, "r1", errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if s.consolidation["r1"].Status != "PENDING" || s.consolidation["r1"].LastError != "boom" {
		t.Fatalf("retry state: %+v", s.consolidation["r1"])
	}
	_ = s.CompleteConsolidation(ctx, "r1", nil)
	if jobs, _ := s.ClaimConsolidation(ctx, 10, time.Minute); len(jobs) != 0 {
		t.Fatal("done items stay done")
	}
	yes := true
	if err := s.PutFeedback(ctx, "t", "r1", memory.Feedback{ActorID: "u", RootCauseCorrect: &yes}); err != nil {
		t.Fatal(err)
	}
	if jobs, _ := s.ClaimConsolidation(ctx, 10, time.Minute); len(jobs) != 1 {
		t.Fatal("feedback must requeue consolidation")
	}
	rec, err := s.RunRecord(ctx, "t", "r1")
	if err != nil || len(rec.Feedback) != 1 {
		t.Fatalf("run record: %v %+v", err, rec)
	}
	if _, err := s.RunCapabilities(ctx, "other", "r1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("capabilities must be tenant scoped")
	}
	_ = s.Close()
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.consolidation["r1"].RunID != "r1" {
		t.Fatal("queue must survive a restart")
	}
}
