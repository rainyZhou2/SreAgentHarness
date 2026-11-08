package localstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/reasoning"
)

func TestDeleteOldReasoningScores(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old", "new"} {
		if err := s.PutReasoningScore(ctx, reasoning.RunScore{TenantID: "t", RunID: id}); err != nil {
			t.Fatal(err)
		}
	}
	rec := s.reasoningScores[scoreKey("t", "old")]
	rec.Score.ScoredAt = time.Now().Add(-100 * 24 * time.Hour)
	s.reasoningScores[scoreKey("t", "old")] = rec
	n, err := s.DeleteOldReasoningScores(ctx, time.Now().Add(-90*24*time.Hour), 100)
	if err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	_ = s.Close()
	s, _ = Open(path)
	defer s.Close()
	if _, err := s.ReasoningScore(ctx, "t", "old"); err != domain.ErrNotFound {
		t.Fatal("deletion must persist")
	}
	if _, err := s.ReasoningScore(ctx, "t", "new"); err != nil {
		t.Fatal(err)
	}
}
