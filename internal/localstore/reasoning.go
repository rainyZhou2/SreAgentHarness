package localstore

import (
	"context"
	"sort"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/reasoning"
)

type reasoningScoreRec struct {
	TenantID string             `json:"tenant_id"`
	Score    reasoning.RunScore `json:"score"`
}

func scoreKey(tenantID, runID string) string { return tenantID + "\x00" + runID }

// PutReasoningScore upserts a run's reasoning score.
func (s *Store) PutReasoningScore(_ context.Context, sc reasoning.RunScore) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc.ScoredAt = now()
	id := scoreKey(sc.TenantID, sc.RunID)
	rec := reasoningScoreRec{TenantID: sc.TenantID, Score: sc}
	s.reasoningScores[id] = rec
	s.put("reasoningscore", id, sc.TenantID, 0, rec)
	return nil
}

// ReasoningScore returns one run's score.
func (s *Store) ReasoningScore(_ context.Context, tenantID, runID string) (reasoning.RunScore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.reasoningScores[scoreKey(tenantID, runID)]
	if !ok {
		return reasoning.RunScore{}, domain.ErrNotFound
	}
	sc := rec.Score
	sc.TenantID = tenantID
	return sc, nil
}

// ReasoningScores lists scores, newest first.
func (s *Store) ReasoningScores(_ context.Context, tenantID string, f reasoning.ScoreFilter) ([]reasoning.RunScore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := f.Limit
	if limit <= 0 {
		limit = 1000
	}
	limit = min(limit, 10000)
	var out []reasoning.RunScore
	for _, rec := range s.reasoningScores {
		sc := rec.Score
		if rec.TenantID != tenantID || (f.ProfileID != "" && sc.ProfileID != f.ProfileID) ||
			(f.Version != 0 && sc.ProfileVersion != f.Version) || (f.Service != "" && sc.Service != f.Service) ||
			sc.ScoredAt.Before(f.Since) {
			continue
		}
		sc.TenantID = tenantID
		out = append(out, sc)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].ScoredAt.Equal(out[j].ScoredAt) {
			return out[i].ScoredAt.After(out[j].ScoredAt)
		}
		return out[i].RunID < out[j].RunID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RunMemoryIDs lists the memories a run retrieved.
func (s *Store) RunMemoryIDs(_ context.Context, tenantID, runID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, st := range sortedSteps(s.steps, runID) {
		if st.TenantID != tenantID {
			continue
		}
		for _, h := range s.memHits[st.ID] {
			if !seen[h.MemoryID] {
				seen[h.MemoryID] = true
				out = append(out, h.MemoryID)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
