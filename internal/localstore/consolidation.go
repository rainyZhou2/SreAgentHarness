package localstore

import (
	"context"
	"sort"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

// consolidationRec mirrors a consolidation_queue row.
type consolidationRec struct {
	TenantID     string     `json:"tenant_id"`
	RunID        string     `json:"run_id"`
	Status       string     `json:"status"` // PENDING | DONE | FAILED
	Attempts     int        `json:"attempts"`
	ClaimedUntil *time.Time `json:"claimed_until,omitempty"`
	LastError    string     `json:"last_error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

const maxConsolidationAttempts = 5

func (s *Store) saveConsolidation(r consolidationRec) {
	s.consolidation[r.RunID] = r
	s.put("consolidation", r.RunID, r.TenantID, 0, r)
}

// enqueueTerminalLocked adds a work item for every completed or failed run
// that has none yet, the way the server enqueues on the terminal transition.
func (s *Store) enqueueTerminalLocked() {
	for id, r := range s.runs {
		if r.Status != domain.RunCompleted && r.Status != domain.RunFailed {
			continue
		}
		if _, ok := s.consolidation[id]; ok {
			continue
		}
		s.saveConsolidation(consolidationRec{TenantID: r.TenantID, RunID: id, Status: "PENDING", CreatedAt: r.CreatedAt})
	}
}

// ClaimConsolidation claims pending work items, oldest first.
func (s *Store) ClaimConsolidation(_ context.Context, limit int, claimFor time.Duration) ([]memory.ConsolidationJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enqueueTerminalLocked()
	t := now()
	var ready []consolidationRec
	for _, r := range s.consolidation {
		if r.Status == "PENDING" && r.Attempts < maxConsolidationAttempts && (r.ClaimedUntil == nil || r.ClaimedUntil.Before(t)) {
			ready = append(ready, r)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].CreatedAt.Equal(ready[j].CreatedAt) {
			return ready[i].CreatedAt.Before(ready[j].CreatedAt)
		}
		return ready[i].RunID < ready[j].RunID
	})
	if limit > 0 && len(ready) > limit {
		ready = ready[:limit]
	}
	out := make([]memory.ConsolidationJob, 0, len(ready))
	until := t.Add(claimFor)
	for _, r := range ready {
		r.Attempts++
		r.ClaimedUntil = &until
		s.saveConsolidation(r)
		out = append(out, memory.ConsolidationJob{TenantID: r.TenantID, RunID: r.RunID})
	}
	return out, nil
}

// CompleteConsolidation marks a work item done or schedules a retry.
func (s *Store) CompleteConsolidation(_ context.Context, runID string, failure error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.consolidation[runID]
	if !ok {
		return nil
	}
	if failure == nil {
		r.Status, r.ClaimedUntil, r.LastError = "DONE", nil, ""
	} else {
		retry := now().Add(30 * time.Second)
		r.ClaimedUntil, r.LastError = &retry, trimErr(failure.Error())
		if r.Attempts >= maxConsolidationAttempts {
			r.Status = "FAILED"
		}
	}
	s.saveConsolidation(r)
	return nil
}

// requeueConsolidationLocked re-runs consolidation, e.g. after feedback.
func (s *Store) requeueConsolidationLocked(tenantID, runID string) {
	r, ok := s.consolidation[runID]
	if !ok {
		r = consolidationRec{TenantID: tenantID, RunID: runID, CreatedAt: now()}
	}
	r.Status, r.Attempts, r.ClaimedUntil = "PENDING", 0, nil
	s.saveConsolidation(r)
}

// RequeueConsolidation re-runs consolidation for a run.
func (s *Store) RequeueConsolidation(_ context.Context, tenantID, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requeueConsolidationLocked(tenantID, runID)
	return nil
}

// RunRecord returns a run with its steps and feedback.
func (s *Store) RunRecord(ctx context.Context, tenantID, runID string) (memory.RunRecord, error) {
	run, err := s.GetRun(ctx, tenantID, runID)
	if err != nil {
		return memory.RunRecord{}, err
	}
	steps, err := s.ListSteps(ctx, tenantID, runID)
	if err != nil {
		return memory.RunRecord{}, err
	}
	fb, err := s.ListFeedback(ctx, tenantID, runID)
	if err != nil {
		return memory.RunRecord{}, err
	}
	return memory.RunRecord{Run: run, Steps: steps, Feedback: fb}, nil
}

// RunCapabilities returns the distinct capabilities a run used.
func (s *Store) RunCapabilities(_ context.Context, tenantID, runID string) ([]domain.CapabilityRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.runs[runID]; !ok || r.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	seen := map[string]bool{}
	var out []domain.CapabilityRef
	for _, st := range sortedSteps(s.steps, runID) {
		for _, c := range s.caps[st.ID] {
			if c.Role != "used" || seen[c.Ref] {
				continue
			}
			seen[c.Ref] = true
			out = append(out, domain.CapabilityRef{Ref: c.Ref, Role: c.Role})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out, nil
}

func trimErr(s string) string {
	if r := []rune(s); len(r) > 500 {
		return string(r[:500])
	}
	return s
}
