package localstore

import (
	"context"
	"sort"
	"time"

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
)

// Memory curation proposals (see package curate), kept as JSON objects like
// everything else in the local store.

// CreateProposal stores p unless a proposal with the same signature exists in
// any status.
func (s *Store) CreateProposal(_ context.Context, p curate.Proposal) (curate.Proposal, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.curations {
		if q.TenantID == p.TenantID && q.Signature == p.Signature {
			return q, false, nil
		}
	}
	s.curations[p.ID] = p
	s.put("curation", p.ID, p.TenantID, 0, p)
	return p, true, nil
}

// ListProposals returns a tenant's proposals (all statuses when status is
// empty), newest first.
func (s *Store) ListProposals(_ context.Context, tenantID, status string, limit int) ([]curate.Proposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []curate.Proposal
	for _, p := range s.curations {
		if p.TenantID == tenantID && (status == "" || p.Status == status) {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) GetProposal(_ context.Context, tenantID, id string) (curate.Proposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.curations[id]
	if !ok || p.TenantID != tenantID {
		return curate.Proposal{}, domain.ErrNotFound
	}
	return p, nil
}

// DecideProposal moves a PENDING proposal to status.
func (s *Store) DecideProposal(_ context.Context, tenantID, id, status, actor, reason string, at time.Time) (curate.Proposal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.curations[id]
	if !ok || p.TenantID != tenantID {
		return curate.Proposal{}, domain.ErrNotFound
	}
	if p.Status != curate.Pending {
		return p, domain.ErrVersionConflict
	}
	p.Status, p.DecidedBy, p.DecisionReason = status, actor, reason
	p.DecidedAt = &at
	s.curations[id] = p
	s.put("curation", p.ID, p.TenantID, 0, p)
	return p, nil
}

// MemoryLastHits reports when each memory was last retrieved into a run.
func (s *Store) MemoryLastHits(_ context.Context, tenantID string, ids []string) (map[string]time.Time, error) {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]time.Time{}
	for stepID, hits := range s.memHits {
		st, ok := s.steps[stepID]
		if !ok || st.TenantID != tenantID {
			continue
		}
		for _, h := range hits {
			if want[h.MemoryID] && st.UpdatedAt.After(out[h.MemoryID]) {
				out[h.MemoryID] = st.UpdatedAt
			}
		}
	}
	return out, nil
}
