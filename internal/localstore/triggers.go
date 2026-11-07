package localstore

import (
	"context"
	"time"

	"go-sre-harness/internal/domain"
)

type triggerClaimRec struct {
	TenantID  string    `json:"tenant_id"`
	Key       string    `json:"key"`
	ClaimID   string    `json:"claim_id"`
	RunID     string    `json:"run_id,omitempty"`
	Hits      int       `json:"hits"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	ExpiresAt time.Time `json:"expires_at"`
}

func triggerKey(tenantID, key string) string { return tenantID + "\x00" + key }

// ClaimTrigger takes or joins the deduplication claim for key (see the
// PostgreSQL store for the semantics).
func (s *Store) ClaimTrigger(_ context.Context, tenantID, key string, window time.Duration, extend bool) (domain.TriggerClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := now()
	id := triggerKey(tenantID, key)
	rec, ok := s.triggerClaims[id]
	fresh := !ok || !rec.ExpiresAt.After(t)
	if fresh {
		rec = triggerClaimRec{TenantID: tenantID, Key: key, ClaimID: newID(), Hits: 1, FirstSeen: t, LastSeen: t,
			ExpiresAt: t.Add(window)}
	} else {
		rec.Hits++
		rec.LastSeen = t
		if extend {
			rec.ExpiresAt = t.Add(window)
		}
	}
	s.triggerClaims[id] = rec
	s.put("triggerclaim", id, tenantID, 0, rec)
	return domain.TriggerClaim{ID: rec.ClaimID, Fresh: fresh, RunID: rec.RunID, Hits: rec.Hits, ExpiresAt: rec.ExpiresAt}, nil
}

// BindTrigger records the run that handles a claim.
func (s *Store) BindTrigger(_ context.Context, tenantID, key, claimID, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := triggerKey(tenantID, key)
	rec, ok := s.triggerClaims[id]
	if !ok || rec.ClaimID != claimID {
		return nil
	}
	rec.RunID = runID
	s.triggerClaims[id] = rec
	s.put("triggerclaim", id, tenantID, 0, rec)
	return nil
}

// ReleaseTrigger drops a claim so the next event starts a new run.
func (s *Store) ReleaseTrigger(_ context.Context, tenantID, key, claimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := triggerKey(tenantID, key)
	if rec, ok := s.triggerClaims[id]; ok && rec.ClaimID == claimID {
		delete(s.triggerClaims, id)
		s.del("triggerclaim", id)
	}
	return nil
}

// PurgeTriggerClaims deletes claims that expired before the given time.
func (s *Store) PurgeTriggerClaims(_ context.Context, before time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for id, rec := range s.triggerClaims {
		if limit > 0 && n >= int64(limit) {
			break
		}
		if rec.ExpiresAt.Before(before) {
			delete(s.triggerClaims, id)
			s.del("triggerclaim", id)
			n++
		}
	}
	return n, nil
}

// CountActions counts planned action intents of a tool since the given time.
func (s *Store) CountActions(_ context.Context, tenantID, tool, target string, since time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for stepID, intentID := range s.intentStep {
		in, ok := s.intents[intentID]
		if !ok || in.ToolName != tool || (target != "" && in.Target != target) {
			continue
		}
		st, ok := s.steps[stepID]
		if !ok || st.TenantID != tenantID || st.CreatedAt.Before(since) {
			continue
		}
		n++
	}
	return n, nil
}
