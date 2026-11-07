package localstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

// memoryRec keeps the tenant with the record; memory.Memory carries it in
// JSON, so the plain value is enough.
type memoryRec struct {
	Mem memory.Memory `json:"mem"`
}

type incidentRec struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	ServiceID     string    `json:"service_id"`
	Environment   string    `json:"environment"`
	Status        string    `json:"status"`
	MemoryVersion int64     `json:"memory_version"`
	CreatedAt     time.Time `json:"created_at"`
}

func (s *Store) saveMemory(rec memoryRec) {
	s.memories[rec.Mem.ID] = rec
	s.put("memory", rec.Mem.ID, rec.Mem.TenantID, 0, rec)
}

func (s *Store) saveIncident(v incidentRec) {
	s.incidents[v.ID] = v
	s.put("incident", v.ID, v.TenantID, 0, v)
}

var memoryTransitions = map[string][]string{
	"CANDIDATE":  {"VALIDATING", "ACTIVE", "REJECTED"},
	"VALIDATING": {"ACTIVE", "REJECTED"},
	"ACTIVE":     {"STALE", "SUPERSEDED", "ARCHIVED"},
	"STALE":      {"ACTIVE", "ARCHIVED"},
	"SUPERSEDED": {"ARCHIVED"},
	"REJECTED":   {"ARCHIVED"},
}

func allowedMemoryTransition(from, to string) bool {
	for _, t := range memoryTransitions[from] {
		if t == to {
			return true
		}
	}
	return false
}

func (s *Store) CreateMemory(ctx context.Context, m memory.NewMemory) (memory.Memory, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.memories {
		cur := rec.Mem
		if cur.TenantID == m.TenantID && cur.Scope == m.Scope && cur.ScopeID == m.ScopeID &&
			cur.ContentHash == m.ContentHash && cur.Status != "REJECTED" && cur.Status != "ARCHIVED" {
			return cur, false, nil
		}
	}
	now := now()
	mem := memory.Memory{ID: newID(), TenantID: m.TenantID, Scope: m.Scope, ScopeID: m.ScopeID, Type: m.Type,
		Status: m.Status, Version: 1, Subject: m.Subject, Title: m.Title, Content: m.Content,
		ContentRef: m.ContentRef, ContentHash: m.ContentHash, SourceRefs: m.SourceRefs, EvidenceRefs: m.EvidenceRefs,
		Trust: m.Trust, Sensitivity: m.Sensitivity, Confidence: m.Confidence, VerifiedBy: m.VerifiedBy,
		Environment: m.Environment, ExpiresAt: m.ExpiresAt, CreatedBy: m.CreatedBy, CreatedAt: now, UpdatedAt: now,
		Embedding: m.Embedding, EmbeddingModel: m.EmbeddingModel}
	s.saveMemory(memoryRec{Mem: mem})
	shards := stringShards(m)
	s.shards[mem.ID] = shards
	s.put("shard", mem.ID, mem.TenantID, 0, shards)
	if mem.Status == "ACTIVE" {
		s.supersedeSubjectLocked(mem.TenantID, mem.ID, mem.Scope, mem.ScopeID, mem.Subject)
	}
	return mem, true, nil
}

func stringShards(m memory.NewMemory) []domain.MemoryShard {
	key := "org:" + m.TenantID
	if m.Scope != "organization" && m.ScopeID != "" {
		key = m.Scope + ":" + m.ScopeID
	}
	out := []domain.MemoryShard{{ShardKey: key, Weight: 1, Source: "scope"}}
	seen := map[string]bool{key: true}
	for _, sh := range m.Shards {
		if sh.ShardKey == "" || seen[sh.ShardKey] {
			continue
		}
		seen[sh.ShardKey] = true
		if sh.Weight == 0 {
			sh.Weight = 0.8
		}
		out = append(out, sh)
	}
	return out
}

func (s *Store) supersedeSubjectLocked(tenantID, keepID, scope, scopeID, subject string) {
	for id, rec := range s.memories {
		m := rec.Mem
		if id == keepID || m.TenantID != tenantID || m.Scope != scope || m.ScopeID != scopeID ||
			m.Subject != subject || m.Status != "ACTIVE" {
			continue
		}
		keep := keepID
		m.Status, m.SupersededBy, m.UpdatedAt = "SUPERSEDED", &keep, now()
		s.saveMemory(memoryRec{Mem: m})
	}
}

func (s *Store) TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.memories[id]
	if !ok || rec.Mem.TenantID != tenantID {
		return memory.Memory{}, domain.ErrNotFound
	}
	cur := rec.Mem
	if from != "" && cur.Status != from {
		return memory.Memory{}, domain.ErrVersionConflict
	}
	if !allowedMemoryTransition(cur.Status, to) {
		return memory.Memory{}, &domain.RejectedError{Reason: fmt.Sprintf("invalid_transition_%s_to_%s", cur.Status, to)}
	}
	trust := cur.Trust
	var verifier *string
	if to == "ACTIVE" {
		verifier = &actor
		if trust == "T3" {
			trust = "T2"
		}
	}
	cur.Status, cur.Trust, cur.UpdatedAt = to, trust, now()
	if verifier != nil {
		cur.VerifiedBy = verifier
	}
	s.saveMemory(memoryRec{Mem: cur})
	if to == "ACTIVE" {
		s.supersedeSubjectLocked(tenantID, id, cur.Scope, cur.ScopeID, cur.Subject)
	}
	return cur, nil
}

func (s *Store) GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.memories[id]
	if !ok || rec.Mem.TenantID != tenantID {
		return memory.Memory{}, domain.ErrNotFound
	}
	return rec.Mem, nil
}

func (s *Store) ListMemories(ctx context.Context, tenantID string, f pg.MemoryFilter) ([]memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	q := strings.ToLower(strings.TrimSpace(f.Query))
	var out []memory.Memory
	for _, rec := range s.memories {
		m := rec.Mem
		if m.TenantID != tenantID {
			continue
		}
		if f.Status != "" && m.Status != f.Status {
			continue
		}
		if f.Scope != "" && m.Scope != f.Scope {
			continue
		}
		if f.ScopeID != "" && m.ScopeID != f.ScopeID {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(m.Title+"\n"+m.Content+"\n"+m.Subject), q) {
			continue
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (s *Store) MemoryCandidates(ctx context.Context, q memory.CandidateQuery) ([]memory.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := q.Limit
	if limit <= 0 {
		limit = 500
	}
	allowed := map[string]bool{"public": true, "internal": true}
	if q.MaxSensitivity == "restricted" {
		allowed["restricted"] = true
	} else if q.MaxSensitivity == "public" {
		allowed = map[string]bool{"public": true}
	}
	services := map[string]bool{}
	for _, svc := range q.Services {
		services[svc] = true
	}
	shards := map[string]bool{}
	for _, sh := range q.Shards {
		shards[sh] = true
	}
	text := strings.ToLower(strings.TrimSpace(q.Text))
	var out []memory.Candidate
	for _, rec := range s.memories {
		m := rec.Mem
		if m.TenantID != q.TenantID || m.Status != "ACTIVE" || !allowed[m.Sensitivity] {
			continue
		}
		if m.ExpiresAt != nil && !m.ExpiresAt.After(now()) {
			continue
		}
		switch m.Scope {
		case "organization", "agent":
		case "service":
			if !services[m.ScopeID] {
				continue
			}
		case "incident":
			if q.IncidentID == "" || m.ScopeID != q.IncidentID {
				continue
			}
		default:
			continue
		}
		shardWeight := 1.0
		if len(shards) > 0 {
			shardWeight = 0
			for _, sh := range s.shards[m.ID] {
				if shards[sh.ShardKey] && sh.Weight > shardWeight {
					shardWeight = sh.Weight
				}
			}
		}
		rank := 0.0
		if text != "" {
			hay := strings.ToLower(m.Title + "\n" + m.Content + "\n" + m.Subject)
			for _, term := range strings.Fields(text) {
				if strings.Contains(hay, term) {
					rank++
				}
			}
		}
		out = append(out, memory.Candidate{Memory: m, FTSRank: rank, ShardWeight: shardWeight})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FTSRank == out[j].FTSRank {
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		}
		return out[i].FTSRank > out[j].FTSRank
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) PutServiceDependency(ctx context.Context, tenantID, service, dependsOn, relation string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantID + "/" + service
	if s.serviceDeps[key] == nil {
		s.serviceDeps[key] = map[string]string{}
	}
	s.serviceDeps[key][dependsOn] = relation
	s.put("servicedep", key+"/"+dependsOn, tenantID, 0,
		map[string]string{"tenant_id": tenantID, "service": service, "depends_on": dependsOn, "relation": relation})
	return nil
}

func (s *Store) ServiceDependencies(ctx context.Context, tenantID, service string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for dep := range s.serviceDeps[tenantID+"/"+service] {
		out = append(out, dep)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) KnownServices(ctx context.Context, tenantID string, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	seen := map[string]bool{}
	for _, rec := range s.memories {
		if rec.Mem.TenantID == tenantID && rec.Mem.Scope == "service" && rec.Mem.ScopeID != "" {
			seen[rec.Mem.ScopeID] = true
		}
	}
	for _, inc := range s.incidents {
		if inc.TenantID == tenantID && inc.ServiceID != "" {
			seen[inc.ServiceID] = true
		}
	}
	for key, m := range s.serviceDeps {
		if strings.HasPrefix(key, tenantID+"/") {
			seen[strings.TrimPrefix(key, tenantID+"/")] = true
			for dep := range m {
				seen[dep] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for svc := range seen {
		out = append(out, svc)
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) ServiceShards(ctx context.Context, tenantID string, services []string, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	want := map[string]bool{}
	for _, svc := range services {
		want[svc] = true
	}
	seen := map[string]bool{}
	for _, rec := range s.memories {
		m := rec.Mem
		if m.TenantID != tenantID || m.Scope != "service" || !want[m.ScopeID] {
			continue
		}
		for _, sh := range s.shards[m.ID] {
			if strings.HasPrefix(sh.ShardKey, "capability:") {
				seen[sh.ShardKey] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) RunIncident(ctx context.Context, tenantID, runID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[runID]
	if !ok || r.TenantID != tenantID || r.IncidentID == nil {
		return "", domain.ErrNotFound
	}
	return *r.IncidentID, nil
}

func (s *Store) PutFeedback(ctx context.Context, tenantID, runID string, f memory.Feedback) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.feedback[runID]
	replaced := false
	for i := range list {
		if list[i].ActorID == f.ActorID {
			list[i], replaced = f, true
			break
		}
	}
	if !replaced {
		list = append(list, f)
	}
	s.feedback[runID] = list
	s.put("feedback", runID+"/"+f.ActorID, tenantID, 0, f)
	// Re-run consolidation so experience quality and reasoning scores pick
	// up the verdict, as the server does.
	s.requeueConsolidationLocked(tenantID, runID)
	if f.RootCauseCorrect != nil {
		moves := [][2]string{{"CANDIDATE", "VALIDATING"}}
		if !*f.RootCauseCorrect {
			moves = [][2]string{{"CANDIDATE", "REJECTED"}, {"VALIDATING", "REJECTED"}, {"ACTIVE", "STALE"}}
		}
		for _, rec := range s.memories {
			m := rec.Mem
			if m.TenantID != tenantID || !containsString(m.SourceRefs, "run:"+runID) {
				continue
			}
			for _, mv := range moves {
				if m.Status == mv[0] {
					m.Status, m.UpdatedAt = mv[1], now()
					s.saveMemory(memoryRec{Mem: m})
					break
				}
			}
		}
	}
	return nil
}

func (s *Store) ListFeedback(ctx context.Context, tenantID, runID string) ([]memory.Feedback, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]memory.Feedback(nil), s.feedback[runID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out, nil
}

func (s *Store) InsertExperience(ctx context.Context, e memory.Experience) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := e.TenantID + "/" + e.RunID
	if len(s.experiences[key]) > 0 {
		return false, nil
	}
	v := pg.ExperienceView{ID: newID(), RunID: e.RunID, Outcome: e.Outcome, LessonsRef: e.LessonsRef,
		QualityState: e.QualityState, CreatedAt: now()}
	s.experiences[key] = append(s.experiences[key], v)
	s.put("experience", key, e.TenantID, 0, struct {
		TenantID string            `json:"tenant_id"`
		View     pg.ExperienceView `json:"view"`
	}{e.TenantID, v})
	return true, nil
}

func (s *Store) ListExperiences(ctx context.Context, tenantID, runID string) ([]pg.ExperienceView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []pg.ExperienceView
	for key, list := range s.experiences {
		if !strings.HasPrefix(key, tenantID+"/") {
			continue
		}
		if runID != "" && key != tenantID+"/"+runID {
			continue
		}
		out = append(out, list...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if len(out) > 200 {
		out = out[:200]
	}
	return out, nil
}

func (s *Store) CreateIncident(ctx context.Context, tenantID, serviceID, environment string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := incidentRec{ID: newID(), TenantID: tenantID, ServiceID: serviceID, Environment: environment,
		Status: "OPEN", CreatedAt: now()}
	s.saveIncident(v)
	return v.ID, nil
}

func (s *Store) AppendIncidentEvent(ctx context.Context, tenantID, incidentID string, e pg.IncidentEvent, expected *int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.incidents[incidentID]
	if !ok || inc.TenantID != tenantID {
		return 0, domain.ErrNotFound
	}
	if expected != nil && *expected != inc.MemoryVersion {
		return 0, domain.ErrVersionConflict
	}
	inc.MemoryVersion++
	if e.Trust == "" {
		e.Trust = "T2"
	}
	e.Version, e.CreatedAt = inc.MemoryVersion, now()
	s.saveIncident(inc)
	s.incidentEvents[incidentID] = append(s.incidentEvents[incidentID], e)
	s.put("incidentevent", fmt.Sprintf("%s/%d", incidentID, e.Version), incidentID, e.Version, e)
	return e.Version, nil
}

func (s *Store) ListIncidentEvents(ctx context.Context, tenantID, incidentID string, upTo int64) ([]pg.IncidentEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.incidents[incidentID]
	if !ok || inc.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	var out []pg.IncidentEvent
	for _, e := range s.incidentEvents[incidentID] {
		if upTo == 0 || e.Version <= upTo {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

var _ = json.Marshal
