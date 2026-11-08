package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
)

// CandidateQuery is the hard-filter stage, evaluated in SQL: tenant, status
// ACTIVE, scope (organization, the services in play, the incident), not
// expired, sensitivity allowed. Only filtered rows reach ranking.
type CandidateQuery struct {
	TenantID       string
	Services       []string
	IncidentID     string
	Text           string
	EmbeddingModel string
	MaxSensitivity string // public | internal | restricted
	Shards         []string
	Limit          int
}

type Candidate struct {
	Memory
	FTSRank     float64
	ShardWeight float64
}

type Store interface {
	MemoryCandidates(ctx context.Context, q CandidateQuery) ([]Candidate, error)
	ServiceDependencies(ctx context.Context, tenantID, service string) ([]string, error)
	KnownServices(ctx context.Context, tenantID string, limit int) ([]string, error)
	ServiceShards(ctx context.Context, tenantID string, services []string, limit int) ([]string, error)
	RunIncident(ctx context.Context, tenantID, runID string) (string, error)
}

// Retriever executes retrieve_memory steps (implements kernel.MemoryRetriever).
type Retriever struct {
	Store     Store
	Embedder  Embedder
	Artifacts artifact.Store
	TopK      int
}

func uniqueStrings(in []string, max int) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

type hitView struct {
	MemoryID string   `json:"memory_id"`
	Version  int64    `json:"version"`
	Title    string   `json:"title"`
	Scope    string   `json:"scope"`
	Trust    string   `json:"trust"`
	Score    float64  `json:"score"`
	Branches []string `json:"branches"`
}

// SearchResult is one retrieval: the kept memories in rank order, their
// fused scores and the branches that found them.
type SearchResult struct {
	Query      string
	Service    string
	Deps       []string
	Shards     []string
	Candidates int
	Kept       []Memory
	Scores     map[string]float64
	Branches   map[string][]string
	Conflicts  map[string][]string
}

// Search runs the retrieval pipeline for a query: hard filters (tenant,
// ACTIVE, scope, expiry, sensitivity), full-text, vector and dependency
// branches, RRF fusion with shard affinity, trust and exclusion filters,
// conflict resolution and top-k. Extra services widen the scope filter.
func (r *Retriever) Search(ctx context.Context, tenantID, runID, query string, extra []string, k int) (SearchResult, error) {
	service := ServiceFromGoal(query)
	services := []string{}
	if service != "" {
		services = append(services, service)
	}
	for _, s := range extra {
		if s = strings.TrimSpace(s); s != "" {
			if service == "" {
				service = s
			}
			services = append(services, s)
		}
	}
	if known, err := r.Store.KnownServices(ctx, tenantID, 200); err == nil {
		lower := strings.ToLower(query)
		for _, s := range known {
			if s == "" || s == service || !strings.Contains(lower, strings.ToLower(s)) {
				continue
			}
			services = append(services, s)
		}
	}
	var deps []string
	if service != "" {
		d, err := r.Store.ServiceDependencies(ctx, tenantID, service)
		if err != nil {
			return SearchResult{}, domain.Retryable(err, 0)
		}
		deps = d
		services = append(services, deps...)
	}
	services = uniqueStrings(services, 50)
	incident := ""
	shards := []string{}
	if runID != "" {
		incident, _ = r.Store.RunIncident(ctx, tenantID, runID)
		shards = append(shards, "run:"+runID)
	}
	for _, s := range services {
		shards = append(shards, "service:"+s)
	}
	if incident != "" {
		shards = append(shards, "incident:"+incident)
	}
	if more, err := r.Store.ServiceShards(ctx, tenantID, services, 20); err == nil {
		shards = append(shards, more...)
	}
	shards = uniqueStrings(shards, 20)
	cands, err := r.Store.MemoryCandidates(ctx, CandidateQuery{TenantID: tenantID, Services: services,
		IncidentID: incident, Text: query, EmbeddingModel: r.Embedder.Model(), MaxSensitivity: "internal",
		Shards: shards, Limit: 500})
	if err != nil {
		return SearchResult{}, domain.Retryable(err, 0)
	}

	byID := map[string]Memory{}
	shardW := map[string]float64{}
	var fts, vec, graph []string
	type scored struct {
		id string
		s  float64
	}
	var ftsS, vecS []scored
	q := r.Embedder.Embed(query)
	depSet := map[string]bool{}
	for _, d := range deps {
		depSet[d] = true
	}
	for _, c := range cands {
		byID[c.ID] = c.Memory
		shardW[c.ID] = c.ShardWeight
		if c.FTSRank > 0 {
			ftsS = append(ftsS, scored{c.ID, c.FTSRank})
		}
		if len(q) > 0 && len(c.Embedding) > 0 {
			if s := Cosine(q, c.Embedding); s > 0.05 {
				vecS = append(vecS, scored{c.ID, s})
			}
		}
		if c.Scope == "service" && depSet[c.ScopeID] {
			graph = append(graph, c.ID)
		}
	}
	sort.SliceStable(ftsS, func(i, j int) bool { return ftsS[i].s > ftsS[j].s })
	sort.SliceStable(vecS, func(i, j int) bool { return vecS[i].s > vecS[j].s })
	for _, s := range ftsS {
		fts = append(fts, s.id)
	}
	for _, s := range vecS {
		vec = append(vec, s.id)
	}
	fused := RRF(fts, vec, graph)
	for id, score := range fused {
		affinity := shardW[id]
		if affinity > 1 {
			affinity = 1
		}
		fused[id] = score * (1 + 0.25*affinity) * ReinforcementBoost(byID[id].Reinforcements)
	}
	ids := make([]string, 0, len(fused))
	for id := range fused {
		ids = append(ids, id)
	}
	sortByScore(ids, fused)

	var ranked []Memory
	for _, id := range ids {
		m := byID[id]
		if m.Trust == "T4" { // untrusted text is never injected as memory
			continue
		}
		if domain.MemoryExcluded(ctx, m.ID) { // retired by the run profile
			continue
		}
		ranked = append(ranked, m)
	}
	kept, conflicts := ResolveConflicts(ranked)
	if k <= 0 {
		k = r.TopK
	}
	if k <= 0 {
		k = 5
	}
	k = domain.MemoryTopK(ctx, k) // the run profile's memory_top_k wins
	if len(kept) > k {
		kept = kept[:k]
	}
	branches := map[string][]string{}
	for name, l := range map[string][]string{"fts": fts, "vector": vec, "graph": graph} {
		for _, id := range l {
			branches[id] = append(branches[id], name)
		}
	}
	for id := range branches {
		sort.Strings(branches[id])
	}
	return SearchResult{Query: query, Service: service, Deps: deps, Shards: shards, Candidates: len(cands),
		Kept: kept, Scores: fused, Branches: branches, Conflicts: conflicts}, nil
}

// personal returns the run initiator's personal memories (none when the
// store keeps none or the run was not started by a user).
func (r *Retriever) personal(ctx context.Context, tenantID, runID string) []Memory {
	ps, ok := r.Store.(PersonalStore)
	if !ok || runID == "" {
		return nil
	}
	user, err := ps.RunInitiator(ctx, tenantID, runID)
	if err != nil || user == "" {
		return nil
	}
	mems, err := ps.PersonalMemories(ctx, tenantID, user, MaxPersonalMemories)
	if err != nil {
		return nil
	}
	out := mems[:0]
	for _, m := range mems {
		if m.Scope == "user" && m.ScopeID == user && m.Trust != "T4" && !domain.MemoryExcluded(ctx, m.ID) {
			out = append(out, m)
		}
	}
	return out
}

func (r *Retriever) Retrieve(ctx context.Context, lease domain.Lease, goal string) (domain.StepSuccess, error) {
	res, err := r.Search(ctx, lease.TenantID, lease.RunID, goal, nil, 0)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	var hits []domain.MemoryHit
	var views []hitView
	for i, m := range res.Kept {
		hits = append(hits, domain.MemoryHit{MemoryID: m.ID, Version: m.Version, Score: res.Scores[m.ID], Rank: i + 1})
		views = append(views, hitView{MemoryID: m.ID, Version: m.Version, Title: m.Title, Scope: m.Scope + ":" + m.ScopeID,
			Trust: m.Trust, Score: res.Scores[m.ID], Branches: res.Branches[m.ID]})
	}
	// Personal memory of the user who started the run is pinned with the
	// rest, so every step and retry of the run sees the same preferences.
	personal := 0
	for _, m := range r.personal(ctx, lease.TenantID, lease.RunID) {
		personal++
		hits = append(hits, domain.MemoryHit{MemoryID: m.ID, Version: m.Version, Rank: len(hits) + 1})
		views = append(views, hitView{MemoryID: m.ID, Version: m.Version, Title: m.Title, Scope: "user", Trust: m.Trust,
			Branches: []string{"personal"}})
	}
	out := map[string]any{"query": goal, "service": res.Service, "dependencies": res.Deps, "shards": res.Shards,
		"candidates": res.Candidates, "embedding_model": r.Embedder.Model(), "hits": views, "conflicts": res.Conflicts,
		"personal": personal}
	data, _ := json.Marshal(out)
	ref, h, err := r.Artifacts.Put(ctx, lease.TenantID, "memory-retrieval", data)
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("artifact upload: %w", err), 0)
	}
	summary, _ := json.Marshal(map[string]any{"hits": len(hits), "candidates": res.Candidates, "conflicts": len(res.Conflicts)})
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "memory_retrieval", MemoryHits: hits,
		Summary: summary}, nil
}
