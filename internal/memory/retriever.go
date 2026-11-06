package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

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
	Limit          int
}

type Candidate struct {
	Memory
	FTSRank float64
}

type Store interface {
	MemoryCandidates(ctx context.Context, q CandidateQuery) ([]Candidate, error)
	ServiceDependencies(ctx context.Context, tenantID, service string) ([]string, error)
	RunIncident(ctx context.Context, tenantID, runID string) (string, error)
}

// Retriever executes retrieve_memory steps (implements kernel.MemoryRetriever).
type Retriever struct {
	Store     Store
	Embedder  Embedder
	Artifacts artifact.Store
	TopK      int
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

func (r *Retriever) Retrieve(ctx context.Context, lease domain.Lease, goal string) (domain.StepSuccess, error) {
	service := ServiceFromGoal(goal)
	services := []string{}
	var deps []string
	if service != "" {
		services = append(services, service)
		d, err := r.Store.ServiceDependencies(ctx, lease.TenantID, service)
		if err != nil {
			return domain.StepSuccess{}, domain.Retryable(err, 0)
		}
		deps = d
		services = append(services, deps...)
	}
	incident, _ := r.Store.RunIncident(ctx, lease.TenantID, lease.RunID)
	cands, err := r.Store.MemoryCandidates(ctx, CandidateQuery{TenantID: lease.TenantID, Services: services,
		IncidentID: incident, Text: goal, EmbeddingModel: r.Embedder.Model(), MaxSensitivity: "internal", Limit: 500})
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(err, 0)
	}

	byID := map[string]Memory{}
	var fts, vec, graph []string
	type scored struct {
		id string
		s  float64
	}
	var ftsS, vecS []scored
	q := r.Embedder.Embed(goal)
	depSet := map[string]bool{}
	for _, d := range deps {
		depSet[d] = true
	}
	for _, c := range cands {
		byID[c.ID] = c.Memory
		if c.FTSRank > 0 {
			ftsS = append(ftsS, scored{c.ID, c.FTSRank})
		}
		if len(c.Embedding) > 0 {
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
		ranked = append(ranked, m)
	}
	kept, conflicts := ResolveConflicts(ranked)
	k := r.TopK
	if k <= 0 {
		k = 5
	}
	k = domain.MemoryTopK(ctx, k) // the run profile's memory_top_k wins
	if len(kept) > k {
		kept = kept[:k]
	}
	branches := func(id string) []string {
		var b []string
		for name, l := range map[string][]string{"fts": fts, "vector": vec, "graph": graph} {
			for _, x := range l {
				if x == id {
					b = append(b, name)
				}
			}
		}
		sort.Strings(b)
		return b
	}
	var hits []domain.MemoryHit
	var views []hitView
	for i, m := range kept {
		hits = append(hits, domain.MemoryHit{MemoryID: m.ID, Version: m.Version, Score: fused[m.ID], Rank: i + 1})
		views = append(views, hitView{MemoryID: m.ID, Version: m.Version, Title: m.Title, Scope: m.Scope + ":" + m.ScopeID,
			Trust: m.Trust, Score: fused[m.ID], Branches: branches(m.ID)})
	}
	out := map[string]any{"query": goal, "service": service, "dependencies": deps, "candidates": len(cands),
		"embedding_model": r.Embedder.Model(), "hits": views, "conflicts": conflicts}
	data, _ := json.Marshal(out)
	ref, h, err := r.Artifacts.Put(ctx, lease.TenantID, "memory-retrieval", data)
	if err != nil {
		return domain.StepSuccess{}, domain.Retryable(fmt.Errorf("artifact upload: %w", err), 0)
	}
	summary, _ := json.Marshal(map[string]any{"hits": len(hits), "candidates": len(cands), "conflicts": len(conflicts)})
	return domain.StepSuccess{OutputRef: ref, OutputHash: h, ArtifactKind: "memory_retrieval", MemoryHits: hits,
		Summary: summary}, nil
}
