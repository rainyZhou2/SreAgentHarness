package memory

import (
	"context"
	"sort"
	"strings"

	"go-sre-harness/internal/domain"
)

// Subject suffixes of the procedural memories the planner reuses (defined
// by the reasoning package, which imports this one).
const (
	planExemplarSuffix     = ":plan_exemplar"
	reasoningPatternSuffix = ":reasoning_pattern"
)

// PlanMemory is one memory offered to the planner.
type PlanMemory struct {
	Memory
	Kind  string  // plan | pattern
	Score float64 // similarity to the goal
}

// PlanMemories returns ACTIVE plan exemplars and reasoning patterns for a
// goal: at most k of each, ranked by similarity to the goal, restricted to
// the organization and the services in play. Memories the run profile
// excludes (domain.WithMemoryExclude) and untrusted (T4) ones are skipped.
func (r *Retriever) PlanMemories(ctx context.Context, tenantID, goal string, services []string, k int) ([]PlanMemory, error) {
	if k <= 0 {
		k = 2
	}
	if s := ServiceFromGoal(goal); s != "" {
		services = append(services, s)
	}
	services = uniqueStrings(services, 10)
	shards := []string{"kind:plan_exemplar"}
	for _, s := range services {
		shards = append(shards, "service:"+s)
	}
	model := ""
	if r.Embedder != nil {
		model = r.Embedder.Model()
	}
	cands, err := r.Store.MemoryCandidates(ctx, CandidateQuery{TenantID: tenantID, Services: services, Text: goal,
		EmbeddingModel: model, MaxSensitivity: "internal", Shards: shards, Limit: 300})
	if err != nil {
		return nil, domain.Retryable(err, 0)
	}
	var q []float32
	if r.Embedder != nil {
		q = r.Embedder.Embed(goal)
	}
	var plans, patterns []PlanMemory
	env, at := r.environment(ctx, tenantID, "", services), r.now()
	for _, c := range cands {
		m := c.Memory
		if ok, _ := m.Recallable(tenantID, env, at); !ok || m.Scope == "user" || (m.Sensitivity != "public" && m.Sensitivity != "internal") {
			continue
		}
		if m.Type != "procedural" || m.Trust == "T4" || domain.MemoryExcluded(ctx, m.ID) {
			continue
		}
		kind := ""
		switch {
		case strings.HasSuffix(m.Subject, planExemplarSuffix):
			kind = "plan"
		case strings.HasSuffix(m.Subject, reasoningPatternSuffix):
			kind = "pattern"
		default:
			continue
		}
		score := c.FTSRank
		if len(q) > 0 && len(c.Embedding) > 0 {
			score += Cosine(q, c.Embedding)
		}
		score *= 1 + 0.25*minf(c.ShardWeight, 1)
		if score <= 0 {
			continue
		}
		pm := PlanMemory{Memory: m, Kind: kind, Score: score}
		if kind == "plan" {
			plans = append(plans, pm)
		} else {
			patterns = append(patterns, pm)
		}
	}
	top := func(xs []PlanMemory) []PlanMemory {
		sort.SliceStable(xs, func(i, j int) bool {
			if xs[i].Score != xs[j].Score {
				return xs[i].Score > xs[j].Score
			}
			return xs[i].ID < xs[j].ID
		})
		if len(xs) > k {
			xs = xs[:k]
		}
		return xs
	}
	return append(top(plans), top(patterns)...), nil
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
