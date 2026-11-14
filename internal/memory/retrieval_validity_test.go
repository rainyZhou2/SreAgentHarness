package memory

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type candidateStore struct{ candidates []Candidate }

func (s candidateStore) MemoryCandidates(context.Context, CandidateQuery) ([]Candidate, error) {
	return s.candidates, nil
}
func (candidateStore) ServiceDependencies(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (candidateStore) KnownServices(context.Context, string, int) ([]string, error) { return nil, nil }
func (candidateStore) ServiceShards(context.Context, string, []string, int) ([]string, error) {
	return nil, nil
}
func (candidateStore) RunIncident(context.Context, string, string) (string, error) { return "", nil }
func (candidateStore) RunInitiator(context.Context, string, string) (string, error) {
	return "user", nil
}
func (s candidateStore) PersonalMemories(context.Context, string, string, int) ([]Memory, error) {
	out := make([]Memory, 0, len(s.candidates))
	for _, c := range s.candidates {
		out = append(out, c.Memory)
	}
	return out, nil
}

func TestEveryRetrievalPathEnforcesMemoryValidity(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	for _, path := range []string{"search", "planner", "personal"} {
		t.Run(path, func(t *testing.T) {
			good := Memory{ID: "good", TenantID: "t", Status: "ACTIVE", Scope: "organization", Type: "procedural", Subject: "good:plan_exemplar", Trust: "T2", Sensitivity: "internal"}
			if path == "personal" {
				good.Scope, good.ScopeID = "user", "user"
			}
			candidates := []Candidate{{Memory: good, FTSRank: 1}}
			for i, mutate := range []func(*Memory){
				func(m *Memory) { m.TenantID = "foreign" },
				func(m *Memory) { m.Status = "STALE" },
				func(m *Memory) { m.Status = "REVOKED" },
				func(m *Memory) { m.Status = "CANDIDATE" },
				func(m *Memory) { m.Scope, m.ScopeID = "user", "another-user" },
				func(m *Memory) { m.ExpiresAt = &past },
				func(m *Memory) { m.SupersededBy = &good.ID },
				func(m *Memory) { m.Applicability = &Applicability{Environments: []string{"staging"}} },
				func(m *Memory) {
					m.Applicability = &Applicability{RiskOfReuse: "high", VersionConstraints: []string{"driver>=550"}}
				},
			} {
				m := good
				m.ID = fmt.Sprintf("invalid-%d", i)
				m.Subject = m.ID + ":plan_exemplar"
				mutate(&m)
				candidates = append(candidates, Candidate{Memory: m, FTSRank: 2})
			}
			r := Retriever{Store: candidateStore{candidates}, Embedder: HashingEmbedder{}, Now: func() time.Time { return now }}
			ctx := WithEnv(context.Background(), Env{Environment: "prod"})
			var kept []Memory
			switch path {
			case "search":
				res, err := r.Search(ctx, "t", "r", "recovery", nil, 10)
				if err != nil {
					t.Fatal(err)
				}
				kept = res.Kept
			case "planner":
				res, err := r.PlanMemories(ctx, "t", "recovery", nil, 10)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range res {
					kept = append(kept, m.Memory)
				}
			case "personal":
				kept = r.personal(ctx, "t", "r")
			}
			if len(kept) != 1 || kept[0].ID != "good" {
				t.Fatalf("unsafe memory recalled: %+v", kept)
			}
		})
	}
}
