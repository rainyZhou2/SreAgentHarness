package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/memory"
)

func TestMemoryTiersPG(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	w := memory.Writer{Embedder: memory.HashingEmbedder{Dims: 256}, Artifacts: artifact.NewMemory()}
	prep := func(nm memory.NewMemory) memory.NewMemory {
		nm.TenantID, nm.CreatedBy = tenant, "test"
		if nm.Type == "" {
			nm.Type = "semantic"
		}
		p, err := w.Prepare(ctx, nm)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Personal memory: scope 'user' is accepted, kept out of shared retrieval.
	pm, created, err := admin.CreateMemory(ctx, prep(memory.NewMemory{Scope: "user", ScopeID: "u1", Status: "ACTIVE",
		Subject: memory.PersonalSubject("answer language"), Title: "answer language", Content: "中文", Sensitivity: "restricted",
		SourceRefs: []string{"run:a"}}))
	if err != nil || !created {
		t.Fatalf("personal: %v", err)
	}
	mine, err := admin.PersonalMemories(ctx, tenant, "u1", 0)
	if err != nil || len(mine) != 1 || mine[0].ID != pm.ID {
		t.Fatalf("personal list: %v %+v", err, mine)
	}
	if other, _ := admin.PersonalMemories(ctx, tenant, "u2", 0); len(other) != 0 {
		t.Fatal("other user")
	}
	cands, _ := admin.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: tenant, Text: "answer language", MaxSensitivity: "restricted"})
	if len(cands) != 0 {
		t.Fatal("personal memory in shared candidates")
	}
	// Same content again from another run: reinforced once; same run again: unchanged.
	again, created, _ := admin.CreateMemory(ctx, prep(memory.NewMemory{Scope: "user", ScopeID: "u1", Status: "ACTIVE",
		Subject: memory.PersonalSubject("answer language"), Title: "answer language", Content: "中文", Sensitivity: "restricted",
		SourceRefs: []string{"run:b"}}))
	if created || again.ID != pm.ID || again.Reinforcements != 1 || again.LastReinforcedAt == nil {
		t.Fatalf("reinforce: %v %+v", created, again)
	}
	again, _, _ = admin.CreateMemory(ctx, prep(memory.NewMemory{Scope: "user", ScopeID: "u1", Status: "ACTIVE",
		Subject: memory.PersonalSubject("answer language"), Title: "answer language", Content: "中文", Sensitivity: "restricted",
		SourceRefs: []string{"run:b"}}))
	if again.Reinforcements != 1 {
		t.Fatal("a known source does not reinforce again")
	}
	// Near duplicate in service scope.
	base := "API latency rises when the connection pool of the api service is exhausted under load."
	a, _, _ := admin.CreateMemory(ctx, prep(memory.NewMemory{Scope: "service", ScopeID: "api", Status: "ACTIVE",
		Subject: "api:root_cause", Title: "API latency", Content: base, SourceRefs: []string{"run:1"}}))
	b, created, _ := admin.CreateMemory(ctx, prep(memory.NewMemory{Scope: "service", ScopeID: "api", Status: "CANDIDATE",
		Subject: "api:root_cause", Title: "API latency", Content: base + "!", SourceRefs: []string{"run:2"}}))
	if created || b.ID != a.ID || b.Reinforcements != 1 {
		t.Fatalf("near duplicate: %v %+v", created, b)
	}
	// Revision: version 2, old version kept, embedding of the new version.
	cur, ok, err := admin.LiveMemoryBySubject(ctx, tenant, "service", "api", "api:root_cause")
	if err != nil || !ok || cur.ID != a.ID {
		t.Fatalf("by subject: %v %v", ok, err)
	}
	p, _ := w.PrepareRevision(ctx, cur, "", "Pool exhaustion; raise max_open_conns to 50.", nil, "user:ops")
	up, err := admin.UpdateMemory(ctx, memory.Revision{TenantID: tenant, ID: a.ID, ExpectedVersion: 1, Prepared: p, Actor: "ops"})
	if err != nil || up.Version != 2 || up.Content != "Pool exhaustion; raise max_open_conns to 50." {
		t.Fatalf("update: %v %+v", err, up)
	}
	if _, err := admin.UpdateMemory(ctx, memory.Revision{TenantID: tenant, ID: a.ID, ExpectedVersion: 1, Prepared: p}); err == nil {
		t.Fatal("stale version must conflict")
	}
	var versions, embs int
	_ = admin.pool.QueryRow(ctx, `SELECT count(*) FROM agent_memory_versions WHERE memory_id=$1`, a.ID).Scan(&versions)
	_ = admin.pool.QueryRow(ctx, `SELECT count(*) FROM memory_embeddings WHERE memory_id=$1 AND memory_version=2`, a.ID).Scan(&embs)
	if versions != 2 || embs != 1 {
		t.Fatalf("versions=%d embeddings(v2)=%d", versions, embs)
	}
	got, _ := admin.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: tenant, Services: []string{"api"}, Text: "max_open_conns",
		EmbeddingModel: "hash-uni-bigram-v2-256", MaxSensitivity: "internal"})
	if len(got) != 1 || got[0].FTSRank <= 0 || len(got[0].Embedding) != 256 || got[0].Reinforcements != 1 {
		t.Fatalf("search sees the new version: %+v", got)
	}
	runID, _ := seedRun(t, admin, tenant)
	if u, err := admin.RunInitiator(ctx, tenant, runID); err != nil || u != "" {
		t.Fatalf("seeded run is not a user run: %q %v", u, err)
	}
}
