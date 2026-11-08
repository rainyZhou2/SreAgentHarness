package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/memory"
)

func TestMemoryCJKSearchAndIndexPG(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	w := memory.Writer{Embedder: memory.HashingEmbedder{Dims: 16}, Artifacts: artifact.NewMemory()}
	p, err := w.Prepare(ctx, memory.NewMemory{TenantID: tenant, Scope: "organization", Type: "episodic", Status: "ACTIVE",
		Title: "数据库连接池耗尽", Content: "订单服务高峰期连接池打满", Subject: "db", Trust: "T2", Sensitivity: "internal",
		CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := admin.CreateMemory(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	cands, err := admin.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: tenant, Text: "连接池耗尽了怎么办",
		EmbeddingModel: "hash-uni-bigram-v2-16", MaxSensitivity: "internal"})
	if err != nil || len(cands) != 1 || cands[0].FTSRank <= 0 || len(cands[0].Embedding) != 16 {
		t.Fatalf("CJK full-text: %v %+v", err, cands)
	}
	// An older row without terms, and a new embedding model: the indexer fills both.
	if _, err := admin.pool.Exec(ctx, `UPDATE agent_memories SET search_terms='', search_terms_v=0 WHERE id=$1`, m.ID); err != nil {
		t.Fatal(err)
	}
	cands, _ = admin.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: tenant, Text: "耗尽", MaxSensitivity: "internal"})
	if len(cands) == 1 && cands[0].FTSRank > 0 {
		t.Fatal("without search terms a CJK fragment must not match")
	}
	neu := memory.HashingEmbedder{Dims: 24}
	x := &memory.Indexer{Store: admin, Embedder: neu, Batch: 1000}
	if _, err := x.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	cands, err = admin.MemoryCandidates(ctx, memory.CandidateQuery{TenantID: tenant, Text: "耗尽",
		EmbeddingModel: neu.Model(), MaxSensitivity: "internal"})
	if err != nil || len(cands) != 1 || cands[0].FTSRank <= 0 || len(cands[0].Embedding) != 24 {
		t.Fatalf("after indexing: %v %+v", err, cands)
	}
	items, _ := admin.MemoriesToIndex(ctx, neu.Model(), 1000)
	for _, it := range items {
		if it.ID == m.ID {
			t.Fatal("indexed memory listed again")
		}
	}
}
