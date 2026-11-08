package memsearch

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/tools"
)

func TestMemorySearchCJKAndBackfill(t *testing.T) {
	ctx := context.Background()
	s, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := memory.HashingEmbedder{Dims: 32}
	w := memory.Writer{Embedder: old, Artifacts: artifact.NewMemory()}
	for _, nm := range []memory.NewMemory{
		{Title: "数据库连接池耗尽", Content: "订单服务高峰期连接池打满，扩大 max_open_conns 后恢复", Subject: "db"},
		{Title: "磁盘写满", Content: "日志未轮转导致磁盘写满", Subject: "disk"},
	} {
		nm.TenantID, nm.Scope, nm.Type, nm.Status, nm.Trust, nm.Sensitivity = "t", "organization", "episodic", "ACTIVE", "T2", "internal"
		nm.CreatedBy = "test"
		p, err := w.Prepare(ctx, nm)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.CreateMemory(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	// Switch embedding models: the indexer backfills the new space.
	neu := memory.HashingEmbedder{Dims: 48}
	r := &memory.Retriever{Store: s, Embedder: neu, TopK: 5}
	n, err := (&memory.Indexer{Store: s, Embedder: neu}).Tick(ctx)
	if err != nil || n != 2 {
		t.Fatalf("backfill: %d %v", n, err)
	}
	if n, _ := (&memory.Indexer{Store: s, Embedder: neu}).Tick(ctx); n != 0 {
		t.Fatal("backfill must be idempotent")
	}
	tool := Tool{Retriever: r}
	cctx := tools.WithCallInfo(ctx, tools.CallInfo{TenantID: "t", RunID: "r1"})
	out, err := tool.Invoke(cctx, "", json.RawMessage(`{"query":"连接池耗尽怎么处理","top_k":3}`))
	if err != nil {
		t.Fatal(err)
	}
	hits := Recalled(out)
	if len(hits) == 0 || hits[0].Title != "数据库连接池耗尽" {
		t.Fatalf("hits: %s", out)
	}
	if hits[0].Ref == "" || len(hits[0].Branches) < 2 {
		t.Fatalf("expected fts+vector branches: %+v", hits[0])
	}
	ex := domain.WithMemoryExclude(cctx, []string{hits[0].MemoryID})
	out, _ = tool.Invoke(ex, "", json.RawMessage(`{"query":"连接池耗尽"}`))
	for _, h := range Recalled(out) {
		if h.MemoryID == hits[0].MemoryID {
			t.Fatal("excluded memory returned")
		}
	}
	if _, err := tool.Invoke(ctx, "", json.RawMessage(`{"query":"x"}`)); err == nil {
		t.Fatal("no tenant must fail")
	}
	if _, err := tool.Invoke(cctx, "", json.RawMessage(`{"query":" "}`)); err == nil {
		t.Fatal("empty query must fail")
	}
}
