// Package wire adapts the memory stores (PostgreSQL and the local SQLite
// store, which share the memory API) to curate.Memories. It lives apart from
// package curate because the stores import curate for its proposal type.
package wire

import (
	"context"
	"time"

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

// MemoryStore is what both stores provide.
type MemoryStore interface {
	ListMemories(ctx context.Context, tenantID string, f pg.MemoryFilter) ([]memory.Memory, error)
	GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error)
	TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error)
	MemoryLastHits(ctx context.Context, tenantID string, ids []string) (map[string]time.Time, error)
}

type adapter struct{ MemoryStore }

// Memories wraps a store for the curator.
func Memories(s MemoryStore) curate.Memories { return adapter{s} }

func (a adapter) ActiveMemories(ctx context.Context, tenantID string, limit int) ([]memory.Memory, error) {
	return a.ListMemories(ctx, tenantID, pg.MemoryFilter{Status: "ACTIVE", Limit: limit})
}
