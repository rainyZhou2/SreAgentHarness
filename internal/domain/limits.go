package domain

import "context"

type memoryTopKKey struct{}

// WithMemoryTopK passes the run profile's memory_top_k to a memory retriever.
func WithMemoryTopK(ctx context.Context, k int) context.Context {
	if k <= 0 {
		return ctx
	}
	return context.WithValue(ctx, memoryTopKKey{}, k)
}

// MemoryTopK returns the profile's memory_top_k, or def when unset.
func MemoryTopK(ctx context.Context, def int) int {
	if k, ok := ctx.Value(memoryTopKKey{}).(int); ok && k > 0 {
		return k
	}
	return def
}

type memoryExcludeKey struct{}

// WithMemoryExclude passes memory ids the run profile hides from retrieval
// (reasoning.patterns_excluded).
func WithMemoryExclude(ctx context.Context, ids []string) context.Context {
	if len(ids) == 0 {
		return ctx
	}
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return context.WithValue(ctx, memoryExcludeKey{}, set)
}

// MemoryExcluded reports whether the run profile hides a memory.
func MemoryExcluded(ctx context.Context, id string) bool {
	set, _ := ctx.Value(memoryExcludeKey{}).(map[string]bool)
	return set[id]
}
