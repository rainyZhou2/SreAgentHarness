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
