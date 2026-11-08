package domain

import (
	"context"
	"testing"
)

func TestMemoryExclude(t *testing.T) {
	ctx := WithMemoryExclude(context.Background(), []string{"m1"})
	if !MemoryExcluded(ctx, "m1") || MemoryExcluded(ctx, "m2") || MemoryExcluded(context.Background(), "m1") {
		t.Fatal("exclusion set mismatch")
	}
}
