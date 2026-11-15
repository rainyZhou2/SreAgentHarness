package localstore

import (
	"testing"
	"time"

	"go-sre-harness/internal/memory"
)

func TestMemorySnapshotRefUsesCurrentVersionVerificationAndEmbedding(t *testing.T) {
	now := time.Now().UTC()
	descriptor := memory.EmbeddingDescriptor{Model: "text-embedding", Version: "v3", Dimensions: 1536}
	rec := memoryRec{Mem: memory.Memory{ID: "m", Version: 4, Status: memory.Approved, EmbeddingVersion: "v3"},
		EmbeddingDescriptors: map[string]memory.EmbeddingDescriptor{descriptor.Key(): descriptor},
		Verifications: map[string]memory.VerificationRecord{
			"old":           {ID: "old", MemoryVersion: 4, CreatedAt: now.Add(-time.Hour)},
			"new":           {ID: "new", MemoryVersion: 4, CreatedAt: now},
			"other-version": {ID: "other-version", MemoryVersion: 3, CreatedAt: now.Add(time.Hour)},
		}}
	ref := memorySnapshotRef(rec, memoryHitRec{MemoryID: "m", Version: 4, Score: 0.8})
	if ref.VerificationID != "new" {
		t.Fatalf("verification: %q", ref.VerificationID)
	}
	if ref.Embedding == nil || ref.Embedding.Model != descriptor.Model || ref.Embedding.Dimensions != descriptor.Dimensions {
		t.Fatalf("embedding: %#v", ref.Embedding)
	}
}
