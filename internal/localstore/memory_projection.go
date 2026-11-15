package localstore

import (
	"sort"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/memorycontext"
)

func memorySnapshotRef(rec memoryRec, hit memoryHitRec) cognitive.MemoryRef {
	m := rec.Mem
	if !m.EmbeddingDescriptor.Valid() && m.EmbeddingVersion != "" {
		keys := make([]string, 0, len(rec.EmbeddingDescriptors))
		for key := range rec.EmbeddingDescriptors {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			descriptor := rec.EmbeddingDescriptors[key]
			if descriptor.Valid() && descriptor.Version == m.EmbeddingVersion {
				m.EmbeddingDescriptor = descriptor
				break
			}
		}
	}
	verificationID := ""
	var latest memory.VerificationRecord
	for _, record := range rec.Verifications {
		if record.MemoryVersion != m.Version {
			continue
		}
		if verificationID == "" || record.CreatedAt.After(latest.CreatedAt) ||
			(record.CreatedAt.Equal(latest.CreatedAt) && record.ID > verificationID) {
			latest, verificationID = record, record.ID
		}
	}
	return memorycontext.Ref(m, hit.Score, verificationID)
}
