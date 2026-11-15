package localstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"go-sre-harness/internal/memory"
)

func migrateMemoryGovernance(ctx context.Context, tx *sql.Tx, _ string) error {
	rows, err := tx.QueryContext(ctx, `SELECT id,data FROM objects WHERE kind='memory'`)
	if err != nil {
		return err
	}
	type record struct {
		id  string
		rec memoryRec
	}
	records := []record{}
	for rows.Next() {
		var r record
		var data []byte
		if err := rows.Scan(&r.id, &data); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(data, &r.rec); err != nil {
			rows.Close()
			return fmt.Errorf("decode memory %s: %w", r.id, err)
		}
		records = append(records, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range records {
		upgraded, err := upgradeMemoryRecord(r.rec)
		if err != nil {
			return fmt.Errorf("upgrade memory %s: %w", r.id, err)
		}
		data, err := json.Marshal(upgraded)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET data=? WHERE kind='memory' AND id=?`, data, r.id); err != nil {
			return err
		}
	}
	return nil
}

func upgradeMemoryRecord(rec memoryRec) (memoryRec, error) {
	switch rec.Mem.Status {
	case "ACTIVE":
		rec.Mem.Status = memory.Approved
		if rec.Mem.ApprovalBasis == "" {
			rec.Mem.ApprovalBasis = "MIGRATED_ACTIVE"
		}
	case "VALIDATING":
		rec.Mem.Status = memory.StatusCandidate
	case "REJECTED", "ARCHIVED":
		rec.Mem.Status = memory.Revoked
	case memory.StatusCandidate, memory.Verified, memory.Approved, memory.Contested, memory.Stale, memory.Revoked, memory.Superseded:
	default:
		return memoryRec{}, fmt.Errorf("unknown status %q", rec.Mem.Status)
	}
	defaults := initialMemoryACL(memory.NewMemory{TenantID: rec.Mem.TenantID, Scope: rec.Mem.Scope,
		ScopeID: rec.Mem.ScopeID, CreatedBy: rec.Mem.CreatedBy}, rec.Mem.ID)
	for _, grant := range defaults {
		found := false
		for _, current := range rec.ACL {
			if current == grant {
				found = true
				break
			}
		}
		if !found {
			rec.ACL = append(rec.ACL, grant)
		}
	}
	embeddings := make(map[string][]float32, len(rec.Embeddings))
	descriptors := make(map[string]memory.EmbeddingDescriptor, len(rec.Embeddings))
	for oldKey, vector := range rec.Embeddings {
		descriptor := rec.EmbeddingDescriptors[oldKey]
		if !descriptor.Valid() {
			descriptor = memory.LegacyEmbeddingDescriptor(oldKey, len(vector))
		}
		if !descriptor.Matches(vector) {
			return memoryRec{}, fmt.Errorf("invalid embedding descriptor %q", oldKey)
		}
		key := descriptor.Key()
		if existing, ok := embeddings[key]; ok && !slices.Equal(existing, vector) {
			return memoryRec{}, fmt.Errorf("conflicting embeddings for descriptor %q", oldKey)
		}
		embeddings[key] = vector
		descriptors[key] = descriptor
	}
	rec.Embeddings, rec.EmbeddingDescriptors = embeddings, descriptors
	if !rec.Mem.EmbeddingDescriptor.Valid() && len(rec.Mem.Embedding) > 0 {
		rec.Mem.EmbeddingDescriptor = memory.LegacyEmbeddingDescriptor(rec.Mem.EmbeddingModel, len(rec.Mem.Embedding))
		if !rec.Mem.EmbeddingDescriptor.Valid() {
			return memoryRec{}, fmt.Errorf("invalid current embedding descriptor")
		}
	}
	rec.Mem.EmbeddingVersion = rec.Mem.EmbeddingDescriptor.Version
	return rec, nil
}
