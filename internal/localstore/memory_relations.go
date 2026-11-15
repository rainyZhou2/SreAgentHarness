package localstore

import (
	"context"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func conflictKey(pair memory.ConflictPair) string { return pair.FirstID + "\x00" + pair.SecondID }

func conflictProjection(rec *memoryRec, other string) {
	for _, id := range rec.Mem.ConflictsWith {
		if id == other {
			return
		}
	}
	rec.Mem.ConflictsWith = append(rec.Mem.ConflictsWith, other)
}

func (s *Store) DeclareMemoryConflict(ctx context.Context, record memory.ConflictRecord) (memory.ConflictRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.withStateTxLocked(ctx, func() error {
		first, ok := s.memories[record.Pair.FirstID]
		if !ok {
			return domain.ErrNotFound
		}
		second, ok := s.memories[record.Pair.SecondID]
		if !ok {
			return domain.ErrNotFound
		}
		pair, err := memory.NewConflictPair(first.Mem, second.Mem)
		if err != nil {
			return err
		}
		if record.Reason == "" || record.CreatedBy == "" {
			return &domain.RejectedError{Reason: "conflict_reason_required"}
		}
		if pair != record.Pair && pair.FirstID != record.Pair.FirstID {
			first, second = second, first
		}
		record.Pair, record.TenantID, record.CreatedAt = pair, first.Mem.TenantID, now()
		record.ResolvedBy, record.ResolvedAt, record.Resolution = "", nil, ""
		key := conflictKey(pair)
		if existing, ok := first.Conflicts[key]; ok && existing.ResolvedAt == nil {
			record = existing
			return nil
		}
		if first.Conflicts == nil {
			first.Conflicts = map[string]memory.ConflictRecord{}
		}
		if second.Conflicts == nil {
			second.Conflicts = map[string]memory.ConflictRecord{}
		}
		first.Conflicts[key], second.Conflicts[key] = record, record
		conflictProjection(&first, second.Mem.ID)
		conflictProjection(&second, first.Mem.ID)
		first.Mem.Status, second.Mem.Status = memory.Contested, memory.Contested
		first.Mem.UpdatedAt, second.Mem.UpdatedAt = record.CreatedAt, record.CreatedAt
		s.saveMemory(first)
		s.saveMemory(second)
		s.put("memory_audit", newID(), record.TenantID, 0, map[string]any{"action": "memory.conflict.declare",
			"first_memory_id": pair.FirstID, "second_memory_id": pair.SecondID, "actor": record.CreatedBy, "reason": record.Reason})
		return nil
	})
	return record, err
}

func (s *Store) ResolveMemoryConflict(ctx context.Context, tenantID string, pair memory.ConflictPair, actor, resolution string) (memory.ConflictRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record memory.ConflictRecord
	err := s.withStateTxLocked(ctx, func() error {
		if pair.FirstID > pair.SecondID {
			pair.FirstID, pair.SecondID = pair.SecondID, pair.FirstID
		}
		first, ok := s.memories[pair.FirstID]
		if !ok || first.Mem.TenantID != tenantID {
			return domain.ErrNotFound
		}
		second, ok := s.memories[pair.SecondID]
		if !ok || second.Mem.TenantID != tenantID {
			return domain.ErrNotFound
		}
		key := conflictKey(pair)
		record, ok = first.Conflicts[key]
		if !ok || record.ResolvedAt != nil {
			return domain.ErrNotFound
		}
		if actor == "" || resolution == "" {
			return &domain.RejectedError{Reason: "conflict_resolution_required"}
		}
		at := now()
		record.ResolvedBy, record.ResolvedAt, record.Resolution = actor, &at, resolution
		first.Conflicts[key], second.Conflicts[key] = record, record
		s.saveMemory(first)
		s.saveMemory(second)
		s.put("memory_audit", newID(), tenantID, 0, map[string]any{"action": "memory.conflict.resolve",
			"first_memory_id": pair.FirstID, "second_memory_id": pair.SecondID, "actor": actor, "resolution": resolution})
		return nil
	})
	return record, err
}

func (s *Store) SupersedeMemory(ctx context.Context, tenantID, oldID, replacementID string, expectedOldVersion int64, actor string) (memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var current memory.Memory
	err := s.withStateTxLocked(ctx, func() error {
		previous, ok := s.memories[oldID]
		if !ok || previous.Mem.TenantID != tenantID {
			return domain.ErrNotFound
		}
		next, ok := s.memories[replacementID]
		if !ok || next.Mem.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if previous.Mem.Version != expectedOldVersion {
			return domain.ErrVersionConflict
		}
		if err := memory.ValidateSupersession(previous.Mem, next.Mem); err != nil {
			return err
		}
		for id, seen := replacementID, map[string]bool{}; id != ""; {
			if id == oldID || seen[id] {
				return &domain.RejectedError{Reason: "supersession_cycle"}
			}
			seen[id] = true
			rec := s.memories[id]
			if rec.Mem.SupersededBy == nil {
				break
			}
			id = *rec.Mem.SupersededBy
		}
		previous.Mem.Status = memory.Superseded
		previous.Mem.SupersededBy = &replacementID
		previous.Mem.UpdatedAt = now()
		s.saveMemory(previous)
		s.put("memory_audit", newID(), tenantID, 0, map[string]any{"action": "memory.supersede",
			"memory_id": oldID, "superseded_by": replacementID, "version": expectedOldVersion, "actor": actor})
		current = previous.Mem
		return nil
	})
	return current, err
}
