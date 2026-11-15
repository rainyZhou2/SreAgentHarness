package localstore

import (
	"context"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func (s *Store) VerifyMemory(ctx context.Context, input memory.VerificationInput, verifier, idempotencyKey string) (memory.VerificationRecord, memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record memory.VerificationRecord
	var current memory.Memory
	err := s.withStateTxLocked(ctx, func() error {
		rec, ok := s.memories[input.MemoryID]
		if !ok || rec.Mem.TenantID != input.TenantID {
			return domain.ErrNotFound
		}
		if existing, ok := rec.Verifications[idempotencyKey]; ok {
			if existing.InputHash != input.InputHash || existing.DecisionRef != input.DecisionRef ||
				existing.OutcomeRef != input.OutcomeRef || existing.MemoryVersion != input.ExpectedVersion {
				return domain.ErrVersionConflict
			}
			return existingResult(existing, rec.Mem, &record, &current)
		}
		if rec.Mem.Version != input.ExpectedVersion {
			return domain.ErrVersionConflict
		}
		input.ActualHash = input.InputHash
		if err := input.Validate(); err != nil {
			return err
		}
		if rec.Mem.Status != memory.StatusCandidate && rec.Mem.Status != memory.Contested && rec.Mem.Status != memory.Stale {
			return &domain.RejectedError{Reason: "memory_not_verifiable_" + rec.Mem.Status}
		}
		now := now()
		record = memory.VerificationRecord{ID: newID(), TenantID: input.TenantID, MemoryID: input.MemoryID,
			MemoryVersion: input.ExpectedVersion, EvidenceRefs: append([]string(nil), input.EvidenceRefs...),
			DecisionRef: input.DecisionRef, OutcomeRef: input.OutcomeRef, InputHash: input.InputHash,
			Verifier: verifier, IdempotencyKey: idempotencyKey, CreatedAt: now}
		rec.Mem.Status = memory.Verified
		rec.Mem.VerifiedBy = stringPointer(verifier)
		rec.Mem.DecisionRef = input.DecisionRef
		rec.Mem.OutcomeRef = input.OutcomeRef
		rec.Mem.UpdatedAt = now
		if rec.Verifications == nil {
			rec.Verifications = map[string]memory.VerificationRecord{}
		}
		rec.Verifications[idempotencyKey] = record
		s.saveMemory(rec)
		s.put("memory_audit", newID(), rec.Mem.TenantID, 0, map[string]any{"action": "memory.verify",
			"memory_id": rec.Mem.ID, "version": rec.Mem.Version, "verification_id": record.ID, "actor": verifier})
		current = rec.Mem
		return nil
	})
	return record, current, err
}

func existingResult(record memory.VerificationRecord, current memory.Memory, outRecord *memory.VerificationRecord, outMemory *memory.Memory) error {
	*outRecord = record
	*outMemory = current
	return nil
}

func (s *Store) ApproveMemory(ctx context.Context, tenantID, id string, expectedVersion int64, approver string) (memory.Memory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var current memory.Memory
	err := s.withStateTxLocked(ctx, func() error {
		rec, ok := s.memories[id]
		if !ok || rec.Mem.TenantID != tenantID {
			return domain.ErrNotFound
		}
		if rec.Mem.Version != expectedVersion {
			return domain.ErrVersionConflict
		}
		if rec.Mem.Status != memory.Verified {
			return &domain.RejectedError{Reason: "memory_not_verified"}
		}
		if err := memory.CanPromote(rec.Mem, approver); err != nil {
			return &domain.RejectedError{Reason: "playbook_promotion: " + err.Error()}
		}
		now := now()
		rec.Mem.Status = memory.Approved
		rec.Mem.ApprovedBy = stringPointer(approver)
		rec.Mem.ApprovedAt = &now
		rec.Mem.ApprovalBasis = "APPROVER"
		rec.Mem.UpdatedAt = now
		s.saveMemory(rec)
		s.put("memory_audit", newID(), tenantID, 0, map[string]any{"action": "memory.approve",
			"memory_id": id, "version": expectedVersion, "actor": approver})
		current = rec.Mem
		return nil
	})
	return current, err
}

func stringPointer(value string) *string {
	return &value
}
