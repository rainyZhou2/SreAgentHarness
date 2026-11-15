package pg

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func (s *Store) VerifyMemory(ctx context.Context, input memory.VerificationInput, verifier, idempotencyKey string) (memory.VerificationRecord, memory.Memory, error) {
	var record memory.VerificationRecord
	var current memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var existing memory.VerificationRecord
		var evidence []byte
		err := tx.QueryRow(ctx, `SELECT id, tenant_id, memory_id, memory_version, evidence_refs, decision_ref, outcome_ref,
			input_hash, verifier, created_at FROM memory_verifications WHERE tenant_id=$1 AND idempotency_key=$2`,
			input.TenantID, idempotencyKey).Scan(&existing.ID, &existing.TenantID, &existing.MemoryID, &existing.MemoryVersion,
			&evidence, &existing.DecisionRef, &existing.OutcomeRef, &existing.InputHash, &existing.Verifier, &existing.CreatedAt)
		if err == nil {
			if existing.InputHash != input.InputHash || existing.DecisionRef != input.DecisionRef ||
				existing.OutcomeRef != input.OutcomeRef || existing.MemoryVersion != input.ExpectedVersion {
				return domain.ErrVersionConflict
			}
			existing.IdempotencyKey = idempotencyKey
			current, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2`,
				input.TenantID, input.MemoryID))
			record = existing
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		cur, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			input.TenantID, input.MemoryID))
		if err != nil {
			return err
		}
		if cur.Version != input.ExpectedVersion {
			return domain.ErrVersionConflict
		}
		if err := input.Validate(); err != nil {
			return err
		}
		computed := memory.VerificationInputFor(cur, input.DecisionRef, input.OutcomeRef)
		if computed.InputHash != input.InputHash || computed.InputHash != input.ActualHash {
			return &domain.RejectedError{Reason: "verification_input_changed"}
		}
		if cur.Status != memory.StatusCandidate && cur.Status != memory.Contested && cur.Status != memory.Stale {
			return &domain.RejectedError{Reason: "memory_not_verifiable_" + cur.Status}
		}
		record = memory.VerificationRecord{ID: newID(), TenantID: input.TenantID, MemoryID: input.MemoryID,
			MemoryVersion: input.ExpectedVersion, DecisionRef: input.DecisionRef, OutcomeRef: input.OutcomeRef,
			InputHash: input.InputHash, Verifier: verifier, IdempotencyKey: idempotencyKey}
		if err := tx.QueryRow(ctx, `INSERT INTO memory_verifications (id, tenant_id, memory_id, memory_version, evidence_refs,
			decision_ref, outcome_ref, input_hash, verifier, idempotency_key) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			RETURNING created_at`, record.ID, record.TenantID, record.MemoryID, record.MemoryVersion, jsonb(input.EvidenceRefs),
			record.DecisionRef, record.OutcomeRef, record.InputHash, record.Verifier, record.IdempotencyKey).Scan(&record.CreatedAt); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE agent_memories SET status='VERIFIED', verified_by=$3, decision_ref=$4, outcome_ref=$5,
			updated_at=now() WHERE tenant_id=$1 AND id=$2 AND current_version=$6 AND status=$7`,
			cur.TenantID, cur.ID, verifier, input.DecisionRef, input.OutcomeRef, cur.Version, cur.Status)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrVersionConflict
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'service',$3,'memory.verify','memory',$4,'applied',$5)`, newID(),
			cur.TenantID, verifier, cur.ID, jsonb(map[string]any{"version": cur.Version, "verification_id": record.ID})); err != nil {
			return err
		}
		current, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2`,
			cur.TenantID, cur.ID))
		return err
	})
	return record, current, err
}

func (s *Store) ApproveMemory(ctx context.Context, tenantID, id string, expectedVersion int64, approver string) (memory.Memory, error) {
	var current memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		locked, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			tenantID, id))
		if err != nil {
			return err
		}
		if locked.Version != expectedVersion {
			return domain.ErrVersionConflict
		}
		if locked.Status != memory.Verified {
			return &domain.RejectedError{Reason: "memory_not_verified"}
		}
		if err := memory.CanPromote(locked, approver); err != nil {
			return &domain.RejectedError{Reason: "playbook_promotion: " + err.Error()}
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_memories SET status='APPROVED', approved_by=$3, approved_at=now(),
			approval_basis='APPROVER', updated_at=now() WHERE tenant_id=$1 AND id=$2 AND current_version=$4 AND status='VERIFIED'`,
			tenantID, id, approver, expectedVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.approve','memory',$4,'applied',$5)`, newID(),
			tenantID, approver, id, jsonb(map[string]any{"version": expectedVersion})); err != nil {
			return err
		}
		current, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2`,
			tenantID, id))
		return err
	})
	return current, err
}
