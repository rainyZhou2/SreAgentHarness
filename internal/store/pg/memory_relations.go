package pg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func (s *Store) DeclareMemoryConflict(ctx context.Context, record memory.ConflictRecord) (memory.ConflictRecord, error) {
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id IN ($2,$3)
			ORDER BY id FOR UPDATE`, record.TenantID, record.Pair.FirstID, record.Pair.SecondID)
		if err != nil {
			return err
		}
		var records []memory.Memory
		for rows.Next() {
			current, err := scanMemory(rows)
			if err != nil {
				rows.Close()
				return err
			}
			records = append(records, current)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(records) != 2 {
			return domain.ErrNotFound
		}
		pair, err := memory.NewConflictPair(records[0], records[1])
		if err != nil {
			return err
		}
		if record.Reason == "" || record.CreatedBy == "" {
			return &domain.RejectedError{Reason: "conflict_reason_required"}
		}
		record.Pair, record.TenantID = pair, records[0].TenantID
		if err := tx.QueryRow(ctx, `INSERT INTO memory_conflicts (tenant_id, first_memory_id, second_memory_id, reason, evidence_refs, created_by)
			VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id, first_memory_id, second_memory_id) DO UPDATE SET
			reason=EXCLUDED.reason, evidence_refs=EXCLUDED.evidence_refs, created_by=EXCLUDED.created_by, created_at=now(),
			resolved_by=NULL, resolved_at=NULL, resolution=NULL
			RETURNING created_at`, record.TenantID, pair.FirstID, pair.SecondID, record.Reason, jsonb(nonNil(record.EvidenceRefs)),
			record.CreatedBy).Scan(&record.CreatedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_memories SET status='CONTESTED', updated_at=now()
			WHERE tenant_id=$1 AND id IN ($2,$3) AND status NOT IN ('REVOKED','SUPERSEDED')`, record.TenantID,
			record.Pair.FirstID, record.Pair.SecondID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.conflict.declare','memory',$4,'applied',$5)`, newID(),
			record.TenantID, record.CreatedBy, record.Pair.FirstID, jsonb(map[string]any{"second_memory_id": record.Pair.SecondID,
				"reason": record.Reason, "evidence_refs": record.EvidenceRefs}))
		return err
	})
	return record, err
}

func (s *Store) ResolveMemoryConflict(ctx context.Context, tenantID string, pair memory.ConflictPair, actor, resolution string) (memory.ConflictRecord, error) {
	if pair.FirstID > pair.SecondID {
		pair.FirstID, pair.SecondID = pair.SecondID, pair.FirstID
	}
	if actor == "" || resolution == "" {
		return memory.ConflictRecord{}, &domain.RejectedError{Reason: "conflict_resolution_required"}
	}
	var record memory.ConflictRecord
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var evidence []byte
		err := tx.QueryRow(ctx, `UPDATE memory_conflicts SET resolved_by=$4, resolved_at=now(), resolution=$5
			WHERE tenant_id=$1 AND first_memory_id=$2 AND second_memory_id=$3 AND resolved_at IS NULL
			RETURNING first_memory_id, second_memory_id, reason, evidence_refs, created_by, created_at, resolved_at`,
			tenantID, pair.FirstID, pair.SecondID, actor, resolution).Scan(&record.Pair.FirstID, &record.Pair.SecondID,
			&record.Reason, &evidence, &record.CreatedBy, &record.CreatedAt, &record.ResolvedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		record.TenantID, record.ResolvedBy, record.Resolution = tenantID, actor, resolution
		if err := json.Unmarshal(evidence, &record.EvidenceRefs); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.conflict.resolve','memory',$4,'applied',$5)`, newID(),
			tenantID, actor, pair.FirstID, jsonb(map[string]string{"second_memory_id": pair.SecondID, "resolution": resolution}))
		return err
	})
	return record, err
}

func (s *Store) SupersedeMemory(ctx context.Context, tenantID, oldID, replacementID string, expectedOldVersion int64, actor string) (memory.Memory, error) {
	var current memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		previous, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, oldID))
		if err != nil {
			return err
		}
		next, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenantID, replacementID))
		if err != nil {
			return err
		}
		if previous.Version != expectedOldVersion {
			return domain.ErrVersionConflict
		}
		if err := memory.ValidateSupersession(previous, next); err != nil {
			return err
		}
		var cycle bool
		if err := tx.QueryRow(ctx, `WITH RECURSIVE chain(id, superseded_by) AS (
			SELECT id, superseded_by FROM agent_memories WHERE tenant_id=$1 AND id=$2
			UNION ALL SELECT m.id, m.superseded_by FROM agent_memories m JOIN chain c ON m.tenant_id=$1 AND m.id=c.superseded_by
		) SELECT EXISTS (SELECT 1 FROM chain WHERE id=$3)`, tenantID, replacementID, oldID).Scan(&cycle); err != nil {
			return err
		}
		if cycle {
			return &domain.RejectedError{Reason: "supersession_cycle"}
		}
		tag, err := tx.Exec(ctx, `UPDATE agent_memories SET status='SUPERSEDED', superseded_by=$3, updated_at=now()
			WHERE tenant_id=$1 AND id=$2 AND current_version=$4`, tenantID, oldID, replacementID, expectedOldVersion)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrVersionConflict
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.supersede','memory',$4,'applied',$5)`, newID(),
			tenantID, actor, oldID, jsonb(map[string]any{"superseded_by": replacementID, "version": expectedOldVersion})); err != nil {
			return err
		}
		current, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2`, tenantID, oldID))
		return err
	})
	return current, err
}
