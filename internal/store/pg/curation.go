package pg

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
)

// CreateProposal stores p unless a proposal with the same signature exists in
// any status (created is false and the existing one is returned).
func (s *Store) CreateProposal(ctx context.Context, p curate.Proposal) (curate.Proposal, bool, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return p, false, err
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO memory_curation_proposals (tenant_id, id, status, signature, data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tenant_id, signature) DO NOTHING`,
		p.TenantID, p.ID, p.Status, p.Signature, data, p.CreatedAt)
	if err != nil {
		return p, false, err
	}
	if tag.RowsAffected() == 1 {
		return p, true, nil
	}
	var raw []byte
	if err := s.pool.QueryRow(ctx, `SELECT data FROM memory_curation_proposals WHERE tenant_id=$1 AND signature=$2`,
		p.TenantID, p.Signature).Scan(&raw); err != nil {
		return p, false, err
	}
	var existing curate.Proposal
	return existing, false, json.Unmarshal(raw, &existing)
}

// ListProposals returns a tenant's proposals (all statuses when status is
// empty), newest first.
func (s *Store) ListProposals(ctx context.Context, tenantID, status string, limit int) ([]curate.Proposal, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT data FROM memory_curation_proposals
		WHERE tenant_id=$1 AND ($2='' OR status=$2) ORDER BY created_at DESC, id LIMIT $3`, tenantID, status, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (curate.Proposal, error) {
		var raw []byte
		var p curate.Proposal
		if err := row.Scan(&raw); err != nil {
			return p, err
		}
		return p, json.Unmarshal(raw, &p)
	})
}

func (s *Store) GetProposal(ctx context.Context, tenantID, id string) (curate.Proposal, error) {
	var raw []byte
	var p curate.Proposal
	err := s.pool.QueryRow(ctx, `SELECT data FROM memory_curation_proposals WHERE tenant_id=$1 AND id=$2`, tenantID, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, domain.ErrNotFound
	}
	if err != nil {
		return p, err
	}
	return p, json.Unmarshal(raw, &p)
}

// DecideProposal moves a PENDING proposal to status; another decision racing
// ahead makes this one fail with ErrVersionConflict.
func (s *Store) DecideProposal(ctx context.Context, tenantID, id, status, actor, reason string, at time.Time) (curate.Proposal, error) {
	var out curate.Proposal
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var raw []byte
		var cur string
		err := tx.QueryRow(ctx, `SELECT data, status FROM memory_curation_proposals WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			tenantID, id).Scan(&raw, &cur)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return err
		}
		if cur != curate.Pending {
			return domain.ErrVersionConflict
		}
		out.Status, out.DecidedBy, out.DecisionReason, out.DecidedAt = status, actor, reason, &at
		data, err := json.Marshal(out)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE memory_curation_proposals SET status=$3, data=$4, decided_at=$5 WHERE tenant_id=$1 AND id=$2`,
			tenantID, id, status, data, at)
		return err
	})
	return out, err
}

// MemoryLastHits reports when each memory was last retrieved into a run.
func (s *Store) MemoryLastHits(ctx context.Context, tenantID string, ids []string) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT h.memory_id::text, max(st.updated_at)
		FROM step_memory_hits h JOIN agent_steps st ON st.tenant_id = h.tenant_id AND st.id = h.step_id
		WHERE h.tenant_id=$1 AND h.memory_id::text = ANY($2) GROUP BY h.memory_id`, tenantID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var at time.Time
		if err := rows.Scan(&id, &at); err != nil {
			return nil, err
		}
		out[id] = at
	}
	return out, rows.Err()
}
