package pg

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/reasoning"
)

// PutReasoningScore upserts a run's reasoning score.
func (s *Store) PutReasoningScore(ctx context.Context, sc reasoning.RunScore) error {
	mj, err := json.Marshal(sc.Metrics)
	if err != nil {
		return err
	}
	ids := sc.MemoryIDs
	if ids == nil {
		ids = []string{}
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO reasoning_scores (tenant_id, run_id, profile_id, profile_version, service,
		outcome, quality, metrics, memory_ids, scored_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now())
		ON CONFLICT (tenant_id, run_id) DO UPDATE SET profile_id=EXCLUDED.profile_id,
		  profile_version=EXCLUDED.profile_version, service=EXCLUDED.service, outcome=EXCLUDED.outcome,
		  quality=EXCLUDED.quality, metrics=EXCLUDED.metrics, memory_ids=EXCLUDED.memory_ids, scored_at=now()`,
		sc.TenantID, sc.RunID, sc.ProfileID, sc.ProfileVersion, sc.Service, sc.Outcome, sc.Metrics.Quality, mj, ids)
	return err
}

const scoreCols = `run_id::text, profile_id, profile_version, service, outcome, metrics, memory_ids, scored_at`

func scanScore(row pgx.Row, tenantID string) (reasoning.RunScore, error) {
	var sc reasoning.RunScore
	var mj []byte
	if err := row.Scan(&sc.RunID, &sc.ProfileID, &sc.ProfileVersion, &sc.Service, &sc.Outcome, &mj, &sc.MemoryIDs,
		&sc.ScoredAt); err != nil {
		return sc, err
	}
	sc.TenantID = tenantID
	return sc, json.Unmarshal(mj, &sc.Metrics)
}

// ReasoningScore returns one run's score.
func (s *Store) ReasoningScore(ctx context.Context, tenantID, runID string) (reasoning.RunScore, error) {
	sc, err := scanScore(s.pool.QueryRow(ctx, `SELECT `+scoreCols+` FROM reasoning_scores
		WHERE tenant_id=$1 AND run_id=$2::uuid`, tenantID, runID), tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return sc, domain.ErrNotFound
	}
	return sc, err
}

// ReasoningScores lists scores, newest first.
func (s *Store) ReasoningScores(ctx context.Context, tenantID string, f reasoning.ScoreFilter) ([]reasoning.RunScore, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 1000
	}
	limit = min(limit, 10000)
	rows, err := s.pool.Query(ctx, `SELECT `+scoreCols+` FROM reasoning_scores
		WHERE tenant_id=$1 AND ($2='' OR profile_id=$2) AND ($3=0 OR profile_version=$3)
		  AND ($4='' OR service=$4) AND scored_at >= $5
		ORDER BY scored_at DESC LIMIT $6`, tenantID, f.ProfileID, f.Version, f.Service, f.Since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []reasoning.RunScore
	for rows.Next() {
		sc, err := scanScore(rows, tenantID)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// RunMemoryIDs lists the memories a run retrieved.
func (s *Store) RunMemoryIDs(ctx context.Context, tenantID, runID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT h.memory_id::text FROM step_memory_hits h
		JOIN agent_steps st ON st.tenant_id = h.tenant_id AND st.id = h.step_id
		WHERE h.tenant_id=$1 AND st.run_id=$2::uuid ORDER BY 1`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
