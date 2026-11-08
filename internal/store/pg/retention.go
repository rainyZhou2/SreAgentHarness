package pg

import (
	"context"
	"time"
)

// DeletePublishedOutbox removes outbox rows published before cutoff.
func (s *Store) DeletePublishedOutbox(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM outbox_messages WHERE id IN (
		SELECT id FROM outbox_messages WHERE published_at IS NOT NULL AND published_at < $1 LIMIT $2)`, cutoff, limit)
	return tag.RowsAffected(), err
}

type ExpiredSegment struct {
	TenantID       string
	ModelRequestID string
	SegmentNo      int
	Ref            string
}

func (s *Store) ExpiredSegments(ctx context.Context, now time.Time, limit int) ([]ExpiredSegment, error) {
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, model_request_id, segment_no, segment_ref FROM model_stream_segments
		WHERE expires_at < $1 ORDER BY expires_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredSegment
	for rows.Next() {
		var e ExpiredSegment
		if err := rows.Scan(&e.TenantID, &e.ModelRequestID, &e.SegmentNo, &e.Ref); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSegment(ctx context.Context, e ExpiredSegment) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM model_stream_segments WHERE model_request_id=$1 AND segment_no=$2`,
		e.ModelRequestID, e.SegmentNo)
	return err
}

// ExpireMemories marks ACTIVE/CANDIDATE memories past expires_at as STALE
// (never deleted: §8 requires an auditable tombstone) and audits each change.
func (s *Store) ExpireMemories(ctx context.Context, now time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `WITH expired AS (
			UPDATE agent_memories SET status='STALE', updated_at=now()
			WHERE id IN (SELECT id FROM agent_memories WHERE status IN ('ACTIVE','CANDIDATE','VALIDATING')
				AND expires_at IS NOT NULL AND expires_at < $1 LIMIT $2)
			RETURNING id, tenant_id)
		INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type, resource_id, outcome, details)
		SELECT gen_random_uuid(), tenant_id, 'system', 'retention', 'memory.expire', 'memory', id::text, 'applied',
			'{"to":"STALE"}'::jsonb FROM expired`, now, limit)
	return tag.RowsAffected(), err
}

// DeleteOldAudit enforces the audit retention window (0 = keep forever).
func (s *Store) DeleteOldAudit(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM security_audit_log WHERE id IN (
		SELECT id FROM security_audit_log WHERE occurred_at < $1 LIMIT $2)`, cutoff, limit)
	return tag.RowsAffected(), err
}

// DeleteOldReasoningScores removes reasoning scores last computed before
// cutoff. Scores are derived data: the run, its steps and thoughts remain.
func (s *Store) DeleteOldReasoningScores(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM reasoning_scores WHERE (tenant_id, run_id) IN (
		SELECT tenant_id, run_id FROM reasoning_scores WHERE scored_at < $1 LIMIT $2)`, cutoff, limit)
	return tag.RowsAffected(), err
}

// RunsByStatus feeds the harness_runs gauge (status label only).
func (s *Store) RunsByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM agent_runs
		WHERE status NOT IN ('COMPLETED','FAILED','CANCELLED') GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// OutboxBacklog is the number of unpublished outbox rows.
func (s *Store) OutboxBacklog(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_messages WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
