package cpsql

import (
	"context"
	"database/sql"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
)

const WorkWakeSubject = "harness.work.wake"

func (s *Store) enqueueWorkWake(ctx context.Context, tx *sql.Tx, tenantID, workID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO work_outbox_messages
		(id, tenant_id, work_id, subject, payload, next_retry_at, created_at) VALUES (?,?,?,?,?,?,?)`),
		cpkit.NewID(), tenantID, workID, WorkWakeSubject, js(map[string]string{"work_id": workID}), s.t(now), s.t(now))
	return err
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, claimFor time.Duration) ([]domain.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	if claimFor <= 0 {
		claimFor = 30 * time.Second
	}
	now := s.now()
	var out []domain.OutboxMessage
	err := s.tx(ctx, func(tx *sql.Tx) error {
		out = out[:0]
		rows, err := tx.QueryContext(ctx, s.q(`SELECT id, tenant_id, subject, payload, publish_attempt FROM work_outbox_messages
			WHERE published_at IS NULL AND next_retry_at<=? AND (claimed_until IS NULL OR claimed_until<?)
			ORDER BY next_retry_at, created_at LIMIT ?`+s.skipLocked()), s.t(now), s.t(now), limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m domain.OutboxMessage
			var payload jb
			if err := rows.Scan(&m.ID, &m.TenantID, &m.Subject, &payload, &m.Attempt); err != nil {
				rows.Close()
				return err
			}
			m.Payload = append([]byte(nil), payload...)
			m.Attempt++
			out = append(out, m)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, m := range out {
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE work_outbox_messages SET claimed_until=?, publish_attempt=publish_attempt+1 WHERE id=?`),
				s.t(now.Add(claimFor)), m.ID); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) MarkPublished(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE work_outbox_messages SET published_at=?, claimed_until=NULL WHERE id=? AND published_at IS NULL`),
		s.t(s.now()), id)
	return err
}

func (s *Store) MarkPublishFailed(ctx context.Context, id string, backoff time.Duration) error {
	if backoff < 0 {
		backoff = 0
	}
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE work_outbox_messages SET claimed_until=NULL, next_retry_at=? WHERE id=? AND published_at IS NULL`),
		s.t(s.now().Add(backoff)), id)
	return err
}
