package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

// ---------- reconciler (§14.2, §13.6) ----------

// RecoverExpiredLeases finds RUNNING runs whose lease expired and recovers
// each in its own short transaction. Returns how many runs were recovered.
func (s *Store) RecoverExpiredLeases(ctx context.Context, limit int) (int, error) {
	ids, err := s.ids(ctx, `SELECT id::text FROM agent_runs WHERE status='RUNNING' AND lease_until < now()
		ORDER BY lease_until LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	// Each run is recovered on its own: one that fails is reported and
	// retried next pass without blocking the rest of the batch.
	n := 0
	var errs []error
	for _, id := range ids {
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			r, err := s.lockRun(ctx, tx, "", id)
			if err != nil {
				return err
			}
			if r.run.Status != domain.RunRunning || r.run.LeaseUntil == nil || r.run.LeaseUntil.After(r.now) {
				return nil // someone renewed or committed meanwhile
			}
			if err := s.recoverExpired(ctx, r); err != nil {
				return err
			}
			if err := r.settle(ctx); err != nil {
				return err
			}
			n++
			return r.save(ctx)
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("run %s: %w", id, err))
		}
	}
	return n, errors.Join(errs...)
}

// ExpireApprovals times out pending approvals so a run never waits forever:
// the blocked step is skipped and the agent re-plans.
func (s *Store) ExpireApprovals(ctx context.Context, limit int) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT a.id::text, i.run_id::text, i.step_id::text FROM approval_requests a
		JOIN action_intents i ON i.id = a.action_intent_id
		WHERE a.status='PENDING' AND a.expires_at < now() ORDER BY a.expires_at LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	type item struct{ id, runID, stepID string }
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.id, &it.runID, &it.stepID); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	var errs []error
	for _, it := range items {
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			r, err := s.lockRun(ctx, tx, "", it.runID)
			if err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `UPDATE approval_requests SET status='EXPIRED', decided_at=now()
				WHERE id=$1 AND status='PENDING' AND expires_at < now()`, it.id)
			if err != nil || tag.RowsAffected() == 0 {
				return err
			}
			if err := r.event(ctx, "approval.expired", "system", "", &it.stepID, nil,
				map[string]string{"approval_request_id": it.id}); err != nil {
				return err
			}
			var stepStatus domain.StepStatus
			if err := tx.QueryRow(ctx, `SELECT status FROM agent_steps WHERE id=$1`, it.stepID).Scan(&stepStatus); err != nil {
				return err
			}
			if stepStatus == domain.StepWaitingApproval && !r.run.Status.Terminal() {
				if err := s.skipBlockedStep(ctx, r, it.stepID, "approval expired"); err != nil {
					return err
				}
			}
			n++
			return r.save(ctx)
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("approval %s: %w", it.id, err))
		}
	}
	return n, errors.Join(errs...)
}

// RewakeStaleQueued re-enqueues wake-ups for runnable runs that have been idle
// longer than idle and have no pending outbox message: the safety net for a
// lost notification (NATS down, message dropped, consumer bug).
func (s *Store) RewakeStaleQueued(ctx context.Context, idle time.Duration, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO outbox_messages (id, tenant_id, aggregate_id, subject, payload)
		SELECT gen_random_uuid(), r.tenant_id, r.id, $3, jsonb_build_object('run_id', r.id::text, 'tenant_id', r.tenant_id)
		FROM agent_runs r
		WHERE r.status='QUEUED' AND r.desired_state='RUNNING' AND (r.not_before IS NULL OR r.not_before <= now())
		  AND r.updated_at < now() - $1 * interval '1 millisecond'
		  AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.aggregate_id = r.id AND o.published_at IS NULL)
		ORDER BY r.updated_at LIMIT $2`, idle.Milliseconds(), limit, WakeSubject)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) ids(ctx context.Context, sql string, args ...any) ([]string, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---------- outbox (§13.3) ----------

type OutboxMessage = domain.OutboxMessage

// ClaimOutbox leases due messages to one publisher; several publishers can
// run concurrently thanks to SKIP LOCKED + claimed_until.
func (s *Store) ClaimOutbox(ctx context.Context, limit int, claimFor time.Duration) ([]OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `UPDATE outbox_messages SET claimed_until = now() + $2 * interval '1 millisecond',
		publish_attempt = publish_attempt + 1
		WHERE id IN (SELECT id FROM outbox_messages WHERE published_at IS NULL AND next_retry_at <= now()
			AND (claimed_until IS NULL OR claimed_until < now())
			ORDER BY next_retry_at, created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id::text, tenant_id, subject, payload, publish_attempt`, limit, claimFor.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		if err := rows.Scan(&m.ID, &m.TenantID, &m.Subject, &m.Payload, &m.Attempt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) MarkPublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET published_at=now(), claimed_until=NULL WHERE id=$1`, id)
	return err
}

func (s *Store) MarkPublishFailed(ctx context.Context, id string, backoff time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_messages SET claimed_until=NULL,
		next_retry_at = now() + $2 * interval '1 millisecond' WHERE id=$1`, id, backoff.Milliseconds())
	return err
}

// OldestUnpublishedAge feeds harness_outbox_oldest_age_seconds.
func (s *Store) OldestUnpublishedAge(ctx context.Context) (time.Duration, error) {
	var secs *float64
	err := s.pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM now() - min(created_at))::float8 FROM outbox_messages
		WHERE published_at IS NULL AND next_retry_at <= now()`).Scan(&secs)
	if err != nil || secs == nil {
		return 0, err
	}
	return time.Duration(*secs * float64(time.Second)), nil
}
