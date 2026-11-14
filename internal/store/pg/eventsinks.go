package pg

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
)

var _ eventsink.Store = (*Store)(nil)

func (s *Store) ClaimEventSink(ctx context.Context, sinkID, owner string, ttl time.Duration, earliest bool) (eventsink.Cursor, bool, error) {
	var c eventsink.Cursor
	if _, err := s.pool.Exec(ctx, `INSERT INTO event_sink_cursors (sink_id, tx_id)
		VALUES ($1, CASE WHEN $2 THEN '0'::xid8 ELSE pg_snapshot_xmin(pg_current_snapshot()) END)
		ON CONFLICT (sink_id) DO NOTHING`, sinkID, earliest); err != nil {
		return c, false, err
	}
	err := s.pool.QueryRow(ctx, `UPDATE event_sink_cursors
		SET owner=$2, lease_until=now() + make_interval(secs => $3)
		WHERE sink_id=$1 AND (owner IS NULL OR owner=$2 OR lease_until < now())
		RETURNING tx_id::text::bigint, global_seq`, sinkID, owner, ttl.Seconds()).Scan(&c.TxID, &c.GlobalSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	return c, err == nil, err
}

func (s *Store) ReadEventLog(ctx context.Context, after eventsink.Cursor, limit int) ([]eventsink.Delivery, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `SELECT tx_id::text::bigint, global_seq, tenant_id, run_id, event_seq, event_id,
		step_id, attempt_id, event_type, actor_type, actor_id, payload, occurred_at, committed_at
		FROM run_events
		WHERE (tx_id, global_seq) > ($1::text::xid8, $2)
		  AND tx_id < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY tx_id, global_seq LIMIT $3`, strconv.FormatInt(after.TxID, 10), after.GlobalSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventsink.Delivery
	for rows.Next() {
		var d eventsink.Delivery
		e := &d.Event
		if err := rows.Scan(&d.Cursor.TxID, &d.Cursor.GlobalSeq, &e.TenantID, &e.RunID, &e.Seq, &e.ID, &e.StepID,
			&e.AttemptID, &e.Type, &e.ActorType, &e.ActorID, &e.Payload, &e.OccurredAt, &e.CommittedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) AdvanceEventSink(ctx context.Context, sinkID, owner string, c eventsink.Cursor, delivered, skipped int, ttl time.Duration) error {
	return s.inKernelTx(ctx, func(tx pgx.Tx) error {
		var locked string
		if err := tx.QueryRow(ctx, `SELECT sink_id FROM event_sink_cursors WHERE sink_id=$1 FOR UPDATE`, sinkID).Scan(&locked); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrLeaseLost
			}
			return err
		}
		// Evaluate expiry after acquiring the lock; an UPDATE predicate alone
		// can be evaluated before a lock wait that outlives the lease.
		tag, err := tx.Exec(ctx, `UPDATE event_sink_cursors
		SET tx_id=$3::text::xid8, global_seq=$4, delivered=delivered+$5, skipped=skipped+$6,
		    lease_until=clock_timestamp() + make_interval(secs => $7), updated_at=clock_timestamp()
		WHERE sink_id=$1 AND owner=$2 AND lease_until > clock_timestamp()
		  AND (tx_id, global_seq) <= ($3::text::xid8, $4)`, sinkID, owner, strconv.FormatInt(c.TxID, 10), c.GlobalSeq, delivered, skipped, ttl.Seconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrLeaseLost
		}
		return nil
	})
}

// EventSinkStatus is a sink's cursor state (admin API).
type EventSinkStatus struct {
	SinkID     string     `json:"sink_id"`
	Owner      *string    `json:"owner,omitempty"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
	Delivered  int64      `json:"delivered"`
	Skipped    int64      `json:"skipped"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

func (s *Store) EventSinkStatuses(ctx context.Context) ([]EventSinkStatus, error) {
	rows, err := s.pool.Query(ctx, `SELECT sink_id, owner, lease_until, delivered, skipped, updated_at
		FROM event_sink_cursors ORDER BY sink_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventSinkStatus
	for rows.Next() {
		var st EventSinkStatus
		if err := rows.Scan(&st.SinkID, &st.Owner, &st.LeaseUntil, &st.Delivered, &st.Skipped, &st.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}
