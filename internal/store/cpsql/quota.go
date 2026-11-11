package cpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/quota"
)

var _ quota.Store = (*Store)(nil)

// lockBudget returns the budget row locked for the transaction, creating it
// on first use and following a changed capacity.
func (s *Store) lockBudget(ctx context.Context, tx *sql.Tx, tenantID, queue, resource string, capacity int64, now time.Time) (quota.Budget, error) {
	b := quota.Budget{TenantID: tenantID, Queue: queue, Resource: resource}
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO admission_budgets (tenant_id, queue, resource, capacity, reserved, version, updated_at)
		VALUES (?,?,?,?,0,0,?) ON CONFLICT (tenant_id, queue, resource) DO NOTHING`), tenantID, queue, resource, max(capacity, 0), s.t(now)); err != nil {
		return b, err
	}
	err := tx.QueryRowContext(ctx, s.q(`SELECT capacity, reserved, version FROM admission_budgets
		WHERE tenant_id=? AND queue=? AND resource=?`+s.forUpdate()), tenantID, queue, resource).Scan(&b.Capacity, &b.Reserved, &b.Version)
	if err != nil {
		return b, err
	}
	if capacity >= 0 && b.Capacity != capacity {
		b.Capacity = capacity
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE admission_budgets SET capacity=?, updated_at=? WHERE tenant_id=? AND queue=? AND resource=?`),
			capacity, s.t(now), tenantID, queue, resource); err != nil {
			return b, err
		}
	}
	return b, nil
}

func (s *Store) addReserved(ctx context.Context, tx *sql.Tx, b quota.Budget, delta int64, now time.Time) error {
	_, err := tx.ExecContext(ctx, s.q(`UPDATE admission_budgets SET reserved = CASE WHEN reserved + ? < 0 THEN 0 ELSE reserved + ? END,
		version = version + 1, updated_at=? WHERE tenant_id=? AND queue=? AND resource=?`), delta, delta, s.t(now), b.TenantID, b.Queue, b.Resource)
	return err
}

const reservationCols = `reservation_id, tenant_id, work_id, idempotency_key, queue, resource, units, fence_token, owner, status, expires_at`

func scanReservation(r rowScanner) (quota.Reservation, string, error) {
	var res quota.Reservation
	var status string
	var exp ts
	err := r.Scan(&res.ID, &res.TenantID, &res.WorkID, &res.DedupKey, &res.Queue, &res.Resource, &res.Units, &res.Token,
		&res.Owner, &status, &exp)
	res.ExpiresAt = exp.T
	return res, status, err
}

// Reserve takes units of a budget under the budget's row lock. The same
// dedup key returns the reservation it already holds (renewed); a released
// or expired key is taken again with a higher fence token.
func (s *Store) Reserve(ctx context.Context, r quota.LeaseRequest) (quota.Reservation, error) {
	if err := r.Validate(); err != nil {
		return quota.Reservation{}, err
	}
	now := r.Now
	if now.IsZero() {
		now = s.now()
	}
	var out quota.Reservation
	err := s.tx(ctx, func(tx *sql.Tx) error {
		out = quota.Reservation{}
		b, err := s.lockBudget(ctx, tx, r.TenantID, r.Queue, r.Resource, r.Capacity, now)
		if err != nil {
			return err
		}
		prev, status, err := scanReservation(tx.QueryRowContext(ctx, s.q(`SELECT `+reservationCols+` FROM admission_reservations
			WHERE tenant_id=? AND idempotency_key=?`), r.TenantID, r.DedupKey))
		switch {
		case err == nil && status == "ACTIVE":
			if prev.Queue != r.Queue || prev.Resource != r.Resource || prev.WorkID != r.WorkID {
				return fmt.Errorf("%w: dedup key %q is held for another work or budget", quota.ErrInvalid, r.DedupKey)
			}
			prev.ExpiresAt, prev.Existing = now.Add(r.TTL), true
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE admission_reservations SET expires_at=?, updated_at=?
				WHERE tenant_id=? AND reservation_id=?`), s.t(prev.ExpiresAt), s.t(now), r.TenantID, prev.ID); err != nil {
				return err
			}
			out = prev
			return nil
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if b.Reserved+r.Units > b.Capacity && !(r.Oversize && b.Reserved == 0 && b.Capacity > 0) {
			return &quota.ExhaustedError{Budget: b, Units: r.Units}
		}
		res := quota.Reservation{ID: cpkit.NewID(), Token: 1, ExpiresAt: now.Add(r.TTL), Owner: r.Owner, Units: r.Units,
			TenantID: r.TenantID, WorkID: r.WorkID, Queue: r.Queue, Resource: r.Resource, DedupKey: r.DedupKey}
		if err == nil {
			// The key was held before (released or expired): reuse the row
			// with a higher token so the previous holder is fenced out.
			res.ID, res.Token = prev.ID, prev.Token+1
			if _, err := tx.ExecContext(ctx, s.q(`UPDATE admission_reservations SET work_id=?, queue=?, resource=?, units=?,
				fence_token=?, owner=?, status='ACTIVE', expires_at=?, updated_at=? WHERE tenant_id=? AND reservation_id=?`),
				res.WorkID, res.Queue, res.Resource, res.Units, res.Token, res.Owner, s.t(res.ExpiresAt), s.t(now),
				res.TenantID, res.ID); err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO admission_reservations (tenant_id, reservation_id, work_id,
			idempotency_key, queue, resource, units, fence_token, owner, status, expires_at, created_at, updated_at)
			VALUES (?,?,?,?,?,?,?,?,?,'ACTIVE',?,?,?)`), res.TenantID, res.ID, res.WorkID, res.DedupKey, res.Queue, res.Resource,
			res.Units, res.Token, res.Owner, s.t(res.ExpiresAt), s.t(now), s.t(now)); err != nil {
			return err
		}
		if err := s.addReserved(ctx, tx, b, r.Units, now); err != nil {
			return err
		}
		out = res
		return nil
	})
	return out, err
}

// Renew extends a held reservation (fenced by the token).
func (s *Store) Renew(ctx context.Context, tenantID, id string, token int64, ttl time.Duration) error {
	now := s.now()
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE admission_reservations SET expires_at=?, updated_at=?
		WHERE tenant_id=? AND reservation_id=? AND fence_token=? AND status='ACTIVE'`), s.t(now.Add(ttl)), s.t(now), tenantID, id, token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return quota.ErrFenced
	}
	return nil
}

// Release returns the units of a held reservation. It is idempotent: a
// second release, or one with a stale token, is ErrFenced and frees nothing.
func (s *Store) Release(ctx context.Context, tenantID, id string, token int64) error {
	now := s.now()
	return s.tx(ctx, func(tx *sql.Tx) error {
		return s.settle(ctx, tx, tenantID, id, token, "RELEASED", now)
	})
}

// settle ends an ACTIVE reservation and returns its units, locking the
// budget first (the same order as Reserve).
func (s *Store) settle(ctx context.Context, tx *sql.Tx, tenantID, id string, token int64, status string, now time.Time) error {
	var queue, resource string
	err := tx.QueryRowContext(ctx, s.q(`SELECT queue, resource FROM admission_reservations WHERE tenant_id=? AND reservation_id=?`),
		tenantID, id).Scan(&queue, &resource)
	if errors.Is(err, sql.ErrNoRows) {
		return quota.ErrFenced
	}
	if err != nil {
		return err
	}
	b, err := s.lockBudget(ctx, tx, tenantID, queue, resource, -1, now)
	if err != nil {
		return err
	}
	var units int64
	err = tx.QueryRowContext(ctx, s.q(`UPDATE admission_reservations SET status=?, updated_at=?
		WHERE tenant_id=? AND reservation_id=? AND fence_token=? AND status='ACTIVE' RETURNING units`),
		status, s.t(now), tenantID, id, token).Scan(&units)
	if errors.Is(err, sql.ErrNoRows) {
		return quota.ErrFenced
	}
	if err != nil {
		return err
	}
	return s.addReserved(ctx, tx, b, -units, now)
}

// Reap expires ACTIVE reservations whose lease ended before the given time.
func (s *Store) Reap(ctx context.Context, before time.Time, limit int) (int, error) {
	return s.ReapReservations(ctx, before, limit, nil, 0)
}

// ReapReservations is Reap with the crash-gap reconciliation: a reservation
// whose admitted work is still in use (the run exists and is not terminal)
// is renewed by ttl instead of freed.
func (s *Store) ReapReservations(ctx context.Context, before time.Time, limit int, inUse quota.InUse, ttl time.Duration) (int, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+reservationCols+` FROM admission_reservations
		WHERE status='ACTIVE' AND expires_at < ? ORDER BY expires_at LIMIT ?`), s.t(before), limit)
	if err != nil {
		return 0, err
	}
	var due []quota.Reservation
	for rows.Next() {
		r, _, err := scanReservation(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	n := 0
	now := s.now()
	for _, r := range due {
		if inUse != nil {
			used, err := inUse(ctx, r)
			if err != nil {
				return n, err
			}
			if used {
				if ttl <= 0 {
					ttl = time.Minute
				}
				if err := s.Renew(ctx, r.TenantID, r.ID, r.Token, ttl); err != nil && !errors.Is(err, quota.ErrFenced) {
					return n, err
				}
				continue
			}
		}
		err := s.tx(ctx, func(tx *sql.Tx) error { return s.settle(ctx, tx, r.TenantID, r.ID, r.Token, "EXPIRED", now) })
		switch {
		case errors.Is(err, quota.ErrFenced): // released or renewed meanwhile
		case err != nil:
			return n, err
		default:
			n++
		}
	}
	return n, nil
}

// ReleaseByKey releases the reservation held under a dedup key, whoever
// took it (the orchestrator releases a run's units when it observes the run
// terminal, possibly on another replica). Unknown or settled keys are a no-op.
func (s *Store) ReleaseByKey(ctx context.Context, tenantID, dedupKey string) error {
	r, status, err := scanReservation(s.db.QueryRowContext(ctx, s.q(`SELECT `+reservationCols+` FROM admission_reservations
		WHERE tenant_id=? AND idempotency_key=?`), tenantID, dedupKey))
	if errors.Is(err, sql.ErrNoRows) || (err == nil && status != "ACTIVE") {
		return nil
	}
	if err != nil {
		return err
	}
	if err := s.Release(ctx, tenantID, r.ID, r.Token); err != nil && !errors.Is(err, quota.ErrFenced) {
		return err
	}
	return nil
}

// Budgets lists a tenant's budgets (GET /v1/quotas).
func (s *Store) Budgets(ctx context.Context, tenantID string) ([]quota.Budget, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT queue, resource, capacity, reserved, version FROM admission_budgets
		WHERE tenant_id=? ORDER BY resource, queue`), tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []quota.Budget
	for rows.Next() {
		b := quota.Budget{TenantID: tenantID}
		if err := rows.Scan(&b.Queue, &b.Resource, &b.Capacity, &b.Reserved, &b.Version); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ActiveReservations lists a tenant's held reservations (diagnostics).
func (s *Store) ActiveReservations(ctx context.Context, tenantID string, limit int) ([]quota.Reservation, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+reservationCols+` FROM admission_reservations
		WHERE tenant_id=? AND status='ACTIVE' ORDER BY expires_at LIMIT ?`), tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []quota.Reservation
	for rows.Next() {
		r, _, err := scanReservation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
