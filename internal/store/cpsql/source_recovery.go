package cpsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
)

const sourceCheckpointColumns = `cursor, owner, fencing_token, lease_until, next_retry_at, last_error, updated_at`

func scanSourceCheckpoint(r rowScanner, tenantID, source string) (eventing.SourceCheckpoint, error) {
	cp := eventing.SourceCheckpoint{TenantID: tenantID, Source: source}
	var owner, lastError ns
	var leaseUntil, nextRetry, updated ts
	if err := r.Scan(&cp.Cursor, &owner, &cp.FencingToken, &leaseUntil, &nextRetry, &lastError, &updated); err != nil {
		return cp, err
	}
	cp.Owner, cp.LastError = owner.S(), lastError.S()
	cp.LeaseUntil, cp.NextRetryAt, cp.UpdatedAt = leaseUntil.ptr(), nextRetry.ptr(), updated.T
	return cp, nil
}

func validSourceCursor(cursor string) bool {
	return utf8.ValidString(cursor) && !strings.ContainsRune(cursor, '\x00') && len(cursor) <= 4096
}

func (s *Store) ClaimSourceCheckpoint(ctx context.Context, tenantID, source, owner string, now time.Time, lease time.Duration) (eventing.SourceCheckpoint, bool, error) {
	var cp eventing.SourceCheckpoint
	if tenantID == "" || source == "" || owner == "" || lease <= 0 {
		return cp, false, fmt.Errorf("invalid source checkpoint claim")
	}
	now = now.UTC()
	acquired := false
	err := s.tx(ctx, func(tx *sql.Tx) error {
		acquired = false
		if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO source_reconcile_checkpoints
			(tenant_id, source, cursor, fencing_token, updated_at) VALUES (?,?,?,0,?) ON CONFLICT (tenant_id, source) DO NOTHING`),
			tenantID, source, "", s.t(now)); err != nil {
			return err
		}
		var err error
		cp, err = scanSourceCheckpoint(tx.QueryRowContext(ctx, s.q(`SELECT `+sourceCheckpointColumns+` FROM source_reconcile_checkpoints
			WHERE tenant_id=? AND source=?`+s.forUpdate()), tenantID, source), tenantID, source)
		if err != nil {
			return err
		}
		if cp.Owner != "" && (cp.LeaseUntil == nil || cp.LeaseUntil.After(now)) {
			return nil
		}
		if cp.NextRetryAt != nil && cp.NextRetryAt.After(now) {
			return nil
		}
		token := cp.FencingToken + 1
		leaseUntil := now.Add(lease)
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE source_reconcile_checkpoints SET owner=?, fencing_token=?, lease_until=?, updated_at=?
			WHERE tenant_id=? AND source=? AND fencing_token=?`), owner, token, s.t(leaseUntil), s.t(now), tenantID, source, cp.FencingToken); err != nil {
			return err
		}
		cp.Owner, cp.FencingToken, cp.LeaseUntil, cp.UpdatedAt = owner, token, &leaseUntil, now
		acquired = true
		return nil
	})
	return cp, acquired, err
}

func (s *Store) HeartbeatSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, until time.Time) error {
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE source_reconcile_checkpoints SET lease_until=?, updated_at=?
		WHERE tenant_id=? AND source=? AND owner=? AND fencing_token=?`), s.t(until), s.t(s.now()), cp.TenantID, cp.Source, cp.Owner, cp.FencingToken)
	if err != nil {
		return err
	}
	return sourceLeaseResult(res)
}

func (s *Store) CommitSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, cursor string) error {
	if !validSourceCursor(cursor) {
		return eventing.ErrInvalidCheckpoint
	}
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE source_reconcile_checkpoints SET cursor=?, owner=NULL, lease_until=NULL,
		next_retry_at=NULL, last_error=NULL, updated_at=? WHERE tenant_id=? AND source=? AND owner=? AND fencing_token=?`),
		cursor, s.t(s.now()), cp.TenantID, cp.Source, cp.Owner, cp.FencingToken)
	if err != nil {
		return err
	}
	return sourceLeaseResult(res)
}

func (s *Store) FailSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, retryAt time.Time, message string) error {
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE source_reconcile_checkpoints SET owner=NULL, lease_until=NULL,
		next_retry_at=?, last_error=?, updated_at=? WHERE tenant_id=? AND source=? AND owner=? AND fencing_token=?`),
		s.t(retryAt), cpkit.Truncate(message, 2000), s.t(s.now()), cp.TenantID, cp.Source, cp.Owner, cp.FencingToken)
	if err != nil {
		return err
	}
	return sourceLeaseResult(res)
}

func (s *Store) ReleaseSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint) error {
	res, err := s.db.ExecContext(ctx, s.q(`UPDATE source_reconcile_checkpoints SET owner=NULL, lease_until=NULL, updated_at=?
		WHERE tenant_id=? AND source=? AND owner=? AND fencing_token=?`), s.t(s.now()), cp.TenantID, cp.Source, cp.Owner, cp.FencingToken)
	if err != nil {
		return err
	}
	return sourceLeaseResult(res)
}

func sourceLeaseResult(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return eventing.ErrSourceFenced
	}
	return nil
}

func (s *Store) SourceCheckpoint(ctx context.Context, tenantID, source string) (eventing.SourceCheckpoint, bool, error) {
	cp, err := scanSourceCheckpoint(s.db.QueryRowContext(ctx, s.q(`SELECT `+sourceCheckpointColumns+` FROM source_reconcile_checkpoints
		WHERE tenant_id=? AND source=?`), tenantID, source), tenantID, source)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, false, nil
	}
	return cp, err == nil, err
}
