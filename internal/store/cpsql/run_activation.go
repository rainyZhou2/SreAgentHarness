package cpsql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go-sre-harness/internal/work"
)

// CheckRunActivation is a read-only compatibility check. It does not hold
// authority after returning; callers that activate must use WithRunActivation.
func (s *Store) CheckRunActivation(ctx context.Context, req work.RunActivation) error {
	return s.tx(ctx, func(tx *sql.Tx) error { return s.checkRunActivation(ctx, tx, req) })
}

// WithRunActivation orders a short Kernel database activation before any Work
// cancellation, replan or revocation that acquires the same Work lock. activate
// must perform only the existing idempotent Kernel activation and honor ctx;
// it must not call Work APIs, execute tools/models, or do other remote I/O.
// Lock order is Work then Kernel Run. This callback is NEVER retried here.
//
// This is an authorization critical section, not a transaction across databases:
// the Kernel commit may survive an error/crash before this transaction finishes.
// Such activation was authorized while the Work lock was held; retries recheck
// current authority and recovery propagates later cancellation. A lost database
// session can lose the lock, so this is not distributed fencing. Work-owned
// Kernel tools remain read-only; production effects require Action authorization.
func (s *Store) WithRunActivation(ctx context.Context, req work.RunActivation, activate func(context.Context) error) error {
	if activate == nil {
		return fmt.Errorf("%w: activation callback required", work.ErrInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// database/sql rolls back a transaction as soon as its BeginTx context is
	// cancelled. Keep its lifetime separate so cancellation cannot release the
	// Work lock while the Kernel callback is still completing. All actual
	// queries and the callback retain the bounded caller context; cleanup is
	// explicit after the callback returns.
	txCtx, cancelTx := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTx()
	stopBeginCancel := context.AfterFunc(ctx, cancelTx)
	defer stopBeginCancel()
	tx, err := conn.BeginTx(txCtx, nil)
	if err != nil {
		return err
	}
	defer func() {
		stop := time.AfterFunc(5*time.Second, cancelTx)
		defer stop.Stop()
		_ = tx.Rollback()
	}()
	if s.d == Postgres {
		if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout','5000',true), set_config('statement_timeout','15000',true)`); err != nil {
			return err
		}
	} else {
		// SQLite has no row locks. A no-op write reserves the writer lock even
		// if another Store handle opens this database in the same process.
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE agent_works SET version=version WHERE tenant_id=? AND id=?`), req.TenantID, req.WorkID); err != nil {
			return err
		}
	}
	if err := s.checkRunActivation(ctx, tx, req); err != nil {
		return err
	}
	if !stopBeginCancel() {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := activate(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stopCommit := time.AfterFunc(5*time.Second, cancelTx)
	defer stopCommit.Stop()
	return tx.Commit()
}

func (s *Store) checkRunActivation(ctx context.Context, tx *sql.Tx, req work.RunActivation) error {
	if req.ExpectedVersion <= 0 || req.LeaseOwner == "" || req.Generation <= 0 || req.StageID == "" || req.RunID == "" {
		return fmt.Errorf("%w: incomplete run activation authority", work.ErrInvalid)
	}
	w, err := s.lockWork(ctx, tx, req.TenantID, req.WorkID)
	if err != nil {
		return err
	}
	if w.Version != req.ExpectedVersion {
		return work.ErrVersionConflict
	}
	if w.LeaseOwner != req.LeaseOwner || w.LeaseUntil == nil || !w.LeaseUntil.After(s.now()) {
		return work.ErrLeaseLost
	}
	stage, ok := w.CurrentStage()
	if w.Status != work.StatusActive || w.Generation != req.Generation || !ok || stage.ID != req.StageID {
		return fmt.Errorf("%w: inactive run owner", work.ErrInvalid)
	}
	var n int
	if err := tx.QueryRowContext(ctx, s.q(`SELECT count(*) FROM work_run_links
		WHERE tenant_id=? AND work_id=? AND run_id=? AND role='owner' AND generation=? AND stage_id=?`),
		req.TenantID, req.WorkID, req.RunID, req.Generation, req.StageID).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: run reservation has no matching owner link", work.ErrInvalid)
	}
	return nil
}
