package orchestration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
)

type SourceRecoveryStore interface {
	ClaimSourceCheckpoint(ctx context.Context, tenantID, source, owner string, now time.Time, lease time.Duration) (eventing.SourceCheckpoint, bool, error)
	HeartbeatSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, until time.Time) error
	CommitSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, cursor string) error
	FailSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint, retryAt time.Time, message string) error
	ReleaseSourceCheckpoint(ctx context.Context, cp eventing.SourceCheckpoint) error
}

type SourceTarget struct {
	TenantID string
	Source   string
}

type SourceRecovery struct {
	Store       SourceRecoveryStore
	Ingestor    *Ingestor
	Reconcilers map[string]eventing.SourceReconciler
	Targets     []SourceTarget
	Owner       string
	Clock       cpkit.Clock
	Interval    time.Duration
	Lease       time.Duration
	Retry       time.Duration
	Batch       int
	Log         *slog.Logger
}

func (r *SourceRecovery) Run(ctx context.Context) error {
	r.setDefaults()
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		if err := r.Tick(ctx); err != nil && ctx.Err() == nil {
			r.logger().Warn("source recovery", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (r *SourceRecovery) Tick(ctx context.Context) error {
	r.setDefaults()
	var errs []error
	for _, target := range r.Targets {
		reconciler, ok := r.Reconcilers[target.Source]
		if !ok || reconciler == nil {
			continue
		}
		if err := r.reconcileOne(ctx, target, reconciler); err != nil && !errors.Is(err, context.Canceled) {
			errs = append(errs, fmt.Errorf("%s/%s: %w", target.TenantID, target.Source, err))
		}
	}
	return errors.Join(errs...)
}

func (r *SourceRecovery) reconcileOne(ctx context.Context, target SourceTarget, reconciler eventing.SourceReconciler) (retErr error) {
	now := r.Clock.Now()
	cp, acquired, err := r.Store.ClaimSourceCheckpoint(ctx, target.TenantID, target.Source, r.Owner, now, r.Lease)
	if err != nil || !acquired {
		return err
	}
	opCtx, cancel := context.WithCancel(ctx)
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	heartbeatDone := make(chan error, 1)
	go r.heartbeat(heartbeatCtx, opCtx, cancel, cp, heartbeatDone)
	defer func() {
		stopHeartbeat()
		cancel()
		if heartbeatErr := <-heartbeatDone; retErr == nil && heartbeatErr != nil {
			retErr = heartbeatErr
		}
	}()
	if err := eventing.ValidateSourceCheckpoint(cp.Cursor); err != nil {
		return r.fail(ctx, cp, err)
	}
	page, err := reconciler.Reconcile(opCtx, eventing.SourceReconcileRequest{
		TenantID: target.TenantID, Source: target.Source, Cursor: cp.Cursor, Limit: r.Batch,
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return r.fail(ctx, cp, err)
	}
	if err := validateSourcePage(cp.Cursor, page); err != nil {
		return r.fail(ctx, cp, err)
	}
	for _, env := range page.Events {
		env.TenantID = target.TenantID
		env.Source = target.Source
		env.ReceivedAt = time.Time{}
		if _, _, err := r.Ingestor.Ingest(opCtx, env); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return r.fail(ctx, cp, fmt.Errorf("ingest %s: %w", env.ID, err))
		}
	}
	next := cp.Cursor
	if len(page.Events) > 0 || page.HighWatermark {
		next = page.NextCursor
	}
	if err := r.Store.CommitSourceCheckpoint(ctx, cp, next); err != nil {
		return err
	}
	return nil
}

func validateSourcePage(current string, page eventing.SourceReconcilePage) error {
	if err := eventing.ValidateSourceCheckpoint(page.NextCursor); err != nil {
		return errors.Join(eventing.ErrInvalidReconcilePage, err)
	}
	if len(page.Events) > 0 {
		if page.NextCursor == "" || page.NextCursor == current {
			return fmt.Errorf("%w: a non-empty page must advance its cursor", eventing.ErrInvalidReconcilePage)
		}
		return nil
	}
	if !page.HighWatermark && page.NextCursor != "" {
		return fmt.Errorf("%w: an empty page cannot advance without a high watermark", eventing.ErrInvalidReconcilePage)
	}
	if page.HighWatermark && page.NextCursor == "" {
		return fmt.Errorf("%w: high watermark is empty", eventing.ErrInvalidReconcilePage)
	}
	return nil
}

func (r *SourceRecovery) heartbeat(ctx, opCtx context.Context, cancel context.CancelFunc, cp eventing.SourceCheckpoint, done chan<- error) {
	ticker := time.NewTicker(r.Lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-opCtx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := r.Store.HeartbeatSourceCheckpoint(ctx, cp, r.Clock.Now().Add(r.Lease)); err != nil {
				cancel()
				done <- err
				return
			}
		}
	}
}

func (r *SourceRecovery) fail(ctx context.Context, cp eventing.SourceCheckpoint, cause error) error {
	failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.Store.FailSourceCheckpoint(failureCtx, cp, r.Clock.Now().Add(r.Retry), cause.Error()); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (r *SourceRecovery) setDefaults() {
	if r.Interval <= 0 {
		r.Interval = 30 * time.Second
	}
	if r.Lease <= 0 {
		r.Lease = time.Minute
	}
	if r.Retry <= 0 {
		r.Retry = time.Minute
	}
	if r.Batch <= 0 {
		r.Batch = 100
	}
	if r.Batch > 1000 {
		r.Batch = 1000
	}
}

func (r *SourceRecovery) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}
