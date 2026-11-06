// Package retention runs the background cleanup jobs of §13.8: published
// outbox rows, expired token-stream segments (rows and objects), expired
// memories (marked STALE, never silently deleted) and, optionally, old
// security audit rows. Durable facts (runs, steps, events) are kept; their
// archival is a deployment decision (partition drop / export).
package retention

import (
	"context"
	"log/slog"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/store/pg"
)

type Store interface {
	DeletePublishedOutbox(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	ExpiredSegments(ctx context.Context, now time.Time, limit int) ([]pg.ExpiredSegment, error)
	DeleteSegment(ctx context.Context, e pg.ExpiredSegment) error
	ExpireMemories(ctx context.Context, now time.Time, limit int) (int64, error)
	DeleteOldAudit(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

type Observer interface{ ObserveRetention(kind string, n int64) }

type Config struct {
	Interval        time.Duration
	OutboxRetention time.Duration // default 24h
	AuditRetention  time.Duration // 0 = keep forever
	Batch           int
}

type Cleaner struct {
	Store     Store
	Artifacts artifact.Store
	Cfg       Config
	Log       *slog.Logger
	Observer  Observer
	Now       func() time.Time
}

type Result struct {
	Outbox, Segments, SegmentObjects, Memories, Audit int64
}

func (c *Cleaner) defaults() {
	if c.Cfg.Interval == 0 {
		c.Cfg.Interval = 5 * time.Minute
	}
	if c.Cfg.OutboxRetention == 0 {
		c.Cfg.OutboxRetention = 24 * time.Hour
	}
	if c.Cfg.Batch == 0 {
		c.Cfg.Batch = 1000
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
}

func (c *Cleaner) Tick(ctx context.Context) (Result, error) {
	c.defaults()
	var res Result
	now := c.Now()
	n, err := c.Store.DeletePublishedOutbox(ctx, now.Add(-c.Cfg.OutboxRetention), c.Cfg.Batch)
	if err != nil {
		return res, err
	}
	res.Outbox = n

	segs, err := c.Store.ExpiredSegments(ctx, now, c.Cfg.Batch)
	if err != nil {
		return res, err
	}
	del, _ := c.Artifacts.(artifact.Deleter)
	for _, s := range segs {
		// Object first: a row without its object is harmless (reads 404),
		// an object without its row would leak.
		if del != nil {
			if err := del.Delete(ctx, s.TenantID, s.Ref); err != nil {
				c.Log.Warn("segment object delete failed", "ref", s.Ref, "err", err)
				continue
			}
			res.SegmentObjects++
		}
		if err := c.Store.DeleteSegment(ctx, s); err != nil {
			return res, err
		}
		res.Segments++
	}
	if res.Memories, err = c.Store.ExpireMemories(ctx, now, c.Cfg.Batch); err != nil {
		return res, err
	}
	if c.Cfg.AuditRetention > 0 {
		if res.Audit, err = c.Store.DeleteOldAudit(ctx, now.Add(-c.Cfg.AuditRetention), c.Cfg.Batch); err != nil {
			return res, err
		}
	}
	if c.Observer != nil {
		c.Observer.ObserveRetention("outbox", res.Outbox)
		c.Observer.ObserveRetention("stream_segment", res.Segments)
		c.Observer.ObserveRetention("memory_expired", res.Memories)
		c.Observer.ObserveRetention("audit", res.Audit)
	}
	return res, nil
}

func (c *Cleaner) Run(ctx context.Context) error {
	c.defaults()
	t := time.NewTicker(c.Cfg.Interval)
	defer t.Stop()
	for {
		if r, err := c.Tick(ctx); err != nil && ctx.Err() == nil {
			c.Log.Warn("retention tick", "err", err)
		} else if r != (Result{}) {
			c.Log.Info("retention", "outbox", r.Outbox, "segments", r.Segments, "memories_expired", r.Memories, "audit", r.Audit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
