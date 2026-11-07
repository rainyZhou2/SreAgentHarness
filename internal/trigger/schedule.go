package trigger

import (
	"context"
	"strconv"
	"time"

	"go-sre-harness/internal/domain"
)

// RunSchedules creates one run per interval for every schedule trigger. The
// dedup claim key is the interval bucket, so several scheduler replicas
// create exactly one run per bucket. It blocks until ctx is cancelled.
func (s *Service) RunSchedules(ctx context.Context) error {
	var scheduled bool
	for _, t := range s.triggers {
		if t.Schedule != nil && !t.Disabled {
			scheduled = true
		}
	}
	if !scheduled {
		<-ctx.Done()
		return ctx.Err()
	}
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		s.fireSchedules(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (s *Service) fireSchedules(ctx context.Context) {
	now := s.now()
	for _, name := range s.order {
		t := s.triggers[name]
		if t.Schedule == nil || t.Disabled {
			continue
		}
		interval := t.Schedule.Interval.D()
		bucket := now.Truncate(interval)
		key := t.Name + ":schedule:" + strconv.FormatInt(bucket.Unix(), 10)
		claim, err := s.Dedup.ClaimTrigger(ctx, t.Tenant, key, interval, false)
		if err != nil {
			s.Log.Warn("schedule claim", "trigger", t.Name, "err", err)
			continue
		}
		if !claim.Fresh && claim.RunID != "" {
			continue
		}
		ictx := map[string]any{"payload": map[string]any{}, "item": map[string]any{}, "headers": map[string]any{},
			"trigger": t.Name, "now": now.Format(time.RFC3339), "bucket": bucket.Format(time.RFC3339)}
		ev, err := render(t, ictx, now)
		if err != nil {
			s.Log.Warn("schedule render", "trigger", t.Name, "err", err)
			_ = s.Dedup.ReleaseTrigger(ctx, t.Tenant, key, claim.ID)
			continue
		}
		if ev.ctx.Window == nil {
			// A patrol covers the interval that just ended.
			ev.ctx.Window = &domain.TimeWindow{Start: bucket.Add(-interval), End: bucket}
		}
		res := s.createForClaim(ctx, t, t.Tenant, "schedule:"+t.Name, ev, key, claim, ItemResult{DedupKey: key})
		if res.Action == ActionError {
			s.Log.Warn("schedule run", "trigger", t.Name, "err", res.Error)
		}
	}
}
