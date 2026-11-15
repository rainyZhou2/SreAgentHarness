package orchestration

import (
	"context"
	"errors"
	"strings"
	"time"

	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/work"
)

func (o *Orchestrator) initWake() {
	o.wakeOnce.Do(func() {
		o.wake = make(map[work.Queue]chan struct{}, len(work.Queues))
		for _, queue := range work.Queues {
			o.wake[queue] = make(chan struct{}, 1)
		}
	})
}

func (o *Orchestrator) HandleWorkWake(_ context.Context, workID string, done func(bool, time.Duration)) {
	if strings.TrimSpace(workID) == "" {
		return
	}
	o.initWake()
	for _, queue := range work.Queues {
		select {
		case o.wake[queue] <- struct{}{}:
		default:
		}
	}
	done(true, 0)
}

func (o *Orchestrator) RunWork(ctx context.Context, broker notify.WorkSubscriber) error {
	if broker == nil {
		return errors.New("work wake broker is required")
	}
	o.initWake()
	return broker.RunWork(ctx, o.HandleWorkWake)
}
