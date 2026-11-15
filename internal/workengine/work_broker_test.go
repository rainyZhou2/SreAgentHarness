package workengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/orchestration"
)

type captureWorkBroker struct{ received chan string }

func (b captureWorkBroker) RunWork(ctx context.Context, h notify.WorkHandler) error {
	h(ctx, "work-id", func(ack bool, _ time.Duration) {
		if ack {
			b.received <- "work-id"
		}
	})
	<-ctx.Done()
	return ctx.Err()
}

func TestEngineRunsWorkBroker(t *testing.T) {
	broker := captureWorkBroker{received: make(chan string, 1)}
	e := &Engine{Orch: &orchestration.Orchestrator{}, workBroker: broker}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.runWorkWake(ctx) }()
	select {
	case <-broker.received:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("broker did not reach the orchestrator")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error=%v", err)
	}
}
