package orchestration

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/work"
)

func TestWorkWakeHintsAreBroadcastAndCoalesced(t *testing.T) {
	o := &Orchestrator{}
	for i := 0; i < 20; i++ {
		acked := false
		o.HandleWorkWake(context.Background(), "work-id", func(ack bool, _ time.Duration) {
			if !ack {
				t.Fatal("locally queued wake was rejected")
			}
			acked = true
		})
		if !acked {
			t.Fatal("wake was not acknowledged")
		}
	}
	for _, queue := range work.Queues {
		if len(o.wake[queue]) != 1 {
			t.Fatalf("queue %s has %d hints", queue, len(o.wake[queue]))
		}
	}
}

func TestWorkWakeRejectsEmptyWorkID(t *testing.T) {
	o := &Orchestrator{}
	called := false
	o.HandleWorkWake(context.Background(), "", func(bool, time.Duration) { called = true })
	if called {
		t.Fatal("empty Work ID was acknowledged")
	}
}
