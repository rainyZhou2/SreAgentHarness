package notify

import (
	"context"
	"testing"
	"time"
)

func TestLocalWorkWakeUsesIndependentConsumer(t *testing.T) {
	broker := NewLocal(1)
	if err := broker.Publish(context.Background(), WorkSubject, "message", []byte(`{"work_id":"work-1"}`)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	received := make(chan string, 1)
	go func() {
		_ = broker.RunWork(ctx, func(_ context.Context, workID string, done func(bool, time.Duration)) {
			received <- workID
			done(true, 0)
			cancel()
		})
	}()
	select {
	case id := <-received:
		if id != "work-1" {
			t.Fatalf("work ID=%q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("work wake was not delivered")
	}
	if len(broker.ch) != 0 {
		t.Fatal("work wake entered the run channel")
	}
}

func TestWorkPayloadRequiresWorkID(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte(`{"run_id":"run-1"}`), []byte(`{"work_id":""}`)} {
		if _, err := workIDOf(payload); err == nil {
			t.Fatalf("accepted payload %s", payload)
		}
	}
}

func TestWorkJetStreamResourcesAreIndependent(t *testing.T) {
	if WorkStreamName == StreamName || WorkConsumerName == ConsumerName || WorkSubject == "harness.run.wake" {
		t.Fatal("work and run broker resources must be independent")
	}
}
