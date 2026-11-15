package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	WorkSubject      = "harness.work.wake"
	WorkStreamName   = "HARNESS_WORKS"
	WorkConsumerName = "harness-work-orchestrators"
)

type WorkHandler func(ctx context.Context, workID string, done func(ack bool, redeliverAfter time.Duration))

type WorkSubscriber interface {
	RunWork(ctx context.Context, h WorkHandler) error
}

func workIDOf(payload []byte) (string, error) {
	var p struct {
		WorkID string `json:"work_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.WorkID == "" {
		return "", fmt.Errorf("notify: bad work payload")
	}
	return p.WorkID, nil
}

func (l *Local) RunWork(ctx context.Context, h WorkHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p := <-l.workCh:
			id, err := workIDOf(p)
			if err != nil {
				continue
			}
			h(ctx, id, func(ack bool, after time.Duration) {
				if ack {
					return
				}
				go func() {
					select {
					case <-time.After(max(after, 100*time.Millisecond)):
						_ = l.Publish(context.Background(), WorkSubject, "", p)
					case <-ctx.Done():
					}
				}()
			})
		}
	}
}

func (j *JetStream) RunWork(ctx context.Context, h WorkHandler) error {
	cons, err := j.js.CreateOrUpdateConsumer(ctx, WorkStreamName, jetstream.ConsumerConfig{
		Durable: WorkConsumerName, AckPolicy: jetstream.AckExplicitPolicy, AckWait: j.AckWait, MaxAckPending: 10000,
		FilterSubject: WorkSubject,
	})
	if err != nil {
		return err
	}
	it, err := cons.Messages(jetstream.PullMaxMessages(j.MaxBatch))
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); it.Stop() }()
	for {
		msg, err := it.Next()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return ctx.Err()
			}
			return err
		}
		id, parseErr := workIDOf(msg.Data())
		if parseErr != nil {
			_ = msg.Term()
			continue
		}
		h(ctx, id, func(ack bool, after time.Duration) {
			if ack {
				_ = msg.Ack()
			} else {
				_ = msg.NakWithDelay(max(after, 100*time.Millisecond))
			}
		})
	}
}
