// Package notify carries run wake-ups. Delivery is at-least-once and may be
// lost or duplicated; correctness never depends on it (ADR-002).
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Handler receives a wake-up. It may block to apply backpressure (e.g. until
// a worker slot is free), may finish asynchronously, and must call done once.
type Handler func(ctx context.Context, runID string, done func(ack bool, redeliverAfter time.Duration))

// Publisher publishes outbox messages; msgID enables broker de-duplication.
type Publisher interface {
	Publish(ctx context.Context, subject, msgID string, payload []byte) error
}

// Subscriber delivers wake-ups to handler, bounded by the caller's pool.
type Subscriber interface {
	Run(ctx context.Context, h Handler) error
}

func runIDOf(payload []byte) (string, error) {
	var p struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.RunID == "" {
		return "", fmt.Errorf("notify: bad payload")
	}
	return p.RunID, nil
}

// ---------- in-process (single binary / tests) ----------

// Local is an in-memory broker. Messages that do not fit are dropped: the
// reconciler re-wakes idle runs, so loss only adds latency.
type Local struct {
	ch   chan []byte
	once sync.Once
}

func NewLocal(buffer int) *Local { return &Local{ch: make(chan []byte, buffer)} }

func (l *Local) Publish(_ context.Context, _, _ string, payload []byte) error {
	select {
	case l.ch <- payload:
	default:
	}
	return nil
}

func (l *Local) Run(ctx context.Context, h Handler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case p := <-l.ch:
			id, err := runIDOf(p)
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
						_ = l.Publish(context.Background(), "", "", p)
					case <-ctx.Done():
					}
				}()
			})
		}
	}
}

// ---------- NATS JetStream ----------

const (
	StreamName   = "HARNESS_RUNS"
	ConsumerName = "harness-workers"
)

type JetStream struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	AckWait  time.Duration
	MaxBatch int
}

// ConnectJetStream connects and ensures the stream exists. The broker may come
// up after the harness (or be restarting), so setup is retried with backoff
// until ctx ends; the database stays the source of truth meanwhile.
func ConnectJetStream(ctx context.Context, url string) (*JetStream, error) {
	backoff := 200 * time.Millisecond
	for {
		j, err := connectOnce(ctx, url)
		if err == nil {
			return j, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w (last error: %v)", ctx.Err(), err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func connectOnce(ctx context.Context, url string) (*JetStream, error) {
	nc, err := nats.Connect(url, nats.Name("go-sre-harness"), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = js.CreateOrUpdateStream(sctx, jetstream.StreamConfig{
		Name: StreamName, Subjects: []string{"harness.run.>"}, Retention: jetstream.WorkQueuePolicy,
		Storage: jetstream.FileStorage, Duplicates: 2 * time.Minute,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &JetStream{nc: nc, js: js, AckWait: 10 * time.Minute, MaxBatch: 64}, nil
}

func (j *JetStream) Close() { j.nc.Close() }

func (j *JetStream) Publish(ctx context.Context, subject, msgID string, payload []byte) error {
	_, err := j.js.Publish(ctx, subject, payload, jetstream.WithMsgID(msgID))
	return err
}

// Run pulls with a durable consumer and hands each message to h, which
// bounds concurrency by blocking while all worker slots are busy. AckWait must exceed a step's
// worst-case duration; a late ack only causes a harmless duplicate wake-up.
func (j *JetStream) Run(ctx context.Context, h Handler) error {
	cons, err := j.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable: ConsumerName, AckPolicy: jetstream.AckExplicitPolicy, AckWait: j.AckWait, MaxAckPending: 10000,
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
		id, perr := runIDOf(msg.Data())
		if perr != nil {
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
