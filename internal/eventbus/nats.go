package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventlog"
)

// NATSConfig configures the JetStream-backed bus.
type NATSConfig struct {
	URL        string
	Stream     string
	MaxAge     time.Duration
	MaxBytes   int64
	Duplicates time.Duration
	Log        *slog.Logger
}

// NATS fans run events out through a JetStream stream. The stream keeps a
// bounded history so a subscriber that reconnects within MaxAge can resume
// without touching PostgreSQL.
type NATS struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string
	log    *slog.Logger
	closed atomic.Bool
}

func ConnectNATS(ctx context.Context, cfg NATSConfig) (*NATS, error) {
	if cfg.Stream == "" {
		cfg.Stream = "HARNESS_EVENTS"
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = time.Hour
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 2 << 30
	}
	if cfg.Duplicates <= 0 {
		cfg.Duplicates = 2 * time.Minute
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	nc, err := nats.Connect(cfg.URL, nats.MaxReconnects(-1), nats.Name("sre-harness-event-bus"))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	b := &NATS{nc: nc, js: js, stream: cfg.Stream, log: cfg.Log}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.Stream,
		Subjects:   []string{"harness.events.>"},
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     cfg.MaxAge,
		MaxBytes:   cfg.MaxBytes,
		Discard:    jetstream.DiscardOld,
		Duplicates: cfg.Duplicates,
		Storage:    jetstream.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	return b, nil
}

func (b *NATS) Healthy() bool { return !b.closed.Load() && b.nc.IsConnected() }

func (b *NATS) Shared() bool { return true }

func (b *NATS) Publish(ctx context.Context, e domain.Event) error {
	if b.closed.Load() {
		return ErrClosed
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(ctx, eventlog.Subject(e.TenantID, e.RunID), data, jetstream.WithMsgID(e.ID))
	return err
}

func (b *NATS) Subscribe(ctx context.Context, tenantID, runID string) (<-chan domain.Event, func(), error) {
	if b.closed.Load() {
		return nil, nil, ErrClosed
	}
	cons, err := b.js.OrderedConsumer(ctx, b.stream, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{eventlog.Subject(tenantID, runID)},
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}
	msgs, err := cons.Messages()
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan domain.Event, 512)
	done := make(chan struct{})
	go func() {
		defer close(ch)
		defer close(done)
		for {
			m, err := msgs.Next()
			if err != nil {
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) && ctx.Err() == nil {
					b.log.Debug("event subscription ended", "run_id", runID, "err", err)
				}
				return
			}
			var e domain.Event
			if json.Unmarshal(m.Data(), &e) != nil {
				continue
			}
			select {
			case ch <- e:
			default:
			}
		}
	}()
	var stopped atomic.Bool
	return ch, func() {
		if stopped.CompareAndSwap(false, true) {
			msgs.Stop()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
	}, nil
}

func (b *NATS) Close() {
	if b.closed.CompareAndSwap(false, true) {
		b.nc.Close()
	}
}
