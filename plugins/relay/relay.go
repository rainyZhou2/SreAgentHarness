// Package relay registers the event relay job that feeds the event bus from
// the durable run event log.
package relay

import (
	"context"
	"fmt"
	"os"
	"time"

	"go-sre-harness/internal/plugin"
	relaycore "go-sre-harness/internal/relay"
)

type Config struct {
	Interval  plugin.Duration `json:"interval,omitempty"`
	Batch     int             `json:"batch,omitempty"`
	LeaseTTL  plugin.Duration `json:"lease_ttl,omitempty"`
	StartFrom string          `json:"start_from,omitempty"`
}

func init() {
	plugin.Register(plugin.Job, "event_relay", "publishes committed run events to the event bus (single active replica, leased cursor)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Runner, error) {
			cfg := Config{Interval: plugin.Duration(250 * time.Millisecond), Batch: 1000,
				LeaseTTL: plugin.Duration(15 * time.Second), StartFrom: "latest"}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Events == nil {
				h.Log.Warn("event relay disabled: no event_bus plugin is enabled")
				return plugin.RunnerFunc(func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				}), nil
			}
			switch cfg.StartFrom {
			case "", "latest", "earliest":
			default:
				return nil, fmt.Errorf("start_from must be latest or earliest, got %q", cfg.StartFrom)
			}
			hostname, _ := os.Hostname()
			return &relaycore.Relay{
				ID:        "event-relay",
				Owner:     fmt.Sprintf("%s-%d", hostname, os.Getpid()),
				Store:     h.Store,
				Publisher: h.Events,
				Log:       h.Log.With("job", "event_relay"),
				Observer:  h.Metrics,
				Batch:     cfg.Batch,
				Interval:  cfg.Interval.D(),
				LeaseTTL:  cfg.LeaseTTL.D(),
				StartFrom: cfg.StartFrom,
			}, nil
		})
}
