// Package messaging registers the built-in broker and stream_bus plugins.
package messaging

import (
	"context"
	"errors"

	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/stream"
)

type LocalConfig struct {
	Buffer int `json:"buffer,omitempty"`
}

type NATSConfig struct {
	URL string `json:"url"`
}

type localBroker struct{ *notify.Local }

func init() {
	plugin.Register(plugin.Broker, "local", "in-process broker; only valid when API, scheduler and worker share a process (mode all)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.MessageBroker, error) {
			cfg := LocalConfig{Buffer: 4096}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Mode != "all" {
				return nil, errors.New("the local broker only works in mode 'all'; configure the nats broker")
			}
			return localBroker{notify.NewLocal(cfg.Buffer)}, nil
		})
	plugin.Register(plugin.Broker, "nats", "NATS JetStream work queue",
		func(ctx context.Context, _ *plugin.Host, c plugin.Config) (plugin.MessageBroker, error) {
			var cfg NATSConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.URL == "" {
				return nil, errors.New("url is required")
			}
			return notify.ConnectJetStream(ctx, cfg.URL)
		})
	plugin.Register(plugin.StreamBus, "local", "in-process delta fan-out",
		func(context.Context, *plugin.Host, plugin.Config) (stream.Bus, error) { return stream.NewLocal(), nil })
	plugin.Register(plugin.StreamBus, "nats", "NATS core subjects for cross-process deltas",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (stream.Bus, error) {
			var cfg NATSConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.URL == "" {
				return nil, errors.New("url is required")
			}
			return stream.ConnectNATS(cfg.URL, h.Log)
		})
}
