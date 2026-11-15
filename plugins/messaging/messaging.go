package messaging

import (
	"context"
	"errors"
	"time"

	"go-sre-harness/internal/eventbus"
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

type EventBusConfig struct {
	URL        string          `json:"url"`
	Stream     string          `json:"stream,omitempty"`
	MaxAge     plugin.Duration `json:"max_age,omitempty"`
	MaxBytes   int64           `json:"max_bytes,omitempty"`
	Duplicates plugin.Duration `json:"duplicates,omitempty"`
}

type localBroker struct{ *notify.Local }

func init() {
	plugin.Register(plugin.Broker, plugin.StaticDefinition("local", "v1.0.0", "in-process broker; only valid when API, scheduler and worker share a process (mode all)",
		plugin.StrictConfig[LocalConfig](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.MessageBroker, error) {
			cfg := LocalConfig{Buffer: 4096}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Mode != "all" {
				return nil, errors.New("the local broker only works in mode 'all'; configure the nats broker")
			}
			return localBroker{notify.NewLocal(cfg.Buffer)}, nil
		}))
	plugin.Register(plugin.Broker, plugin.Define("nats", "v1.0.0", "NATS JetStream work queue",
		plugin.StrictConfig[NATSConfig](),
		func(ctx context.Context, _ *plugin.Host, c plugin.Config) (plugin.Component[plugin.MessageBroker], error) {
			var cfg NATSConfig
			if err := c.Decode(&cfg); err != nil {
				return plugin.Component[plugin.MessageBroker]{}, err
			}
			if cfg.URL == "" {
				return plugin.Component[plugin.MessageBroker]{}, errors.New("url is required")
			}
			broker, err := notify.ConnectJetStream(ctx, cfg.URL)
			if err != nil {
				return plugin.Component[plugin.MessageBroker]{}, err
			}
			component := plugin.Static[plugin.MessageBroker](broker)
			component.Lifecycle = plugin.LifecycleFuncs{OnStop: func(context.Context) error {
				broker.Close()
				return nil
			}}
			return component, nil
		}))
	plugin.Register(plugin.StreamBus, plugin.StaticDefinition("local", "v1.0.0", "in-process delta fan-out", nil,
		func(context.Context, *plugin.Host, plugin.Config) (stream.Bus, error) { return stream.NewLocal(), nil }))
	plugin.Register(plugin.StreamBus, plugin.Define("nats", "v1.0.0", "NATS core subjects for cross-process deltas",
		plugin.StrictConfig[NATSConfig](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Component[stream.Bus], error) {
			var cfg NATSConfig
			if err := c.Decode(&cfg); err != nil {
				return plugin.Component[stream.Bus]{}, err
			}
			if cfg.URL == "" {
				return plugin.Component[stream.Bus]{}, errors.New("url is required")
			}
			bus, err := stream.ConnectNATS(cfg.URL, h.Log)
			if err != nil {
				return plugin.Component[stream.Bus]{}, err
			}
			component := plugin.Static[stream.Bus](bus)
			component.Lifecycle = plugin.LifecycleFuncs{OnStop: func(context.Context) error {
				bus.Close()
				return nil
			}}
			return component, nil
		}))
	plugin.Register(plugin.EventBus, plugin.StaticDefinition("local", "v1.0.0", "in-process event fan-out; only valid when publisher and subscribers share a process", nil,
		func(context.Context, *plugin.Host, plugin.Config) (eventbus.Bus, error) {
			return eventbus.NewLocal(), nil
		}))
	plugin.Register(plugin.EventBus, plugin.Define("nats", "v1.0.0", "NATS JetStream stream of committed run events with bounded replay",
		plugin.StrictConfig[EventBusConfig](),
		func(ctx context.Context, h *plugin.Host, c plugin.Config) (plugin.Component[eventbus.Bus], error) {
			cfg := EventBusConfig{MaxAge: plugin.Duration(time.Hour), MaxBytes: 2 << 30}
			if err := c.Decode(&cfg); err != nil {
				return plugin.Component[eventbus.Bus]{}, err
			}
			if cfg.URL == "" {
				return plugin.Component[eventbus.Bus]{}, errors.New("url is required")
			}
			bus, err := eventbus.ConnectNATS(ctx, eventbus.NATSConfig{
				URL: cfg.URL, Stream: cfg.Stream, MaxAge: cfg.MaxAge.D(), MaxBytes: cfg.MaxBytes,
				Duplicates: cfg.Duplicates.D(), Log: h.Log,
			})
			if err != nil {
				return plugin.Component[eventbus.Bus]{}, err
			}
			component := plugin.Static[eventbus.Bus](bus)
			component.Lifecycle = plugin.LifecycleFuncs{OnStop: func(context.Context) error {
				bus.Close()
				return nil
			}, CheckHealth: func(context.Context) plugin.Health {
				if bus.Healthy() {
					return plugin.Health{Status: plugin.Healthy}
				}
				return plugin.Health{Status: plugin.Unhealthy, Code: "event_bus_disconnected"}
			}}
			return component, nil
		}))
}
