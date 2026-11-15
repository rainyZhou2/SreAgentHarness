// Package sretools registers the built-in tool plugins. Risk classes, trust
// tiers and allowlists come from this operator-owned config, never from the
// tools themselves.
package sretools

import (
	"context"
	"errors"

	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
	"go-sre-harness/internal/tools/remote"
	"go-sre-harness/internal/tools/sretools"
	"go-sre-harness/internal/tools/web"
)

type WebConfig struct {
	MaxBytes int64           `json:"max_bytes,omitempty"`
	Timeout  plugin.Duration `json:"timeout,omitempty"`
}

type KubernetesConfig struct {
	sretools.KubernetesConfig
	InCluster bool `json:"in_cluster,omitempty"`
}

type static []tools.Tool

func (s static) Tools(context.Context) ([]tools.Tool, error) { return s, nil }

// mcpSource registers discovered tools itself (names are mcp.<server>.<tool>).
type mcpSource struct{ m *mcpexec.Manager }

func (s mcpSource) Tools(ctx context.Context) ([]tools.Tool, error) {
	s.m.Refresh(ctx)
	return nil, nil
}
func (s mcpSource) Close() { s.m.Close() }

type remoteSource struct {
	c    *remote.Client
	name string
	h    *plugin.Host
}

func (s remoteSource) Tools(ctx context.Context) ([]tools.Tool, error) {
	ts, skipped, err := s.c.Discover(ctx)
	if err != nil {
		// A backend that is down at startup is skipped; the harness keeps running.
		s.h.Log.Warn("remote executor discovery failed", "executor", s.name, "err", err)
		return nil, nil
	}
	s.h.Log.Info("remote tools discovered", "executor", s.name, "tools", len(ts), "not_allowlisted", skipped)
	return ts, nil
}
func (s remoteSource) Close() error { return s.c.Close() }

func init() {
	plugin.Register(plugin.Tools, plugin.StaticDefinition("demo", "v1.0.0", "local metrics (R0) and restart (R2) tools", nil,
		func(context.Context, *plugin.Host, plugin.Config) (plugin.ToolSource, error) {
			return static(tools.DemoTools()), nil
		}))
	plugin.Register(plugin.Tools, plugin.StaticDefinition("web", "v1.0.0", "web.fetch (R0): read a public page as text; untrusted output",
		plugin.StrictConfig[WebConfig](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.ToolSource, error) {
			var cfg WebConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return static([]tools.Tool{web.NewFetchTool(web.FetchConfig{
				MaxBytes: cfg.MaxBytes, Timeout: cfg.Timeout.D(), Policy: h.Network})}), nil
		}))
	plugin.Register(plugin.Tools, plugin.StaticDefinition("prometheus", "v1.0.0", "prom.query / prom.alerts (read-only, R0)",
		plugin.StrictConfig[sretools.PrometheusConfig](),
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (plugin.ToolSource, error) {
			var cfg sretools.PrometheusConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			p, err := sretools.NewPrometheus(cfg)
			if err != nil {
				return nil, err
			}
			return static(p.Tools()), nil
		}))
	plugin.Register(plugin.Tools, plugin.StaticDefinition("kubernetes", "v1.0.0", "k8s.deployment_status (R0), k8s.rollout_restart (R2, dry-run by default)",
		plugin.StrictConfig[KubernetesConfig](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.ToolSource, error) {
			var cfg KubernetesConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			kc := cfg.KubernetesConfig
			if cfg.InCluster {
				kc = sretools.InClusterConfig(kc)
			}
			k, err := sretools.NewKubernetes(kc)
			if err != nil {
				return nil, err
			}
			h.Log.Info("kubernetes tools", "dry_run", k.DryRun(), "namespaces", kc.Namespaces)
			return static(k.Tools()), nil
		}))
	plugin.Register(plugin.Tools, plugin.Define("mcp", "v1.0.0", "one MCP server (stdio or streamable HTTP); allowlisted tools only",
		plugin.StrictConfig[mcpexec.ServerConfig](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Component[plugin.ToolSource], error) {
			var cfg mcpexec.ServerConfig
			if err := c.Decode(&cfg); err != nil {
				return plugin.Component[plugin.ToolSource]{}, err
			}
			s, err := mcpexec.New(cfg, nil, h.Log)
			if err != nil {
				return plugin.Component[plugin.ToolSource]{}, err
			}
			source := mcpSource{&mcpexec.Manager{Servers: []*mcpexec.Server{s}, Registry: h.Tools, Log: h.Log}}
			component := plugin.Static[plugin.ToolSource](source)
			component.Lifecycle = plugin.LifecycleFuncs{OnStop: func(context.Context) error {
				source.Close()
				return nil
			}}
			return component, nil
		}))
	plugin.Register(plugin.Tools, plugin.Define("remote", "v1.0.0", "one remote gRPC executor (mTLS); allowlisted capabilities only",
		plugin.StrictConfig[remote.Config](),
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Component[plugin.ToolSource], error) {
			var cfg remote.Config
			if err := c.Decode(&cfg); err != nil {
				return plugin.Component[plugin.ToolSource]{}, err
			}
			if cfg.Name == "" {
				return plugin.Component[plugin.ToolSource]{}, errors.New("name is required")
			}
			client, err := remote.Dial(cfg, nil, h.Log)
			if err != nil {
				return plugin.Component[plugin.ToolSource]{}, err
			}
			source := remoteSource{c: client, name: cfg.Name, h: h}
			component := plugin.Static[plugin.ToolSource](source)
			component.Lifecycle = plugin.LifecycleFuncs{OnStop: func(context.Context) error {
				return source.Close()
			}}
			return component, nil
		}))
}
