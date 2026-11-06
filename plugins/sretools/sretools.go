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
)

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
	plugin.Register(plugin.Tools, "demo", "demo.metrics (R0) and demo.restart (R2) for local runs",
		func(context.Context, *plugin.Host, plugin.Config) (plugin.ToolSource, error) {
			return static(tools.DemoTools()), nil
		})
	plugin.Register(plugin.Tools, "prometheus", "prom.query / prom.alerts (read-only, R0)",
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
		})
	plugin.Register(plugin.Tools, "kubernetes", "k8s.deployment_status (R0), k8s.rollout_restart (R2, dry-run by default)",
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
		})
	plugin.Register(plugin.Tools, "mcp", "one MCP server (stdio or streamable HTTP); allowlisted tools only",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.ToolSource, error) {
			var cfg mcpexec.ServerConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			s, err := mcpexec.New(cfg, nil, h.Log)
			if err != nil {
				return nil, err
			}
			return mcpSource{&mcpexec.Manager{Servers: []*mcpexec.Server{s}, Registry: h.Tools, Log: h.Log}}, nil
		})
	plugin.Register(plugin.Tools, "remote", "one remote gRPC executor (mTLS); allowlisted capabilities only",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.ToolSource, error) {
			var cfg remote.Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.Name == "" {
				return nil, errors.New("name is required")
			}
			cl, err := remote.Dial(cfg, nil, h.Log)
			if err != nil {
				return nil, err
			}
			return remoteSource{c: cl, name: cfg.Name, h: h}, nil
		})
}
