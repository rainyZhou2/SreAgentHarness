// Package toolconfig builds the tool registry from an operator-owned JSON
// file (HARNESS_TOOLS_CONFIG). Risk classes, allowlists and trust tiers all
// come from this file, never from the tools themselves.
package toolconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
	"go-sre-harness/internal/tools/remote"
	"go-sre-harness/internal/tools/sretools"
)

type Config struct {
	DemoTools  *bool                      `json:"demo_tools,omitempty"` // default true
	Prometheus *sretools.PrometheusConfig `json:"prometheus,omitempty"`
	Kubernetes *K8s                       `json:"kubernetes,omitempty"`
	MCPServers []mcpexec.ServerConfig     `json:"mcp_servers,omitempty"`
	Remote     []remote.Config            `json:"remote_executors,omitempty"`
}

type K8s struct {
	sretools.KubernetesConfig
	InCluster bool `json:"in_cluster,omitempty"`
}

func Load(path string) (Config, error) {
	var c Config
	if path == "" {
		return c, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	dec := json.NewDecoder(bytesReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("tools config %s: %w", path, err)
	}
	return c, nil
}

// Built is the assembled registry plus what must be closed on shutdown.
type Built struct {
	Registry *tools.Registry
	MCP      *mcpexec.Manager
	remotes  []*remote.Client
}

func (b *Built) Close() {
	if b.MCP != nil {
		b.MCP.Close()
	}
	for _, r := range b.remotes {
		_ = r.Close()
	}
}

// Build registers every configured tool. Remote backends that are down at
// startup are logged and skipped (the harness keeps running without them).
func Build(ctx context.Context, c Config, log *slog.Logger) (*Built, error) {
	reg := tools.NewRegistry()
	b := &Built{Registry: reg}
	if c.DemoTools == nil || *c.DemoTools {
		for _, t := range tools.DemoTools() {
			reg.Register(t)
		}
	}
	if c.Prometheus != nil {
		p, err := sretools.NewPrometheus(*c.Prometheus)
		if err != nil {
			return nil, err
		}
		for _, t := range p.Tools() {
			reg.Register(t)
		}
	}
	if c.Kubernetes != nil {
		kc := c.Kubernetes.KubernetesConfig
		if c.Kubernetes.InCluster {
			kc = sretools.InClusterConfig(kc)
		}
		k, err := sretools.NewKubernetes(kc)
		if err != nil {
			return nil, err
		}
		for _, t := range k.Tools() {
			reg.Register(t)
		}
		log.Info("kubernetes tools registered", "dry_run", k.DryRun(), "namespaces", kc.Namespaces)
	}
	if len(c.MCPServers) > 0 {
		m := &mcpexec.Manager{Registry: reg, Log: log}
		for _, sc := range c.MCPServers {
			s, err := mcpexec.New(sc, nil, log)
			if err != nil {
				return nil, err
			}
			m.Servers = append(m.Servers, s)
		}
		m.Refresh(ctx)
		b.MCP = m
	}
	for _, rc := range c.Remote {
		cl, err := remote.Dial(rc, nil, log)
		if err != nil {
			return nil, err
		}
		b.remotes = append(b.remotes, cl)
		ts, skipped, err := cl.Discover(ctx)
		if err != nil {
			log.Warn("remote executor discovery failed", "executor", rc.Name, "err", err)
			continue
		}
		for _, t := range ts {
			reg.Register(t)
		}
		log.Info("remote tools registered", "executor", rc.Name, "registered", len(ts), "not_allowlisted", skipped)
	}
	return b, nil
}
