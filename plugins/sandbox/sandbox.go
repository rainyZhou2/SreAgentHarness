// Package sandbox registers the sandbox backends.
package sandbox

import (
	"context"
	"os"
	"path/filepath"

	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/sandbox"
)

// ContainerConfig is the `container` backend's plugin config.
type ContainerConfig struct {
	Runtime        string          `json:"runtime,omitempty"` // docker (default) | podman | a path
	Image          string          `json:"image"`
	Root           string          `json:"root,omitempty"`    // host directory for run workspaces
	Network        string          `json:"network,omitempty"` // none (default)
	CPUs           float64         `json:"cpus,omitempty"`
	MemoryMB       int             `json:"memory_mb,omitempty"`
	Pids           int             `json:"pids,omitempty"`
	TmpMB          int             `json:"tmp_mb,omitempty"`
	Timeout        plugin.Duration `json:"timeout,omitempty"`
	MaxOutputBytes int             `json:"max_output_bytes,omitempty"`
	WorkspaceTTL   plugin.Duration `json:"workspace_ttl,omitempty"`
	ExtraArgs      []string        `json:"extra_args,omitempty"`
}

func init() {
	plugin.Register(plugin.Sandbox, "container", "commands in a fresh container per call (docker/podman; gVisor via extra_args), no network, run workspace mounted",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (sandbox.Backend, error) {
			var cfg ContainerConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.Root == "" {
				cfg.Root = filepath.Join(os.TempDir(), "harness-sandbox")
			}
			return sandbox.NewContainer(sandbox.ContainerConfig{Runtime: cfg.Runtime, Image: cfg.Image, Root: cfg.Root,
				Network: cfg.Network, WorkspaceTTL: cfg.WorkspaceTTL.D(), ExtraArgs: cfg.ExtraArgs,
				Limits: sandbox.Limits{CPUs: cfg.CPUs, MemoryMB: cfg.MemoryMB, Pids: cfg.Pids, TmpMB: cfg.TmpMB,
					Timeout: cfg.Timeout.D(), MaxOutputBytes: cfg.MaxOutputBytes},
				Log: h.Log.With("component", "sandbox")})
		})
}
