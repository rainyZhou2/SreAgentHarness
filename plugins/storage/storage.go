// Package storage registers the built-in artifact_store plugins.
package storage

import (
	"context"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/plugin"
)

type FSConfig struct {
	Root string `json:"root"`
}

func init() {
	plugin.Register(plugin.ArtifactStore, "fs", "local filesystem store (single node or shared volume)",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (artifact.Store, error) {
			cfg := FSConfig{Root: "./data/artifacts"}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return &artifact.FS{Root: cfg.Root}, nil
		})
	plugin.Register(plugin.ArtifactStore, "memory", "in-memory store (tests and demos; lost on restart)",
		func(context.Context, *plugin.Host, plugin.Config) (artifact.Store, error) {
			return artifact.NewMemory(), nil
		})
}
