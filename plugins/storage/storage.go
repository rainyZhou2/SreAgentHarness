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

// S3Config configures the "s3" artifact store.
type S3Config struct {
	artifact.S3Config
	// LegacyFSRoot keeps "fs://" refs readable after a move from the
	// filesystem store: new artifacts go to S3, old ones are still served
	// from this directory.
	LegacyFSRoot string `json:"legacy_fs_root,omitempty"`
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
	plugin.Register(plugin.ArtifactStore, "s3", "S3-compatible object store (AWS S3, MinIO): content-addressed, encrypted at rest, verified on read and write",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (artifact.Store, error) {
			var cfg S3Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			s3, err := artifact.NewS3(cfg.S3Config)
			if err != nil {
				return nil, err
			}
			if cfg.LegacyFSRoot == "" {
				return s3, nil
			}
			// Refs written before the move to S3 stay readable.
			return &artifact.Mux{Primary: s3, Readers: map[string]artifact.Store{"s3": s3, "fs": &artifact.FS{Root: cfg.LegacyFSRoot}}}, nil
		})
	plugin.Register(plugin.ArtifactStore, "memory", "in-memory store (tests and demos; lost on restart)",
		func(context.Context, *plugin.Host, plugin.Config) (artifact.Store, error) {
			return artifact.NewMemory(), nil
		})
}
