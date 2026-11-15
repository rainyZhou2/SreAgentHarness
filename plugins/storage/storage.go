package storage

import (
	"context"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/plugin"
)

type FSConfig struct {
	Root string `json:"root"`
}

type S3Config struct {
	artifact.S3Config
	LegacyFSRoot string `json:"legacy_fs_root,omitempty"`
}

func init() {
	plugin.Register(plugin.ArtifactStore, plugin.StaticDefinition("fs", "v1.0.0", "local filesystem store (single node or shared volume)",
		plugin.StrictConfig[FSConfig](),
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (artifact.Store, error) {
			cfg := FSConfig{Root: "./data/artifacts"}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return &artifact.FS{Root: cfg.Root}, nil
		}))
	plugin.Register(plugin.ArtifactStore, plugin.StaticDefinition("s3", "v1.0.0", "S3-compatible object store (AWS S3, MinIO): content-addressed, encrypted at rest, verified on read and write",
		plugin.StrictConfig[S3Config](),
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (artifact.Store, error) {
			var cfg S3Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			cfg.ManagedRetention = true
			s3, err := artifact.NewS3(cfg.S3Config)
			if err != nil {
				return nil, err
			}
			if cfg.LegacyFSRoot == "" {
				return s3, nil
			}
			return &artifact.Mux{Primary: s3, Readers: map[string]artifact.Store{"s3": s3, "fs": &artifact.FS{Root: cfg.LegacyFSRoot}}}, nil
		}))
	plugin.Register(plugin.ArtifactStore, plugin.StaticDefinition("memory", "v1.0.0", "in-memory store for isolated tests; lost on restart", nil,
		func(context.Context, *plugin.Host, plugin.Config) (artifact.Store, error) {
			return artifact.NewMemory(), nil
		}))
}
