// Package memory registers the built-in embedder, memory_retriever and
// consolidator job plugins.
package memory

import (
	"context"
	"errors"
	"time"

	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/plugin"
)

type HashingConfig struct {
	Dims int `json:"dims,omitempty"`
}

type RetrieverConfig struct {
	TopK int `json:"top_k,omitempty"`
}

type JobConfig struct {
	Interval plugin.Duration `json:"interval,omitempty"`
}

func init() {
	plugin.Register(plugin.Embedder, "hashing", "dependency-free uni/bigram hashing embedder",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (memory.Embedder, error) {
			var cfg HashingConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return memory.HashingEmbedder{Dims: cfg.Dims}, nil
		})
	plugin.Register(plugin.MemoryRetriever, "hybrid", "lexical + vector retrieval with RRF, scope filters and conflict resolution",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (kernel.MemoryRetriever, error) {
			cfg := RetrieverConfig{TopK: 5}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Embedder == nil {
				return nil, errors.New("no embedder plugin is enabled")
			}
			return &memory.Retriever{Store: h.Store, Embedder: h.Embedder, Artifacts: h.Artifacts, TopK: cfg.TopK}, nil
		})
	plugin.Register(plugin.Job, "consolidator", "turns finished runs into candidate memories and experiences",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Runner, error) {
			cfg := JobConfig{Interval: plugin.Duration(10 * time.Second)}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			reg := h.Tools
			return &memory.Consolidator{Store: h.Store, Artifacts: h.Artifacts,
				Writer: memory.Writer{Embedder: h.Embedder, Artifacts: h.Artifacts},
				SideEffecting: func(name string) bool {
					t, ok := reg.Lookup(name)
					return !ok || t.Descriptor().Risk.HasSideEffects()
				}, Interval: cfg.Interval.D(), Log: h.Log}, nil
		})
}
