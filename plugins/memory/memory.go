// Package memory registers the built-in embedder, memory_retriever and
// consolidator job plugins.
package memory

import (
	"context"
	"errors"
	"os"
	"time"

	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/reasoning"
)

type HashingConfig struct {
	Dims int `json:"dims,omitempty"`
}

// HTTPConfig configures an OpenAI-compatible embeddings endpoint.
type HTTPConfig struct {
	BaseURL    string          `json:"base_url"`
	Model      string          `json:"model"`
	APIKeyEnv  string          `json:"api_key_env,omitempty"`
	Dimensions int             `json:"dimensions,omitempty"`
	Timeout    plugin.Duration `json:"timeout,omitempty"`
}

type RetrieverConfig struct {
	TopK int `json:"top_k,omitempty"`
}

type JobConfig struct {
	Interval plugin.Duration `json:"interval,omitempty"`
	// Index backfills CJK search terms and embeddings for the current
	// embedding model (default on).
	Index *bool `json:"index,omitempty"`
	// IndexBatch is the memories indexed per tick (default 50).
	IndexBatch int `json:"index_batch,omitempty"`
	// Reasoning scores every finished run and distills reasoning patterns
	// (default on).
	Reasoning *ReasoningConfig `json:"reasoning,omitempty"`
}

type ReasoningConfig struct {
	Enabled *bool `json:"enabled,omitempty"`
	// Distill stores reasoning patterns as candidate memories (default on).
	Distill *bool `json:"distill,omitempty"`
}

func on(b *bool) bool { return b == nil || *b }

func init() {
	plugin.Register(plugin.Embedder, "hashing", "dependency-free uni/bigram hashing embedder",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (memory.Embedder, error) {
			var cfg HashingConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return memory.HashingEmbedder{Dims: cfg.Dims}, nil
		})
	plugin.Register(plugin.Embedder, "http", "OpenAI-compatible embeddings endpoint (OpenAI, Azure, vLLM, Ollama, LiteLLM, …)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (memory.Embedder, error) {
			var cfg HTTPConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.BaseURL == "" || cfg.Model == "" {
				return nil, errors.New("http embedder: base_url and model are required")
			}
			key := ""
			if cfg.APIKeyEnv != "" {
				key = os.Getenv(cfg.APIKeyEnv)
			}
			return &memory.HTTPEmbedder{BaseURL: cfg.BaseURL, ModelName: cfg.Model, APIKey: key,
				Dimensions: cfg.Dimensions, Timeout: cfg.Timeout.D(), Log: h.Log.With("component", "embedder")}, nil
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
			writer := memory.Writer{Embedder: h.Embedder, Artifacts: h.Artifacts}
			cons := &memory.Consolidator{Store: h.Store, Artifacts: h.Artifacts, Writer: writer,
				SideEffecting: func(name string) bool {
					t, ok := reg.Lookup(name)
					return !ok || t.Descriptor().Risk.HasSideEffects()
				}, Interval: cfg.Interval.D(), Log: h.Log}
			if on(cfg.Index) && h.Store != nil {
				cons.Indexer = &memory.Indexer{Store: h.Store, Embedder: h.Embedder, Batch: cfg.IndexBatch,
					Log: h.Log.With("component", "memory_indexer")}
			}
			if rc := cfg.Reasoning; rc == nil || on(rc.Enabled) {
				rec := reasoning.NewRecorder(h.Store, h.Artifacts, writer, h.Log.With("hook", "reasoning"))
				rec.Distill = rc == nil || on(rc.Distill)
				cons.Hooks = append(cons.Hooks, rec)
			}
			return cons, nil
		})
}
