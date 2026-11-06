// Package models registers the built-in model_provider plugins. Each one
// contributes its adapter, endpoints and alias memberships; the route policy
// is composed from the enabled providers in config order.
package models

import (
	"context"
	"errors"

	"go-sre-harness/internal/cognitive"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/openai"
	"go-sre-harness/internal/plugin"
)

// Common lets operators override alias membership and endpoint metadata.
type Common struct {
	// Aliases replaces the provider's default alias membership.
	Aliases map[string][]string `json:"aliases,omitempty"`
	// Endpoints replaces the provider's default endpoints.
	Endpoints []gw.Endpoint `json:"endpoints,omitempty"`
}

func (c Common) apply(b plugin.ModelBackend) plugin.ModelBackend {
	if len(c.Endpoints) > 0 {
		b.Endpoints = c.Endpoints
	}
	if c.Aliases != nil {
		b.Aliases = c.Aliases
	}
	return b
}

type AnthropicConfig struct {
	Common
	APIKey  string          `json:"api_key"`
	BaseURL string          `json:"base_url,omitempty"`
	Effort  string          `json:"effort,omitempty"`
	Timeout plugin.Duration `json:"timeout,omitempty"`
}

type OpenAIConfig struct {
	Common
	BaseURL                string `json:"base_url"`
	APIKey                 string `json:"api_key,omitempty"`
	Model                  string `json:"model"`
	ContextWindow          int64  `json:"context_window,omitempty"`
	MaxOutput              int64  `json:"max_output,omitempty"`
	UseMaxCompletionTokens bool   `json:"use_max_completion_tokens,omitempty"`
}

func init() {
	plugin.Register(plugin.ModelProvider, "scripted", "deterministic rule-based model; keeps the harness runnable without keys",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (plugin.ModelBackend, error) {
			var cfg Common
			if err := c.Decode(&cfg); err != nil {
				return plugin.ModelBackend{}, err
			}
			return cfg.apply(plugin.ModelBackend{Provider: cognitive.ScriptedModel(cognitive.RuleBased{}),
				Endpoints: []gw.Endpoint{gw.ScriptedEndpoint()},
				Aliases: map[string][]string{"sre-reasoning": {"local-scripted"}, "sre-fast": {"local-scripted"},
					"local": {"local-scripted"}}}), nil
		})
	plugin.Register(plugin.ModelProvider, "anthropic", "Claude via the Anthropic Messages API",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (plugin.ModelBackend, error) {
			var cfg AnthropicConfig
			if err := c.Decode(&cfg); err != nil {
				return plugin.ModelBackend{}, err
			}
			if cfg.APIKey == "" {
				return plugin.ModelBackend{}, errors.New("api_key is required (use ${ANTHROPIC_API_KEY})")
			}
			p := anthropic.New(anthropic.Config{APIKey: cfg.APIKey, BaseURL: cfg.BaseURL, Effort: cfg.Effort, Timeout: cfg.Timeout.D()})
			return cfg.apply(plugin.ModelBackend{Provider: p, Endpoints: gw.AnthropicEndpoints(),
				Aliases: map[string][]string{"sre-reasoning": {"anthropic-opus-5-5", "anthropic-sonnet-5-5"},
					"sre-fast": {"anthropic-haiku-5-5"}}}), nil
		})
	plugin.Register(plugin.ModelProvider, "openai", "OpenAI-compatible chat completions (vLLM, SGLang, ...)",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (plugin.ModelBackend, error) {
			var cfg OpenAIConfig
			if err := c.Decode(&cfg); err != nil {
				return plugin.ModelBackend{}, err
			}
			if cfg.BaseURL == "" || cfg.Model == "" {
				return plugin.ModelBackend{}, errors.New("base_url and model are required")
			}
			p := openai.New(openai.Config{BaseURL: cfg.BaseURL, APIKey: cfg.APIKey, UseMaxCompletionTokens: cfg.UseMaxCompletionTokens})
			return cfg.apply(plugin.ModelBackend{Provider: p,
				Endpoints: []gw.Endpoint{gw.OpenAICompatibleEndpoint(cfg.Model, cfg.ContextWindow, cfg.MaxOutput)},
				Aliases:   map[string][]string{"sre-reasoning": {"openai-compatible"}, "sre-fast": {"openai-compatible"}}}), nil
		})
}
