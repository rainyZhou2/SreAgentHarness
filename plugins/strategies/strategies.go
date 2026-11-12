// Package strategies registers the built-in reasoning strategies. A profile
// version selects one by name (spec.strategy).
package strategies

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/profile"
)

// LLMConfig sets defaults for profiles that leave fields empty.
type LLMConfig struct {
	ModelAlias          string `json:"model_alias,omitempty"`
	Effort              string `json:"effort,omitempty"`
	PromptPrefixVersion string `json:"prompt_prefix_version,omitempty"`
	MaxOutputTokens     int64  `json:"max_output_tokens,omitempty"`
	MaxContextTokens    int64  `json:"max_context_tokens,omitempty"`
}

func init() {
	plugin.Register(plugin.Strategy, "rule_based", "deterministic rules; no model calls",
		func(context.Context, *plugin.Host, plugin.Config) (plugin.StrategyBuilder, error) {
			return plugin.StrategyBuilderFunc(func(profile.Spec, plugin.StrategyRuntime) (cognitive.Strategy, error) {
				return cognitive.RuleBased{}, nil
			}), nil
		})
	plugin.Register(plugin.Strategy, "llm", "model-driven ReAct loop through the model gateway",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (plugin.StrategyBuilder, error) {
			var cfg LLMConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return plugin.StrategyBuilderFunc(func(spec profile.Spec, rt plugin.StrategyRuntime) (cognitive.Strategy, error) {
				alias := first(spec.ModelAlias, cfg.ModelAlias, rt.Settings.ModelAlias, "sre-reasoning")
				if len(spec.AllowedAliases) > 0 && !contains(spec.AllowedAliases, alias) {
					if spec.ModelAlias != "" {
						return nil, fmt.Errorf("model_alias %q is not in the profile's allowed_aliases %v", alias, spec.AllowedAliases)
					}
					alias = spec.AllowedAliases[0] // the deployment default is not allowed: use the profile's first choice
				}
				ca := rt.Context
				if ca == nil {
					ca = SectionedAssembler{Base: cognitive.DefaultAssembler()}
				}
				asm, err := ca.NewBuilder(cognitive.AssemblerOptions{
					Effort:              first(spec.Effort, cfg.Effort, rt.Settings.ModelEffort),
					PromptPrefixVersion: first(spec.PromptPrefixVersion, cfg.PromptPrefixVersion),
					MaxOutputTokens:     firstInt(spec.MaxOutputTokens, cfg.MaxOutputTokens),
					MaxContextTokens:    firstInt(spec.MaxContextTokens, cfg.MaxContextTokens),
					Playbook:            spec.Playbook(),
				})
				if err != nil {
					return nil, err
				}
				return &cognitive.LLM{Gateway: rt.Gateway, Artifacts: rt.Artifacts, Assembler: asm, Alias: alias, Deltas: rt.Deltas}, nil
			}), nil
		})
}

// SectionedAssembler is the built-in context assembler: four prefix-cache
// friendly sections (§33.1) with optional custom instructions.
type SectionedAssembler struct{ Base cognitive.Assembler }

func (s SectionedAssembler) NewBuilder(o cognitive.AssemblerOptions) (cognitive.ContextBuilder, error) {
	return s.Base.Apply(o), nil
}

// SectionedConfig configures the sectioned assembler.
type SectionedConfig struct {
	// Instructions replace the default role text of section [A]; the
	// framework's trust rules are always appended. InstructionsFile reads
	// them from a file instead.
	Instructions        string `json:"instructions,omitempty"`
	InstructionsFile    string `json:"instructions_file,omitempty"`
	PromptPrefixVersion string `json:"prompt_prefix_version,omitempty"`
	MaxObservationBytes int    `json:"max_observation_bytes,omitempty"`
	MaxContextTokens    int64  `json:"max_context_tokens,omitempty"`
	MaxOutputTokens     int64  `json:"max_output_tokens,omitempty"`
}

// LLMCompactorConfig configures the model-backed compactor.
type LLMCompactorConfig struct {
	CompactorConfig
	// Alias is the model alias used for summaries (default sre-reasoning).
	Alias           string `json:"alias,omitempty"`
	MaxSummaryBytes int    `json:"max_summary_bytes,omitempty"`
}

// CompactorConfig configures the deterministic compactor.
type CompactorConfig struct {
	Keep     int `json:"keep,omitempty"`      // newest observations kept verbatim (default 2)
	MaxBytes int `json:"max_bytes,omitempty"` // body bytes per folded observation (default 400)
}

func init() {
	plugin.Register(plugin.ContextAssembler, "sectioned", "system/tools/history/dynamic sections ordered for prefix caching; optional custom instructions",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (cognitive.ContextAssembler, error) {
			var cfg SectionedConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.Instructions != "" && cfg.InstructionsFile != "" {
				return nil, errors.New("set instructions or instructions_file, not both")
			}
			base := cognitive.DefaultAssembler()
			base.Instructions = cfg.Instructions
			if cfg.InstructionsFile != "" {
				b, err := os.ReadFile(cfg.InstructionsFile)
				if err != nil {
					return nil, err
				}
				base.Instructions = string(b)
			}
			if cfg.MaxObservationBytes > 0 {
				base.MaxObservationBytes = cfg.MaxObservationBytes
			}
			base = base.Apply(cognitive.AssemblerOptions{PromptPrefixVersion: cfg.PromptPrefixVersion,
				MaxContextTokens: cfg.MaxContextTokens, MaxOutputTokens: cfg.MaxOutputTokens})
			return SectionedAssembler{Base: base}, nil
		})
	plugin.Register(plugin.Compactor, "deterministic", "one line per folded observation with truncated bodies; reproducible on retry",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (kernel.Compactor, error) {
			var cfg CompactorConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return kernel.DeterministicCompactor{Keep: cfg.Keep, MaxBytes: cfg.MaxBytes}, nil
		})
	plugin.Register(plugin.Compactor, "llm", "model-written summaries of folded observations; citations kept, deterministic fallback on any failure",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (kernel.Compactor, error) {
			var cfg LLMCompactorConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Gateway == nil {
				return nil, errors.New("llm compactor needs a model gateway (a model_provider plugin)")
			}
			if cfg.Alias == "" {
				cfg.Alias = "sre-reasoning"
			}
			return &kernel.LLMCompactor{Gateway: h.Gateway, Alias: cfg.Alias, Keep: cfg.Keep, MaxBytes: cfg.MaxBytes,
				MaxSummaryBytes: cfg.MaxSummaryBytes, Log: h.Log}, nil
		})
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func first(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstInt(vs ...int64) int64 {
	for _, v := range vs {
		if v > 0 {
			return v
		}
	}
	return 0
}
