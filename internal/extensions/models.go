package extensions

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/openai"
)

// KindModel is a model provider a tenant configured (API key and model).
// Its endpoint is entitled to that tenant only and is preferred over the
// deployment's models for the reasoning aliases.
const KindModel Kind = "model"

// ModelSpec configures one tenant model.
type ModelSpec struct {
	Provider string `json:"provider"` // anthropic | openai (OpenAI-compatible)
	Model    string `json:"model"`
	APIKey   string `json:"api_key,omitempty"`
	BaseURL  string `json:"base_url,omitempty"`
	// Effort (anthropic): low | medium | high | xhigh | max; empty = API default.
	Effort        string `json:"effort,omitempty"`
	ContextWindow int64  `json:"context_window,omitempty"`
	MaxOutput     int64  `json:"max_output,omitempty"`
}

// ModelAliases are the aliases a tenant model serves.
var ModelAliases = []string{"sre-reasoning", "sre-fast"}

// AnthropicModels are the model IDs offered in the client.
var AnthropicModels = []string{"claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"}

func (s *ModelSpec) check() error {
	s.Provider, s.Model = strings.TrimSpace(s.Provider), strings.TrimSpace(s.Model)
	switch s.Provider {
	case "anthropic":
		if s.Model == "" {
			s.Model = AnthropicModels[0]
		}
		if s.APIKey == "" {
			return errors.New("api_key is required for anthropic")
		}
		switch s.Effort {
		case "", "low", "medium", "high", "xhigh", "max":
		default:
			return fmt.Errorf("effort must be low, medium, high, xhigh or max")
		}
	case "openai":
		if s.Model == "" || s.BaseURL == "" {
			return errors.New("model and base_url are required for an OpenAI-compatible provider")
		}
		if s.Effort != "" {
			return errors.New("effort applies to anthropic only")
		}
	default:
		return errors.New(`provider must be "anthropic" or "openai"`)
	}
	if s.BaseURL != "" {
		if err := checkURL(s.BaseURL); err != nil {
			return fmt.Errorf("base_url: %w", err)
		}
	}
	if s.ContextWindow < 0 || s.MaxOutput < 0 || s.ContextWindow > 10_000_000 || s.MaxOutput > 1_000_000 {
		return errors.New("context_window / max_output out of range")
	}
	return nil
}

// namedProvider gives an adapter a unique registry ID.
type namedProvider struct {
	id string
	gw.Provider
}

func (n namedProvider) ID() string { return n.id }

func modelEndpointID(tenant, name string) string { return "tenant:" + tenant + ":" + name }

// buildModel turns a spec into a provider and an endpoint entitled to tenant.
func buildModel(tenant, name string, s ModelSpec) (gw.Provider, gw.Endpoint) {
	id := modelEndpointID(tenant, name)
	var ep gw.Endpoint
	var p gw.Provider
	switch s.Provider {
	case "anthropic":
		p = anthropic.New(anthropic.Config{APIKey: s.APIKey, BaseURL: s.BaseURL, Effort: s.Effort, Timeout: 10 * time.Minute})
		ep = gw.Endpoint{ModelID: s.Model, ModelVersion: s.Model, Capabilities: gw.Capabilities{Tools: true, Streaming: true},
			ContextWindow: 200_000, MaxOutput: 64_000, Pricing: gw.Pricing{Version: "unknown"}, ChatTemplateVersion: "anthropic-messages-v1"}
		for _, t := range gw.AnthropicEndpoints() {
			if t.ModelID == s.Model {
				ep = t
			}
		}
	default:
		p = openai.New(openai.Config{BaseURL: s.BaseURL, APIKey: s.APIKey})
		ep = gw.OpenAICompatibleEndpoint(s.Model, s.ContextWindow, s.MaxOutput)
	}
	if s.ContextWindow > 0 {
		ep.ContextWindow = s.ContextWindow
	}
	if s.MaxOutput > 0 {
		ep.MaxOutput = s.MaxOutput
	}
	ep.ID, ep.ProviderID, ep.Tenants, ep.Priority = id, id, []string{tenant}, -10
	return namedProvider{id: id, Provider: p}, ep
}

// ModelOverlay receives the tenants' models (the model gateway).
type ModelOverlay interface {
	SetOverlay(gw.Overlay)
}

// overlay collects every tenant's enabled models; callers hold r.mu.
func (r *Runtime) overlayLocked() gw.Overlay {
	var o gw.Overlay
	o.Aliases = map[string][]string{}
	tenants := make([]string, 0, len(r.tenants))
	for t := range r.tenants {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	for _, t := range tenants {
		ts := r.tenants[t]
		keys := make([]string, 0)
		for k, e := range ts.entries {
			if k.kind == KindModel && e.rec.Enabled && e.model != nil {
				keys = append(keys, k.name)
			}
		}
		sort.Strings(keys)
		for _, n := range keys {
			m := ts.entries[key{KindModel, n}].model
			o.Endpoints = append(o.Endpoints, m.ep)
			o.Providers = append(o.Providers, m.prov)
			for _, a := range ModelAliases {
				o.Aliases[a] = append(o.Aliases[a], m.ep.ID)
			}
		}
	}
	return o
}

type modelEntry struct {
	prov gw.Provider
	ep   gw.Endpoint
}

// TestModel sends a one-line request through a model spec (not stored)
// and returns the reply, so an admin can check the key and model.
func TestModel(ctx context.Context, tenant, name string, s ModelSpec) (string, string, error) {
	p, ep := buildModel(tenant, name, s)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req := gw.Request{ModelAlias: "test", MaxOutputTokens: 32, NeedStreaming: true,
		System:   []gw.SystemBlock{{Text: "You are a connectivity check. Reply with the single word: pong"}},
		Messages: []gw.Message{{Role: gw.RoleUser, Parts: []gw.Part{{Type: gw.PartText, Text: "ping"}}}}}
	var b strings.Builder
	err := p.Stream(ctx, ep, req, func(ev gw.Event) {
		if ev.Type == gw.EvTextDelta {
			b.WriteString(ev.Text)
		}
	})
	return strings.TrimSpace(b.String()), ep.ModelID, err
}
