package extensions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/gemini"
	"go-sre-harness/internal/gateway/openai"
)

// KindModel is a model provider a tenant configured (API key and model).
// Its endpoint is entitled to that tenant only and is preferred over the
// deployment's models for the reasoning aliases.
const KindModel Kind = "model"

// ModelSpec configures one tenant model.
type ModelSpec struct {
	// Provider is the wire protocol: anthropic | openai (OpenAI-compatible
	// chat completions, which also covers OpenAI itself, DeepSeek, Qwen,
	// ERNIE, GLM, Kimi, Doubao and self-hosted servers) | gemini.
	Provider string `json:"provider"`
	// Preset names the vendor template the client used (display only).
	Preset  string `json:"preset,omitempty"`
	Model   string `json:"model"`
	APIKey  string `json:"api_key,omitempty"`
	BaseURL string `json:"base_url,omitempty"`
	// Effort (anthropic): low | medium | high | xhigh | max; empty = API default.
	Effort        string `json:"effort,omitempty"`
	ContextWindow int64  `json:"context_window,omitempty"`
	MaxOutput     int64  `json:"max_output,omitempty"`
	// MaxCompletionTokens (openai) sends max_completion_tokens instead of
	// max_tokens, as OpenAI's reasoning models require.
	MaxCompletionTokens bool `json:"max_completion_tokens,omitempty"`
	// Default makes this the tenant's preferred model; the others are fallbacks.
	Default bool `json:"default,omitempty"`
}

var presetRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

// ModelAliases are the aliases a tenant model serves.
var ModelAliases = []string{"sre-reasoning", "sre-fast"}

// AnthropicModels are the model IDs offered in the client.
var AnthropicModels = []string{"claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"}

func (s *ModelSpec) check(o Options) error {
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
	case "gemini":
		if s.Model == "" {
			s.Model = "gemini-flash-latest"
		}
		if s.APIKey == "" {
			return errors.New("api_key is required for gemini")
		}
		if s.Effort != "" {
			return errors.New("effort applies to anthropic only")
		}
	default:
		return errors.New(`provider must be "anthropic", "openai" or "gemini"`)
	}
	if s.MaxCompletionTokens && s.Provider != "openai" {
		return errors.New("max_completion_tokens applies to openai only")
	}
	if s.Preset != "" && !presetRE.MatchString(s.Preset) {
		return errors.New("preset must be a short lowercase slug")
	}
	if s.BaseURL != "" {
		if err := checkURL(s.BaseURL, o.Network); err != nil {
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
// client carries the tenant network policy.
func buildModel(tenant, name string, s ModelSpec, client *http.Client) (gw.Provider, gw.Endpoint) {
	id := modelEndpointID(tenant, name)
	var ep gw.Endpoint
	var p gw.Provider
	switch s.Provider {
	case "anthropic":
		p = anthropic.New(anthropic.Config{APIKey: s.APIKey, BaseURL: s.BaseURL, Effort: s.Effort, Timeout: 10 * time.Minute,
			HTTPClient: client})
		ep = gw.Endpoint{ModelID: s.Model, ModelVersion: s.Model, Capabilities: gw.Capabilities{Tools: true, Streaming: true},
			ContextWindow: 200_000, MaxOutput: 64_000, Pricing: gw.Pricing{Version: "unknown"}, ChatTemplateVersion: "anthropic-messages-v1"}
		for _, t := range gw.AnthropicEndpoints() {
			if t.ModelID == s.Model {
				ep = t
			}
		}
	case "gemini":
		p = gemini.New(gemini.Config{APIKey: s.APIKey, BaseURL: s.BaseURL, HTTPClient: client})
		ep = gw.GeminiEndpoint(s.Model, s.ContextWindow, s.MaxOutput)
	default:
		p = openai.New(openai.Config{BaseURL: s.BaseURL, APIKey: s.APIKey, UseMaxCompletionTokens: s.MaxCompletionTokens, HTTPClient: client})
		ep = gw.OpenAICompatibleEndpoint(s.Model, s.ContextWindow, s.MaxOutput)
	}
	if s.ContextWindow > 0 {
		ep.ContextWindow = s.ContextWindow
	}
	if s.MaxOutput > 0 {
		ep.MaxOutput = s.MaxOutput
	}
	ep.ID, ep.ProviderID, ep.Tenants, ep.Priority = id, id, []string{tenant}, -10
	if s.Default {
		ep.Priority = -20
	}
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
		// The default model first, then by name: alias order is the fallback order.
		sort.Slice(keys, func(i, j int) bool {
			di, dj := ts.entries[key{KindModel, keys[i]}].model.ep.Priority, ts.entries[key{KindModel, keys[j]}].model.ep.Priority
			if di != dj {
				return di < dj
			}
			return keys[i] < keys[j]
		})
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
func (r *Runtime) TestModel(ctx context.Context, tenant, name string, s ModelSpec) (string, string, error) {
	p, ep := buildModel(tenant, name, s, r.Options.Network.Client(time.Minute))
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

// ListModels asks the provider which models the key can use (GET /models),
// so an admin can pick a current model ID instead of typing one. It goes
// through the same network policy as model calls.
func (r *Runtime) ListModels(ctx context.Context, s ModelSpec) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	base := strings.TrimRight(s.BaseURL, "/")
	var u string
	hdr := http.Header{}
	switch s.Provider {
	case "anthropic":
		if base == "" {
			base = "https://api.anthropic.com"
		}
		u = base + "/v1/models?limit=1000"
		hdr.Set("x-api-key", s.APIKey)
		hdr.Set("anthropic-version", "2023-06-01")
	case "gemini":
		if base == "" {
			base = gemini.DefaultBaseURL
		}
		u = base + "/models?pageSize=1000"
		hdr.Set("x-goog-api-key", s.APIKey)
	default:
		u = base + "/models"
		if s.APIKey != "" {
			hdr.Set("Authorization", "Bearer "+s.APIKey)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header = hdr
	resp, err := r.Options.Network.Client(0).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(body))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		Models []struct {
			Name    string   `json:"name"`
			Methods []string `json:"supportedGenerationMethods"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("unexpected response: %w", err)
	}
	var ids []string
	for _, d := range out.Data {
		ids = append(ids, d.ID)
	}
	for _, m := range out.Models {
		for _, meth := range m.Methods {
			if meth == "generateContent" || meth == "streamGenerateContent" {
				ids = append(ids, strings.TrimPrefix(m.Name, "models/"))
				break
			}
		}
	}
	sort.Strings(ids)
	if len(ids) > 500 {
		ids = ids[:500]
	}
	return ids, nil
}
