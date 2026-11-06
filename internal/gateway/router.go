package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Pricing is per million tokens, in micro-dollars (1e-6 USD) per token unit.
type Pricing struct {
	Version            string  `json:"version"`
	InputPerMTok       float64 `json:"input_per_mtok"`        // USD
	OutputPerMTok      float64 `json:"output_per_mtok"`       // USD
	CachedInputPerMTok float64 `json:"cached_input_per_mtok"` // USD
	CacheWritePerMTok  float64 `json:"cache_write_per_mtok"`  // USD
}

// CostMicros returns the cost in micro-USD.
func (p Pricing) CostMicros(u Usage) int64 {
	uncached := u.InputTokens
	if uncached < 0 {
		uncached = 0
	}
	usd := float64(uncached)*p.InputPerMTok + float64(u.OutputTokens)*p.OutputPerMTok +
		float64(u.CachedInputTokens)*p.CachedInputPerMTok + float64(u.CacheWriteTokens)*p.CacheWritePerMTok
	return int64(usd) // (tokens × USD/MTok) = micro-USD
}

type Capabilities struct {
	Tools     bool `json:"tools"`
	Streaming bool `json:"streaming"`
}

// Endpoint is one concrete deployment of a model (§30.2).
type Endpoint struct {
	ID            string       `json:"id"`
	ProviderID    string       `json:"provider_id"` // registry key of the Provider adapter
	ModelID       string       `json:"model_id"`
	ModelVersion  string       `json:"model_version"`
	Region        string       `json:"region"`
	Capabilities  Capabilities `json:"capabilities"`
	ContextWindow int64        `json:"context_window"`
	MaxOutput     int64        `json:"max_output"`
	Pricing       Pricing      `json:"pricing"`
	// Tenants restricts entitlement; empty = all tenants.
	Tenants  []string `json:"tenants,omitempty"`
	Priority int      `json:"priority"` // lower wins
	Weight   float64  `json:"weight"`   // tie-break preference (higher wins)
	// ChatTemplateVersion is recorded in the prompt manifest (§33.2).
	ChatTemplateVersion string `json:"chat_template_version"`
}

// RoutePolicy maps an alias to candidate endpoints. Its content hash and
// version are recorded on every model request (§30.3).
type RoutePolicy struct {
	ID        string              `json:"id"`
	Version   int64               `json:"version"`
	Aliases   map[string][]string `json:"aliases"` // alias → endpoint IDs in preference order
	Endpoints []Endpoint          `json:"endpoints"`
	// MaxAttempts bounds pre-send fallbacks inside one model request.
	MaxAttempts int `json:"max_attempts"`
}

func (p RoutePolicy) Hash() string {
	b, _ := json.Marshal(p)
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

func (p RoutePolicy) endpoint(id string) (Endpoint, bool) {
	for _, e := range p.Endpoints {
		if e.ID == id {
			return e, true
		}
	}
	return Endpoint{}, false
}

// Candidate is an endpoint that passed the hard filters, with the reason
// recorded for audit (model_request_attempts.route_reason).
type Candidate struct {
	Endpoint Endpoint       `json:"endpoint"`
	Score    float64        `json:"score"`
	Reason   map[string]any `json:"reason"`
}

// Route applies hard filters then a soft score (§30.3). It never relaxes a
// hard filter to find a candidate: no candidate is a CAPABILITY_MISMATCH or
// REJECTED_BY_POLICY error.
//
// The hard filters (entitlement, region, capabilities, context window,
// circuit state) are part of the framework; only the soft score is
// pluggable (Scorer, the route_policy plugin kind). A nil scorer uses
// PriorityScorer.
func (p RoutePolicy) Route(req Request, sc Scope, tokenEstimate int64, br *Breakers, scorer Scorer, now time.Time) ([]Candidate, error) {
	if scorer == nil {
		scorer = PriorityScorer{}
	}
	ids, ok := p.Aliases[req.ModelAlias]
	if !ok {
		return nil, errf(ErrRejectedByPolicy, false, "unknown model alias %q", req.ModelAlias)
	}
	var out []Candidate
	rejected := map[string]string{}
	for rank, id := range ids {
		e, ok := p.endpoint(id)
		if !ok {
			rejected[id] = "endpoint not configured"
			continue
		}
		switch {
		case len(e.Tenants) > 0 && !contains(e.Tenants, sc.TenantID):
			rejected[id] = "tenant not entitled"
		case sc.Region != "" && e.Region != "" && e.Region != sc.Region:
			rejected[id] = "region mismatch"
		case req.NeedTools && !e.Capabilities.Tools:
			rejected[id] = "no tool support"
		case req.NeedStreaming && !e.Capabilities.Streaming:
			rejected[id] = "no streaming support"
		case e.ContextWindow > 0 && tokenEstimate+req.MaxOutputTokens > e.ContextWindow:
			rejected[id] = "context window too small"
		case br != nil && !br.Allow(e.ID, now):
			rejected[id] = "circuit open"
		default:
			in := ScoreInput{Request: req, Scope: sc, Endpoint: e, Rank: rank, TokenEstimate: tokenEstimate}
			if br != nil {
				in.RecentFailures = br.Failures(e.ID)
			}
			score := scorer.Score(in)
			out = append(out, Candidate{Endpoint: e, Score: score, Reason: map[string]any{
				"alias": req.ModelAlias, "rank": rank, "priority": e.Priority, "score": score,
				"scorer": scorer.Name(), "policy": fmt.Sprintf("%s@%d", p.ID, p.Version)}})
		}
	}
	if len(out) == 0 {
		code := ErrCapabilityMismatch
		for _, r := range rejected {
			if r == "circuit open" {
				code = ErrProviderUnavailable
			}
			if r == "tenant not entitled" || r == "region mismatch" {
				code = ErrRejectedByPolicy
			}
		}
		return nil, errf(code, false, "no endpoint for alias %q: %v", req.ModelAlias, rejected)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// ---------- circuit breaker ----------

// Breakers is a per-endpoint consecutive-failure circuit breaker with a
// half-open probe after the cool-down.
type Breakers struct {
	mu        sync.Mutex
	Threshold int
	Cooldown  time.Duration
	state     map[string]*breaker
}

type breaker struct {
	failures  int
	openUntil time.Time
}

func NewBreakers(threshold int, cooldown time.Duration) *Breakers {
	return &Breakers{Threshold: threshold, Cooldown: cooldown, state: map[string]*breaker{}}
}

func (b *Breakers) get(id string) *breaker {
	s, ok := b.state[id]
	if !ok {
		s = &breaker{}
		b.state[id] = s
	}
	return s
}

func (b *Breakers) Allow(id string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	return s.openUntil.IsZero() || !now.Before(s.openUntil)
}

func (b *Breakers) Failures(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.get(id).failures
}

func (b *Breakers) Record(id string, ok bool, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.get(id)
	if ok {
		s.failures, s.openUntil = 0, time.Time{}
		return
	}
	s.failures++
	if s.failures >= b.Threshold {
		s.openUntil = now.Add(b.Cooldown)
	}
}

// ---------- default configuration ----------

// Published list prices (USD per MTok). Cached reads are 0.1× input and
// 5-minute cache writes 1.25× input for Claude models.
var (
	PriceOpus55   = Pricing{Version: "anthropic-2026-11", InputPerMTok: 4, OutputPerMTok: 20, CachedInputPerMTok: 0.20, CacheWritePerMTok: 5}
	PriceSonnet55 = Pricing{Version: "anthropic-2026-11", InputPerMTok: 2, OutputPerMTok: 10, CachedInputPerMTok: 0.10, CacheWritePerMTok: 2.5}
	PriceHaiku55  = Pricing{Version: "anthropic-2026-11", InputPerMTok: 0.10, OutputPerMTok: 0.50, CachedInputPerMTok: 0.01, CacheWritePerMTok: 0.125}
)

// ScriptedEndpoint is the deterministic local model (always available).
func ScriptedEndpoint() Endpoint {
	return Endpoint{ID: "local-scripted", ProviderID: "scripted", ModelID: "rule-based-sre", ModelVersion: "1",
		Capabilities: Capabilities{Tools: true, Streaming: true}, ContextWindow: 1_000_000, MaxOutput: 32_000,
		Pricing: Pricing{Version: "free"}, Priority: 9, ChatTemplateVersion: "neutral-v1"}
}

// AnthropicEndpoints are the Claude 5.5 endpoints.
func AnthropicEndpoints() []Endpoint {
	return []Endpoint{
		{ID: "anthropic-opus-5-5", ProviderID: "anthropic", ModelID: "claude-opus-5-5", ModelVersion: "claude-opus-5-5",
			Capabilities: Capabilities{Tools: true, Streaming: true}, ContextWindow: 1_000_000, MaxOutput: 128_000,
			Pricing: PriceOpus55, Priority: 0, ChatTemplateVersion: "anthropic-messages-v1"},
		{ID: "anthropic-sonnet-5-5", ProviderID: "anthropic", ModelID: "claude-sonnet-5-5", ModelVersion: "claude-sonnet-5-5",
			Capabilities: Capabilities{Tools: true, Streaming: true}, ContextWindow: 1_000_000, MaxOutput: 64_000,
			Pricing: PriceSonnet55, Priority: 1, ChatTemplateVersion: "anthropic-messages-v1"},
		{ID: "anthropic-haiku-5-5", ProviderID: "anthropic", ModelID: "claude-haiku-5-5", ModelVersion: "claude-haiku-5-5",
			Capabilities: Capabilities{Tools: true, Streaming: true}, ContextWindow: 200_000, MaxOutput: 64_000,
			Pricing: PriceHaiku55, Priority: 0, ChatTemplateVersion: "anthropic-messages-v1"},
	}
}

// OpenAICompatibleEndpoint is a self-hosted OpenAI-compatible model.
func OpenAICompatibleEndpoint(model string, contextWindow, maxOutput int64) Endpoint {
	if contextWindow <= 0 {
		contextWindow = 128_000
	}
	if maxOutput <= 0 {
		maxOutput = 16_000
	}
	return Endpoint{ID: "openai-compatible", ProviderID: "openai", ModelID: model,
		ModelVersion: model, Capabilities: Capabilities{Tools: true, Streaming: true}, ContextWindow: contextWindow,
		MaxOutput: maxOutput, Pricing: Pricing{Version: "self-hosted"}, Priority: 2, ChatTemplateVersion: "openai-chat-v1"}
}

// PolicyPart is one provider's contribution to the route policy.
type PolicyPart struct {
	Endpoints []Endpoint
	Aliases   map[string][]string
}

// ComposePolicy merges contributions in order: endpoint IDs must be unique,
// and alias lists are concatenated (earlier parts are preferred).
func ComposePolicy(id string, parts ...PolicyPart) (RoutePolicy, error) {
	p := RoutePolicy{ID: id, Version: 1, MaxAttempts: 2, Aliases: map[string][]string{}}
	seen := map[string]bool{}
	for _, part := range parts {
		for _, e := range part.Endpoints {
			if seen[e.ID] {
				return p, fmt.Errorf("duplicate model endpoint %q", e.ID)
			}
			seen[e.ID] = true
			p.Endpoints = append(p.Endpoints, e)
		}
	}
	for _, part := range parts {
		keys := make([]string, 0, len(part.Aliases))
		for k := range part.Aliases {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, alias := range keys {
			for _, eid := range part.Aliases[alias] {
				if !seen[eid] {
					return p, fmt.Errorf("alias %q references unknown endpoint %q", alias, eid)
				}
				if !contains(p.Aliases[alias], eid) {
					p.Aliases[alias] = append(p.Aliases[alias], eid)
				}
			}
		}
	}
	return p, nil
}

// DefaultPolicy builds the route policy from what is configured: the
// scripted local model always exists so the harness runs without a key.
func DefaultPolicy(anthropicEnabled, openaiEnabled bool, openaiModel string) RoutePolicy {
	var parts []PolicyPart
	if anthropicEnabled {
		parts = append(parts, PolicyPart{Endpoints: AnthropicEndpoints(), Aliases: map[string][]string{
			"sre-reasoning": {"anthropic-opus-5-5", "anthropic-sonnet-5-5"}, "sre-fast": {"anthropic-haiku-5-5"}}})
	}
	if openaiEnabled {
		parts = append(parts, PolicyPart{Endpoints: []Endpoint{OpenAICompatibleEndpoint(openaiModel, 0, 0)},
			Aliases: map[string][]string{"sre-reasoning": {"openai-compatible"}, "sre-fast": {"openai-compatible"}}})
	}
	parts = append(parts, PolicyPart{Endpoints: []Endpoint{ScriptedEndpoint()},
		Aliases: map[string][]string{"sre-reasoning": {"local-scripted"}, "sre-fast": {"local-scripted"}, "local": {"local-scripted"}}})
	p, err := ComposePolicy("default", parts...)
	if err != nil {
		panic(err)
	}
	return p
}
