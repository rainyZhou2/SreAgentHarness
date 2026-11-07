package envctx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/netguard"
	"go-sre-harness/internal/tools"
)

// queryScope is the expression context providers render templates in.
func queryScope(q Query, service string) map[string]any {
	services := make([]any, 0, len(q.Services))
	for _, s := range q.Services {
		services = append(services, s)
	}
	labels := map[string]any{}
	for k, v := range q.Labels {
		labels[k] = v
	}
	scope := map[string]any{"service": service, "services": services, "incident": q.Incident, "labels": labels,
		"goal": q.Goal, "tenant": q.TenantID, "run_id": q.RunID}
	if !q.Start.IsZero() {
		scope["start"] = q.Start.UTC().Format(time.RFC3339)
		scope["start_unix"] = float64(q.Start.Unix())
	}
	if !q.End.IsZero() {
		scope["end"] = q.End.UTC().Format(time.RFC3339)
		scope["end_unix"] = float64(q.End.Unix())
	}
	if !q.Now.IsZero() {
		scope["now"] = q.Now.UTC().Format(time.RFC3339)
	}
	return scope
}

// targets returns the services to query: each service once, or one call
// with an empty service when the run names none.
func targets(q Query, perService bool, max int) []string {
	if !perService || len(q.Services) == 0 {
		return []string{firstOr(q.Services, "")}
	}
	if max <= 0 {
		max = 5
	}
	if len(q.Services) > max {
		return q.Services[:max]
	}
	return q.Services
}

func firstOr(s []string, def string) string {
	if len(s) > 0 {
		return s[0]
	}
	return def
}

// ---------- static ----------

// StaticFact is a configured fact; Services are globs ("*" = every run).
type StaticFact struct {
	Services []string `json:"services,omitempty"`
	Kind     string   `json:"kind"`
	Subject  string   `json:"subject,omitempty"`
	Summary  string   `json:"summary"`
	Ref      string   `json:"ref,omitempty"`
}

// Static serves operator-maintained facts (ownership, runbooks, topology
// notes) matched by service.
type Static struct {
	ID    string
	Items []StaticFact
}

func (s Static) Name() string { return orName(s.ID, "static") }

func (s Static) Facts(_ context.Context, q Query) ([]domain.ContextFact, error) {
	var out []domain.ContextFact
	for _, it := range s.Items {
		subject := it.Subject
		match := len(it.Services) == 0
		for _, pattern := range it.Services {
			for _, svc := range q.Services {
				if glob(pattern, svc) {
					match = true
					if subject == "" {
						subject = svc
					}
				}
			}
			if pattern == "*" {
				match = true
			}
		}
		if match {
			out = append(out, domain.ContextFact{Kind: it.Kind, Subject: subject, Summary: it.Summary, Ref: it.Ref, Trust: "T2"})
		}
	}
	return out, nil
}

// ---------- http ----------

// HTTPConfig queries a JSON endpoint. The response is either a list of facts
// or {"facts": [...]}; each fact has kind, subject, summary, at, ref.
type HTTPConfig struct {
	ID string `json:"id,omitempty"`
	// URL is a template (${service}, ${start}, ${end}, ${incident}, ...);
	// interpolated values are query-escaped.
	URL string `json:"url"`
	// Headers values may reference environment variables as ${env:NAME}.
	Headers    map[string]string `json:"headers,omitempty"`
	PerService bool              `json:"per_service,omitempty"`
	Kind       string            `json:"kind,omitempty"` // default kind for facts without one
	Trust      string            `json:"trust,omitempty"`
	MaxBytes   int64             `json:"max_bytes,omitempty"` // default 256 KiB
}

// HTTP is a context provider backed by an HTTP endpoint.
type HTTP struct {
	Cfg    HTTPConfig
	Client *http.Client
}

// NewHTTP validates the configuration; network decides which addresses may
// be dialed.
func NewHTTP(cfg HTTPConfig, network netguard.NetPolicy, timeout time.Duration) (*HTTP, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("http context provider: url is required")
	}
	if err := flow.ValidateInterpolation(cfg.URL, nil); err != nil {
		return nil, fmt.Errorf("http context provider: url: %w", err)
	}
	probe := strings.ReplaceAll(cfg.URL, "${", "")
	if u, err := url.Parse(probe); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("http context provider: url must be http(s)")
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &HTTP{Cfg: cfg, Client: network.Client(timeout)}, nil
}

func (h *HTTP) Name() string { return orName(h.Cfg.ID, "http") }

func (h *HTTP) Facts(ctx context.Context, q Query) ([]domain.ContextFact, error) {
	var out []domain.ContextFact
	for _, svc := range targets(q, h.Cfg.PerService, 5) {
		facts, err := h.fetch(ctx, q, svc)
		if err != nil {
			return out, err
		}
		out = append(out, facts...)
	}
	return out, nil
}

func (h *HTTP) fetch(ctx context.Context, q Query, service string) ([]domain.ContextFact, error) {
	scope := queryScope(q, service)
	escaped := map[string]any{}
	for k, v := range scope {
		if s, ok := v.(string); ok {
			escaped[k] = url.QueryEscape(s)
		} else {
			escaped[k] = v
		}
	}
	raw, err := flow.Interpolate(h.Cfg.URL, escaped)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, flow.Stringify(raw), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range h.Cfg.Headers {
		req.Header.Set(k, expandEnv(v))
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	limit := h.Cfg.MaxBytes
	if limit <= 0 {
		limit = 256 << 10
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return decodeFacts(body, h.Cfg.Kind, service, h.Cfg.Trust)
}

type wireFact struct {
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Summary string `json:"summary"`
	At      string `json:"at"`
	Ref     string `json:"ref"`
}

func decodeFacts(body []byte, kind, service, trust string) ([]domain.ContextFact, error) {
	var list []wireFact
	if err := json.Unmarshal(body, &list); err != nil {
		var wrapped struct {
			Facts []wireFact `json:"facts"`
		}
		if err2 := json.Unmarshal(body, &wrapped); err2 != nil {
			return nil, fmt.Errorf("response is not a fact list: %w", err)
		}
		list = wrapped.Facts
	}
	if trust == "" {
		trust = "T3"
	}
	out := make([]domain.ContextFact, 0, len(list))
	for _, w := range list {
		f := domain.ContextFact{Kind: orName(w.Kind, kind), Subject: orName(w.Subject, service), Summary: w.Summary,
			Ref: w.Ref, Trust: trust}
		if t, err := time.Parse(time.RFC3339Nano, w.At); err == nil {
			f.At = &t
		}
		out = append(out, f)
	}
	return out, nil
}

// expandEnv replaces ${env:NAME} with the environment variable.
func expandEnv(v string) string {
	for {
		i := strings.Index(v, "${env:")
		if i < 0 {
			return v
		}
		j := strings.Index(v[i:], "}")
		if j < 0 {
			return v
		}
		name := v[i+6 : i+j]
		v = v[:i] + os.Getenv(name) + v[i+j+1:]
	}
}

// ---------- tool ----------

// ToolConfig calls a registered read-only tool and turns its result into a
// fact (e.g. an MCP "recent deployments" tool).
type ToolConfig struct {
	ID         string         `json:"id,omitempty"`
	Tool       string         `json:"tool"`
	Args       map[string]any `json:"args,omitempty"` // templates over the query scope
	PerService bool           `json:"per_service,omitempty"`
	Kind       string         `json:"kind,omitempty"` // default "tool"
	MaxBytes   int            `json:"max_bytes,omitempty"`
}

// Tool is a context provider backed by a registry tool. Only R0 tools are
// called: a context provider must never change anything.
type Tool struct {
	Cfg      ToolConfig
	Registry *tools.Registry
}

func (t Tool) Name() string { return orName(t.Cfg.ID, "tool:"+t.Cfg.Tool) }

func (t Tool) Facts(ctx context.Context, q Query) ([]domain.ContextFact, error) {
	tool, ok := t.Registry.LookupFor(q.TenantID, t.Cfg.Tool)
	if !ok {
		return nil, fmt.Errorf("tool %s is not registered", t.Cfg.Tool)
	}
	desc := tool.Descriptor()
	if desc.Risk != domain.RiskR0 {
		return nil, fmt.Errorf("tool %s is %s; context providers only call R0 tools", desc.Name, desc.Risk)
	}
	maxBytes := t.Cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 2000
	}
	var out []domain.ContextFact
	for _, svc := range targets(q, t.Cfg.PerService, 5) {
		var args any = map[string]any{}
		if t.Cfg.Args != nil {
			v, err := flow.InterpolateValue(map[string]any(t.Cfg.Args), queryScope(q, svc))
			if err != nil {
				return out, err
			}
			args = v
		}
		raw, _ := json.Marshal(args)
		cctx := tools.WithCallInfo(ctx, tools.CallInfo{TenantID: q.TenantID, RunID: q.RunID})
		res, err := tool.Invoke(cctx, "context:"+q.RunID+":"+t.Name()+":"+svc, raw)
		if err != nil {
			return out, err
		}
		text := strings.TrimSpace(string(res))
		if len(text) > maxBytes {
			text = text[:maxBytes] + "…"
		}
		trust := desc.Trust
		if trust == "" {
			trust = "T3"
		}
		out = append(out, domain.ContextFact{Kind: orName(t.Cfg.Kind, "tool"), Subject: svc, Summary: text,
			Ref: tools.CapabilityRef(desc), Trust: trust})
	}
	return out, nil
}

// ---------- dependencies ----------

// DependencyStore reads the service dependency graph.
type DependencyStore interface {
	ServiceDependencies(ctx context.Context, tenantID, service string) ([]string, error)
}

// Dependencies reports each service's known dependencies.
type Dependencies struct {
	Store DependencyStore
}

func (Dependencies) Name() string { return "dependencies" }

func (d Dependencies) Facts(ctx context.Context, q Query) ([]domain.ContextFact, error) {
	var out []domain.ContextFact
	for _, svc := range targets(q, true, 5) {
		if svc == "" {
			continue
		}
		deps, err := d.Store.ServiceDependencies(ctx, q.TenantID, svc)
		if err != nil {
			return out, err
		}
		if len(deps) == 0 {
			continue
		}
		out = append(out, domain.ContextFact{Kind: "topology", Subject: svc,
			Summary: svc + " depends on " + strings.Join(deps, ", "), Trust: "T2"})
	}
	return out, nil
}

func orName(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// glob matches "*" (any sequence) and "?" (one rune).
func glob(pattern, s string) bool {
	p, str := []rune(pattern), []rune(s)
	pi, si, star, mark := 0, 0, -1, 0
	for si < len(str) {
		switch {
		case pi < len(p) && (p[pi] == '?' || p[pi] == str[si]):
			pi++
			si++
		case pi < len(p) && p[pi] == '*':
			star, mark = pi, si
			pi++
		case star >= 0:
			pi = star + 1
			mark++
			si = mark
		default:
			return false
		}
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

// ---------- configuration ----------

// Config builds a provider from configuration (local mode and the plugin).
type Config struct {
	Type   string       `json:"type"` // static | http | tool | dependencies
	ID     string       `json:"id,omitempty"`
	Static []StaticFact `json:"facts,omitempty"`
	HTTP   *HTTPConfig  `json:"http,omitempty"`
	Tool   *ToolConfig  `json:"tool,omitempty"`
}

// Deps are the services a configured provider may need.
type Deps struct {
	Registry     *tools.Registry
	Dependencies DependencyStore
	Network      netguard.NetPolicy
	Timeout      time.Duration
}

// Build constructs a provider from configuration.
func Build(c Config, d Deps) (Provider, error) {
	switch c.Type {
	case "static":
		if len(c.Static) == 0 {
			return nil, fmt.Errorf("static context provider: facts are required")
		}
		for i, f := range c.Static {
			if strings.TrimSpace(f.Summary) == "" {
				return nil, fmt.Errorf("static context provider: facts[%d].summary is required", i)
			}
		}
		return Static{ID: c.ID, Items: c.Static}, nil
	case "http":
		if c.HTTP == nil {
			return nil, fmt.Errorf("http context provider: http config is required")
		}
		cfg := *c.HTTP
		if cfg.ID == "" {
			cfg.ID = c.ID
		}
		return NewHTTP(cfg, d.Network, d.Timeout)
	case "tool":
		if c.Tool == nil || c.Tool.Tool == "" {
			return nil, fmt.Errorf("tool context provider: tool is required")
		}
		if d.Registry == nil {
			return nil, fmt.Errorf("tool context provider: no tool registry")
		}
		cfg := *c.Tool
		if cfg.ID == "" {
			cfg.ID = c.ID
		}
		if err := flow.ValidateInterpolations(map[string]any(cfg.Args), nil); err != nil {
			return nil, fmt.Errorf("tool context provider: args: %w", err)
		}
		return Tool{Cfg: cfg, Registry: d.Registry}, nil
	case "dependencies":
		if d.Dependencies == nil {
			return nil, fmt.Errorf("dependencies context provider: no dependency store")
		}
		return Dependencies{Store: d.Dependencies}, nil
	}
	return nil, fmt.Errorf("unknown context provider type %q (static, http, tool, dependencies)", c.Type)
}
