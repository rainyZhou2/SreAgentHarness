// Package extensions holds what a tenant adds at runtime, without a
// restart: MCP servers, HTTP tools and skills.
//
// Records live in Postgres (tenant scoped). Every process runs a Runtime
// that syncs them, so workers and API replicas converge within one sync
// interval. The framework floor still applies to everything added here:
//   - an operator (admin) sets each tool's risk class; an MCP server's own
//     metadata never grants permissions;
//   - process-wide tools win name clashes, so a tenant cannot shadow them;
//   - a run sees a tool only if its pinned profile allows it;
//   - policy and approvals gate side effects exactly as for built-in tools.
package extensions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools/mcpexec"
	"go-sre-harness/internal/tools/web"
)

// Kind of extension record.
type Kind string

const (
	KindMCP   Kind = "mcp_server"
	KindHTTP  Kind = "http_tool"
	KindSkill Kind = "skill"
	KindWeb   Kind = "web"
)

// Record is one stored extension.
type Record struct {
	TenantID  string          `json:"-"`
	Kind      Kind            `json:"kind"`
	Name      string          `json:"name"`
	Spec      json.RawMessage `json:"spec"`
	Enabled   bool            `json:"enabled"`
	Version   int64           `json:"version"`
	UpdatedBy string          `json:"updated_by"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// Store persists records. tenantID "" lists every tenant (runtime sync).
type Store interface {
	ListExtensions(ctx context.Context, tenantID string) ([]Record, error)
	UpsertExtension(ctx context.Context, r Record, expectedVersion *int64) (Record, error)
	DeleteExtension(ctx context.Context, tenantID string, kind Kind, name string) error
}

// ErrNotFound / ErrConflict are returned by stores.
var (
	ErrNotFound = errors.New("extension not found")
	ErrConflict = errors.New("extension version conflict")
)

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,47}$`)

// MCPSpec configures one MCP server. Allow maps remote tool names to the
// operator's policy; tools not listed are never exposed.
type MCPSpec struct {
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Command string            `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     []string          `json:"env,omitempty"`

	Allow          map[string]mcpexec.ToolPolicy `json:"allow"`
	CallTimeout    string                        `json:"call_timeout,omitempty"`
	MaxOutputBytes int                           `json:"max_output_bytes,omitempty"`
}

// HTTPSpec is a tool backed by an HTTP endpoint: the arguments are POSTed
// as JSON with an Idempotency-Key header; the JSON (or text) response is
// the tool output.
type HTTPSpec struct {
	Description    string            `json:"description"`
	URL            string            `json:"url"`
	Method         string            `json:"method,omitempty"` // POST (default) or GET (args as query)
	Headers        map[string]string `json:"headers,omitempty"`
	InputSchema    json.RawMessage   `json:"input_schema,omitempty"`
	Risk           domain.RiskClass  `json:"risk"`
	TargetArg      string            `json:"target_arg,omitempty"`
	Trust          string            `json:"trust,omitempty"` // default T3
	Timeout        string            `json:"timeout,omitempty"`
	MaxOutputBytes int               `json:"max_output_bytes,omitempty"`
}

// SkillSpec is a playbook rendered into the system prompt.
type SkillSpec struct {
	Description  string `json:"description"`
	Instructions string `json:"instructions"`
	// Mode: "always" (default) applies to every run; "manual" only to
	// chats that invoke the skill with /name.
	Mode string `json:"mode,omitempty"`
}

// WebSpec configures the tenant's web.search backend. web.fetch is built in
// and needs no configuration.
type WebSpec struct {
	Provider   string `json:"provider"` // brave | tavily | bing | generic
	Endpoint   string `json:"endpoint,omitempty"`
	APIKey     string `json:"api_key,omitempty"`
	MaxResults int    `json:"max_results,omitempty"`
	Timeout    string `json:"timeout,omitempty"`
}

// Limits on what can be stored.
const (
	maxSkillInstructions = 16 << 10
	maxDescription       = 1000
)

// Options are deployment-level limits on runtime extensions.
type Options struct {
	// AllowStdio permits MCP servers started as local commands. Off by
	// default: it lets an admin run programs on every harness host.
	AllowStdio bool
	// Network limits the destinations of HTTP tools, MCP servers and
	// tenant models (see NetPolicy).
	Network NetPolicy
}

// Validate checks a spec and returns it normalized.
func Validate(kind Kind, name string, spec json.RawMessage, o Options) (json.RawMessage, error) {
	if !nameRE.MatchString(name) {
		return nil, fmt.Errorf("name must match %s", nameRE)
	}
	strict := func(v any) error {
		dec := json.NewDecoder(strings.NewReader(string(spec)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(v); err != nil {
			return fmt.Errorf("invalid spec: %w", err)
		}
		return nil
	}
	switch kind {
	case KindMCP:
		var s MCPSpec
		if err := strict(&s); err != nil {
			return nil, err
		}
		if err := s.check(o); err != nil {
			return nil, err
		}
		return json.Marshal(s)
	case KindHTTP:
		var s HTTPSpec
		if err := strict(&s); err != nil {
			return nil, err
		}
		if err := s.check(o); err != nil {
			return nil, err
		}
		return json.Marshal(s)
	case KindModel:
		var s ModelSpec
		if err := strict(&s); err != nil {
			return nil, err
		}
		if err := s.check(o); err != nil {
			return nil, err
		}
		return json.Marshal(s)
	case KindSkill:
		var s SkillSpec
		if err := strict(&s); err != nil {
			return nil, err
		}
		s.Description, s.Instructions = strings.TrimSpace(s.Description), strings.TrimSpace(s.Instructions)
		switch {
		case s.Description == "" || len(s.Description) > maxDescription:
			return nil, fmt.Errorf("description is required (at most %d bytes)", maxDescription)
		case s.Instructions == "" || len(s.Instructions) > maxSkillInstructions:
			return nil, fmt.Errorf("instructions are required (at most %d bytes)", maxSkillInstructions)
		case s.Mode != "" && s.Mode != "always" && s.Mode != "manual":
			return nil, errors.New(`mode must be "always" or "manual"`)
		}
		if s.Mode == "always" {
			s.Mode = ""
		}
		return json.Marshal(s)
	case KindWeb:
		var s WebSpec
		if err := strict(&s); err != nil {
			return nil, err
		}
		if err := web.ValidateSearch(web.SearchConfig{Provider: s.Provider, Endpoint: s.Endpoint,
			APIKey: s.APIKey, MaxResults: s.MaxResults}); err != nil {
			return nil, err
		}
		if s.Endpoint != "" {
			if err := checkURL(s.Endpoint, o.Network); err != nil {
				return nil, err
			}
		}
		return json.Marshal(s)
	}
	return nil, fmt.Errorf("unknown kind %q", kind)
}

func validRisk(r domain.RiskClass) bool {
	switch r {
	case domain.RiskR0, domain.RiskR1, domain.RiskR2, domain.RiskR3:
		return true
	}
	return false
}

func checkURL(raw string, n NetPolicy) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("url must be an absolute http(s) URL")
	}
	if u.User != nil {
		return fmt.Errorf("url must not embed credentials; use headers")
	}
	return n.CheckHost(u)
}

func checkDuration(v string) error {
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 || d > 10*time.Minute {
		return fmt.Errorf("invalid duration %q (0 < d <= 10m)", v)
	}
	return nil
}

func (s *MCPSpec) check(o Options) error {
	switch {
	case s.URL != "" && s.Command != "":
		return errors.New("set either url (streamable HTTP) or command (stdio), not both")
	case s.URL != "":
		if err := checkURL(s.URL, o.Network); err != nil {
			return err
		}
	case s.Command != "":
		if !o.AllowStdio {
			return errors.New("stdio MCP servers are disabled on this deployment (HARNESS_MCP_ALLOW_STDIO=true enables them)")
		}
	default:
		return errors.New("url or command is required")
	}
	for name, p := range s.Allow {
		if !validRisk(p.Risk) {
			return fmt.Errorf("allow[%s]: risk must be R0..R3", name)
		}
		if p.Trust != "" && p.Trust != "T2" && p.Trust != "T3" && p.Trust != "T4" {
			return fmt.Errorf("allow[%s]: trust must be T2, T3 or T4", name)
		}
	}
	if s.MaxOutputBytes < 0 || s.MaxOutputBytes > 1<<20 {
		return errors.New("max_output_bytes must be 0..1048576")
	}
	return checkDuration(s.CallTimeout)
}

func (s *HTTPSpec) check(o Options) error {
	s.Description = strings.TrimSpace(s.Description)
	if s.Description == "" || len(s.Description) > maxDescription {
		return fmt.Errorf("description is required (at most %d bytes)", maxDescription)
	}
	if err := checkURL(s.URL, o.Network); err != nil {
		return err
	}
	s.Method = strings.ToUpper(s.Method)
	if s.Method == "" {
		s.Method = "POST"
	}
	if s.Method != "POST" && s.Method != "GET" {
		return errors.New("method must be POST or GET")
	}
	if !validRisk(s.Risk) {
		return errors.New("risk must be R0..R3")
	}
	if s.Method == "GET" && s.Risk.HasSideEffects() {
		return errors.New("GET tools must be R0 (side effects need POST and an idempotency key)")
	}
	if len(s.InputSchema) > 0 {
		var v map[string]any
		if json.Unmarshal(s.InputSchema, &v) != nil || v["type"] != "object" {
			return errors.New(`input_schema must be a JSON Schema object with "type": "object"`)
		}
	}
	if s.Trust == "" {
		s.Trust = "T3"
	}
	if s.Trust != "T2" && s.Trust != "T3" && s.Trust != "T4" {
		return errors.New("trust must be T2, T3 or T4")
	}
	if s.MaxOutputBytes < 0 || s.MaxOutputBytes > 1<<20 {
		return errors.New("max_output_bytes must be 0..1048576")
	}
	return checkDuration(s.Timeout)
}

// ToolPrefix is the name prefix of a kind's tools.
func ToolPrefix(kind Kind, name string) string {
	if kind == KindMCP {
		return "mcp." + name + "."
	}
	return "http." + name
}

// MaskSecrets blanks header and env values for display; a later update
// that sends a blank value keeps the stored one (MergeSecrets).
func MaskSecrets(kind Kind, spec json.RawMessage) json.RawMessage {
	var m map[string]any
	if json.Unmarshal(spec, &m) != nil {
		return spec
	}
	if h, ok := m["headers"].(map[string]any); ok {
		for k := range h {
			h[k] = ""
		}
	}
	if _, ok := m["api_key"]; ok {
		m["api_key"] = ""
	}
	if env, ok := m["env"].([]any); ok {
		for i, e := range env {
			if s, ok := e.(string); ok {
				k, _, _ := strings.Cut(s, "=")
				env[i] = k + "="
			}
		}
	}
	out, _ := json.Marshal(m)
	return out
}

// destinationKeys are the spec fields that decide where stored secrets are
// sent. Changing any of them requires re-entering the secrets.
var destinationKeys = []string{"url", "base_url", "provider", "endpoint", "command", "args"}

// ErrSecretsNeedReentry is returned by MergeSecrets when a blank secret
// would be filled in for a spec whose destination changed.
var ErrSecretsNeedReentry = errors.New("the destination (url, base_url, provider, endpoint, command or args) changed: re-enter the secret values instead of leaving them blank")

// MergeSecrets fills blank header/env/api_key values in next from prev. It
// refuses when the destination changed, so an update that does not know a
// stored secret can never send it somewhere new.
func MergeSecrets(prev, next json.RawMessage) (json.RawMessage, error) {
	var p, n map[string]any
	if json.Unmarshal(prev, &p) != nil || json.Unmarshal(next, &n) != nil {
		return next, nil
	}
	filled := false
	if k, _ := n["api_key"].(string); k == "" {
		if pk, _ := p["api_key"].(string); pk != "" {
			n["api_key"], filled = pk, true
		}
	}
	if nh, ok := n["headers"].(map[string]any); ok {
		ph, _ := p["headers"].(map[string]any)
		for k, v := range nh {
			if pv, _ := ph[k].(string); v == "" && pv != "" {
				nh[k], filled = pv, true
			}
		}
	}
	if ne, ok := n["env"].([]any); ok {
		prevEnv := map[string]string{}
		if pe, ok := p["env"].([]any); ok {
			for _, e := range pe {
				if s, ok := e.(string); ok {
					k, v, _ := strings.Cut(s, "=")
					prevEnv[k] = v
				}
			}
		}
		for i, e := range ne {
			if s, ok := e.(string); ok {
				if k, v, _ := strings.Cut(s, "="); v == "" && prevEnv[k] != "" {
					ne[i], filled = k+"="+prevEnv[k], true
				}
			}
		}
	}
	if filled {
		for _, k := range destinationKeys {
			if !sameValue(p[k], n[k]) {
				return nil, ErrSecretsNeedReentry
			}
		}
	}
	out, _ := json.Marshal(n)
	return out, nil
}

// sameValue compares two decoded JSON values; absent and "" are the same.
func sameValue(a, b any) bool {
	if a == nil {
		a = ""
	}
	if b == nil {
		b = ""
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}
