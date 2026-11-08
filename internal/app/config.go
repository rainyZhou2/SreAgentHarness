package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/toolconfig"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/internal/workengine"
)

// File is the harness configuration file (HARNESS_CONFIG, YAML or JSON).
// Core settings (database, listener, lease timing) stay in environment
// variables; everything pluggable is selected here. ${VAR} references are
// expanded from the environment, so secrets need not live in the file.
type File struct {
	Settings plugin.Settings `json:"settings"`
	Models   ModelSettings   `json:"models"`
	Strategy StrategyConfig  `json:"strategy"`
	// Plugins maps a kind (see GET /v1/admin/plugins) to its plugin specs.
	// A kind present here replaces the environment-derived default for that
	// kind; kinds left out keep their defaults.
	Plugins map[string]plugin.Specs `json:"plugins"`
	// Triggers turn inbound webhooks and schedules into runs.
	Triggers []trigger.Config `json:"triggers,omitempty"`
	// ToolOverlays attach operator metadata (labels, lifecycle, time
	// arguments, offload size) to tools by name, whatever their source.
	ToolOverlays map[string]tools.Overlay `json:"tool_overlays,omitempty"`
	// Work configures the v2 control plane (templates, correlation rules);
	// it is used only with HARNESS_WORK_ENGINE_ENABLED.
	Work workengine.Config `json:"work,omitempty"`
}

type ModelSettings struct {
	DailyBudgetUSD       float64 `json:"daily_budget_usd,omitempty"`
	MaxInflightPerTenant int     `json:"max_inflight_per_tenant,omitempty"`
	// Circuit breaker per endpoint: open after this many consecutive
	// failures (default 3) for CircuitCooldown (default 30s).
	CircuitFailureThreshold int             `json:"circuit_failure_threshold,omitempty"`
	CircuitCooldown         plugin.Duration `json:"circuit_cooldown,omitempty"`
}

type StrategyConfig struct {
	// Default strategy for profiles that do not name one.
	Default string `json:"default,omitempty"`
	// Aliases maps legacy or alternative names to a strategy plugin instance.
	Aliases map[string]string `json:"aliases,omitempty"`
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// expand replaces ${VAR} and ${VAR:-default}; a bare $ is left alone.
func expand(b []byte) []byte {
	return envRef.ReplaceAllFunc(b, func(m []byte) []byte {
		sub := envRef.FindSubmatch(m)
		if v, ok := os.LookupEnv(string(sub[1])); ok && v != "" {
			return []byte(v)
		}
		return sub[3]
	})
}

// LoadFile reads a config file, merging it over the environment defaults.
func LoadFile(path string, base File) (File, error) {
	if path == "" {
		return base, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return base, err
	}
	js, err := toJSON(path, raw)
	if err != nil {
		return base, fmt.Errorf("%s: %w", path, err)
	}
	var f File
	dec := json.NewDecoder(bytes.NewReader(js))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return base, fmt.Errorf("%s: %w", path, err)
	}
	return merge(base, f)
}

// toJSON converts YAML to JSON (after ${VAR} expansion inside string values).
func toJSON(path string, raw []byte) ([]byte, error) {
	ext := strings.ToLower(filepath.Ext(path))
	trimmed := bytes.TrimSpace(raw)
	if ext == ".json" || (len(trimmed) > 0 && trimmed[0] == '{') {
		return expandJSONStrings(raw)
	}
	var v any
	if err := yaml.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	v, err := normalize(v)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return expandJSONStrings(b)
}

// expandJSONStrings expands env references inside JSON string values only,
// re-encoding each so values containing quotes cannot break the document.
func expandJSONStrings(b []byte) ([]byte, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(x any) any {
		switch t := x.(type) {
		case string:
			return string(expand([]byte(t)))
		case map[string]any:
			for k, e := range t {
				t[k] = walk(e)
			}
		case []any:
			for i, e := range t {
				t[i] = walk(e)
			}
		}
		return x
	}
	return json.Marshal(walk(v))
}

func normalize(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		for k, e := range t {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			t[k] = n
		}
		return t, nil
	case map[any]any:
		m := map[string]any{}
		for k, e := range t {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			m[fmt.Sprint(k)] = n
		}
		return m, nil
	case []any:
		for i, e := range t {
			n, err := normalize(e)
			if err != nil {
				return nil, err
			}
			t[i] = n
		}
		return t, nil
	}
	return v, nil
}

func merge(base, f File) (File, error) {
	known := map[string]bool{}
	for _, k := range plugin.Kinds() {
		known[k.Name] = true
	}
	out := base
	if out.Plugins == nil {
		out.Plugins = map[string]plugin.Specs{}
	} else {
		cp := map[string]plugin.Specs{}
		for k, v := range out.Plugins {
			cp[k] = v
		}
		out.Plugins = cp
	}
	var unknown []string
	for k, v := range f.Plugins {
		if !known[k] {
			unknown = append(unknown, k)
			continue
		}
		out.Plugins[k] = v
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return out, fmt.Errorf("unknown plugin kinds %v", unknown)
	}
	if f.Settings.ModelAlias != "" {
		out.Settings.ModelAlias = f.Settings.ModelAlias
	}
	if f.Settings.ModelEffort != "" {
		out.Settings.ModelEffort = f.Settings.ModelEffort
	}
	if f.Settings.DefaultTenant != "" {
		out.Settings.DefaultTenant = f.Settings.DefaultTenant
	}
	if f.Settings.DefaultProfile != "" {
		out.Settings.DefaultProfile = f.Settings.DefaultProfile
	}
	if f.Settings.DefaultProfileVersion != 0 {
		out.Settings.DefaultProfileVersion = f.Settings.DefaultProfileVersion
	}
	if f.Models.CircuitFailureThreshold != 0 {
		out.Models.CircuitFailureThreshold = f.Models.CircuitFailureThreshold
	}
	if f.Models.CircuitCooldown != 0 {
		out.Models.CircuitCooldown = f.Models.CircuitCooldown
	}
	if f.Models.DailyBudgetUSD != 0 {
		out.Models.DailyBudgetUSD = f.Models.DailyBudgetUSD
	}
	if f.Models.MaxInflightPerTenant != 0 {
		out.Models.MaxInflightPerTenant = f.Models.MaxInflightPerTenant
	}
	if f.Strategy.Default != "" {
		out.Strategy.Default = f.Strategy.Default
	}
	if f.Strategy.Aliases != nil {
		out.Strategy.Aliases = f.Strategy.Aliases
	}
	if f.Triggers != nil {
		out.Triggers = f.Triggers
	}
	if f.ToolOverlays != nil {
		out.ToolOverlays = f.ToolOverlays
	}
	return out, nil
}

// Env reads configuration from environment variables.
type Env func(string) string

func (e Env) str(k, def string) string {
	if v := e(k); v != "" {
		return v
	}
	return def
}

func (e Env) dur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(e(k)); err == nil {
		return d
	}
	return def
}

func (e Env) int(k string, def int) int {
	if n, err := strconv.Atoi(e(k)); err == nil {
		return n
	}
	return def
}

func (e Env) int64(k string, def int64) int64 {
	if n, err := strconv.ParseInt(e(k), 10, 64); err == nil {
		return n
	}
	return def
}

func (e Env) float(k string, def float64) float64 {
	if f, err := strconv.ParseFloat(e(k), 64); err == nil {
		return f
	}
	return def
}

func (e Env) bool(k string, def bool) bool {
	if b, err := strconv.ParseBool(e(k)); err == nil {
		return b
	}
	return def
}

func one(name string, cfg any) plugin.Specs { return plugin.Specs{plugin.NewSpec(name, cfg)} }

// FromEnv derives the plugin configuration from the legacy environment
// variables, so existing deployments keep working without a config file.
func FromEnv(e Env) (File, error) {
	f := File{Plugins: map[string]plugin.Specs{}}
	f.Settings = plugin.Settings{ModelAlias: e.str("HARNESS_MODEL_ALIAS", "sre-reasoning"), ModelEffort: e("HARNESS_MODEL_EFFORT"),
		DefaultTenant: e.str("HARNESS_DEFAULT_TENANT", "tenant-a"), DefaultProfile: "sre-diagnosis", DefaultProfileVersion: 7}
	budget, _ := strconv.ParseFloat(e.str("HARNESS_MODEL_DAILY_BUDGET_USD", "0"), 64)
	f.Models = ModelSettings{DailyBudgetUSD: budget, MaxInflightPerTenant: e.int("HARNESS_MODEL_MAX_INFLIGHT_PER_TENANT", 32)}
	f.Strategy = StrategyConfig{Default: "llm", Aliases: map[string]string{"rule_based_demo": "llm"}}
	if e.str("HARNESS_STRATEGY", "llm") == "rule" {
		f.Strategy = StrategyConfig{Default: "rule_based", Aliases: map[string]string{"llm": "rule_based", "rule_based_demo": "rule_based"}}
	}
	p := f.Plugins
	p["telemetry"] = one("otlp", nil)
	p["artifact_store"] = one("fs", map[string]any{"root": e.str("HARNESS_ARTIFACT_DIR", "./data/artifacts")})
	p["policy"] = one("default", nil)
	if url := e("HARNESS_NATS_URL"); url != "" {
		p["broker"] = one("nats", map[string]any{"url": url})
		p["stream_bus"] = one("nats", map[string]any{"url": url})
		p["event_bus"] = one("nats", map[string]any{"url": url,
			"stream":    e.str("HARNESS_EVENT_STREAM", "HARNESS_EVENTS"),
			"max_age":   e.dur("HARNESS_EVENT_STREAM_MAX_AGE", time.Hour).String(),
			"max_bytes": e.int64("HARNESS_EVENT_STREAM_MAX_BYTES", 2<<30)})
	} else {
		p["broker"] = one("local", nil)
		p["stream_bus"] = one("local", nil)
		p["event_bus"] = one("local", nil)
	}

	var models plugin.Specs
	if key := e("ANTHROPIC_API_KEY"); key != "" {
		models = append(models, plugin.NewSpec("anthropic", map[string]any{"api_key": key,
			"base_url": e("HARNESS_ANTHROPIC_BASE_URL"), "effort": e("HARNESS_ANTHROPIC_EFFORT")}))
	}
	if key := e("GEMINI_API_KEY"); key != "" {
		models = append(models, plugin.NewSpec("gemini", map[string]any{"api_key": key,
			"base_url": e("HARNESS_GEMINI_BASE_URL"), "model": e("HARNESS_GEMINI_MODEL")}))
	}
	if url, model := e("HARNESS_OPENAI_BASE_URL"), e("HARNESS_OPENAI_MODEL"); url != "" && model != "" {
		models = append(models, plugin.NewSpec("openai", map[string]any{"base_url": url, "model": model,
			"api_key": e("HARNESS_OPENAI_API_KEY")}))
	}
	p["model_provider"] = append(models, plugin.NewSpec("scripted", nil))

	p["profile_source"] = one("builtin", nil)
	p["route_policy"] = one("priority", nil)
	p["context_assembler"] = one("sectioned", nil)
	p["compactor"] = one("deterministic", nil)
	if url := e("HARNESS_EVENT_WEBHOOK_URL"); url != "" {
		cfg := map[string]any{"url": url, "format": e.str("HARNESS_EVENT_WEBHOOK_FORMAT", "json"),
			"secret": e("HARNESS_EVENT_WEBHOOK_SECRET")}
		if t := e("HARNESS_EVENT_TYPES"); t != "" {
			var types []string
			for _, x := range strings.Split(t, ",") {
				if x = strings.TrimSpace(x); x != "" {
					types = append(types, x)
				}
			}
			cfg["types"] = types
		}
		p["event_sink"] = one("webhook", cfg)
	}

	p["embedder"] = one("hashing", nil)
	p["memory_retriever"] = one("hybrid", map[string]any{"top_k": e.int("HARNESS_MEMORY_TOP_K", 5)})
	p["strategy"] = plugin.Specs{{Name: "llm"}, {Name: "rule_based"}}
	p["grader"] = one("substring", nil)
	p["job"] = plugin.Specs{
		plugin.NewSpec("consolidator", map[string]any{"interval": e.dur("HARNESS_CONSOLIDATE_INTERVAL", 10*time.Second).String()}),
		plugin.NewSpec("rollout_monitor", map[string]any{"interval": e.dur("HARNESS_ROLLOUT_MONITOR_INTERVAL", time.Minute).String()}),
		plugin.NewSpec("event_relay", map[string]any{
			"interval":   e.dur("HARNESS_RELAY_INTERVAL", 250*time.Millisecond).String(),
			"batch":      e.int("HARNESS_RELAY_BATCH", 1000),
			"lease_ttl":  e.dur("HARNESS_RELAY_LEASE_TTL", 15*time.Second).String(),
			"start_from": "latest"}),
		plugin.NewSpec("retention", map[string]any{
			"interval":         e.dur("HARNESS_RETENTION_INTERVAL", 5*time.Minute).String(),
			"outbox_retention": e.dur("HARNESS_OUTBOX_RETENTION", 24*time.Hour).String(),
			"audit_retention":  e.dur("HARNESS_AUDIT_RETENTION", 0).String()}),
	}
	p["api"] = plugin.Specs{{Name: "memory"}, {Name: "eval"}, {Name: "extensions"}}

	var authn plugin.Specs
	for _, m := range strings.Split(e.str("HARNESS_AUTH_MODE", "dev"), ",") {
		switch strings.TrimSpace(m) {
		case "token":
			authn = append(authn, plugin.NewSpec("token", map[string]any{"tokens_file": e("HARNESS_AUTH_TOKENS_FILE")}))
		case "oidc":
			authn = append(authn, plugin.NewSpec("oidc", map[string]any{"issuer": e("HARNESS_OIDC_ISSUER"),
				"audience": e("HARNESS_OIDC_AUDIENCE"), "jwks_url": e("HARNESS_OIDC_JWKS_URL"),
				"tenant_claim": e("HARNESS_OIDC_TENANT_CLAIM"), "roles_claim": e("HARNESS_OIDC_ROLES_CLAIM")}))
		case "dev":
			authn = append(authn, plugin.NewSpec("dev", map[string]any{"allow_non_loopback": e.bool("HARNESS_ALLOW_DEV_AUTH", false)}))
		case "":
		default:
			return f, fmt.Errorf("unknown HARNESS_AUTH_MODE %q", m)
		}
	}
	p["authenticator"] = authn

	tools, err := toolSpecs(e("HARNESS_TOOLS_CONFIG"))
	if err != nil {
		return f, err
	}
	p["tools"] = tools
	return f, nil
}

// toolSpecs converts the legacy HARNESS_TOOLS_CONFIG file into tool plugin specs.
func toolSpecs(path string) (plugin.Specs, error) {
	c, err := toolconfig.Load(path)
	if err != nil {
		return nil, err
	}
	var out plugin.Specs
	if c.DemoTools == nil || *c.DemoTools {
		out = append(out, plugin.Spec{Name: "demo"})
	}
	out = append(out, plugin.Spec{Name: "web"})
	if c.Prometheus != nil {
		out = append(out, plugin.NewSpec("prometheus", c.Prometheus))
	}
	if c.Kubernetes != nil {
		out = append(out, plugin.NewSpec("kubernetes", c.Kubernetes))
	}
	for _, s := range c.MCPServers {
		sp := plugin.NewSpec("mcp", s)
		sp.Instance = "mcp:" + s.Name
		out = append(out, sp)
	}
	for _, r := range c.Remote {
		sp := plugin.NewSpec("remote", r)
		sp.Instance = "remote:" + r.Name
		out = append(out, sp)
	}
	return out, nil
}
