// Package profiles registers the built-in profile_source plugins.
package profiles

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/profile"
)

// DemoSpec is the profile the harness has always seeded for local runs.
const DemoSpec = `{"strategy":"llm","tools":["demo.metrics","demo.restart"],"max_steps":12}`

// ExtensionsSpec (sre-diagnosis@2, the default) also allows the tenant's
// runtime tools: MCP servers (mcp.*) and HTTP tools (http.*) added through
// /v1/extensions. Policy and approvals still apply to every call.
const ExtensionsSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*"],"max_steps":12}`

// WebSpec (sre-diagnosis@3) also allows the built-in web tools.
// web.fetch is always available; web.search needs a tenant backend.
const WebSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*","web.*"],"max_steps":12}`

// ThoughtSpec (sre-diagnosis@4) adds the explicit decision trace contract
// (plan/reflection/verification) on top of WebSpec.
const ThoughtSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*","web.*"],"max_steps":12,"prompt_prefix_version":"sre-prefix-v5"}`

// MCPToolSpec (sre-diagnosis@5) keeps the trace contract and gives
// tool-heavy MCP investigations enough steps to finish.
const MCPToolSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*","web.*"],"max_steps":20,"prompt_prefix_version":"sre-prefix-v5"}`

// MCPDeepSpec (sre-diagnosis@6) covers topology investigations that need to
// combine several MCP sources before concluding.
const MCPDeepSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*","web.*"],"max_steps":30,"prompt_prefix_version":"sre-prefix-v5"}`

// CapabilityMapSpec (sre-diagnosis@7, the default) adds the bounded [E]
// capability map to the prompt.
const CapabilityMapSpec = `{"strategy":"llm","tools":["demo.*","mcp.*","http.*","web.*"],"max_steps":30,"prompt_prefix_version":"sre-prefix-v6","capability_map":true}`

// FlowSpec (sre-diagnosis@8) demonstrates the orchestration DSL: a triage
// agent, parallel metrics/log branches, a join, a human approval gate and a
// remediation tool. The flow can only use tools the profile allows.
const FlowSpec = `{
  "strategy":"flow",
  "tools":["demo.*","mcp.*","http.*","web.*"],
  "max_steps":40,
  "prompt_prefix_version":"sre-prefix-v6",
  "capability_map":true,
  "flow":{
    "version":1,
    "entry":"triage",
    "inputs":[{"name":"goal","required":true}],
    "limits":{"max_steps":20,"max_children":4,"max_total_runs":8,"max_depth":2},
    "nodes":[
      {"id":"triage","type":"agent","role":"triage",
       "prompt":"Classify the incident, identify the affected service and state what evidence is needed.",
       "tools":["demo.metrics"],
       "next":[{"to":"diagnose"}]},
      {"id":"diagnose","type":"parallel","branches":["metrics","logs"],"next":[{"to":"join"}]},
      {"id":"metrics","type":"tool","tool":"demo.metrics","args":{"service":"${input.goal}"}},
      {"id":"logs","type":"agent","role":"log-analyst",
       "prompt":"Inspect the available evidence and summarize the most likely cause.",
       "tools":["demo.metrics"]},
      {"id":"join","type":"join","wait":["diagnose"],"join_policy":"all","on_error":"fail",
       "next":[{"to":"approve"}]},
      {"id":"approve","type":"approval","risk":"R2","approvers":1,"next":[{"to":"remediate"}]},
      {"id":"remediate","type":"tool","tool":"demo.restart","args":{"service":"${input.goal}"},
       "next":[{"to":"finish"}]},
      {"id":"finish","type":"end","output":"${nodes.logs.output}"}
    ]
  }
}`

// AutoSpec (sre-diagnosis@10) is the run profile for automatic planning. The
// planner may choose any registered tool; policy, risk and approvals still
// gate every side effect. Operators can override this profile in deployment.
const AutoSpec = `{"strategy":"llm","tools":["*"],"max_steps":40,"prompt_prefix_version":"sre-prefix-v6","capability_map":true}`

// InteractiveFlowSpec (sre-diagnosis@9) exercises a human input gate and a
// nested subflow. The answer is available to the subflow as input.vars.answer.
const InteractiveFlowSpec = `{
  "strategy":"flow",
  "tools":["demo.*"],
  "max_steps":40,
  "prompt_prefix_version":"sre-prefix-v6",
  "flow":{
    "version":1,
    "entry":"ask",
    "inputs":[{"name":"goal","required":true}],
    "limits":{"max_steps":20,"max_children":2,"max_total_runs":4,"max_depth":2},
    "nodes":[
      {"id":"ask","type":"input","output":"answer",
       "input_schema":{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]},
       "next":[{"to":"sub"}]},
      {"id":"sub","type":"subflow","flow":{
        "version":1,
        "entry":"work",
        "limits":{"max_steps":5,"max_children":1,"max_total_runs":2,"max_depth":1},
        "nodes":[
          {"id":"work","type":"agent","role":"worker",
           "prompt":"Use the answer provided by the parent flow and summarize it.",
           "tools":["demo.metrics"],
           "next":[{"to":"done"}]},
          {"id":"done","type":"end","output":"${nodes.work.output}"}
        ]
      },"next":[{"to":"finish"}]},
      {"id":"finish","type":"end","output":"${nodes.sub.output}"}
    ]
  }
}`

// BuiltinConfig configures the builtin source.
type BuiltinConfig struct {
	ID      string          `json:"id,omitempty"`      // default sre-diagnosis
	Version int64           `json:"version,omitempty"` // default 1
	Spec    json.RawMessage `json:"spec,omitempty"`    // default DemoSpec
	Tenants []string        `json:"tenants,omitempty"` // default settings.default_tenant
}

// FileConfig configures the file source.
type FileConfig struct {
	// Path is a profile file or a directory of *.yaml, *.yml and *.json files.
	// Each file holds one definition or a list of them:
	//   {id, version, tenant?, spec: {...}}
	Path string `json:"path"`
	// Tenants receive definitions that name no tenant (default settings.default_tenant).
	Tenants []string `json:"tenants,omitempty"`
}

func init() {
	plugin.Register(plugin.ProfileSource, "builtin", "the demo sre-diagnosis@1 and @2 (extensions) profiles (or one profile given inline)",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (profile.Source, error) {
			cfg := BuiltinConfig{ID: "sre-diagnosis", Version: 1, Spec: json.RawMessage(DemoSpec)}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			defs := forTenants(profile.Definition{ID: cfg.ID, Version: cfg.Version, Spec: cfg.Spec, Origin: "builtin", Seed: true},
				cfg.Tenants, h.Settings.DefaultTenant)
			if cfg.ID == "sre-diagnosis" && cfg.Version == 1 && string(cfg.Spec) == DemoSpec {
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 2, Spec: json.RawMessage(ExtensionsSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 3, Spec: json.RawMessage(WebSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 4, Spec: json.RawMessage(ThoughtSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 5, Spec: json.RawMessage(MCPToolSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 6, Spec: json.RawMessage(MCPDeepSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 7, Spec: json.RawMessage(CapabilityMapSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 8, Spec: json.RawMessage(FlowSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 9, Spec: json.RawMessage(InteractiveFlowSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
				defs = append(defs, forTenants(profile.Definition{ID: cfg.ID, Version: 10, Spec: json.RawMessage(AutoSpec),
					Origin: "builtin", Seed: true}, cfg.Tenants, h.Settings.DefaultTenant)...)
			}
			return profile.SourceFunc(func(context.Context) ([]profile.Definition, error) { return defs, nil }), nil
		})
	plugin.Register(plugin.ProfileSource, "file", "profile definitions from a YAML/JSON file or directory",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (profile.Source, error) {
			var cfg FileConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if cfg.Path == "" {
				return nil, errors.New("path is required")
			}
			return profile.SourceFunc(func(context.Context) ([]profile.Definition, error) {
				return LoadPath(cfg.Path, cfg.Tenants, h.Settings.DefaultTenant)
			}), nil
		})
}

// LoadPath reads definitions from a file or directory.
func LoadPath(path string, tenants []string, defaultTenant string) ([]profile.Definition, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	files := []string{path}
	if st.IsDir() {
		files = nil
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".yaml", ".yml", ".json":
				if !e.IsDir() {
					files = append(files, filepath.Join(path, e.Name()))
				}
			}
		}
		sort.Strings(files)
	}
	var out []profile.Definition
	for _, f := range files {
		defs, err := loadFile(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		for _, d := range defs {
			out = append(out, forTenants(d, tenants, defaultTenant)...)
		}
	}
	return out, nil
}

func loadFile(path string) ([]profile.Definition, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var v any
	if err := yaml.Unmarshal(raw, &v); err != nil { // YAML is a superset of JSON
		return nil, err
	}
	js, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var list []json.RawMessage
	if t := strings.TrimSpace(string(js)); strings.HasPrefix(t, "[") {
		if err := json.Unmarshal(js, &list); err != nil {
			return nil, err
		}
	} else {
		list = []json.RawMessage{js}
	}
	var out []profile.Definition
	for i, item := range list {
		var d profile.Definition
		if err := strictJSON(item, &d); err != nil {
			return nil, fmt.Errorf("definition %d: %w", i, err)
		}
		d.Origin = path
		out = append(out, d)
	}
	return out, nil
}

func strictJSON(b []byte, v any) error {
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func forTenants(d profile.Definition, tenants []string, def string) []profile.Definition {
	if d.Tenant != "" {
		return []profile.Definition{d}
	}
	if len(tenants) == 0 {
		tenants = []string{def}
	}
	var out []profile.Definition
	for _, t := range tenants {
		c := d
		c.Tenant = t
		out = append(out, c)
	}
	return out
}
