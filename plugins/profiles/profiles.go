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
