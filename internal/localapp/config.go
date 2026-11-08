package localapp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/internal/workengine"
)

// LocalFile is the optional desktop configuration
// (HARNESS_LOCAL_CONFIG, default <data dir>/harness.local.json). Every
// section is optional; an absent file keeps the built-in defaults.
type LocalFile struct {
	// Policy replaces the default risk-class policy with rules. The
	// framework floor (R2 approval, R3 deny) always applies.
	Policy *policy.RuleConfig `json:"policy,omitempty"`
	// Triggers turn local webhooks and schedules into runs. Local triggers
	// always run in the local tenant.
	Triggers []trigger.Config `json:"triggers,omitempty"`
	// ContextProviders contribute environment facts pinned per run.
	ContextProviders []envctx.Config `json:"context_providers,omitempty"`
	// ToolOverlays attach labels, lifecycle, time arguments and offload
	// size to tools by name.
	ToolOverlays map[string]tools.Overlay `json:"tool_overlays,omitempty"`
	// OutputOffloadBytes overrides the size above which tool results are
	// offloaded (0 = default, negative = never offload).
	OutputOffloadBytes int `json:"output_offload_bytes,omitempty"`
	// Planner patterns override the router defaults; ["none"] disables a
	// list.
	PlannerReactPatterns []string `json:"planner_react_patterns,omitempty"`
	PlannerFlowPatterns  []string `json:"planner_flow_patterns,omitempty"`
	// Work configures the v2 control plane (HARNESS_WORK_ENGINE_ENABLED):
	// templates and correlation rules.
	Work workengine.Config `json:"work,omitempty"`
	// WorkExtensions enables compiled-in work_extension plugins (action
	// executors, metric sources, simulators).
	WorkExtensions plugin.Specs `json:"work_extensions,omitempty"`
}

// loadLocalFile reads the configuration. A missing default file is not an
// error; a missing explicit file is.
func loadLocalFile(path string, explicit bool) (LocalFile, error) {
	var f LocalFile
	if path == "" {
		return f, nil
	}
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) && !explicit {
			return f, nil
		}
		return f, fmt.Errorf("local config %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return f, fmt.Errorf("local config %s: %w", path, err)
	}
	for i := range f.Triggers {
		// The desktop runtime has one tenant; a trigger cannot name another.
		if f.Triggers[i].Tenant != "" && f.Triggers[i].Tenant != tenant {
			return f, fmt.Errorf("local config: trigger %q: tenant must be %q or empty", f.Triggers[i].Name, tenant)
		}
		if f.Triggers[i].Schedule != nil {
			f.Triggers[i].Tenant = tenant
		}
	}
	names := make([]string, 0, len(f.ToolOverlays))
	for n := range f.ToolOverlays {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := f.ToolOverlays[n].Validate(n); err != nil {
			return f, fmt.Errorf("local config: %w", err)
		}
	}
	return f, nil
}
