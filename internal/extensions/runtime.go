package extensions

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"go-sre-harness/internal/cognitive"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/mcpexec"
	"go-sre-harness/internal/tools/web"
)

// Runtime turns stored records into live tools and skills. It implements
// tools.TenantSource and the kernel's skill source.
type Runtime struct {
	Store    Store
	Options  Options
	Log      *slog.Logger
	Interval time.Duration // sync period (default 5s)
	// Dialer overrides the MCP transport (tests); nil uses the spec's url/command.
	Dialer func(name string, s MCPSpec) mcpexec.Dialer
	// HTTPClient overrides the HTTP tools' client (tests); nil enforces
	// Options.Network.
	HTTPClient *http.Client
	// Models receives the tenants' configured models (the model gateway);
	// nil ignores model records.
	Models ModelOverlay

	syncMu  sync.Mutex
	mu      sync.RWMutex
	tenants map[string]*tenantState
	wake    chan struct{}
	once    sync.Once
}

type key struct {
	kind Kind
	name string
}

type entry struct {
	rec      Record
	srv      *mcpexec.Server
	tools    []tools.Tool
	skill    *cognitive.Skill
	model    *modelEntry
	err      string
	missing  []string
	syncedAt time.Time
}

type tenantState struct {
	entries map[key]*entry
	tools   []tools.Tool
	skills  []cognitive.Skill
}

// Status describes one extension as this process sees it.
type Status struct {
	Version  int64     `json:"version"`
	State    string    `json:"state"` // ready | error | disabled
	Error    string    `json:"error,omitempty"`
	Tools    []string  `json:"tools,omitempty"`
	Missing  []string  `json:"missing,omitempty"` // allowlisted but not offered by the server
	SyncedAt time.Time `json:"synced_at"`
}

const mcpRetry = 30 * time.Second

func (r *Runtime) init() {
	r.once.Do(func() {
		r.wake = make(chan struct{}, 1)
		if r.Log == nil {
			r.Log = slog.Default()
		}
		if r.Interval == 0 {
			r.Interval = 5 * time.Second
		}
	})
}

// Notify asks Run to sync now (after a local write).
func (r *Runtime) Notify() {
	r.init()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run syncs until ctx ends.
func (r *Runtime) Run(ctx context.Context) error {
	r.init()
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	defer r.Close()
	for {
		if err := r.Sync(ctx); err != nil && ctx.Err() == nil {
			r.Log.Warn("extensions sync failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-r.wake:
		}
	}
}

// Close disconnects every MCP server.
func (r *Runtime) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ts := range r.tenants {
		for _, e := range ts.entries {
			if e.srv != nil {
				_ = e.srv.Close()
			}
		}
	}
	r.tenants = nil
}

// Sync loads every record and rebuilds what changed.
func (r *Runtime) Sync(ctx context.Context) error {
	r.init()
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	recs, err := r.Store.ListExtensions(ctx, "")
	if err != nil {
		return err
	}
	byTenant := map[string]map[key]Record{}
	for _, rec := range recs {
		if byTenant[rec.TenantID] == nil {
			byTenant[rec.TenantID] = map[key]Record{}
		}
		byTenant[rec.TenantID][key{rec.Kind, rec.Name}] = rec
	}

	r.mu.RLock()
	old := r.tenants
	r.mu.RUnlock()
	next := map[string]*tenantState{}
	var closeLater []*mcpexec.Server
	for tenant, want := range byTenant {
		ts := &tenantState{entries: map[key]*entry{}}
		prev := old[tenant]
		for k, rec := range want {
			var e *entry
			if prev != nil {
				e = prev.entries[k]
			}
			reuse := e != nil && e.rec.Version == rec.Version && e.rec.Enabled == rec.Enabled &&
				!(k.kind == KindMCP && rec.Enabled && e.err != "" && time.Since(e.syncedAt) > mcpRetry)
			if !reuse {
				if e != nil && e.srv != nil {
					closeLater = append(closeLater, e.srv)
				}
				e = r.build(ctx, rec)
			}
			ts.entries[k] = e
		}
		ts.index()
		next[tenant] = ts
	}
	// Records that disappeared: disconnect their servers.
	for tenant, ts := range old {
		for k, e := range ts.entries {
			if _, ok := byTenant[tenant][k]; !ok && e.srv != nil {
				closeLater = append(closeLater, e.srv)
			}
		}
	}
	r.mu.Lock()
	r.tenants = next
	var overlay gw.Overlay
	if r.Models != nil {
		overlay = r.overlayLocked()
	}
	r.mu.Unlock()
	if r.Models != nil {
		r.Models.SetOverlay(overlay)
	}
	for _, s := range closeLater {
		_ = s.Close()
	}
	return nil
}

func (ts *tenantState) index() {
	ts.tools, ts.skills = nil, nil
	keys := make([]key, 0, len(ts.entries))
	for k := range ts.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].name < keys[j].name
	})
	for _, k := range keys {
		e := ts.entries[k]
		if !e.rec.Enabled {
			continue
		}
		ts.tools = append(ts.tools, e.tools...)
		if e.skill != nil {
			ts.skills = append(ts.skills, *e.skill)
		}
	}
}

func (r *Runtime) build(ctx context.Context, rec Record) *entry {
	e := &entry{rec: rec, syncedAt: time.Now()}
	if !rec.Enabled {
		return e
	}
	switch rec.Kind {
	case KindSkill:
		var s SkillSpec
		if err := json.Unmarshal(rec.Spec, &s); err != nil {
			e.err = err.Error()
			return e
		}
		e.skill = &cognitive.Skill{Name: rec.Name, Description: s.Description, Instructions: s.Instructions, Manual: s.Mode == "manual"}
	case KindModel:
		var s ModelSpec
		if err := json.Unmarshal(rec.Spec, &s); err != nil {
			e.err = err.Error()
			return e
		}
		p, ep := buildModel(rec.TenantID, rec.Name, s, r.Options.Network.Client(10*time.Minute))
		e.model = &modelEntry{prov: p, ep: ep}
	case KindHTTP:
		var s HTTPSpec
		if err := json.Unmarshal(rec.Spec, &s); err != nil {
			e.err = err.Error()
			return e
		}
		client := r.HTTPClient
		if client == nil {
			client = r.Options.Network.Client(0)
		}
		e.tools = []tools.Tool{newHTTPTool(rec.Name, s, client)}
	case KindMCP:
		var s MCPSpec
		if err := json.Unmarshal(rec.Spec, &s); err != nil {
			e.err = err.Error()
			return e
		}
		srv, err := r.server(rec.Name, s)
		if err != nil {
			e.err = err.Error()
			return e
		}
		dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		ts, _, missing, err := srv.Discover(dctx)
		cancel()
		e.srv, e.missing = srv, missing
		if err != nil {
			e.err = "discovery failed: " + err.Error()
			r.Log.Warn("mcp discovery failed", "tenant", rec.TenantID, "server", rec.Name, "err", err)
			return e
		}
		e.tools = ts
	case KindWeb:
		var s WebSpec
		if err := json.Unmarshal(rec.Spec, &s); err != nil {
			e.err = err.Error()
			return e
		}
		cfg := web.SearchConfig{Provider: s.Provider, Endpoint: s.Endpoint, APIKey: s.APIKey,
			MaxResults: s.MaxResults, Policy: r.Options.Network, Client: r.HTTPClient}
		if s.Timeout != "" {
			d, err := time.ParseDuration(s.Timeout)
			if err != nil {
				e.err = "invalid timeout: " + err.Error()
				return e
			}
			cfg.Timeout = d
		}
		t, err := web.NewSearchTool(rec.Name, cfg)
		if err != nil {
			e.err = err.Error()
			return e
		}
		e.tools = []tools.Tool{t}
	}
	return e
}

func (r *Runtime) server(name string, s MCPSpec) (*mcpexec.Server, error) {
	if err := s.check(r.Options); err != nil {
		return nil, err
	}
	cfg := mcpexec.ServerConfig{Name: name, URL: s.URL, Headers: s.Headers, Command: s.Command, Args: s.Args, Env: s.Env,
		Allow: s.Allow, MaxOutputBytes: s.MaxOutputBytes, Transport: r.Options.Network.Transport()}
	if d, err := time.ParseDuration(s.CallTimeout); err == nil {
		cfg.CallTimeout = mcpexec.Duration(d)
	}
	var dial mcpexec.Dialer
	if r.Dialer != nil {
		dial = r.Dialer(name, s)
	}
	return mcpexec.New(cfg, dial, r.Log)
}

// Probe connects to an MCP server described by spec (not stored) and lists
// every tool it offers, so an admin can choose what to allow.
func (r *Runtime) Probe(ctx context.Context, name string, s MCPSpec) ([]mcpexec.RemoteTool, error) {
	r.init()
	srv, err := r.server(name, s)
	if err != nil {
		return nil, err
	}
	defer srv.Close()
	pctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return srv.ListAll(pctx)
}

// TenantTools implements tools.TenantSource.
func (r *Runtime) TenantTools(tenantID string) []tools.Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ts := r.tenants[tenantID]; ts != nil {
		return ts.tools
	}
	return nil
}

// Skills returns the tenant's enabled skills, sorted by name.
func (r *Runtime) Skills(tenantID string) []cognitive.Skill {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ts := r.tenants[tenantID]; ts != nil {
		out := append([]cognitive.Skill(nil), ts.skills...)
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}
	return nil
}

// StatusOf reports how this process sees a record.
func (r *Runtime) StatusOf(tenantID string, kind Kind, name string) (Status, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ts := r.tenants[tenantID]
	if ts == nil {
		return Status{}, false
	}
	e := ts.entries[key{kind, name}]
	if e == nil {
		return Status{}, false
	}
	st := Status{Version: e.rec.Version, State: "ready", Error: e.err, Missing: e.missing, SyncedAt: e.syncedAt}
	switch {
	case !e.rec.Enabled:
		st.State = "disabled"
	case e.err != "":
		st.State = "error"
	}
	for _, t := range e.tools {
		st.Tools = append(st.Tools, t.Descriptor().Name)
	}
	sort.Strings(st.Tools)
	return st, true
}

var _ tools.TenantSource = (*Runtime)(nil)
