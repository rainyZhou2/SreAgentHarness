package plugin

import (
	"context"
	"errors"
	"fmt"
	"go-sre-harness/internal/extensions"
	"io"
	"log/slog"
	"sort"
	"sync"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventbus"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/netguard"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
)

// Host carries the shared services plugins may use. Fields are filled in
// bootstrap order (see the Kind docs): a plugin can rely on everything built
// in an earlier phase, e.g. a tool plugin sees Artifacts but not Gateway.
type Host struct {
	Mode     string // all | api | worker | scheduler
	HTTPAddr string
	Log      *slog.Logger
	Store    *pg.Store
	Metrics  *obs.Metrics

	Artifacts artifact.Store
	Tools     *tools.Registry
	Deltas    stream.Bus
	Events    eventbus.Bus
	Network   netguard.NetPolicy
	Policy    policy.Gate
	Gateway   *gw.Gateway
	Embedder  memory.Embedder
	Eval      *eval.Service // set before the job and api phases
	Server    *api.Server   // set before the api phase
	// Extensions holds the tenants' runtime MCP servers, HTTP tools and
	// skills (set after the tools phase).
	Extensions *extensions.Runtime

	// Settings holds the application-level settings (model defaults etc.).
	Settings Settings

	shared *hostShared
	kind   string
	inst   string
}

type hostShared struct {
	mu       sync.Mutex
	active   []Active
	closers  []func(context.Context) error
	runners  []namedRunner
	services map[string]any
}

type namedRunner struct {
	id string
	r  Runner
}

// Active is a built plugin instance.
type Active struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Instance string `json:"instance"`
}

// Runner is a background lifecycle hook: any plugin instance implementing it
// (every job, and e.g. a tool source that refreshes periodically) is started
// after bootstrap and stopped by context cancellation. A Runner returning an
// error stops the process.
type Runner interface {
	Run(ctx context.Context) error
}

// RunnerFunc adapts a function to Runner.
type RunnerFunc func(ctx context.Context) error

func (f RunnerFunc) Run(ctx context.Context) error { return f(ctx) }

// NewHost creates the root host.
func NewHost(mode string, log *slog.Logger) *Host {
	return &Host{Mode: mode, Log: log, shared: &hostShared{services: map[string]any{}}}
}

func (h *Host) scoped(kind, inst string) *Host {
	c := *h
	c.kind, c.inst = kind, inst
	if h.Log != nil {
		c.Log = h.Log.With("plugin", kind+"/"+inst)
	}
	return &c
}

// OnClose registers a shutdown hook (run in reverse registration order).
func (h *Host) OnClose(fn func(context.Context) error) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	h.shared.closers = append(h.shared.closers, fn)
}

// Go starts a background task with the application lifecycle.
func (h *Host) Go(r Runner) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	h.shared.runners = append(h.shared.runners, namedRunner{id: h.kind + "/" + h.inst, r: r})
}

// Provide publishes a value other plugins can Lookup (cross-plugin services).
func (h *Host) Provide(key string, v any) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	h.shared.services[key] = v
}

// Lookup returns a value published with Provide.
func (h *Host) Lookup(key string) (any, bool) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	v, ok := h.shared.services[key]
	return v, ok
}

func (h *Host) track(kind, name, inst string, v any) {
	h.shared.mu.Lock()
	h.shared.active = append(h.shared.active, Active{Kind: kind, Name: name, Instance: inst})
	if r, ok := v.(Runner); ok {
		h.shared.runners = append(h.shared.runners, namedRunner{id: kind + "/" + inst, r: r})
	}
	h.shared.mu.Unlock()
	switch c := v.(type) {
	case interface{ Shutdown(context.Context) error }:
		h.OnClose(c.Shutdown)
	case io.Closer:
		h.OnClose(func(context.Context) error { return c.Close() })
	case interface{ Close() }:
		h.OnClose(func(context.Context) error { c.Close(); return nil })
	}
}

// Active lists the instances built so far.
func (h *Host) Active() []Active {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	out := append([]Active(nil), h.shared.active...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// Runners returns the background tasks registered with Go.
func (h *Host) Runners() map[string]Runner {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	out := map[string]Runner{}
	for i, r := range h.shared.runners {
		id := r.id
		if _, dup := out[id]; dup {
			id = fmt.Sprintf("%s#%d", id, i)
		}
		out[id] = r.r
	}
	return out
}

// Close runs every shutdown hook in reverse order.
func (h *Host) Close(ctx context.Context) error {
	h.shared.mu.Lock()
	cs := h.shared.closers
	h.shared.closers = nil
	h.shared.mu.Unlock()
	var errs []error
	for i := len(cs) - 1; i >= 0; i-- {
		if err := cs[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Settings are application-level knobs shared by several plugins.
type Settings struct {
	// ModelAlias / ModelEffort are the defaults for strategies whose profile
	// does not set them.
	ModelAlias  string `json:"model_alias,omitempty"`
	ModelEffort string `json:"model_effort,omitempty"`
	// DefaultTenant receives profiles whose definition names no tenant.
	DefaultTenant string `json:"default_tenant,omitempty"`
	// DefaultProfile / DefaultProfileVersion are used by runs that name no profile.
	DefaultProfile        string `json:"default_profile,omitempty"`
	DefaultProfileVersion int64  `json:"default_profile_version,omitempty"`
}
