// Package plugin is the harness extension framework: everything outside the
// kernel (state machine, fencing/leases, transactional commit, outbox) is a
// plugin of some Kind. Plugins register a named Factory at init time; the
// application config selects which instances are built and with what config.
//
//	func init() {
//		plugin.Register(plugin.Tools, "hello", "greets", func(ctx context.Context, h *plugin.Host, c plugin.Config) (plugin.ToolSource, error) { ... })
//	}
//
// Compile a plugin in with a blank import; enable it in the config file.
package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Kind is a typed extension point. T is the interface (or value type) every
// plugin of this kind produces.
type Kind[T any] struct {
	name string
}

func (k Kind[T]) Name() string { return k.name }

// KindInfo describes an extension point.
type KindInfo struct {
	Name  string `json:"name"`
	Multi bool   `json:"multi"` // several instances may be active at once
	Doc   string `json:"doc"`
}

// NewKind declares an extension point. Kinds are declared once, at package
// level; plugins of external modules may declare their own kinds too.
func NewKind[T any](name string, multi bool, doc string) Kind[T] {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := kinds[name]; dup {
		panic("plugin: duplicate kind " + name)
	}
	kinds[name] = KindInfo{Name: name, Multi: multi, Doc: doc}
	return Kind[T]{name: name}
}

// Factory builds one plugin instance from its config.
type Factory[T any] func(ctx context.Context, h *Host, cfg Config) (T, error)

// Info describes a registered plugin.
type Info struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	Doc  string `json:"doc"`
}

type entry struct {
	info  Info
	build func(ctx context.Context, h *Host, cfg Config) (any, error)
}

var (
	mu    sync.RWMutex
	kinds = map[string]KindInfo{}
	reg   = map[string]map[string]entry{}
)

// Register adds a plugin factory. It panics on a duplicate (kind, name):
// registration happens in init functions, so this fails fast at startup.
func Register[T any](k Kind[T], name, doc string, f Factory[T]) {
	if name == "" || f == nil {
		panic("plugin: empty name or nil factory for kind " + k.name)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := kinds[k.name]; !ok {
		panic("plugin: unknown kind " + k.name)
	}
	if reg[k.name] == nil {
		reg[k.name] = map[string]entry{}
	}
	if _, dup := reg[k.name][name]; dup {
		panic(fmt.Sprintf("plugin: duplicate %s plugin %q", k.name, name))
	}
	reg[k.name][name] = entry{info: Info{Kind: k.name, Name: name, Doc: doc},
		build: func(ctx context.Context, h *Host, cfg Config) (any, error) { return f(ctx, h, cfg) }}
}

// Has reports whether a plugin is compiled in.
func Has[T any](k Kind[T], name string) bool {
	mu.RLock()
	defer mu.RUnlock()
	_, ok := reg[k.name][name]
	return ok
}

// Build instantiates one plugin. The instance is recorded on the host, and
// its Close/Shutdown method (if any) is scheduled for shutdown.
func Build[T any](ctx context.Context, k Kind[T], h *Host, s Spec) (T, error) {
	var zero T
	mu.RLock()
	e, ok := reg[k.name][s.Name]
	mu.RUnlock()
	if !ok {
		return zero, fmt.Errorf("%s plugin %q is not compiled in (registered: %v)", k.name, s.Name, Names(k))
	}
	instance := s.Instance
	if instance == "" {
		instance = s.Name
	}
	cfg := Config{Raw: s.Config, kind: k.name, name: s.Name}
	v, err := e.build(ctx, h.scoped(k.name, instance), cfg)
	if err != nil {
		return zero, fmt.Errorf("%s plugin %q: %w", k.name, s.Name, err)
	}
	t, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("%s plugin %q returned %T", k.name, s.Name, v)
	}
	h.track(k.name, s.Name, instance, v)
	return t, nil
}

// BuildAll instantiates every enabled spec of a multi kind, in order.
func BuildAll[T any](ctx context.Context, k Kind[T], h *Host, specs Specs) ([]T, error) {
	// Validate the whole list before building anything.
	seen := map[string]bool{}
	for _, s := range specs.Enabled() {
		id := s.Instance
		if id == "" {
			id = s.Name
		}
		if seen[id] {
			return nil, fmt.Errorf("%s: duplicate instance %q (set \"instance\" to run a plugin twice)", k.name, id)
		}
		seen[id] = true
	}
	var out []T
	for _, s := range specs.Enabled() {
		v, err := Build(ctx, k, h, s)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// BuildOne instantiates the single enabled spec of a single-instance kind.
func BuildOne[T any](ctx context.Context, k Kind[T], h *Host, specs Specs) (T, error) {
	var zero T
	en := specs.Enabled()
	if len(en) != 1 {
		return zero, fmt.Errorf("%s: exactly one plugin must be enabled, got %d", k.name, len(en))
	}
	return Build(ctx, k, h, en[0])
}

// Names lists the plugins registered for a kind.
func Names[T any](k Kind[T]) []string {
	mu.RLock()
	defer mu.RUnlock()
	var out []string
	for n := range reg[k.name] {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Registered lists every compiled-in plugin.
func Registered() []Info {
	mu.RLock()
	defer mu.RUnlock()
	var out []Info
	for _, m := range reg {
		for _, e := range m {
			out = append(out, e.info)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Kinds lists every declared extension point.
func Kinds() []KindInfo {
	mu.RLock()
	defer mu.RUnlock()
	var out []KindInfo
	for _, k := range kinds {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Config is a plugin's raw configuration.
type Config struct {
	Raw        json.RawMessage
	kind, name string
}

// Empty reports whether no configuration was given.
func (c Config) Empty() bool {
	t := bytes.TrimSpace(c.Raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null")) || bytes.Equal(t, []byte("{}"))
}

// Decode strictly decodes the config into v (unknown fields are errors). An
// empty config leaves v unchanged, so callers set defaults first.
func (c Config) Decode(v any) error {
	if len(bytes.TrimSpace(c.Raw)) == 0 || bytes.Equal(bytes.TrimSpace(c.Raw), []byte("null")) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(c.Raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// Spec selects a plugin and configures one instance of it.
type Spec struct {
	Name string `json:"name"`
	// Instance distinguishes several instances of the same plugin (default: Name).
	Instance string          `json:"instance,omitempty"`
	Enabled  *bool           `json:"enabled,omitempty"` // default true
	Config   json.RawMessage `json:"config,omitempty"`
}

// Specs is an ordered list of plugin specs. In config files it may be
// written as one object, a list of objects, a bare name or a list of names.
type Specs []Spec

func (s *Specs) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch {
	case len(b) == 0 || bytes.Equal(b, []byte("null")):
		*s = nil
		return nil
	case b[0] == '"':
		var n string
		if err := json.Unmarshal(b, &n); err != nil {
			return err
		}
		*s = Specs{{Name: n}}
		return nil
	case b[0] == '{':
		var one Spec
		if err := strictUnmarshal(b, &one); err != nil {
			return err
		}
		*s = Specs{one}
		return nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	out := Specs{}
	for _, r := range raw {
		var one Specs
		if err := one.UnmarshalJSON(r); err != nil {
			return err
		}
		out = append(out, one...)
	}
	*s = out
	return nil
}

// Enabled drops specs with enabled: false.
func (s Specs) Enabled() []Spec {
	var out []Spec
	for _, x := range s {
		if x.Enabled == nil || *x.Enabled {
			out = append(out, x)
		}
	}
	return out
}

// Spec is a convenience constructor for code-built configs.
func NewSpec(name string, cfg any) Spec {
	if cfg == nil {
		return Spec{Name: name}
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return Spec{Name: name, Config: b}
}

func strictUnmarshal(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
