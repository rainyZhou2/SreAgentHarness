package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var ErrComponentInvalid = errors.New("plugin component invalid")

type Kind[T any] struct {
	name string
}

func (k Kind[T]) Name() string { return k.name }

type KindInfo struct {
	Name            string   `json:"name"`
	Multi           bool     `json:"multi"`
	Doc             string   `json:"doc"`
	ContractVersion string   `json:"contract_version"`
	AllowedServices []string `json:"allowed_services,omitempty"`
}

type registry struct {
	mu    sync.RWMutex
	kinds map[string]KindInfo
	reg   map[string]map[string]entry
}

type entry struct {
	descriptor Descriptor
	validate   func(Config) error
	build      func(context.Context, *Host, Config) (untypedComponent, error)
}

type untypedComponent struct {
	value     any
	lifecycle Lifecycle
	tasks     []NamedTask
}

var defaultRegistry = newRegistry()

func newRegistry() *registry {
	return &registry{kinds: map[string]KindInfo{}, reg: map[string]map[string]entry{}}
}

func NewKind[T any](info KindInfo) Kind[T] {
	return newKindOn[T](defaultRegistry, info)
}

func newKindOn[T any](r *registry, info KindInfo) Kind[T] {
	if !pluginNamePattern.MatchString(info.Name) {
		panic("plugin: invalid kind " + info.Name)
	}
	if _, err := parseSemanticVersion(info.ContractVersion); err != nil {
		panic("plugin: invalid kind contract " + info.Name)
	}
	if err := uniqueNames("allowed_service", info.AllowedServices); err != nil {
		panic("plugin: " + err.Error())
	}
	info.AllowedServices = append([]string(nil), info.AllowedServices...)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, duplicate := r.kinds[info.Name]; duplicate {
		panic("plugin: duplicate kind " + info.Name)
	}
	r.kinds[info.Name] = info
	return Kind[T]{name: info.Name}
}

func Register[T any](k Kind[T], definition Definition[T]) {
	registerDefinition(defaultRegistry, k, definition)
}

func registerDefinition[T any](r *registry, k Kind[T], definition Definition[T]) {
	if definition.Initialize == nil || definition.ValidateConfig == nil {
		panic("plugin: nil definition callback for kind " + k.name)
	}
	descriptor := cloneDescriptor(definition.Descriptor)
	descriptor.Kind = k.name
	r.mu.Lock()
	defer r.mu.Unlock()
	kind, ok := r.kinds[k.name]
	if !ok {
		panic("plugin: unknown kind " + k.name)
	}
	if err := descriptor.Validate(); err != nil {
		panic("plugin: " + err.Error())
	}
	allowed := map[string]bool{}
	for _, service := range kind.AllowedServices {
		allowed[service] = true
	}
	for _, service := range descriptor.PermissionScope.Services {
		if !allowed[service] {
			panic(fmt.Sprintf("plugin: %s/%s service %q is not allowed", k.name, descriptor.Name, service))
		}
	}
	if r.reg[k.name] == nil {
		r.reg[k.name] = map[string]entry{}
	}
	if _, duplicate := r.reg[k.name][descriptor.Name]; duplicate {
		panic(fmt.Sprintf("plugin: duplicate %s plugin %q", k.name, descriptor.Name))
	}
	validate := definition.ValidateConfig
	initialize := definition.Initialize
	r.reg[k.name][descriptor.Name] = entry{
		descriptor: descriptor,
		validate:   validate,
		build: func(ctx context.Context, h *Host, cfg Config) (untypedComponent, error) {
			component, err := initialize(ctx, h, cfg)
			return untypedComponent{value: component.Value, lifecycle: component.Lifecycle, tasks: component.Tasks}, err
		},
	}
}

func Has[T any](k Kind[T], name string) bool {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()
	_, ok := defaultRegistry.reg[k.name][name]
	return ok
}

func Build[T any](ctx context.Context, k Kind[T], h *Host, s Spec) (T, error) {
	return build(defaultRegistry, ctx, k, h, s)
}

func build[T any](r *registry, ctx context.Context, k Kind[T], h *Host, s Spec) (T, error) {
	var zero T
	r.mu.RLock()
	kind, kindOK := r.kinds[k.name]
	e, ok := r.reg[k.name][s.Name]
	r.mu.RUnlock()
	if !kindOK || !ok {
		return zero, fmt.Errorf("%s plugin %q is not compiled in (registered: %v)", k.name, s.Name, Names(k))
	}
	coreVersion := h.CoreVersion
	if coreVersion == "" {
		coreVersion = "dev"
	}
	if err := e.descriptor.CheckCompatibility(coreVersion, kind.ContractVersion); err != nil {
		return zero, fmt.Errorf("%s plugin %q: %w", k.name, s.Name, err)
	}
	instance := s.Instance
	if instance == "" {
		instance = s.Name
	}
	if !pluginNamePattern.MatchString(instance) {
		return zero, fmt.Errorf("%s plugin %q: invalid instance %q", k.name, s.Name, instance)
	}
	cfg := Config{Raw: s.Config, kind: k.name, name: s.Name}
	if err := e.validate(cfg); err != nil {
		return zero, fmt.Errorf("%s plugin %q config: %w", k.name, s.Name, err)
	}
	initCtx, cancel := context.WithTimeout(ctx, e.descriptor.ResourceBudget.StartTimeout)
	component, err := e.build(initCtx, h.scoped(k.name, instance, e.descriptor.PermissionScope), cfg)
	cancel()
	if err != nil {
		if component.lifecycle != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), e.descriptor.ResourceBudget.StopTimeout)
			_ = component.lifecycle.Stop(stopCtx)
			stopCancel()
		}
		return zero, fmt.Errorf("%s plugin %q initialize: %w", k.name, s.Name, err)
	}
	value, ok := component.value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: %s plugin %q returned %T", ErrComponentInvalid, k.name, s.Name, component.value)
	}
	if err := validateUntypedComponent(component, e.descriptor.ResourceBudget); err != nil {
		if component.lifecycle != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), e.descriptor.ResourceBudget.StopTimeout)
			_ = component.lifecycle.Stop(stopCtx)
			stopCancel()
		}
		return zero, fmt.Errorf("%s plugin %q: %w", k.name, s.Name, err)
	}
	active := Active{Kind: k.name, Name: s.Name, Instance: instance, Descriptor: cloneDescriptor(e.descriptor), State: Initialized}
	if err := h.trackComponent(active, component); err != nil {
		return zero, err
	}
	return value, nil
}

func validateUntypedComponent(component untypedComponent, budget ResourceBudget) error {
	if component.lifecycle == nil {
		return fmt.Errorf("%w: nil lifecycle", ErrComponentInvalid)
	}
	if len(component.tasks) > budget.MaxTasks {
		return fmt.Errorf("%w: %d tasks exceed budget %d", ErrComponentInvalid, len(component.tasks), budget.MaxTasks)
	}
	seen := map[string]bool{}
	for _, task := range component.tasks {
		if !pluginNamePattern.MatchString(task.Name) || task.Task == nil || seen[task.Name] {
			return fmt.Errorf("%w: invalid task %q", ErrComponentInvalid, task.Name)
		}
		seen[task.Name] = true
	}
	return nil
}

func ValidateSpecs(specs map[string]Specs, coreVersion string) error {
	return validateSpecs(defaultRegistry, specs, coreVersion)
}

func validateSpecs(r *registry, specs map[string]Specs, coreVersion string) error {
	if coreVersion == "" {
		coreVersion = "dev"
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	kinds := make([]string, 0, len(specs))
	for kind := range specs {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	var errs []error
	for _, kindName := range kinds {
		kind, ok := r.kinds[kindName]
		if !ok {
			errs = append(errs, fmt.Errorf("plugin kind %q is not compiled in", kindName))
			continue
		}
		enabled := specs[kindName].Enabled()
		if !kind.Multi && len(enabled) > 1 {
			errs = append(errs, fmt.Errorf("%s: at most one plugin can be enabled, got %d", kindName, len(enabled)))
		}
		seen := map[string]bool{}
		for _, spec := range enabled {
			e, ok := r.reg[kindName][spec.Name]
			if !ok {
				errs = append(errs, fmt.Errorf("plugin %s/%s is not compiled into this binary", kindName, spec.Name))
				continue
			}
			instance := spec.Instance
			if instance == "" {
				instance = spec.Name
			}
			if !pluginNamePattern.MatchString(instance) {
				errs = append(errs, fmt.Errorf("plugin %s/%s: invalid instance %q", kindName, spec.Name, instance))
				continue
			}
			if seen[instance] {
				errs = append(errs, fmt.Errorf("%s: duplicate instance %q", kindName, instance))
			}
			seen[instance] = true
			if err := e.descriptor.CheckCompatibility(coreVersion, kind.ContractVersion); err != nil {
				errs = append(errs, fmt.Errorf("plugin %s/%s: %w", kindName, spec.Name, err))
				continue
			}
			if err := e.validate(Config{Raw: spec.Config, kind: kindName, name: spec.Name}); err != nil {
				errs = append(errs, fmt.Errorf("plugin %s/%s config: %w", kindName, spec.Name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func BuildAll[T any](ctx context.Context, k Kind[T], h *Host, specs Specs) ([]T, error) {
	return buildAll(defaultRegistry, ctx, k, h, specs)
}

func buildAll[T any](r *registry, ctx context.Context, k Kind[T], h *Host, specs Specs) ([]T, error) {
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
	mark := h.mark()
	var out []T
	for _, s := range specs.Enabled() {
		v, err := build(r, ctx, k, h, s)
		if err != nil {
			return nil, errors.Join(err, h.rollbackStartTo(mark))
		}
		out = append(out, v)
	}
	return out, nil
}

func BuildOne[T any](ctx context.Context, k Kind[T], h *Host, specs Specs) (T, error) {
	var zero T
	en := specs.Enabled()
	if len(en) != 1 {
		return zero, fmt.Errorf("%s: exactly one plugin must be enabled, got %d", k.name, len(en))
	}
	return Build(ctx, k, h, en[0])
}

func Names[T any](k Kind[T]) []string {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()
	var out []string
	for name := range defaultRegistry.reg[k.name] {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

type Info = Descriptor

func Registered() []Info { return defaultRegistry.registered() }

func (r *registry) registered() []Info {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Info
	for _, plugins := range r.reg {
		for _, e := range plugins {
			out = append(out, cloneDescriptor(e.descriptor))
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

func Kinds() []KindInfo {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()
	var out []KindInfo
	for _, kind := range defaultRegistry.kinds {
		kind.AllowedServices = append([]string(nil), kind.AllowedServices...)
		out = append(out, kind)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func cloneDescriptor(d Descriptor) Descriptor {
	d.Capabilities = append([]string(nil), d.Capabilities...)
	d.ConfigurationSchema = append(json.RawMessage(nil), d.ConfigurationSchema...)
	d.PermissionScope.Services = append([]string(nil), d.PermissionScope.Services...)
	d.DataClasses = append([]DataClass(nil), d.DataClasses...)
	d.ErrorContract.RetryableCodes = append([]string(nil), d.ErrorContract.RetryableCodes...)
	d.ErrorContract.PublicCodes = append([]string(nil), d.ErrorContract.PublicCodes...)
	d.ErrorContract.UnhealthyCodes = append([]string(nil), d.ErrorContract.UnhealthyCodes...)
	d.Observability.Metrics = append([]string(nil), d.Observability.Metrics...)
	d.Observability.LogFields = append([]string(nil), d.Observability.LogFields...)
	return d
}

type Config struct {
	Raw        json.RawMessage
	kind, name string
}

func (c Config) Empty() bool {
	trimmed := bytes.TrimSpace(c.Raw)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte("{}"))
}

func (c Config) Decode(v any) error {
	if len(bytes.TrimSpace(c.Raw)) == 0 || bytes.Equal(bytes.TrimSpace(c.Raw), []byte("null")) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(c.Raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

type Spec struct {
	Name     string          `json:"name"`
	Instance string          `json:"instance,omitempty"`
	Enabled  *bool           `json:"enabled,omitempty"`
	Config   json.RawMessage `json:"config,omitempty"`
}

type Specs []Spec

func (s *Specs) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	switch {
	case len(data) == 0 || bytes.Equal(data, []byte("null")):
		*s = nil
		return nil
	case data[0] == '"':
		var name string
		if err := json.Unmarshal(data, &name); err != nil {
			return err
		}
		*s = Specs{{Name: name}}
		return nil
	case data[0] == '{':
		var one Spec
		if err := strictUnmarshal(data, &one); err != nil {
			return err
		}
		*s = Specs{one}
		return nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out := Specs{}
	for _, item := range raw {
		var one Specs
		if err := one.UnmarshalJSON(item); err != nil {
			return err
		}
		out = append(out, one...)
	}
	*s = out
	return nil
}

func (s Specs) Enabled() []Spec {
	var out []Spec
	for _, spec := range s {
		if spec.Enabled == nil || *spec.Enabled {
			out = append(out, spec)
		}
	}
	return out
}

func NewSpec(name string, cfg any) Spec {
	if cfg == nil {
		return Spec{Name: name}
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return Spec{Name: name, Config: data}
}

func strictUnmarshal(data []byte, v any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
