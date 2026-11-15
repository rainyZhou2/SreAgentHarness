package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventbus"
	"go-sre-harness/internal/extensions"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/netguard"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
)

var (
	ErrInvalidTransition = errors.New("plugin invalid lifecycle transition")
	ErrHostClosing       = errors.New("plugin host closing")
	ErrStartFailed       = errors.New("plugin start failed")
	ErrReadyFailed       = errors.New("plugin ready failed")
	ErrDrainTimeout      = errors.New("plugin drain timeout")
	ErrStopFailed        = errors.New("plugin stop failed")
	ErrTaskFailed        = errors.New("plugin task failed")
	ErrHealthFailed      = errors.New("plugin health failed")
)

type Host struct {
	Mode        string
	CoreVersion string
	HTTPAddr    string
	Log         *slog.Logger
	Store       *pg.Store
	Metrics     *obs.Metrics

	Artifacts artifact.Store
	Tools     *tools.Registry
	Deltas    stream.Bus
	Events    eventbus.Bus
	Network   netguard.NetPolicy
	Policy    policy.Gate
	Security  security.Authorizer
	Gateway   *gw.Gateway

	OfflineGateway  *gw.Gateway
	Embedder        memory.Embedder
	MemoryAuthority memory.VerificationAuthority
	Eval            *eval.Service
	Server          *api.Server
	Extensions      *extensions.Runtime
	Settings        Settings

	shared *hostShared
	kind   string
	inst   string
	scope  PermissionScope
}

type hostShared struct {
	mu        sync.RWMutex
	instances []*managedInstance
	closers   []func(context.Context) error
	services  map[string]any
	started   bool
	closing   bool
	runCtx    context.Context
	cancel    context.CancelFunc
	tasks     sync.WaitGroup
	failures  chan error
	closeOnce sync.Once
	closeErr  error
}

type managedInstance struct {
	mu        sync.Mutex
	active    Active
	component untypedComponent
	started   bool
	ready     bool
	stopOnce  sync.Once
	stopErr   error
}

type Active struct {
	Kind       string     `json:"kind"`
	Name       string     `json:"name"`
	Instance   string     `json:"instance"`
	Descriptor Descriptor `json:"descriptor"`
	State      State      `json:"state"`
	Health     Health     `json:"health"`
	UpdatedAt  time.Time  `json:"updated_at"`
	ErrorCode  string     `json:"error_code,omitempty"`
}

func NewHost(mode string, log *slog.Logger) *Host {
	return &Host{
		Mode: mode,
		Log:  log,
		shared: &hostShared{
			services: map[string]any{},
			failures: make(chan error, 1),
		},
	}
}

func (h *Host) scoped(kind, inst string, scope PermissionScope) *Host {
	scoped := *h
	scoped.kind, scoped.inst, scoped.scope = kind, inst, scope
	scoped.scope.Services = append([]string(nil), scope.Services...)
	if h.Log != nil {
		scoped.Log = h.Log.With("plugin", kind+"/"+inst)
	}
	return &scoped
}

func (h *Host) OnClose(fn func(context.Context) error) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	h.shared.closers = append(h.shared.closers, fn)
}

func (h *Host) Provide(key string, value any) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	h.shared.services[key] = value
}

func (h *Host) Lookup(key string) (any, bool) {
	if h.kind != "" || h.inst != "" {
		allowed := false
		for _, service := range h.scope.Services {
			if service == key {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, false
		}
	}
	h.shared.mu.RLock()
	defer h.shared.mu.RUnlock()
	value, ok := h.shared.services[key]
	return value, ok
}

func (h *Host) trackComponent(active Active, component untypedComponent) error {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	if h.shared.closing {
		return fmt.Errorf("%w: %s/%s", ErrHostClosing, active.Kind, active.Instance)
	}
	for _, existing := range h.shared.instances {
		if existing.active.Kind == active.Kind && existing.active.Instance == active.Instance {
			return fmt.Errorf("%w: duplicate instance %s/%s", ErrComponentInvalid, active.Kind, active.Instance)
		}
	}
	active.UpdatedAt = time.Now().UTC()
	h.shared.instances = append(h.shared.instances, &managedInstance{active: active, component: component})
	return nil
}

func (h *Host) Active() []Active {
	h.shared.mu.Lock()
	instances := append([]*managedInstance(nil), h.shared.instances...)
	h.shared.mu.Unlock()
	out := make([]Active, 0, len(instances))
	for _, instance := range instances {
		instance.mu.Lock()
		active := instance.active
		active.Descriptor = cloneDescriptor(active.Descriptor)
		instance.mu.Unlock()
		out = append(out, active)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Instance < out[j].Instance
	})
	return out
}

func (h *Host) Start(ctx context.Context) error {
	h.shared.mu.Lock()
	if h.shared.closing {
		h.shared.mu.Unlock()
		return ErrHostClosing
	}
	if h.shared.started {
		h.shared.mu.Unlock()
		return fmt.Errorf("%w: host already started", ErrInvalidTransition)
	}
	h.shared.started = true
	h.shared.runCtx, h.shared.cancel = context.WithCancel(ctx)
	instances := append([]*managedInstance(nil), h.shared.instances...)
	h.shared.mu.Unlock()

	for _, instance := range instances {
		if err := instance.start(ctx); err != nil {
			return h.rollbackStart(err)
		}
		h.startTasks(instance)
		if err := instance.operate(); err != nil {
			select {
			case taskErr := <-h.shared.failures:
				return h.rollbackStart(taskErr)
			default:
				return h.rollbackStart(err)
			}
		}
		h.startHealthMonitor(instance)
	}
	return nil
}

func (h *Host) rollbackStart(startErr error) error {
	h.shared.mu.Lock()
	if h.shared.cancel != nil {
		h.shared.cancel()
	}
	h.shared.mu.Unlock()
	closeErr := h.Close(context.Background())
	return errors.Join(startErr, closeErr)
}

func (h *Host) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-h.shared.failures:
		return err
	}
}

func (h *Host) Ready() bool {
	h.shared.mu.Lock()
	started := h.shared.started
	closing := h.shared.closing
	instances := append([]*managedInstance(nil), h.shared.instances...)
	h.shared.mu.Unlock()
	if !started || closing {
		return false
	}
	for _, instance := range instances {
		instance.mu.Lock()
		ready := instance.active.State == Operating && instance.active.Health.Status != Unhealthy
		instance.mu.Unlock()
		if !ready {
			return false
		}
	}
	return true
}

func (h *Host) startTasks(instance *managedInstance) {
	h.shared.mu.Lock()
	ctx := h.shared.runCtx
	h.shared.mu.Unlock()
	instance.mu.Lock()
	kind, instanceID := instance.active.Kind, instance.active.Instance
	maxConcurrent := instance.active.Descriptor.ResourceBudget.MaxConcurrent
	instance.mu.Unlock()
	var slots chan struct{}
	if maxConcurrent > 0 {
		slots = make(chan struct{}, maxConcurrent)
	}
	var previous <-chan struct{}
	for _, named := range instance.component.tasks {
		named := named
		waitFor := previous
		var acquired chan struct{}
		if slots != nil {
			acquired = make(chan struct{})
			previous = acquired
		}
		h.shared.tasks.Add(1)
		go func() {
			defer h.shared.tasks.Done()
			if slots != nil {
				if waitFor != nil {
					select {
					case <-waitFor:
					case <-ctx.Done():
						return
					}
				}
				select {
				case slots <- struct{}{}:
					close(acquired)
					defer func() { <-slots }()
				case <-ctx.Done():
					return
				}
			}
			err := named.Task.Run(ctx)
			if ctx.Err() != nil && (err == nil || errors.Is(err, context.Canceled)) {
				return
			}
			if err == nil {
				err = errors.New("task exited before cancellation")
			}
			wrapped := fmt.Errorf("%w: %s/%s task %s: %v", ErrTaskFailed,
				kind, instanceID, named.Name, err)
			instance.fail("task_failed")
			h.reportFailure(wrapped)
		}()
	}
}

func (h *Host) startHealthMonitor(instance *managedInstance) {
	h.shared.mu.Lock()
	ctx := h.shared.runCtx
	h.shared.mu.Unlock()
	h.shared.tasks.Add(1)
	go func() {
		defer h.shared.tasks.Done()
		ticker := time.NewTicker(instance.active.Descriptor.ResourceBudget.HealthInterval)
		defer ticker.Stop()
		for {
			if h.checkHealth(ctx, instance) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (h *Host) checkHealth(ctx context.Context, instance *managedInstance) bool {
	healthCtx, cancel := context.WithTimeout(ctx, instance.active.Descriptor.ResourceBudget.HealthTimeout)
	health := instance.component.lifecycle.Health(healthCtx)
	cancel()
	if health.CheckedAt.IsZero() {
		health.CheckedAt = time.Now().UTC()
	}
	instance.mu.Lock()
	instance.active.Health = health
	instance.active.UpdatedAt = time.Now().UTC()
	fatalCodes := instance.active.Descriptor.ErrorContract.UnhealthyCodes
	instance.mu.Unlock()
	if health.Status != Unhealthy {
		return ctx.Err() != nil
	}
	for _, code := range fatalCodes {
		if code == health.Code {
			instance.fail("health_unhealthy")
			h.reportFailure(fmt.Errorf("%w: %s/%s: %s", ErrHealthFailed,
				instance.active.Kind, instance.active.Instance, health.Code))
			return true
		}
	}
	return ctx.Err() != nil
}

func (h *Host) reportFailure(err error) {
	select {
	case h.shared.failures <- err:
	default:
	}
}

func (h *Host) Close(ctx context.Context) error {
	h.shared.closeOnce.Do(func() {
		h.shared.closeErr = h.close(ctx)
	})
	return h.shared.closeErr
}

func (h *Host) close(ctx context.Context) error {
	h.shared.mu.Lock()
	h.shared.closing = true
	if h.shared.cancel != nil {
		h.shared.cancel()
	}
	instances := append([]*managedInstance(nil), h.shared.instances...)
	closers := append([]func(context.Context) error(nil), h.shared.closers...)
	h.shared.closers = nil
	h.shared.mu.Unlock()

	var errs []error
	for i := len(instances) - 1; i >= 0; i-- {
		if err := instances[i].close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := h.waitTasks(ctx); err != nil {
		errs = append(errs, err)
	}
	for i := len(closers) - 1; i >= 0; i-- {
		if err := closers[i](ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *Host) waitTasks(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.shared.tasks.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("plugin tasks: %w", ctx.Err())
	}
}

func (h *Host) rollbackStartTo(mark int) error {
	h.shared.mu.Lock()
	instances := append([]*managedInstance(nil), h.shared.instances[mark:]...)
	h.shared.instances = h.shared.instances[:mark]
	h.shared.mu.Unlock()
	var errs []error
	for i := len(instances) - 1; i >= 0; i-- {
		if err := instances[i].close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *Host) mark() int {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()
	return len(h.shared.instances)
}

func (i *managedInstance) start(ctx context.Context) error {
	i.mu.Lock()
	if i.active.State != Initialized {
		state := i.active.State
		i.mu.Unlock()
		return fmt.Errorf("%w: cannot start from %s", ErrInvalidTransition, state)
	}
	i.active.State = Starting
	i.active.UpdatedAt = time.Now().UTC()
	i.started = true
	i.mu.Unlock()

	startCtx, cancel := context.WithTimeout(ctx, i.active.Descriptor.ResourceBudget.StartTimeout)
	err := i.component.lifecycle.Start(startCtx)
	cancel()
	if err != nil {
		i.fail("start_failed")
		return fmt.Errorf("%w: %s/%s: %v", ErrStartFailed, i.active.Kind, i.active.Instance, err)
	}
	readyCtx, cancel := context.WithTimeout(ctx, i.active.Descriptor.ResourceBudget.ReadyTimeout)
	err = i.component.lifecycle.Ready(readyCtx)
	cancel()
	if err != nil {
		i.fail("ready_failed")
		return fmt.Errorf("%w: %s/%s: %v", ErrReadyFailed, i.active.Kind, i.active.Instance, err)
	}
	i.mu.Lock()
	i.active.State = Ready
	i.active.UpdatedAt = time.Now().UTC()
	i.ready = true
	i.mu.Unlock()
	return nil
}

func (i *managedInstance) operate() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.active.State != Ready {
		return fmt.Errorf("%w: cannot operate from %s", ErrInvalidTransition, i.active.State)
	}
	i.active.State = Operating
	i.active.UpdatedAt = time.Now().UTC()
	return nil
}

func (i *managedInstance) close() error {
	i.mu.Lock()
	state := i.active.State
	shouldDrain := i.ready && state != Stopped && state != Stopping
	i.mu.Unlock()
	var errs []error
	if shouldDrain {
		i.mu.Lock()
		i.active.State = Draining
		i.active.UpdatedAt = time.Now().UTC()
		i.mu.Unlock()
		drainCtx, cancel := context.WithTimeout(context.Background(), i.active.Descriptor.ResourceBudget.DrainTimeout)
		err := i.component.lifecycle.Drain(drainCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("%w: %s/%s", ErrDrainTimeout, i.active.Kind, i.active.Instance)
			} else {
				err = fmt.Errorf("plugin drain: %s/%s: %w", i.active.Kind, i.active.Instance, err)
			}
			errs = append(errs, err)
		}
	}
	if err := i.stop(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (i *managedInstance) stop() error {
	i.stopOnce.Do(func() {
		i.mu.Lock()
		i.active.State = Stopping
		i.active.UpdatedAt = time.Now().UTC()
		i.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), i.active.Descriptor.ResourceBudget.StopTimeout)
		err := i.component.lifecycle.Stop(ctx)
		cancel()
		i.mu.Lock()
		defer i.mu.Unlock()
		if err != nil {
			i.active.State = Failed
			i.active.ErrorCode = "stop_failed"
			i.stopErr = fmt.Errorf("%w: %s/%s: %v", ErrStopFailed, i.active.Kind, i.active.Instance, err)
		} else {
			i.active.State = Stopped
			i.active.ErrorCode = ""
		}
		i.active.UpdatedAt = time.Now().UTC()
	})
	return i.stopErr
}

func (i *managedInstance) fail(code string) {
	i.mu.Lock()
	i.active.State = Failed
	i.active.ErrorCode = code
	i.active.UpdatedAt = time.Now().UTC()
	i.mu.Unlock()
}

type Settings struct {
	ModelAlias            string `json:"model_alias,omitempty"`
	ModelEffort           string `json:"model_effort,omitempty"`
	DefaultTenant         string `json:"default_tenant,omitempty"`
	DefaultProfile        string `json:"default_profile,omitempty"`
	DefaultProfileVersion int64  `json:"default_profile_version,omitempty"`
}
