package plugin

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

type recordingLifecycle struct {
	mu         sync.Mutex
	events     *[]string
	name       string
	startErr   error
	readyErr   error
	drainErr   error
	blockDrain bool
	stopErr    error
	drainSeen  chan struct{}
	stopSeen   chan struct{}
	drainCall  int
	stopCall   int
	health     Health
}

func newRecordingLifecycle(name string, events *[]string) *recordingLifecycle {
	return &recordingLifecycle{name: name, events: events, drainSeen: make(chan struct{}), stopSeen: make(chan struct{}), health: Health{Status: Healthy}}
}

func (l *recordingLifecycle) record(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	*l.events = append(*l.events, l.name+"."+event)
}

func (l *recordingLifecycle) Start(context.Context) error {
	l.record("start")
	return l.startErr
}
func (l *recordingLifecycle) Ready(context.Context) error {
	l.record("ready")
	return l.readyErr
}
func (l *recordingLifecycle) Drain(ctx context.Context) error {
	l.mu.Lock()
	l.drainCall++
	l.mu.Unlock()
	l.record("drain")
	close(l.drainSeen)
	if l.blockDrain {
		<-ctx.Done()
		return ctx.Err()
	}
	return l.drainErr
}
func (l *recordingLifecycle) Stop(context.Context) error {
	l.mu.Lock()
	l.stopCall++
	l.mu.Unlock()
	l.record("stop")
	close(l.stopSeen)
	return l.stopErr
}
func (l *recordingLifecycle) Health(context.Context) Health {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.health
}
func (l *recordingLifecycle) calls() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.drainCall, l.stopCall
}

func lifecycleComponent(name string, lifecycle Lifecycle, tasks ...NamedTask) (Active, untypedComponent) {
	descriptor := Define[string](name, "v1.0.0", "", nil, nil).Descriptor
	descriptor.Kind = "test"
	descriptor.ResourceBudget.StartTimeout = time.Second
	descriptor.ResourceBudget.ReadyTimeout = time.Second
	descriptor.ResourceBudget.DrainTimeout = 30 * time.Millisecond
	descriptor.ResourceBudget.StopTimeout = time.Second
	descriptor.ResourceBudget.HealthTimeout = 50 * time.Millisecond
	descriptor.ResourceBudget.HealthInterval = time.Hour
	descriptor.ResourceBudget.MaxTasks = len(tasks)
	active := Active{Kind: "test", Name: name, Instance: name, Descriptor: descriptor, State: Initialized}
	return active, untypedComponent{value: name, lifecycle: lifecycle, tasks: tasks}
}

func TestHostLifecycleSuccess(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	taskStarted := make(chan State, 1)
	var host *Host
	task := TaskFunc(func(ctx context.Context) error {
		for _, active := range host.Active() {
			if active.Instance == "alpha" {
				taskStarted <- active.State
			}
		}
		<-ctx.Done()
		return ctx.Err()
	})
	host = NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle, NamedTask{Name: "work", Task: task})
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if state := <-taskStarted; state != Ready && state != Operating {
		t.Fatalf("task started in state %s", state)
	}
	if got := host.Active()[0].State; got != Operating {
		t.Fatalf("expected operating, got %s", got)
	}
	cancel()
	if err := host.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"alpha.start", "alpha.ready", "alpha.drain", "alpha.stop"}) {
		t.Fatalf("unexpected events: %v", events)
	}
}

type hostStateKey struct{}

func TestHostStartFailureRollsBackReverse(t *testing.T) {
	events := []string{}
	first := newRecordingLifecycle("first", &events)
	second := newRecordingLifecycle("second", &events)
	second.readyErr = errors.New("not ready")
	host := NewHost("test", nil)
	for _, lifecycle := range []*recordingLifecycle{first, second} {
		active, component := lifecycleComponent(lifecycle.name, lifecycle)
		if err := host.trackComponent(active, component); err != nil {
			t.Fatal(err)
		}
	}
	err := host.Start(context.Background())
	if !errors.Is(err, ErrReadyFailed) {
		t.Fatalf("expected ready failure, got %v", err)
	}
	if !reflect.DeepEqual(events, []string{"first.start", "first.ready", "second.start", "second.ready", "second.stop", "first.drain", "first.stop"}) {
		t.Fatalf("unexpected rollback: %v", events)
	}
}

func TestHostDrainTimeoutStillStops(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	lifecycle.blockDrain = true
	host := NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle)
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := host.Close(context.Background())
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	drainCalls, stopCalls := lifecycle.calls()
	if drainCalls != 1 || stopCalls != 1 {
		t.Fatalf("drain=%d stop=%d", drainCalls, stopCalls)
	}
	if got := host.Active()[0].State; got != Stopped {
		t.Fatalf("expected stopped, got %s", got)
	}
}

func TestHostCloseConcurrentAndIdempotent(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	host := NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle)
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- host.Close(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected close result: %v", err)
		}
	}
	drainCalls, stopCalls := lifecycle.calls()
	if drainCalls != 1 || stopCalls != 1 {
		t.Fatalf("drain=%d stop=%d", drainCalls, stopCalls)
	}
}

func TestHostTaskFailurePropagates(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	failure := errors.New("connection lost")
	task := TaskFunc(func(context.Context) error { return failure })
	host := NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle, NamedTask{Name: "work", Task: task})
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); err != nil && !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("expected task failure, got %v", err)
	} else if err == nil {
		if err := host.Wait(context.Background()); !errors.Is(err, ErrTaskFailed) {
			t.Fatalf("expected task failure from wait, got %v", err)
		}
	}
	if got := host.Active()[0].State; got != Failed && got != Stopped {
		t.Fatalf("unexpected state %s", got)
	}
}

func TestHostWaitReturnsTaskFailure(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	release := make(chan error, 1)
	task := TaskFunc(func(context.Context) error { return <-release })
	host := NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle, NamedTask{Name: "work", Task: task})
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	release <- errors.New("failed")
	if err := host.Wait(context.Background()); !errors.Is(err, ErrTaskFailed) {
		t.Fatalf("expected task failure, got %v", err)
	}
	if got := host.Active()[0].State; got != Failed {
		t.Fatalf("expected failed, got %s", got)
	}
}

func TestHostRejectsLateRegistrationAndSecondStart(t *testing.T) {
	events := []string{}
	lifecycle := newRecordingLifecycle("alpha", &events)
	host := NewHost("test", nil)
	active, component := lifecycleComponent("alpha", lifecycle)
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(context.Background()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	_ = host.Close(context.Background())
	if err := host.trackComponent(active, component); !errors.Is(err, ErrHostClosing) {
		t.Fatalf("expected host closing, got %v", err)
	}
}

func TestHostMaxConcurrentSerializesTasks(t *testing.T) {
	events := []string{}
	host := NewHost("test", nil)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	blockUntilDone := func(started chan struct{}, release <-chan struct{}) Task {
		return TaskFunc(func(ctx context.Context) error {
			close(started)
			if release != nil {
				<-release
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}
	active, component := lifecycleComponent("alpha", newRecordingLifecycle("alpha", &events),
		NamedTask{Name: "first", Task: blockUntilDone(firstStarted, releaseFirst)},
		NamedTask{Name: "second", Task: blockUntilDone(secondStarted, nil)})
	active.Descriptor.ResourceBudget.MaxConcurrent = 1
	if err := host.trackComponent(active, component); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := host.Start(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first registered task did not acquire the first concurrency slot")
	}
	select {
	case <-secondStarted:
		t.Fatal("second task exceeded the instance concurrency budget")
	default:
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-ctx.Done():
		t.Fatal("second task did not start after the first released its slot")
	case <-time.After(time.Second):
		t.Fatal("second task did not start after the first released its slot")
	}
	cancel()
	if err := host.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHostScopesServiceLookup(t *testing.T) {
	host := NewHost("test", nil)
	host.Provide("artifacts", "store")
	if value, ok := host.Lookup("artifacts"); !ok || value != "store" {
		t.Fatal("core host could not resolve a service")
	}
	allowed := host.scoped("test", "allowed", PermissionScope{Services: []string{"artifacts"}})
	if value, ok := allowed.Lookup("artifacts"); !ok || value != "store" {
		t.Fatal("declared service was not resolved")
	}
	denied := host.scoped("test", "denied", PermissionScope{})
	if _, ok := denied.Lookup("artifacts"); ok {
		t.Fatal("undeclared service was resolved")
	}
}

func TestHostActiveUsesStableSortAndCopies(t *testing.T) {
	host := NewHost("test", nil)
	for _, name := range []string{"zeta", "alpha"} {
		lifecycle := newRecordingLifecycle(name, &[]string{})
		active, component := lifecycleComponent(name, lifecycle)
		if err := host.trackComponent(active, component); err != nil {
			t.Fatal(err)
		}
	}
	first := host.Active()
	first[0].Descriptor.Capabilities = append(first[0].Descriptor.Capabilities, "mutated")
	second := host.Active()
	if len(second[0].Descriptor.Capabilities) != 0 {
		t.Fatal("active snapshot aliases descriptor")
	}
	if second[0].Instance != "alpha" || second[1].Instance != "zeta" {
		t.Fatalf("unexpected order: %s", fmt.Sprint(second[0].Instance, second[1].Instance))
	}
}
