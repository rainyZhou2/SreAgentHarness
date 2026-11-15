package plugin

import (
	"context"
	"time"
)

type State string

const (
	Discovered  State = "discovered"
	Validated   State = "validated"
	Initialized State = "initialized"
	Starting    State = "starting"
	Ready       State = "ready"
	Operating   State = "operating"
	Draining    State = "draining"
	Stopping    State = "stopping"
	Stopped     State = "stopped"
	Failed      State = "failed"
)

type HealthStatus string

const (
	Healthy   HealthStatus = "healthy"
	Degraded  HealthStatus = "degraded"
	Unhealthy HealthStatus = "unhealthy"
)

type Health struct {
	Status    HealthStatus `json:"status"`
	Code      string       `json:"code,omitempty"`
	Message   string       `json:"message,omitempty"`
	CheckedAt time.Time    `json:"checked_at"`
}

type Lifecycle interface {
	Start(context.Context) error
	Ready(context.Context) error
	Drain(context.Context) error
	Stop(context.Context) error
	Health(context.Context) Health
}

type Task interface {
	Run(context.Context) error
}

type TaskFunc func(context.Context) error

func (f TaskFunc) Run(ctx context.Context) error { return f(ctx) }

type NamedTask struct {
	Name string
	Task Task
}

type Component[T any] struct {
	Value     T
	Lifecycle Lifecycle
	Tasks     []NamedTask
}

type LifecycleFuncs struct {
	OnStart     func(context.Context) error
	OnReady     func(context.Context) error
	OnDrain     func(context.Context) error
	OnStop      func(context.Context) error
	CheckHealth func(context.Context) Health
}

func (l LifecycleFuncs) Start(ctx context.Context) error {
	if l.OnStart != nil {
		return l.OnStart(ctx)
	}
	return nil
}
func (l LifecycleFuncs) Ready(ctx context.Context) error {
	if l.OnReady != nil {
		return l.OnReady(ctx)
	}
	return nil
}
func (l LifecycleFuncs) Drain(ctx context.Context) error {
	if l.OnDrain != nil {
		return l.OnDrain(ctx)
	}
	return nil
}
func (l LifecycleFuncs) Stop(ctx context.Context) error {
	if l.OnStop != nil {
		return l.OnStop(ctx)
	}
	return nil
}
func (l LifecycleFuncs) Health(ctx context.Context) Health {
	if l.CheckHealth != nil {
		return l.CheckHealth(ctx)
	}
	return Health{Status: Healthy, CheckedAt: time.Now().UTC()}
}

type staticLifecycle struct{}

func (staticLifecycle) Start(context.Context) error { return nil }
func (staticLifecycle) Ready(context.Context) error { return nil }
func (staticLifecycle) Drain(context.Context) error { return nil }
func (staticLifecycle) Stop(context.Context) error  { return nil }
func (staticLifecycle) Health(context.Context) Health {
	return Health{Status: Healthy, CheckedAt: time.Now().UTC()}
}

func Static[T any](value T) Component[T] {
	return Component[T]{Value: value, Lifecycle: staticLifecycle{}}
}

func Background(value Task, name string) Component[Task] {
	return Component[Task]{
		Value:     value,
		Lifecycle: staticLifecycle{},
		Tasks:     []NamedTask{{Name: name, Task: value}},
	}
}
