package plugin

import (
	"context"
	"encoding/json"
	"testing"
)

type definitionSettings struct {
	Endpoint string `json:"endpoint"`
}

func TestStrictConfig(t *testing.T) {
	validate := StrictConfig[definitionSettings]()
	if err := validate(Config{Raw: json.RawMessage(`{"endpoint":"https://service.internal"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := validate(Config{Raw: json.RawMessage(`{"unknown":true}`)}); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}

func TestDefineDefaults(t *testing.T) {
	d := Define("remote", "v1.0.0", "remote store", StrictConfig[definitionSettings](),
		func(context.Context, *Host, Config) (Component[string], error) {
			return Static("value"), nil
		})
	if d.Descriptor.CompatibilityPolicy != CompatibleMajor || d.Descriptor.MinimumCoreVersion != "v0.0.0" {
		t.Fatalf("unexpected compatibility defaults: %+v", d.Descriptor)
	}
	if d.Descriptor.ResourceBudget != DefaultResourceBudget() {
		t.Fatalf("unexpected resource defaults: %+v", d.Descriptor.ResourceBudget)
	}
	if d.Initialize == nil || d.ValidateConfig == nil {
		t.Fatal("definition callbacks must be explicit")
	}
	withoutConfig := Define("static", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) {
			return Static("value"), nil
		})
	if err := withoutConfig.ValidateConfig(Config{Raw: json.RawMessage(`{"endpoint":"unexpected"}`)}); err == nil {
		t.Fatal("expected config rejection for a config-free plugin")
	}
}

func TestStaticComponent(t *testing.T) {
	component := Static("value")
	if component.Value != "value" || component.Lifecycle == nil || len(component.Tasks) != 0 {
		t.Fatalf("unexpected component: %+v", component)
	}
	if err := component.Lifecycle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Lifecycle.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Lifecycle.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Lifecycle.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if health := component.Lifecycle.Health(context.Background()); health.Status != Healthy {
		t.Fatalf("unexpected health: %+v", health)
	}
}

func TestBackgroundComponent(t *testing.T) {
	called := make(chan struct{})
	task := TaskFunc(func(context.Context) error {
		close(called)
		return nil
	})
	component := Background(task, "refresh")
	if component.Value == nil || len(component.Tasks) != 1 || component.Tasks[0].Name != "refresh" {
		t.Fatalf("unexpected background component: %+v", component)
	}
	if err := component.Tasks[0].Task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-called
}

func TestStaticDefinition(t *testing.T) {
	definition := StaticDefinition("memory", "v1.0.0", "memory store", nil,
		func(context.Context, *Host, Config) (string, error) { return "value", nil })
	component, err := definition.Initialize(context.Background(), nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if component.Value != "value" || len(component.Tasks) != 0 || component.Lifecycle == nil {
		t.Fatalf("unexpected component: %+v", component)
	}
}

func TestBackgroundDefinition(t *testing.T) {
	definition := BackgroundDefinition("worker", "v1.0.0", "worker", nil,
		func(context.Context, *Host, Config) (Task, error) {
			return TaskFunc(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}), nil
		})
	component, err := definition.Initialize(context.Background(), nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if definition.Descriptor.ResourceBudget.MaxTasks != 1 || len(component.Tasks) != 1 || component.Tasks[0].Name != "worker" {
		t.Fatalf("unexpected background definition: %+v", definition.Descriptor)
	}
}

func TestLifecycleFuncs(t *testing.T) {
	stopped := false
	lifecycle := LifecycleFuncs{OnStop: func(context.Context) error {
		stopped = true
		return nil
	}}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stopped || lifecycle.Health(context.Background()).Status != Healthy {
		t.Fatal("lifecycle function defaults were not applied")
	}
}
