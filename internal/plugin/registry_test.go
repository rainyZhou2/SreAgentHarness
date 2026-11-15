package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestNewKindUsesExplicitVersionedInfo(t *testing.T) {
	kind := NewKind[string](KindInfo{
		Name:            "versioned_test",
		Multi:           true,
		Doc:             "versioned extension point",
		ContractVersion: "v1.2.0",
		AllowedServices: []string{"artifacts"},
	})
	if kind.Name() != "versioned_test" {
		t.Fatalf("unexpected kind %q", kind.Name())
	}
	for _, info := range Kinds() {
		if info.Name == kind.Name() {
			if info.ContractVersion != "v1.2.0" || !info.Multi || !reflect.DeepEqual(info.AllowedServices, []string{"artifacts"}) {
				t.Fatalf("kind metadata was not preserved: %+v", info)
			}
			return
		}
	}
	t.Fatal("kind was not registered")
}

func TestKindRequiresContractVersion(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected missing contract version to panic")
		}
	}()
	newKindOn[string](newRegistry(), KindInfo{Name: "missing_contract"})
}

func TestRegistryBuildValidatesBeforeInitialize(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	called := 0
	definition := Define("remote", "v1.0.0", "", StrictConfig[definitionSettings](),
		func(context.Context, *Host, Config) (Component[string], error) {
			called++
			return Static("value"), nil
		})
	definition.Descriptor.MinimumCoreVersion = "v9.0.0"
	registerDefinition(r, k, definition)
	host := NewHost("test", nil)
	host.CoreVersion = "v1.0.0"
	_, err := build(r, context.Background(), k, host, Spec{Name: "remote"})
	if !errors.Is(err, ErrIncompatible) {
		t.Fatalf("expected incompatibility, got %v", err)
	}
	if called != 0 {
		t.Fatal("initializer called for incompatible plugin")
	}
	definition.Descriptor.MinimumCoreVersion = "v0.0.0"
	r = newRegistry()
	k = newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	registerDefinition(r, k, definition)
	_, err = build(r, context.Background(), k, host, Spec{Name: "remote", Config: json.RawMessage(`{"unknown":true}`)})
	if err == nil {
		t.Fatal("expected invalid config")
	}
	if called != 0 {
		t.Fatal("initializer called for invalid config")
	}
}

func TestRegistryBuildInitializesAndTracksComponent(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	registerDefinition(r, k, Define("memory", "v1.2.0", "memory store", nil,
		func(context.Context, *Host, Config) (Component[string], error) {
			return Static("value"), nil
		}))
	host := NewHost("test", nil)
	host.CoreVersion = "v1.0.0"
	value, err := build(r, context.Background(), k, host, Spec{Name: "memory", Instance: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	if value != "value" {
		t.Fatalf("unexpected value %q", value)
	}
	active := host.Active()
	if len(active) != 1 || active[0].Instance != "primary" || active[0].State != Initialized {
		t.Fatalf("unexpected active components: %+v", active)
	}
	if active[0].Descriptor.Version != "v1.2.0" || active[0].Descriptor.Kind != "test_store" {
		t.Fatalf("unexpected descriptor: %+v", active[0].Descriptor)
	}
}

func TestRegistryRejectsInvalidComponent(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	registerDefinition(r, k, Define("broken", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) {
			return Component[string]{Value: "value"}, nil
		}))
	host := NewHost("test", nil)
	host.CoreVersion = "v1.0.0"
	_, err := build(r, context.Background(), k, host, Spec{Name: "broken"})
	if !errors.Is(err, ErrComponentInvalid) {
		t.Fatalf("expected component error, got %v", err)
	}
	if len(host.Active()) != 0 {
		t.Fatal("invalid component was tracked")
	}
}

func TestRegistryRejectsDuplicateRegistration(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	definition := Define("memory", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) { return Static("value"), nil })
	registerDefinition(r, k, definition)
	defer func() {
		if recover() == nil {
			t.Fatal("expected duplicate registration panic")
		}
	}()
	registerDefinition(r, k, definition)
}

func TestRegistryPermissionAllowlist(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0", AllowedServices: []string{"artifacts"}})
	definition := Define("memory", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) { return Static("value"), nil })
	definition.Descriptor.PermissionScope.Services = []string{"store"}
	defer func() {
		if recover() == nil {
			t.Fatal("expected permission panic")
		}
	}()
	registerDefinition(r, k, definition)
}

func TestRegistryRegisteredSortsDescriptors(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	for _, name := range []string{"zeta", "alpha"} {
		registerDefinition(r, k, Define(name, "v1.0.0", "", nil,
			func(context.Context, *Host, Config) (Component[string], error) { return Static("value"), nil }))
	}
	descriptors := r.registered()
	names := []string{descriptors[0].Name, descriptors[1].Name}
	if !reflect.DeepEqual(names, []string{"alpha", "zeta"}) {
		t.Fatalf("unexpected order: %v", names)
	}
}

func TestBuildAllRollsBackInitializedComponents(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	events := []string{}
	first := newRecordingLifecycle("first", &events)
	registerDefinition(r, k, Define("first", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) {
			return Component[string]{Value: "first", Lifecycle: first}, nil
		}))
	registerDefinition(r, k, Define("second", "v1.0.0", "", nil,
		func(context.Context, *Host, Config) (Component[string], error) {
			return Component[string]{}, errors.New("initialize failed")
		}))
	host := NewHost("test", nil)
	host.CoreVersion = "v1.0.0"
	_, err := buildAll(r, context.Background(), k, host, Specs{{Name: "first"}, {Name: "second"}})
	if err == nil {
		t.Fatal("expected initialization failure")
	}
	_, stopCalls := first.calls()
	if stopCalls != 1 {
		t.Fatalf("expected first component stop once, got %d", stopCalls)
	}
	if len(host.Active()) != 0 {
		t.Fatalf("rolled back components remain active: %+v", host.Active())
	}
}

func TestValidateSpecsDoesNotInitialize(t *testing.T) {
	r := newRegistry()
	k := newKindOn[string](r, KindInfo{Name: "test_store", ContractVersion: "v1.0.0"})
	called := false
	registerDefinition(r, k, Define("remote", "v1.0.0", "", StrictConfig[definitionSettings](),
		func(context.Context, *Host, Config) (Component[string], error) {
			called = true
			return Static("value"), nil
		}))
	valid := map[string]Specs{"test_store": {{Name: "remote", Config: json.RawMessage(`{"endpoint":"https://service.internal"}`)}}}
	if err := validateSpecs(r, valid, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("validation initialized plugin")
	}
	if err := validateSpecs(r, map[string]Specs{"test_store": nil}, "v1.0.0"); err != nil {
		t.Fatalf("optional single-instance kind must allow zero enabled plugins: %v", err)
	}
	if err := validateSpecs(r, map[string]Specs{"test_store": {{Name: "remote", Instance: "INVALID"}}}, "v1.0.0"); err == nil {
		t.Fatal("expected invalid instance rejection")
	}
	invalid := map[string]Specs{"test_store": {{Name: "remote", Config: json.RawMessage(`{"unknown":true}`)}}}
	if err := validateSpecs(r, invalid, "v1.0.0"); err == nil {
		t.Fatal("expected invalid config rejection")
	}
	if err := validateSpecs(r, map[string]Specs{"test_store": {{Name: "missing"}}}, "v1.0.0"); err == nil {
		t.Fatal("expected unknown plugin rejection")
	}
	if err := validateSpecs(r, valid, "v0.1.0"); err != nil {
		t.Fatal(err)
	}
}
