package plugin

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func validDescriptor() Descriptor {
	return Descriptor{
		Name:                "store.primary",
		Version:             "v1.2.3",
		ContractVersion:     "v1.0.0",
		MinimumCoreVersion:  "v1.0.0",
		CompatibilityPolicy: CompatibleMajor,
		PermissionScope: PermissionScope{
			Services: []string{"artifacts", "security"},
		},
		DataClasses:    []DataClass{DataInternal, DataConfidential},
		ResourceBudget: DefaultResourceBudget(),
	}
}

func TestDescriptorValidate(t *testing.T) {
	d := validDescriptor()
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*Descriptor)
	}{
		{"name", func(d *Descriptor) { d.Name = "Bad Name" }},
		{"version", func(d *Descriptor) { d.Version = "latest" }},
		{"contract", func(d *Descriptor) { d.ContractVersion = "1" }},
		{"core", func(d *Descriptor) { d.MinimumCoreVersion = "v01.0.0" }},
		{"policy", func(d *Descriptor) { d.CompatibilityPolicy = "unknown" }},
		{"service", func(d *Descriptor) { d.PermissionScope.Services = []string{"artifacts", "artifacts"} }},
		{"data class", func(d *Descriptor) { d.DataClasses = []DataClass{"unknown"} }},
		{"start timeout", func(d *Descriptor) { d.ResourceBudget.StartTimeout = 0 }},
		{"health interval", func(d *Descriptor) { d.ResourceBudget.HealthInterval = 0 }},
		{"tasks", func(d *Descriptor) { d.ResourceBudget.MaxTasks = -1 }},
		{"concurrency", func(d *Descriptor) { d.ResourceBudget.MaxConcurrent = -1 }},
		{"error code", func(d *Descriptor) { d.ErrorContract.PublicCodes = []string{"bad code"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			invalid := validDescriptor()
			tt.mutate(&invalid)
			if err := invalid.Validate(); !errors.Is(err, ErrDescriptorInvalid) {
				t.Fatalf("expected descriptor error, got %v", err)
			}
		})
	}
}

func TestDescriptorValidateSchema(t *testing.T) {
	d := validDescriptor()
	d.ConfigurationSchema = json.RawMessage(`{"type":"object"}`)
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{`[]`, `{`, `"object"`} {
		d.ConfigurationSchema = json.RawMessage(schema)
		if err := d.Validate(); !errors.Is(err, ErrDescriptorInvalid) {
			t.Fatalf("schema %s: expected descriptor error, got %v", schema, err)
		}
	}
}

func TestDescriptorCompatibility(t *testing.T) {
	d := validDescriptor()
	d.ContractVersion = "v1.1.0"
	d.MinimumCoreVersion = "v1.2.0"
	if err := d.CheckCompatibility("v1.3.0", "v1.4.0"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ core, contract string }{
		{"v1.1.0", "v1.4.0"},
		{"v1.3.0", "v2.0.0"},
		{"v1.3.0", "v1.0.0"},
	} {
		if err := d.CheckCompatibility(tc.core, tc.contract); !errors.Is(err, ErrIncompatible) {
			t.Fatalf("core=%s contract=%s: %v", tc.core, tc.contract, err)
		}
	}
	d.CompatibilityPolicy = ExactContract
	if err := d.CheckCompatibility("v1.3.0", "v1.1.0"); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckCompatibility("v1.3.0", "v1.2.0"); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("expected exact contract mismatch, got %v", err)
	}
}

func TestDevelopmentCoreCompatibility(t *testing.T) {
	d := validDescriptor()
	d.MinimumCoreVersion = "v0.0.0"
	if err := d.CheckCompatibility("dev", "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	d.MinimumCoreVersion = "v1.0.0"
	if err := d.CheckCompatibility("dev", "v1.0.0"); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("expected development core rejection, got %v", err)
	}
}

func TestDefaultResourceBudget(t *testing.T) {
	b := DefaultResourceBudget()
	if b.StartTimeout < time.Second || b.StopTimeout < time.Second || b.DrainTimeout <= b.StopTimeout {
		t.Fatalf("unsafe defaults: %+v", b)
	}
}
