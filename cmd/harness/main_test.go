package main

import (
	"testing"

	"go-sre-harness/internal/app"
	"go-sre-harness/internal/plugin"
)

func TestExampleConfigSatisfiesCompiledPluginContracts(t *testing.T) {
	config, err := app.LoadFile("../../deploy/harness.example.yaml", app.File{})
	if err != nil {
		t.Fatalf("load example config: %v", err)
	}
	if err := plugin.ValidateSpecs(config.Plugins, app.Version); err != nil {
		t.Fatalf("validate example plugins: %v", err)
	}
}
