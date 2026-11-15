package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/plugin"
)

var validationPluginKindID atomic.Uint64

func TestCheckConfigValidatesPluginConfigWithoutInitialization(t *testing.T) {
	kind := plugin.NewKind[string](plugin.KindInfo{
		Name:            fmt.Sprintf("app_validation_test_%d", validationPluginKindID.Add(1)),
		Multi:           true,
		ContractVersion: "v1.0.0",
	})
	initialized := false
	plugin.Register(kind, plugin.Define("offline", "v1.0.0", "", plugin.StrictConfig[struct {
		Endpoint string `json:"endpoint"`
	}](), func(context.Context, *plugin.Host, plugin.Config) (plugin.Component[string], error) {
		initialized = true
		return plugin.Static("value"), nil
	}))
	config := File{Plugins: map[string]plugin.Specs{
		kind.Name(): {{Name: "offline", Config: json.RawMessage(`{"endpoint":"https://service.internal"}`)}},
	}}
	if check := pluginConfigCheck(checkConfig(config)); check == nil || check.Status != governance.Pass {
		t.Fatalf("valid plugin configuration was rejected: %+v", check)
	}
	config.Plugins[kind.Name()][0].Config = json.RawMessage(`{"unknown":true}`)
	check := pluginConfigCheck(checkConfig(config))
	if check == nil || check.Status != governance.Fail {
		t.Fatalf("invalid plugin configuration was accepted: %+v", check)
	}
	if initialized {
		t.Fatal("configuration validation initialized a plugin")
	}
}

func pluginConfigCheck(checks []governance.Check) *governance.Check {
	for i := range checks {
		if checks[i].ID == "config.plugins" {
			return &checks[i]
		}
	}
	return nil
}
