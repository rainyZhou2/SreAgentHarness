package workengine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/api"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/store/cpsql"
)

type recoveryExtension struct {
	source string
}

func (recoveryExtension) Actions() []action.Executor                     { return nil }
func (recoveryExtension) MetricSources() map[string]outcome.MetricSource { return nil }
func (e recoveryExtension) SourceReconcilers() map[string]eventing.SourceReconciler {
	return map[string]eventing.SourceReconciler{e.source: sourceReconcilerStub{}}
}

type sourceReconcilerStub struct{}

func (sourceReconcilerStub) Reconcile(context.Context, eventing.SourceReconcileRequest) (eventing.SourceReconcilePage, error) {
	return eventing.SourceReconcilePage{}, nil
}

func TestSourceRecoveryAssembly(t *testing.T) {
	store, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	kernelStore, err := localstore.Open(filepath.Join(t.TempDir(), "kernel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer kernelStore.Close()
	newEngine := func(extensions ...recoveryExtension) (*Engine, error) {
		values := make([]plugin.WorkExtension, len(extensions))
		for i := range extensions {
			values[i] = extensions[i]
		}
		return New(Options{Store: store, Server: &api.Server{Store: kernelStore}, Extensions: values,
			Flags: Flags{Engine: true, Actions: false}, Config: Config{EventSources: map[string]eventing.SourcePolicy{
				"alerts":       {Tenants: []string{"t2", "t1"}},
				"disabled":     {Tenants: []string{"t1"}, Disabled: true},
				"webhook-only": {Tenants: []string{"t1"}},
			}}})
	}
	eng, err := newEngine(recoveryExtension{source: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if eng.SourceRecovery == nil {
		t.Fatal("source recovery was not assembled")
	}
	want := []orchestration.SourceTarget{{TenantID: "t1", Source: "alerts"}, {TenantID: "t2", Source: "alerts"}}
	if len(eng.SourceRecovery.Targets) != len(want) {
		t.Fatalf("targets=%v", eng.SourceRecovery.Targets)
	}
	for i := range want {
		if eng.SourceRecovery.Targets[i] != want[i] {
			t.Fatalf("targets=%v", eng.SourceRecovery.Targets)
		}
	}
	if eng.SourceRecovery.Reconcilers["alerts"] == nil {
		t.Fatal("registered reconciler is missing")
	}
	_, err = newEngine(recoveryExtension{source: "alerts"}, recoveryExtension{source: "alerts"})
	if err == nil || !strings.Contains(err.Error(), "registered twice") {
		t.Fatalf("duplicate reconciler err=%v", err)
	}
}
