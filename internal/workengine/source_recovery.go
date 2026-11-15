package workengine

import (
	"fmt"
	"sort"

	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/plugin"
)

func newSourceRecovery(o Options, ingestor *orchestration.Ingestor) (*orchestration.SourceRecovery, error) {
	reconcilers := map[string]eventing.SourceReconciler{}
	for _, extension := range o.Extensions {
		contributor, ok := extension.(plugin.SourceReconcilers)
		if !ok {
			continue
		}
		for name, reconciler := range contributor.SourceReconcilers() {
			if _, exists := reconcilers[name]; exists {
				return nil, fmt.Errorf("source reconciler %q is registered twice", name)
			}
			if name == "" || reconciler == nil {
				return nil, fmt.Errorf("source reconciler %q is invalid", name)
			}
			reconcilers[name] = reconciler
		}
	}
	var targets []orchestration.SourceTarget
	for source, policy := range o.Config.EventSources {
		if policy.Disabled || reconcilers[source] == nil {
			continue
		}
		tenants := append([]string(nil), policy.Tenants...)
		sort.Strings(tenants)
		for _, tenantID := range tenants {
			if tenantID != "" {
				targets = append(targets, orchestration.SourceTarget{TenantID: tenantID, Source: source})
			}
		}
	}
	if len(targets) == 0 {
		return nil, nil
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Source == targets[j].Source {
			return targets[i].TenantID < targets[j].TenantID
		}
		return targets[i].Source < targets[j].Source
	})
	return &orchestration.SourceRecovery{Store: o.Store, Ingestor: ingestor, Reconcilers: reconcilers, Targets: targets,
		Owner: o.Owner, Clock: o.Store.Clock, Log: o.Log}, nil
}
