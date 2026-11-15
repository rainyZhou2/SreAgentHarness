package workengine

import (
	"context"
	"fmt"
	"sort"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// eventSources validates the operator's event source policies.
func eventSources(m map[string]eventing.SourcePolicy) (map[string]eventing.SourcePolicy, error) {
	for name, p := range m {
		if name == "" || len(name) > 128 {
			return nil, fmt.Errorf("work.event_sources: invalid source name %q", name)
		}
		if p.MaxLateness < 0 {
			return nil, fmt.Errorf("work.event_sources.%s: max_lateness must not be negative", name)
		}
	}
	return m, nil
}

// neighbors adapts the world's neighborhood to the ingestor.
func neighbors(ws *world.Service) func(ctx context.Context, tenantID, subject string) ([]string, error) {
	return func(ctx context.Context, tenantID, subject string) ([]string, error) {
		m, err := ws.Neighbors(ctx, tenantID, subject, 1)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(m))
		for ref, d := range m {
			if d == 1 {
				out = append(out, ref)
			}
		}
		sort.Strings(out)
		return out, nil
	}
}

// changeCorrelationTool is sre.change_correlation: the changes around a
// symptom, ranked, with the checks that would confirm or dismiss each.
func changeCorrelationTool(store impact.Store, ws *world.Service, clock cpkit.Clock) tools.Tool {
	return impact.Tool(store, ws, clock)
}

// RunWaker returns the event sink that wakes Works when their runs end.
func (e *Engine) RunWaker() *orchestration.RunWaker {
	return &orchestration.RunWaker{Store: e.store, Clock: e.store.Clock, Log: e.log, Metrics: e.metrics}
}
