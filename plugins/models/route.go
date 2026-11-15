package models

import (
	"context"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/plugin"
)

func init() {
	plugin.Register(plugin.RoutePolicy, plugin.StaticDefinition("priority", "v1.0.0", "endpoint priority, then alias order and weight, minus recent failures (default)", nil,
		func(context.Context, *plugin.Host, plugin.Config) (gw.Scorer, error) { return gw.PriorityScorer{}, nil }))
	plugin.Register(plugin.RoutePolicy, plugin.StaticDefinition("weighted", "v1.0.0", "configurable weights for priority, alias rank, expected cost, failures and context headroom",
		plugin.StrictConfig[gw.WeightedScorer](),
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (gw.Scorer, error) {
			s := gw.DefaultWeightedScorer()
			if err := c.Decode(&s); err != nil {
				return nil, err
			}
			return s, nil
		}))
}
