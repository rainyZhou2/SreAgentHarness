package workengine

import (
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/store/cpsql"
)

// SchedulingConfig is the "work.scheduling" section (design §6.3).
type SchedulingConfig struct {
	// Aging raises a waiting Work's effective priority by one per interval,
	// up to MaxBoost (default 2m and 20; "0" disables aging).
	Aging    *cpkit.Duration `json:"aging,omitempty"`
	MaxBoost int             `json:"max_boost,omitempty"`
	// TenantWeights enables weighted fair queuing between tenants.
	TenantWeights map[string]int `json:"tenant_weights,omitempty"`
	FairTenants   *bool          `json:"fair_tenants,omitempty"`
}

func (c *SchedulingConfig) fairness() cpsql.Fairness {
	f := cpsql.Fairness{Aging: 2 * time.Minute, MaxBoost: 20, FairTenants: true}
	if c == nil {
		return f
	}
	if c.Aging != nil {
		f.Aging = c.Aging.D()
	}
	if c.MaxBoost > 0 {
		f.MaxBoost = min(c.MaxBoost, 200)
	}
	f.TenantWeights = c.TenantWeights
	if c.FairTenants != nil {
		f.FairTenants = *c.FairTenants
	}
	return f
}
