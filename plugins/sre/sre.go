// Package sre is the production SRE domain pack for the v2 control plane:
// real adapters for Prometheus/Alertmanager, Kubernetes, OpenTelemetry,
// GitOps (GitHub + Argo CD), Istio traffic, a CMDB and a Jira-compatible
// tracker, plus the AI-infrastructure capabilities. Each adapter is a
// capability.Capability; the plugin assembles the configured ones into a
// capability.Set, which enforces the domain hard limits (read tools are
// R0, R3 actions are refused, operator risk floors only raise risk, fact
// trust is capped).
//
// Configuration (work_extension "sre"):
//
//	{"name": "sre", "config": {
//	  "tenant": "acme",
//	  "prometheus":   {"url": "http://prometheus:9090", "queries": {...}},
//	  "alertmanager": {},
//	  "kubernetes":   {"in_cluster": true, "namespaces": ["prod"], "dry_run": false},
//	  "traffic":      {"max_step": 50},
//	  "otel":         {"url": "http://jaeger-query:16686"},
//	  "gitops":       {"owner": "acme", "repo": "deploy", "paths": ["apps/"], "token_env": "GITHUB_TOKEN", "argocd": {}},
//	  "cmdb":         {"url": "https://cmdb.internal"},
//	  "ticket":       {"url": "https://jira.internal", "projects": ["OPS"], "token_env": "JIRA_TOKEN"},
//	  "risk_floor":   {"deployment.scale": "R2"},
//	  "authoritative": ["cmdb"],
//	  "disabled":     ["node.drain"]
//	}}
package sre

import (
	"context"
	"fmt"

	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/plugin"
)

// Config selects and configures the adapters. A nil section is disabled.
type Config struct {
	Tenant       string              `json:"tenant"`
	Prometheus   *PrometheusConfig   `json:"prometheus,omitempty"`
	Alertmanager *AlertmanagerConfig `json:"alertmanager,omitempty"`
	Kubernetes   *KubernetesConfig   `json:"kubernetes,omitempty"`
	// Traffic needs Kubernetes (it uses the same API client).
	Traffic *TrafficConfig `json:"traffic,omitempty"`
	OTel    *OTelConfig    `json:"otel,omitempty"`
	GitOps  *GitOpsConfig  `json:"gitops,omitempty"`
	CMDB    *CMDBConfig    `json:"cmdb,omitempty"`
	Ticket  *TicketConfig  `json:"ticket,omitempty"`
	AIInfra *AIInfraConfig `json:"ai_infra,omitempty"`
	// Monitoring enables rule-as-code (validate, draft, apply through
	// GitOps). The apply action needs the gitops section.
	Monitoring *MonitoringConfig `json:"monitoring,omitempty"`

	RiskFloor     map[string]domain.RiskClass `json:"risk_floor,omitempty"`
	Authoritative []string                    `json:"authoritative,omitempty"`
	Disabled      []string                    `json:"disabled,omitempty"`
}

func init() {
	plugin.Register(plugin.WorkExt, "sre", "SRE domain pack: Prometheus/Alertmanager, Kubernetes, OTel, GitOps/Argo CD, Istio traffic, CMDB, Jira, AI infra",
		func(ctx context.Context, h *plugin.Host, c plugin.Config) (plugin.WorkExtension, error) {
			var cfg Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			opts := capability.Options{}
			if h != nil {
				opts.Log = h.Log
			}
			set, err := Build(cfg, opts)
			if err != nil {
				return nil, err
			}
			// Read tools join the shared registry so agents can call them
			// from stage runs; an existing tool of the same name wins only
			// by failing the boot (no silent shadowing).
			if h != nil && h.Tools != nil {
				for _, t := range set.Tools() {
					if _, dup := h.Tools.Lookup(t.Descriptor().Name); dup {
						return nil, fmt.Errorf("sre: tool %q is already registered", t.Descriptor().Name)
					}
				}
				for _, t := range set.Tools() {
					h.Tools.Register(t)
				}
			}
			return set, nil
		})
}

// Build assembles the configured capabilities. opts.Tenant, RiskFloor,
// Authoritative and Disabled are taken from cfg.
func Build(cfg Config, opts capability.Options) (*capability.Set, error) {
	caps, err := Capabilities(cfg)
	if err != nil {
		return nil, err
	}
	if len(caps) == 0 {
		return nil, fmt.Errorf("sre: no adapter is configured")
	}
	opts.Tenant = cfg.Tenant
	if opts.Tenant == "" {
		opts.Tenant = "local"
	}
	opts.RiskFloor = cfg.RiskFloor
	opts.Authoritative = map[string]bool{}
	for _, id := range cfg.Authoritative {
		opts.Authoritative[id] = true
	}
	opts.Disabled = map[string]bool{}
	for _, n := range cfg.Disabled {
		opts.Disabled[n] = true
	}
	return capability.NewSet(opts, caps...)
}

// Capabilities constructs the configured adapters.
func Capabilities(cfg Config) ([]capability.Capability, error) {
	var caps []capability.Capability
	var prom *prometheus
	if cfg.Prometheus != nil {
		p, err := newPrometheus(*cfg.Prometheus)
		if err != nil {
			return nil, err
		}
		prom = p
		caps = append(caps, p)
	}
	if cfg.Alertmanager != nil {
		caps = append(caps, &alertmanager{cfg: *cfg.Alertmanager})
	}
	var k *kubernetes
	if cfg.Kubernetes != nil {
		var err error
		if k, err = newKubernetes(*cfg.Kubernetes); err != nil {
			return nil, err
		}
		caps = append(caps, k)
	}
	if cfg.Traffic != nil {
		if k == nil {
			return nil, fmt.Errorf("sre: traffic requires the kubernetes section")
		}
		caps = append(caps, newTraffic(*cfg.Traffic, k.k))
	}
	if cfg.OTel != nil {
		o, err := newOTel(*cfg.OTel)
		if err != nil {
			return nil, err
		}
		caps = append(caps, o)
	}
	var git *gitops
	if cfg.GitOps != nil {
		g, err := newGitOps(*cfg.GitOps)
		if err != nil {
			return nil, err
		}
		git = g
		caps = append(caps, g)
	}
	if cfg.Monitoring != nil {
		m, err := newMonitoring(*cfg.Monitoring, git, prom)
		if err != nil {
			return nil, err
		}
		caps = append(caps, m)
	}
	if cfg.CMDB != nil {
		c, err := newCMDB(*cfg.CMDB)
		if err != nil {
			return nil, err
		}
		caps = append(caps, c)
	}
	if cfg.Ticket != nil {
		t, err := newTicket(*cfg.Ticket)
		if err != nil {
			return nil, err
		}
		caps = append(caps, t)
	}
	if cfg.AIInfra != nil {
		ai, err := newAIInfra(*cfg.AIInfra, k)
		if err != nil {
			return nil, err
		}
		caps = append(caps, ai...)
	}
	return caps, nil
}
