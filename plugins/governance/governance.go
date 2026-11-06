// Package governance registers the built-in policy, grader and governance
// job plugins (rollout monitor, retention).
package governance

import (
	"context"
	"errors"
	"time"

	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/retention"
)

type PolicyConfig struct {
	Version                string          `json:"version,omitempty"`
	Environment            string          `json:"environment,omitempty"`
	ApprovalTTL            plugin.Duration `json:"approval_ttl,omitempty"`
	AllowR1WithoutApproval bool            `json:"allow_r1_without_approval,omitempty"`
}

type IntervalConfig struct {
	Interval plugin.Duration `json:"interval,omitempty"`
}

type RetentionConfig struct {
	Interval        plugin.Duration `json:"interval,omitempty"`
	OutboxRetention plugin.Duration `json:"outbox_retention,omitempty"`
	AuditRetention  plugin.Duration `json:"audit_retention,omitempty"` // 0 = keep forever
	Batch           int             `json:"batch,omitempty"`
}

func init() {
	plugin.Register(plugin.Policy, "default", "risk-class policy: R0 allow, R1 allow outside production, R2 approval, R3 deny",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (policy.Gate, error) {
			d := policy.Default()
			cfg := PolicyConfig{Version: d.Version, Environment: d.Environment, ApprovalTTL: plugin.Duration(d.ApprovalTTL)}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return policy.Engine{Version: cfg.Version, Environment: cfg.Environment, ApprovalTTL: cfg.ApprovalTTL.D(),
				AllowR1WithoutApproval: cfg.AllowR1WithoutApproval}, nil
		})
	plugin.Register(plugin.Grader, "substring", "root cause hit = answer contains a ground-truth label (case-insensitive)",
		func(context.Context, *plugin.Host, plugin.Config) (eval.Grader, error) {
			return eval.SubstringGrader, nil
		})
	plugin.Register(plugin.Job, "rollout_monitor", "auto-rolls back canaries that breach guardrails",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Runner, error) {
			cfg := IntervalConfig{Interval: plugin.Duration(time.Minute)}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if h.Eval == nil {
				return nil, errors.New("evaluation service unavailable")
			}
			svc, every := h.Eval, cfg.Interval.D()
			return plugin.RunnerFunc(func(ctx context.Context) error { return svc.Run(ctx, every) }), nil
		})
	plugin.Register(plugin.Job, "retention", "deletes published outbox rows, expired segments/memories and old audit entries",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (plugin.Runner, error) {
			cfg := RetentionConfig{Interval: plugin.Duration(5 * time.Minute), OutboxRetention: plugin.Duration(24 * time.Hour)}
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return &retention.Cleaner{Store: h.Store, Artifacts: h.Artifacts, Log: h.Log, Observer: h.Metrics,
				Cfg: retention.Config{Interval: cfg.Interval.D(), OutboxRetention: cfg.OutboxRetention.D(),
					AuditRetention: cfg.AuditRetention.D(), Batch: cfg.Batch}}, nil
		})
}
