package sre

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/action/actiontest"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/domain"
)

const goodRule = `groups:
- name: search-slo
  rules:
  - alert: SearchLatencyP99High
    expr: histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{service="search-api"}[5m]))) > 0.8
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: search-api P99 latency is above 800ms
      runbook_url: https://runbooks.example.com/search-latency
`

func TestLintRules(t *testing.T) {
	if rep := LintRules(MonitoringConfig{}, goodRule); !rep.OK || rep.Rules != 1 || len(rep.Alerts) != 1 || rep.Warnings != 0 {
		t.Fatalf("a good rule must pass cleanly: %+v", rep)
	}
	wrapped := "apiVersion: monitoring.coreos.com/v1\nkind: PrometheusRule\nspec:\n  " + strings.ReplaceAll(goodRule, "\n", "\n  ")
	if rep := LintRules(MonitoringConfig{}, wrapped); !rep.OK || rep.Rules != 1 {
		t.Fatalf("a PrometheusRule resource must lint like its groups: %+v", rep)
	}
	for name, c := range map[string]struct{ doc, want string }{
		"bad yaml":          {"groups: [", "not valid YAML"},
		"no groups":         {"foo: bar", "no rule groups"},
		"unbalanced expr":   {strings.Replace(goodRule, "> 0.8", "> (0.8", 1), "unclosed"},
		"missing severity":  {strings.Replace(goodRule, "      severity: critical\n", "      team: search\n", 1), "label severity is required"},
		"unknown severity":  {strings.Replace(goodRule, "critical", "sev0", 1), "is not one of"},
		"templated label":   {strings.Replace(goodRule, "severity: critical", "severity: critical\n      pod: '{{ $labels.pod }}'", 1), "unbounded cardinality"},
		"bad for":           {strings.Replace(goodRule, "for: 5m", "for: soon", 1), "is not a duration"},
		"alert and record":  {strings.Replace(goodRule, "  - alert: SearchLatencyP99High", "  - alert: SearchLatencyP99High\n    record: x:y:z", 1), "exactly one of alert and record"},
		"lower-case alert":  {strings.Replace(goodRule, "SearchLatencyP99High", "search_latency", 1), "CamelCase"},
		"duplicated groups": {goodRule + strings.TrimPrefix(goodRule, "groups:\n"), "duplicated"},
	} {
		rep := LintRules(MonitoringConfig{}, c.doc)
		got, _ := json.Marshal(rep.Issues)
		if rep.OK || !strings.Contains(string(got), c.want) {
			t.Errorf("%s: ok=%v issues=%s", name, rep.OK, got)
		}
	}
	strict := LintRules(MonitoringConfig{RequireRunbook: true}, strings.Replace(goodRule, "      runbook_url: https://runbooks.example.com/search-latency\n", "", 1))
	if strict.OK {
		t.Fatal("require_runbook must make a missing runbook an error")
	}
}

// §5.7 / PR-14: a rule reaches the repository only as a validated pull
// request through the R2 action; an invalid document or a path outside the
// rule directories writes nothing.
func TestMonitoringApplyViaGitOps(t *testing.T) {
	gh := newFakeGitHub()
	fl := &faulty{next: gh.handler}
	srv := serve(t, fl.handler)
	set, err := Build(Config{Tenant: "acme",
		GitOps:     &GitOpsConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}, Owner: "acme", Repo: "deploy", Paths: []string{"monitoring/", "apps/"}},
		Monitoring: &MonitoringConfig{RulePaths: []string{"monitoring/rules"}}}, capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := action.NewRegistry()
	var apply action.Executor
	for _, a := range set.Actions() {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
		if a.Descriptor().Name == "monitoring.apply_via_gitops" {
			apply = a
		}
	}
	if apply == nil || apply.Descriptor().Risk != domain.RiskR2 {
		t.Fatalf("monitoring.apply_via_gitops must be registered as R2")
	}
	names := map[string]bool{}
	for _, tl := range set.Tools() {
		names[tl.Descriptor().Name] = tl.Descriptor().Risk == domain.RiskR0
	}
	if !names["monitoring.rule_validate"] || !names["monitoring.rule_propose"] {
		t.Fatalf("read-only rule tools: %v", names)
	}
	const file = "git://acme/deploy/monitoring/rules/search.yaml"
	for name, bad := range map[string]action.Plan{
		"invalid rule":  plan("monitoring.apply_via_gitops", file, map[string]string{"content": strings.Replace(goodRule, "severity: critical", "team: x", 1), "title": "t"}),
		"outside rules": plan("monitoring.apply_via_gitops", "git://acme/deploy/apps/checkout/values.yaml", map[string]string{"content": goodRule, "title": "t"}),
		"not yaml file": plan("monitoring.apply_via_gitops", "git://acme/deploy/monitoring/rules/search.json", map[string]string{"content": goodRule, "title": "t"}),
	} {
		if pf, _ := apply.Preflight(ctx, bad); pf.Allowed {
			t.Errorf("%s: preflight allowed", name)
		}
		if _, err := apply.Execute(ctx, bad, "k-"+name); err == nil {
			t.Errorf("%s: executed", name)
		}
	}
	if gh.puts != 0 || len(gh.pulls) != 0 {
		t.Fatalf("a refused rule wrote to the repository: puts=%d pulls=%d", gh.puts, len(gh.pulls))
	}
	if tp := MonitoringRuleTemplate(); tp.Normalize() != nil {
		t.Fatal("monitoring-rule template invalid")
	}

	n := 0
	rep := actiontest.Certify(t, actiontest.Case{Executor: apply, Registry: reg, Inject: fl.inject,
		Reset: func() {
			gh.mu.Lock()
			gh.branches, gh.pulls, gh.puts = map[string]bool{"main": true}, nil, 0
			gh.files = map[string]map[string]string{"main": {"apps/checkout/values.yaml": "image: v2\n"}}
			gh.mu.Unlock()
		},
		Plan: func() action.Plan {
			n++
			p := plan("monitoring.apply_via_gitops", file, map[string]string{"content": goodRule, "title": "Add search P99 alert"})
			p.EffectKey = fmt.Sprintf("rule-%d", n)
			return p
		},
		State: func() string {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			return fmt.Sprintf("branches=%d pulls=%d puts=%d", len(gh.branches), len(gh.pulls), gh.puts)
		}})
	if len(rep.Checked) < 5 {
		t.Fatalf("contract checks: %v", rep.Checked)
	}
}
