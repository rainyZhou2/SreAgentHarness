package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
)

// Monitoring rules are changed as code (design §5.7): a rule is drafted
// and validated without touching anything (R0), goes out as a GitOps pull
// request through a controlled action (R2), is merged by a reviewer and
// synced by the GitOps controller, and the Work only succeeds when
// Prometheus reports the rule loaded and healthy. There is no path that
// edits a Prometheus configuration directly.
//
//	monitoring.rule_validate    R0  lint a PrometheusRule document
//	monitoring.rule_propose     R0  render a candidate rule and lint it (a draft)
//	monitoring.apply_via_gitops R2  open the pull request for a validated rule file

// MonitoringConfig configures rule-as-code.
type MonitoringConfig struct {
	// RulePaths are the repository path prefixes rule files may live under
	// (they must also be inside the GitOps writable paths).
	RulePaths []string `json:"rule_paths"`
	// RequiredLabels every alert must set (default: severity).
	RequiredLabels []string `json:"required_labels,omitempty"`
	// Severities are the allowed severity values (default: critical,
	// warning, info).
	Severities []string `json:"severities,omitempty"`
	// RequireRunbook demands a runbook_url annotation on alerts.
	RequireRunbook bool `json:"require_runbook,omitempty"`
	// MaxRules bounds one document (default 200).
	MaxRules int `json:"max_rules,omitempty"`
	// PromtoolPath, when set, also runs `promtool check rules` on a
	// temporary copy of the document. The path is operator configuration;
	// the tool never runs a command taken from a request.
	PromtoolPath string `json:"promtool_path,omitempty"`
}

// LintIssue is one finding of the rule lint.
type LintIssue struct {
	Severity string `json:"severity"` // error | warning
	Group    string `json:"group,omitempty"`
	Rule     string `json:"rule,omitempty"`
	Message  string `json:"message"`
}

// LintReport is the result of validating a rule document.
type LintReport struct {
	OK       bool        `json:"ok"`
	Groups   int         `json:"groups"`
	Rules    int         `json:"rules"`
	Alerts   []string    `json:"alerts,omitempty"`
	Errors   int         `json:"errors"`
	Warnings int         `json:"warnings"`
	Issues   []LintIssue `json:"issues,omitempty"`
	Promtool string      `json:"promtool,omitempty"` // passed | failed: <output> | not configured
}

type ruleDoc struct {
	// A PrometheusRule custom resource nests the groups under spec; a plain
	// rule file has them at the top.
	Kind string `yaml:"kind"`
	Spec struct {
		Groups []ruleGroup `yaml:"groups"`
	} `yaml:"spec"`
	Groups []ruleGroup `yaml:"groups"`
}

type ruleGroup struct {
	Name     string     `yaml:"name"`
	Interval string     `yaml:"interval"`
	Rules    []ruleSpec `yaml:"rules"`
}

type ruleSpec struct {
	Alert       string            `yaml:"alert"`
	Record      string            `yaml:"record"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

var (
	alertNameRe  = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]{2,99}$`)
	recordNameRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]{2,199}$`)
	// Templated label values make one alert per distinct value of whatever
	// is interpolated: unbounded cardinality in Alertmanager.
	templatedRe = regexp.MustCompile(`\{\{|\$labels|\$value`)
	// High-cardinality identifiers that must not become alert labels.
	volatileLabels = map[string]bool{"pod": true, "instance": true, "container_id": true, "trace_id": true, "request_id": true,
		"user_id": true, "session_id": true, "ip": true, "path": true, "url": true}
)

// balanced checks brackets and quotes of an expression. It is not a PromQL
// parser; it catches the truncations and paste errors that would make the
// whole rule file fail to load (promtool, when configured, does the rest).
func balanced(expr string) error {
	var stack []rune
	pairs := map[rune]rune{')': '(', ']': '[', '}': '{'}
	var quote rune
	for i, r := range expr {
		if quote != 0 {
			if r == quote && (i == 0 || expr[i-1] != '\\') {
				quote = 0
			}
			continue
		}
		switch r {
		case '"', '\'', '`':
			quote = r
		case '(', '[', '{':
			stack = append(stack, r)
		case ')', ']', '}':
			if len(stack) == 0 || stack[len(stack)-1] != pairs[r] {
				return fmt.Errorf("unbalanced %q", r)
			}
			stack = stack[:len(stack)-1]
		}
	}
	if quote != 0 {
		return fmt.Errorf("unterminated string")
	}
	if len(stack) > 0 {
		return fmt.Errorf("unclosed %q", stack[len(stack)-1])
	}
	return nil
}

// LintRules validates a rule document against the operator's conventions.
func LintRules(cfg MonitoringConfig, content string) LintReport {
	rep := LintReport{Promtool: "not configured"}
	add := func(sev, group, rule, format string, a ...any) {
		rep.Issues = append(rep.Issues, LintIssue{Severity: sev, Group: group, Rule: rule, Message: fmt.Sprintf(format, a...)})
		if sev == "error" {
			rep.Errors++
		} else {
			rep.Warnings++
		}
	}
	if len(content) > 256<<10 {
		add("error", "", "", "document exceeds 256 KiB")
		return rep
	}
	var doc ruleDoc
	dec := yaml.NewDecoder(strings.NewReader(content))
	if err := dec.Decode(&doc); err != nil {
		add("error", "", "", "not valid YAML: %v", err)
		return rep
	}
	groups := doc.Groups
	if doc.Kind == "PrometheusRule" || len(doc.Spec.Groups) > 0 {
		groups = doc.Spec.Groups
	}
	if len(groups) == 0 {
		add("error", "", "", "no rule groups (expected groups: or a PrometheusRule with spec.groups)")
		return rep
	}
	required := cfg.RequiredLabels
	if len(required) == 0 {
		required = []string{"severity"}
	}
	severities := cfg.Severities
	if len(severities) == 0 {
		severities = []string{"critical", "warning", "info"}
	}
	maxRules := cfg.MaxRules
	if maxRules <= 0 {
		maxRules = 200
	}
	seenGroups, seenAlerts := map[string]bool{}, map[string]bool{}
	for _, g := range groups {
		rep.Groups++
		if g.Name == "" {
			add("error", "", "", "a group has no name")
		}
		if seenGroups[g.Name] {
			add("error", g.Name, "", "group name is duplicated")
		}
		seenGroups[g.Name] = true
		if g.Interval != "" {
			if d, err := time.ParseDuration(g.Interval); err != nil || d < 5*time.Second {
				add("error", g.Name, "", "interval %q is not a duration of at least 5s", g.Interval)
			}
		}
		if len(g.Rules) == 0 {
			add("error", g.Name, "", "group has no rules")
		}
		for _, r := range g.Rules {
			rep.Rules++
			name := firstNonEmptyStr(r.Alert, r.Record)
			switch {
			case (r.Alert == "") == (r.Record == ""):
				add("error", g.Name, name, "a rule needs exactly one of alert and record")
				continue
			case r.Alert != "" && !alertNameRe.MatchString(r.Alert):
				add("error", g.Name, name, "alert name must be CamelCase ([A-Z][A-Za-z0-9_]+)")
			case r.Record != "" && !recordNameRe.MatchString(r.Record):
				add("error", g.Name, name, "record name is not a valid metric name")
			}
			if strings.TrimSpace(r.Expr) == "" {
				add("error", g.Name, name, "expr is empty")
			} else if err := balanced(r.Expr); err != nil {
				add("error", g.Name, name, "expr: %v", err)
			}
			if r.Record != "" {
				if r.For != "" || len(r.Annotations) > 0 {
					add("error", g.Name, name, "a recording rule takes no for or annotations")
				}
				continue
			}
			rep.Alerts = append(rep.Alerts, r.Alert)
			if seenAlerts[g.Name+"/"+r.Alert] {
				add("warning", g.Name, name, "alert name is repeated in the group")
			}
			seenAlerts[g.Name+"/"+r.Alert] = true
			if r.For == "" {
				add("warning", g.Name, name, "no for: duration; the alert fires on a single evaluation")
			} else if _, err := time.ParseDuration(r.For); err != nil {
				add("error", g.Name, name, "for %q is not a duration", r.For)
			}
			for _, l := range required {
				if r.Labels[l] == "" {
					add("error", g.Name, name, "label %s is required", l)
				}
			}
			if sev := r.Labels["severity"]; sev != "" && !containsString(severities, sev) {
				add("error", g.Name, name, "severity %q is not one of %v", sev, severities)
			}
			for k, v := range r.Labels {
				switch {
				case templatedRe.MatchString(v):
					add("error", g.Name, name, "label %s is templated: every distinct value becomes its own alert (unbounded cardinality)", k)
				case volatileLabels[k]:
					add("warning", g.Name, name, "label %s is a high-cardinality identifier; put it in an annotation", k)
				}
			}
			if r.Annotations["summary"] == "" && r.Annotations["description"] == "" {
				add("warning", g.Name, name, "no summary or description annotation")
			}
			if r.Annotations["runbook_url"] == "" {
				sev := "warning"
				if cfg.RequireRunbook {
					sev = "error"
				}
				add(sev, g.Name, name, "no runbook_url annotation")
			}
		}
	}
	if rep.Rules > maxRules {
		add("error", "", "", "%d rules exceed the limit of %d per document", rep.Rules, maxRules)
	}
	sort.Strings(rep.Alerts)
	rep.OK = rep.Errors == 0
	return rep
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func firstNonEmptyStr(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// promtool runs `promtool check rules` on a private temporary copy. The
// binary path comes from operator configuration and the only argument is
// the temporary file: nothing from the request reaches the command line.
func promtool(ctx context.Context, bin, content string) string {
	dir, err := os.MkdirTemp("", "harness-rules-")
	if err != nil {
		return "failed: " + err.Error()
	}
	defer os.RemoveAll(dir)
	// promtool wants plain rule groups; unwrap a PrometheusRule resource.
	var doc ruleDoc
	if yaml.Unmarshal([]byte(content), &doc) == nil && len(doc.Spec.Groups) > 0 {
		var raw struct {
			Spec struct {
				Groups any `yaml:"groups"`
			} `yaml:"spec"`
		}
		if yaml.Unmarshal([]byte(content), &raw) == nil {
			if b, err := yaml.Marshal(map[string]any{"groups": raw.Spec.Groups}); err == nil {
				content = string(b)
			}
		}
	}
	file := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		return "failed: " + err.Error()
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, "check", "rules", file)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "failed: " + cpkit.Truncate(strings.ReplaceAll(string(out), dir, ""), 1500)
	}
	return "passed"
}

type monitoring struct {
	capability.Base
	cfg  MonitoringConfig
	g    *gitops     // nil: validate and propose only
	prom *prometheus // nil: no inventory check
}

func newMonitoring(cfg MonitoringConfig, g *gitops, p *prometheus) (*monitoring, error) {
	for _, rp := range cfg.RulePaths {
		if rp == "" || strings.Contains(rp, "..") || strings.HasPrefix(rp, "/") {
			return nil, fmt.Errorf("monitoring: rule path %q is invalid", rp)
		}
		if g != nil && !g.writable(path.Join(rp, "x.yaml")) {
			return nil, fmt.Errorf("monitoring: rule path %q is outside the gitops writable paths", rp)
		}
	}
	if cfg.PromtoolPath != "" && !filepath.IsAbs(cfg.PromtoolPath) {
		return nil, fmt.Errorf("monitoring: promtool_path must be absolute")
	}
	return &monitoring{cfg: cfg, g: g, prom: p}, nil
}

func (m *monitoring) ID() string      { return "monitoring" }
func (m *monitoring) Version() string { return "1" }

func (m *monitoring) lint(ctx context.Context, content string) LintReport {
	rep := LintRules(m.cfg, content)
	if rep.OK && m.cfg.PromtoolPath != "" {
		rep.Promtool = promtool(ctx, m.cfg.PromtoolPath, content)
		if rep.Promtool != "passed" {
			rep.OK = false
			rep.Errors++
			rep.Issues = append(rep.Issues, LintIssue{Severity: "error", Message: "promtool check rules failed"})
		}
	}
	return rep
}

func (m *monitoring) RegisterTools(r capability.ToolRegistry) error {
	for _, t := range []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "monitoring.rule_validate", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			Description: "Validate a PrometheusRule or rule-group YAML document against the operator's conventions. Changes nothing.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"content":{"type":"string","maxLength":262144}},"required":["content"],"additionalProperties":false}`)},
			Fn: func(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
				var a struct {
					Content string `json:"content"`
				}
				if err := json.Unmarshal(raw, &a); err != nil {
					return nil, fmt.Errorf("invalid arguments: %w", err)
				}
				return json.Marshal(m.lint(ctx, a.Content))
			}},
		&tools.Func{Desc: tools.Descriptor{Name: "monitoring.rule_propose", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			Description: "Render a candidate alert rule as YAML, validate it and compare it with the loaded rules. A draft only: nothing is written.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"group":{"type":"string"},"alert":{"type":"string"},` +
				`"expr":{"type":"string"},"for":{"type":"string"},"severity":{"type":"string"},"summary":{"type":"string"},` +
				`"runbook_url":{"type":"string"},"labels":{"type":"object","additionalProperties":{"type":"string"}}},` +
				`"required":["group","alert","expr","severity","summary"],"additionalProperties":false}`)},
			Fn: m.toolPropose},
	} {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}

func (m *monitoring) toolPropose(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Group, Alert, Expr, For, Severity, Summary string
		RunbookURL                                 string            `json:"runbook_url"`
		Labels                                     map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	labels := map[string]string{"severity": a.Severity}
	for k, v := range a.Labels {
		labels[k] = v
	}
	ann := map[string]string{"summary": a.Summary}
	if a.RunbookURL != "" {
		ann["runbook_url"] = a.RunbookURL
	}
	rule := map[string]any{"alert": a.Alert, "expr": a.Expr, "labels": labels, "annotations": ann}
	if a.For != "" {
		rule["for"] = a.For
	}
	body, err := yaml.Marshal(map[string]any{"groups": []any{map[string]any{"name": a.Group, "rules": []any{rule}}}})
	if err != nil {
		return nil, err
	}
	out := map[string]any{"content": string(body), "lint": m.lint(ctx, string(body)),
		"next": "review the draft, then propose monitoring.apply_via_gitops with this content for a rule file path"}
	if m.prom != nil {
		// An alert of the same name already loaded would be shadowed or
		// duplicated: tell the author before a pull request exists.
		if loaded, why, err := m.prom.RuleLoaded(ctx, a.Group, a.Alert); err == nil {
			out["already_loaded"] = loaded
			if why != "" {
				out["loaded_state"] = why
			}
		} else {
			out["inventory"] = "unavailable: the loaded rules could not be read"
		}
	}
	return json.Marshal(out)
}

func (m *monitoring) RegisterActions(r capability.ActionRegistry) error {
	if m.g == nil {
		return nil
	}
	return r.AddAction(&applyRules{m: m, inner: &proposeChange{m.g}})
}

// applyRules is monitoring.apply_via_gitops: the only way a monitoring
// rule reaches production. It refuses a document that fails validation or
// a path outside the rule directories, and then opens the pull request
// through the same idempotent GitOps path as any other change. Merging
// stays with the repository's reviewers.
type applyRules struct {
	m     *monitoring
	inner *proposeChange
}

func (a *applyRules) Descriptor() action.Descriptor {
	d := a.inner.Descriptor()
	d.Name, d.Risk = "monitoring.apply_via_gitops", domain.RiskR2
	d.Description = "Open a pull request that adds or changes a validated Prometheus rule file. The rule takes effect only after review, merge and GitOps sync."
	d.BlastRadius = "alerting_rules"
	d.Compensation = "gitops.revert"
	return d
}

func (a *applyRules) guard(ctx context.Context, p action.Plan) (LintReport, error) {
	fp, args, err := a.inner.check(p)
	if err != nil {
		return LintReport{}, err
	}
	inRules := false
	for _, rp := range a.m.cfg.RulePaths {
		inRules = inRules || strings.HasPrefix(fp, strings.TrimSuffix(rp, "/")+"/")
	}
	if !inRules {
		return LintReport{}, capability.NotApplied("%s is not under a monitoring rule path", fp)
	}
	if ext := path.Ext(fp); ext != ".yaml" && ext != ".yml" {
		return LintReport{}, capability.NotApplied("rule files are YAML")
	}
	rep := a.m.lint(ctx, args.Content)
	if !rep.OK {
		var msgs []string
		for _, i := range rep.Issues {
			if i.Severity == "error" {
				msgs = append(msgs, strings.TrimSpace(i.Group+"/"+i.Rule+": "+i.Message))
			}
		}
		return rep, capability.NotApplied("the rule document fails validation: %s", cpkit.Truncate(strings.Join(msgs, "; "), 600))
	}
	return rep, nil
}

func (a *applyRules) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	rep, err := a.guard(ctx, p)
	if err != nil {
		return refused(err.Error()), nil
	}
	pf, err := a.inner.Preflight(ctx, p)
	if err == nil && pf.Allowed {
		pf.EvidenceRefs = append(pf.EvidenceRefs, fmt.Sprintf("lint:%d rules, %d alerts, %d warnings, promtool %s", rep.Rules,
			len(rep.Alerts), rep.Warnings, rep.Promtool))
	}
	return pf, err
}

func (a *applyRules) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	// Validate again at dispatch: the conventions may have tightened since
	// the plan was approved, and nothing has been written yet.
	if _, err := a.guard(ctx, p); err != nil {
		return nil, capability.BeforeWrite(err)
	}
	return a.inner.Execute(ctx, p, key)
}

func (a *applyRules) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	return a.inner.Inspect(ctx, p)
}

// MonitoringRuleTemplate is the rule-as-code scenario (design §18.2):
// draft and validate, apply through GitOps with approval, wait for the
// merge and sync, then verify Prometheus loaded the rule.
func MonitoringRuleTemplate() work.Template {
	return work.Template{ID: "monitoring-rule", Version: 1, Kind: work.KindMonitoring, Stages: []work.Stage{
		{ID: "draft", Type: work.StageRun, Phase: "assess", Run: &work.RunSpec{Goal: "Monitoring request for ${subjects}: " +
			"${goal.intent}. ${goal.description}\nCheck which metrics exist and which rules are loaded (obs.sources, " +
			"prometheus.rules). Draft the rule with monitoring.rule_propose and fix every lint error " +
			"(monitoring.rule_validate). Then propose {\"proposed_action\":{\"name\":\"monitoring.apply_via_gitops\"," +
			"\"target\":\"<the git:// rule file subject>\",\"args\":{\"content\":<yaml>,\"title\":...,\"description\":...}}}."}},
		{ID: "apply", Type: work.StageAction, Phase: "mitigate", OnFailure: "draft", Action: &work.ActionSpec{
			FromProposal: "draft", Allowed: []string{"monitoring.apply_via_gitops"}}},
		{ID: "merge", Type: work.StageConfirm, Phase: "review", Confirm: &work.ConfirmSpec{
			Prompt: "Review and merge the pull request, let the GitOps controller sync it, then confirm."}},
		{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: "draft", OnTimeout: work.OnFailBlock,
			Verify: &work.VerifySpec{RecheckEvery: cpkit.Duration(time.Minute), Timeout: cpkit.Duration(time.Hour)}},
	}}
}

func (m *monitoring) WorkTemplates() []work.Template {
	if m.g == nil {
		return nil
	}
	return []work.Template{MonitoringRuleTemplate()}
}
