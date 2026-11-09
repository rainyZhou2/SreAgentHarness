package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// PrometheusConfig configures the Prometheus capability: an outcome metric
// source, read tools (query, alerts, rule inventory) and a collector that
// records firing alerts as world facts.
type PrometheusConfig struct {
	capability.HTTPConfig
	// Queries are named PromQL templates usable as outcome metrics
	// ("error_rate"). ${namespace}, ${name}, ${service} and ${target} are
	// replaced from the target ref, escaped for a label matcher. A query
	// not listed here is refused: the model never writes PromQL that
	// verifies its own success.
	Queries map[string]string `json:"queries"`
	// SubjectLabels maps alert labels to subjects (see subjectFromLabels).
	SubjectLabels SubjectMapping `json:"subject_labels,omitempty"`
	// CollectEvery is the alert collection interval (default 60s, 0 off).
	CollectEvery capability.Duration `json:"collect_every,omitempty"`
	MaxSeries    int                 `json:"max_series,omitempty"`
}

// SubjectMapping derives a subject ref from labels: the first template
// whose labels are all present wins, e.g.
// ["deployment://${namespace}/${deployment}", "service://${namespace}/${service}"].
type SubjectMapping []string

var defaultMapping = SubjectMapping{
	"deployment://${namespace}/${deployment}",
	"service://${namespace}/${service}",
	"service://${namespace}/${app}",
	"service://${service}",
	"service://${job}",
}

var placeholder = regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}`)

// Subject returns the ref for labels, or "".
func (m SubjectMapping) Subject(labels map[string]string) string {
	if len(m) == 0 {
		m = defaultMapping
	}
	for _, tpl := range m {
		ok := true
		ref := placeholder.ReplaceAllStringFunc(tpl, func(s string) string {
			v := labels[placeholder.FindStringSubmatch(s)[1]]
			if v == "" || strings.ContainsAny(v, "/?#\\ ") {
				ok = false
			}
			return v
		})
		if !ok {
			continue
		}
		if n, err := cpkit.NormalizeRef(ref); err == nil {
			return n
		}
	}
	return ""
}

type prometheus struct {
	capability.Base
	cfg PrometheusConfig
	c   *capability.Client
}

func newPrometheus(cfg PrometheusConfig) (*prometheus, error) {
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("prometheus: %w", err)
	}
	for name, q := range cfg.Queries {
		if !regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`).MatchString(name) || strings.TrimSpace(q) == "" {
			return nil, fmt.Errorf("prometheus: invalid query %q", name)
		}
	}
	if cfg.MaxSeries <= 0 {
		cfg.MaxSeries = 20
	}
	if cfg.CollectEvery == 0 {
		cfg.CollectEvery = capability.Duration(time.Minute)
	}
	return &prometheus{cfg: cfg, c: c}, nil
}

func (p *prometheus) ID() string      { return "prometheus" }
func (p *prometheus) Version() string { return "1" }

func (p *prometheus) RegisterOutcomes(r capability.OutcomeRegistry) error {
	if err := r.AddMetricSource("prometheus", p); err != nil {
		return err
	}
	return r.AddMetricSource("prometheus_rules", ruleSource{p})
}

// ruleSource is the outcome check of the monitoring-config scenario: the
// query is "<group>/<rule name>" and the series is 1 while the rule is
// loaded and healthy, 0 otherwise (one sample at evaluation time — rule
// state has no history in the Prometheus API).
type ruleSource struct{ p *prometheus }

func (s ruleSource) QueryRange(ctx context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	group, name, ok := strings.Cut(q.Query, "/")
	if !ok || group == "" || name == "" {
		return outcome.MetricResult{}, fmt.Errorf("prometheus_rules: query must be <group>/<rule name>")
	}
	loaded, why, err := s.p.RuleLoaded(ctx, group, name)
	if err != nil {
		return outcome.MetricResult{}, err
	}
	v := 0.0
	if loaded {
		v = 1
	}
	ev := s.p.c.URL() + "/rules"
	if why != "" {
		ev += "#" + url.PathEscape(cpkit.Truncate(why, 200))
	}
	return outcome.MetricResult{Samples: []outcome.Sample{{T: q.End, V: v}}, EvidenceRef: ev}, nil
}

func (p *prometheus) RegisterWorld(r capability.WorldRegistry) error {
	if p.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("alerts", p.cfg.CollectEvery.D(), capability.CollectorFunc(p.collectAlerts))
}

func (p *prometheus) RegisterTools(r capability.ToolRegistry) error {
	for _, t := range []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "prometheus.query", Version: "1", Risk: domain.RiskR0, Trust: "T3",
			Source: "native", TargetArg: "query", Description: "Run a read-only PromQL instant query (bounded series).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}`)},
			Fn: p.toolQuery},
		&tools.Func{Desc: tools.Descriptor{Name: "prometheus.alerts", Version: "1", Risk: domain.RiskR0, Trust: "T3",
			Source: "native", Description: "List active alerts with their labels and subject refs.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"filter":{"type":"string"}},"additionalProperties":false}`)},
			Fn: p.toolAlerts},
		&tools.Func{Desc: tools.Descriptor{Name: "prometheus.rules", Version: "1", Risk: domain.RiskR0, Trust: "T2",
			Source: "native", Description: "Inventory of loaded alerting and recording rules (name, group, health, last error).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"group":{"type":"string"},"name":{"type":"string"}},"additionalProperties":false}`)},
			Fn: p.toolRules},
	} {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}

// ---- metric source ----

// labelValue escapes a value for a PromQL label matcher.
func labelValue(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// targetVars splits a ref into template variables.
func targetVars(ref string) map[string]string {
	scheme, path, _ := strings.Cut(ref, "://")
	parts := strings.Split(path, "/")
	v := map[string]string{"target": ref, "kind": scheme, "name": parts[len(parts)-1]}
	if len(parts) >= 2 {
		v["namespace"] = parts[0]
	}
	v["service"] = v["name"]
	return v
}

func expand(tpl string, vars map[string]string) (string, error) {
	var missing []string
	out := placeholder.ReplaceAllStringFunc(tpl, func(s string) string {
		k := placeholder.FindStringSubmatch(s)[1]
		v, ok := vars[k]
		if !ok {
			missing = append(missing, k)
		}
		return labelValue(v)
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("query needs %s, which the target does not provide", strings.Join(missing, ", "))
	}
	return out, nil
}

type promMatrix struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error"`
}

func sampleOf(p [2]any) (outcome.Sample, error) {
	ts, ok := p[0].(float64)
	vs, ok2 := p[1].(string)
	if !ok || !ok2 {
		return outcome.Sample{}, fmt.Errorf("malformed sample %v", p)
	}
	v, err := strconv.ParseFloat(vs, 64)
	if err != nil {
		return outcome.Sample{}, err
	}
	sec := int64(ts)
	return outcome.Sample{T: time.Unix(sec, int64((ts-float64(sec))*1e9)).UTC(), V: v}, nil
}

// QueryRange implements outcome.MetricSource. The query name selects an
// operator-configured template; it must yield exactly one series.
func (p *prometheus) QueryRange(ctx context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	tpl, ok := p.cfg.Queries[q.Query]
	if !ok {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: query %q is not configured", q.Query)
	}
	expr, err := expand(tpl, targetVars(q.TargetRef))
	if err != nil {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: %s: %w", q.Query, err)
	}
	span := q.End.Sub(q.Start)
	if span <= 0 || span > 24*time.Hour {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: range must be within (0, 24h]")
	}
	step := max(span/120, 15*time.Second)
	var m promMatrix
	if err := p.c.Get(ctx, "/api/v1/query_range", url.Values{"query": {expr},
		"start": {strconv.FormatInt(q.Start.Unix(), 10)}, "end": {strconv.FormatInt(q.End.Unix(), 10)},
		"step": {strconv.Itoa(int(step.Seconds()))}}, &m); err != nil {
		return outcome.MetricResult{}, err
	}
	if m.Status != "success" {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: %s", m.Error)
	}
	if len(m.Data.Result) != 1 {
		return outcome.MetricResult{}, fmt.Errorf("prometheus: query %s returned %d series, want exactly 1 (aggregate it)", q.Query, len(m.Data.Result))
	}
	var res outcome.MetricResult
	for _, v := range m.Data.Result[0].Values {
		s, err := sampleOf(v)
		if err != nil {
			return outcome.MetricResult{}, err
		}
		res.Samples = append(res.Samples, s)
	}
	res.EvidenceRef = p.c.URL() + "/graph?g0.expr=" + url.QueryEscape(expr)
	return res, nil
}

// ---- alerts ----

type promAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"`
	ActiveAt    time.Time         `json:"activeAt"`
	Value       string            `json:"value"`
}

func (p *prometheus) alerts(ctx context.Context) ([]promAlert, error) {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Alerts []promAlert `json:"alerts"`
		} `json:"data"`
	}
	if err := p.c.Get(ctx, "/api/v1/alerts", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Data.Alerts, nil
}

func (p *prometheus) collectAlerts(ctx context.Context, now time.Time) (capability.Observation, error) {
	as, err := p.alerts(ctx)
	if err != nil {
		return capability.Observation{}, err
	}
	var o capability.Observation
	for _, a := range as {
		if a.State != "firing" {
			continue
		}
		subj := p.cfg.SubjectLabels.Subject(a.Labels)
		name := strings.ToLower(predicateSafe(a.Labels["alertname"]))
		if subj == "" || name == "" {
			continue
		}
		at := a.ActiveAt
		if at.IsZero() || at.After(now) {
			at = now
		}
		o.Facts = append(o.Facts, world.Fact{SubjectRef: subj, Predicate: "alert." + name,
			Value: "firing severity=" + a.Labels["severity"], ValidFrom: at, TrustTier: world.TrustTelemetry,
			EvidenceRef: "prometheus://alerts/" + name})
	}
	return o, nil
}

var predicateBad = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func predicateSafe(s string) string { return cpkit.Truncate(predicateBad.ReplaceAllString(s, "_"), 80) }

func (p *prometheus) toolQuery(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(args, &a); err != nil || strings.TrimSpace(a.Query) == "" {
		return nil, fmt.Errorf("%w: query is required", domain.ErrNotDispatched)
	}
	var m promMatrix
	if err := p.c.Get(ctx, "/api/v1/query", url.Values{"query": {a.Query}}, &m); err != nil {
		return nil, err
	}
	type series struct {
		Labels map[string]string `json:"labels"`
		Value  float64           `json:"value"`
	}
	out := []series{}
	for i, r := range m.Data.Result {
		if i >= p.cfg.MaxSeries {
			break
		}
		s, err := sampleOf(r.Value)
		if err != nil {
			continue
		}
		out = append(out, series{Labels: r.Metric, Value: s.V})
	}
	return json.Marshal(map[string]any{"series": out, "total": len(m.Data.Result), "truncated": len(m.Data.Result) > len(out)})
}

func (p *prometheus) toolAlerts(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Filter string `json:"filter"`
	}
	_ = json.Unmarshal(args, &a)
	as, err := p.alerts(ctx)
	if err != nil {
		return nil, err
	}
	type item struct {
		Name     string            `json:"alertname"`
		State    string            `json:"state"`
		Subject  string            `json:"subject_ref,omitempty"`
		Labels   map[string]string `json:"labels"`
		Summary  string            `json:"summary,omitempty"`
		ActiveAt time.Time         `json:"active_at"`
	}
	out := []item{}
	for _, x := range as {
		if a.Filter != "" && !labelsContain(x.Labels, a.Filter) {
			continue
		}
		out = append(out, item{Name: x.Labels["alertname"], State: x.State, Subject: p.cfg.SubjectLabels.Subject(x.Labels),
			Labels: x.Labels, Summary: cpkit.Truncate(x.Annotations["summary"], 300), ActiveAt: x.ActiveAt})
		if len(out) >= 100 {
			break
		}
	}
	return json.Marshal(map[string]any{"alerts": out})
}

func labelsContain(l map[string]string, f string) bool {
	for _, v := range l {
		if strings.Contains(v, f) {
			return true
		}
	}
	return false
}

func (p *prometheus) toolRules(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Group, Name string
	}
	_ = json.Unmarshal(args, &a)
	var resp struct {
		Data struct {
			Groups []struct {
				Name  string `json:"name"`
				File  string `json:"file"`
				Rules []struct {
					Name      string            `json:"name"`
					Type      string            `json:"type"`
					Health    string            `json:"health"`
					LastError string            `json:"lastError"`
					State     string            `json:"state"`
					Query     string            `json:"query"`
					Labels    map[string]string `json:"labels"`
				} `json:"rules"`
			} `json:"groups"`
		} `json:"data"`
	}
	if err := p.c.Get(ctx, "/api/v1/rules", nil, &resp); err != nil {
		return nil, err
	}
	type rule struct {
		Group, File, Name, Type, Health, State, LastError, Query string
	}
	out := []rule{}
	for _, g := range resp.Data.Groups {
		if a.Group != "" && g.Name != a.Group {
			continue
		}
		for _, r := range g.Rules {
			if a.Name != "" && r.Name != a.Name {
				continue
			}
			out = append(out, rule{g.Name, g.File, r.Name, r.Type, r.Health, r.State, cpkit.Truncate(r.LastError, 200),
				cpkit.Truncate(r.Query, 300)})
			if len(out) >= 200 {
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group+out[i].Name < out[j].Group+out[j].Name })
	return json.Marshal(map[string]any{"rules": out})
}

// RuleLoaded reports whether a rule is loaded and healthy (outcome check of
// the monitoring-config scenario).
func (p *prometheus) RuleLoaded(ctx context.Context, group, name string) (bool, string, error) {
	raw, err := p.toolRules(ctx, "", json.RawMessage(fmt.Sprintf(`{"Group":%q,"Name":%q}`, group, name)))
	if err != nil {
		return false, "", err
	}
	var r struct {
		Rules []struct{ Health, LastError string } `json:"rules"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return false, "", err
	}
	if len(r.Rules) == 0 {
		return false, "rule not loaded", nil
	}
	if h := r.Rules[0].Health; h != "" && h != "ok" && h != "unknown" {
		return false, "rule health " + h + ": " + r.Rules[0].LastError, nil
	}
	return true, "", nil
}

// ---- Alertmanager webhook ----

// AlertmanagerConfig configures the "alertmanager" event source
// (POST /v1/sources/alertmanager/events with the Alertmanager webhook
// payload, version 4) and its default correlation rules.
type AlertmanagerConfig struct {
	SubjectLabels SubjectMapping `json:"subject_labels,omitempty"`
	// Template is the Work template for alert incidents (default
	// "incident"); Window the correlation window (default 30m).
	Template string              `json:"template,omitempty"`
	Window   capability.Duration `json:"window,omitempty"`
	// Group is the correlation group alerts share with deploy, Kubernetes
	// and ticket events on the same subject (default "service-health");
	// Lookback is how far back a new incident picks up the group's earlier
	// events, e.g. the deploy that preceded the alert (default 1h).
	Group    string              `json:"group,omitempty"`
	Lookback capability.Duration `json:"lookback,omitempty"`
	// NoRules skips the default rules (operator defines its own).
	NoRules bool `json:"no_rules,omitempty"`
}

type alertmanager struct {
	capability.Base
	cfg AlertmanagerConfig
}

func (a *alertmanager) ID() string      { return "alertmanager" }
func (a *alertmanager) Version() string { return "1" }

func (a *alertmanager) RegisterEvents(r capability.EventRegistry) error {
	if err := r.AddSource("alertmanager", capability.NormalizerFunc(a.normalize)); err != nil {
		return err
	}
	if a.cfg.NoRules {
		return nil
	}
	tpl := a.cfg.Template
	if tpl == "" {
		tpl = "incident"
	}
	window := cpkit.Duration(30 * time.Minute)
	if a.cfg.Window > 0 {
		window = cpkit.Duration(a.cfg.Window.D())
	}
	group := a.cfg.Group
	if group == "" {
		group = "service-health"
	}
	lookback := cpkit.Duration(time.Hour)
	if a.cfg.Lookback != 0 {
		lookback = cpkit.Duration(max(a.cfg.Lookback.D(), 0))
	}
	// Alerts on one subject form one incident for the window; a resolved
	// alert attaches to the incident it belongs to (it never opens one).
	if err := r.AddRule(eventing.Rule{Name: "alertmanager-firing", Group: group, Lookback: lookback, Match: eventing.Predicate{Sources: []string{"alertmanager"},
		EventTypes: []string{"alert.firing"}}, Key: "subject", Window: window, WorkKind: "incident", Template: tpl,
		Title: "${label.alertname} on ${subject}", Intent: "restore service health"}); err != nil {
		return err
	}
	return r.AddRule(eventing.Rule{Name: "alertmanager-resolved", Group: group, Match: eventing.Predicate{Sources: []string{"alertmanager"},
		EventTypes: []string{"alert.resolved"}}, Key: "subject", Window: window, Action: "attach"})
}

type amPayload struct {
	Version string `json:"version"`
	Status  string `json:"status"`
	Alerts  []struct {
		Status       string            `json:"status"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     time.Time         `json:"startsAt"`
		EndsAt       time.Time         `json:"endsAt"`
		GeneratorURL string            `json:"generatorURL"`
		Fingerprint  string            `json:"fingerprint"`
	} `json:"alerts"`
}

func (a *alertmanager) normalize(_ context.Context, tenantID string, _ http.Header, body []byte, now time.Time) ([]eventing.Envelope, error) {
	var p amPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("alertmanager: payload: %w", err)
	}
	if p.Version != "" && p.Version != "4" {
		return nil, fmt.Errorf("alertmanager: unsupported webhook version %q", p.Version)
	}
	var out []eventing.Envelope
	for _, al := range p.Alerts {
		subj := a.cfg.SubjectLabels.Subject(al.Labels)
		if subj == "" {
			subj = "alert://" + predicateSafe(al.Labels["alertname"])
		}
		fp := al.Fingerprint
		if fp == "" {
			fp = cpkit.MustHash(al.Labels)[7:23]
		}
		typ, at := "alert.firing", al.StartsAt
		if al.Status == "resolved" {
			typ, at = "alert.resolved", al.EndsAt
		}
		if at.IsZero() || at.After(now.Add(5*time.Minute)) {
			at = now
		}
		labels := map[string]string{}
		keys := make([]string, 0, len(al.Labels))
		for k := range al.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if len(labels) >= eventing.MaxLabels-2 {
				break
			}
			labels[k] = cpkit.Truncate(al.Labels[k], eventing.MaxLabelValue)
		}
		payload, _ := json.Marshal(map[string]any{"summary": cpkit.Truncate(al.Annotations["summary"], 1000),
			"description": cpkit.Truncate(al.Annotations["description"], 2000), "generator_url": al.GeneratorURL,
			"runbook_url": al.Annotations["runbook_url"]})
		out = append(out, eventing.Envelope{
			// The id changes per state change; the dedup key groups one
			// alert's firings.
			ID:       fmt.Sprintf("am:%s:%s:%d", fp, al.Status, al.StartsAt.Unix()),
			TenantID: tenantID, Source: "alertmanager", Type: typ, SubjectRef: subj, DedupKey: "am:" + fp,
			OccurredAt: at, ObservedAt: now, Confidence: 1, Labels: labels, Payload: payload})
	}
	return out, nil
}
