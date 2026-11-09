package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// OTelConfig configures the OpenTelemetry capability against a
// Jaeger-compatible query API (Jaeger, or Grafana Tempo's Jaeger query
// endpoint). It is read-only: the service graph becomes "calls" relations,
// traces are searchable, and "trace_error_ratio" verifies recovery.
type OTelConfig struct {
	capability.HTTPConfig
	// Namespace prefixes service refs (service://<namespace>/<service>)
	// when the tracer's service names carry no namespace.
	Namespace    string              `json:"namespace,omitempty"`
	CollectEvery capability.Duration `json:"collect_every,omitempty"`
	// Lookback is the service-graph window (default 1h).
	Lookback capability.Duration `json:"lookback,omitempty"`
	// SampleTraces bounds traces fetched per metric bucket (default 200).
	SampleTraces int `json:"sample_traces,omitempty"`
}

type otel struct {
	capability.Base
	cfg OTelConfig
	c   *capability.Client
}

func newOTel(cfg OTelConfig) (*otel, error) {
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("otel: %w", err)
	}
	if cfg.CollectEvery == 0 {
		cfg.CollectEvery = capability.Duration(5 * time.Minute)
	}
	if cfg.Lookback <= 0 {
		cfg.Lookback = capability.Duration(time.Hour)
	}
	if cfg.SampleTraces <= 0 {
		cfg.SampleTraces = 200
	}
	return &otel{cfg: cfg, c: c}, nil
}

func (o *otel) ID() string      { return "opentelemetry" }
func (o *otel) Version() string { return "1" }

func (o *otel) RegisterWorld(r capability.WorldRegistry) error {
	if o.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("service-graph", o.cfg.CollectEvery.D(), capability.CollectorFunc(o.collect))
}

func (o *otel) RegisterOutcomes(r capability.OutcomeRegistry) error {
	return r.AddMetricSource("otel", o)
}

func (o *otel) RegisterTools(r capability.ToolRegistry) error {
	for _, t := range []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "otel.traces", Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
			TargetArg: "service", Description: "Search recent traces of a service (optionally errors only); returns span summaries.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"},"operation":{"type":"string"},` +
				`"errors_only":{"type":"boolean"},"minutes":{"type":"integer","minimum":1,"maximum":1440},"limit":{"type":"integer","minimum":1,"maximum":50}},` +
				`"required":["service"],"additionalProperties":false}`)},
			Fn: o.toolTraces},
		&tools.Func{Desc: tools.Descriptor{Name: "otel.dependencies", Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
			Description: "Service dependency edges (caller -> callee, call count) over the lookback window.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"additionalProperties":false}`)},
			Fn: o.toolDeps},
	} {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}

func (o *otel) serviceRef(name string) string {
	if strings.ContainsAny(name, "/?#\\ ") || name == "" {
		return ""
	}
	ref := "service://" + name
	if o.cfg.Namespace != "" {
		ref = "service://" + o.cfg.Namespace + "/" + name
	}
	n, err := cpkit.NormalizeRef(ref)
	if err != nil {
		return ""
	}
	return n
}

// serviceName maps a ref back to the tracer's service name.
func (o *otel) serviceName(ref string) string {
	rest := strings.TrimPrefix(ref, "service://")
	if o.cfg.Namespace != "" {
		rest = strings.TrimPrefix(rest, o.cfg.Namespace+"/")
	}
	if i := strings.LastIndex(rest, "/"); i >= 0 {
		rest = rest[i+1:]
	}
	return rest
}

type dependency struct {
	Parent    string `json:"parent"`
	Child     string `json:"child"`
	CallCount int64  `json:"callCount"`
}

func (o *otel) deps(ctx context.Context, now time.Time) ([]dependency, error) {
	var resp struct {
		Data []dependency `json:"data"`
	}
	err := o.c.Get(ctx, "/api/dependencies", url.Values{"endTs": {strconv.FormatInt(now.UnixMilli(), 10)},
		"lookback": {strconv.FormatInt(o.cfg.Lookback.D().Milliseconds(), 10)}}, &resp)
	return resp.Data, err
}

func (o *otel) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	ds, err := o.deps(ctx, now)
	if err != nil {
		return capability.Observation{}, err
	}
	var obs capability.Observation
	for _, d := range ds {
		from, to := o.serviceRef(d.Parent), o.serviceRef(d.Child)
		if from == "" || to == "" || from == to {
			continue
		}
		obs.Relations = append(obs.Relations, world.Relation{FromRef: from, Type: "calls", ToRef: to})
	}
	return obs, nil
}

type jaegerTrace struct {
	TraceID string `json:"traceID"`
	Spans   []struct {
		SpanID        string `json:"spanID"`
		OperationName string `json:"operationName"`
		StartTime     int64  `json:"startTime"` // µs
		Duration      int64  `json:"duration"`  // µs
		ProcessID     string `json:"processID"`
		Tags          []struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		} `json:"tags"`
	} `json:"spans"`
	Processes map[string]struct {
		ServiceName string `json:"serviceName"`
	} `json:"processes"`
}

func (t jaegerTrace) hasError() bool {
	for _, s := range t.Spans {
		for _, tag := range s.Tags {
			switch tag.Key {
			case "error":
				if b, ok := tag.Value.(bool); ok && b {
					return true
				}
				if s, ok := tag.Value.(string); ok && s == "true" {
					return true
				}
			case "otel.status_code":
				if s, ok := tag.Value.(string); ok && s == "ERROR" {
					return true
				}
			}
		}
	}
	return false
}

func (o *otel) traces(ctx context.Context, service, operation string, start, end time.Time, limit int, errorsOnly bool) ([]jaegerTrace, error) {
	q := url.Values{"service": {service}, "start": {strconv.FormatInt(start.UnixMicro(), 10)},
		"end": {strconv.FormatInt(end.UnixMicro(), 10)}, "limit": {strconv.Itoa(limit)}}
	if operation != "" {
		q.Set("operation", operation)
	}
	if errorsOnly {
		q.Set("tags", `{"error":"true"}`)
	}
	var resp struct {
		Data []jaegerTrace `json:"data"`
	}
	err := o.c.Get(ctx, "/api/traces", q, &resp)
	return resp.Data, err
}

// QueryRange implements outcome.MetricSource: "trace_error_ratio" is the
// share of sampled traces of the target service with an error span, per
// bucket. It is a sample, not a rate: use it as a recovery check next to
// a metrics-based objective.
func (o *otel) QueryRange(ctx context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	if q.Query != "trace_error_ratio" {
		return outcome.MetricResult{}, fmt.Errorf("otel: unknown query %q (supported: trace_error_ratio)", q.Query)
	}
	svc := o.serviceName(q.TargetRef)
	span := q.End.Sub(q.Start)
	if span <= 0 || span > 6*time.Hour {
		return outcome.MetricResult{}, fmt.Errorf("otel: range must be within (0, 6h]")
	}
	buckets := min(max(int(span/time.Minute), 1), 12)
	step := span / time.Duration(buckets)
	var res outcome.MetricResult
	for i := 0; i < buckets; i++ {
		s, e := q.Start.Add(time.Duration(i)*step), q.Start.Add(time.Duration(i+1)*step)
		ts, err := o.traces(ctx, svc, "", s, e, o.cfg.SampleTraces, false)
		if err != nil {
			return outcome.MetricResult{}, err
		}
		if len(ts) == 0 {
			continue
		}
		bad := 0
		for _, t := range ts {
			if t.hasError() {
				bad++
			}
		}
		res.Samples = append(res.Samples, outcome.Sample{T: e, V: float64(bad) / float64(len(ts))})
	}
	res.EvidenceRef = o.c.URL() + "/search?service=" + url.QueryEscape(svc)
	return res, nil
}

func (o *otel) toolTraces(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Service, Operation string
		ErrorsOnly         bool `json:"errors_only"`
		Minutes, Limit     *int
	}
	if err := json.Unmarshal(args, &a); err != nil || a.Service == "" {
		return nil, fmt.Errorf("%w: service is required", domain.ErrNotDispatched)
	}
	minutes, limit := 30, 20
	if a.Minutes != nil {
		minutes = min(max(*a.Minutes, 1), 1440)
	}
	if a.Limit != nil {
		limit = min(max(*a.Limit, 1), 50)
	}
	end := time.Now()
	ts, err := o.traces(ctx, o.serviceName("service://"+a.Service), a.Operation, end.Add(-time.Duration(minutes)*time.Minute), end, limit, a.ErrorsOnly)
	if err != nil {
		return nil, err
	}
	type spanSum struct {
		Service, Operation string
		DurationMS         float64
		Error              bool
	}
	type traceSum struct {
		TraceID string    `json:"trace_id"`
		Root    string    `json:"root"`
		Spans   int       `json:"spans"`
		Error   bool      `json:"error"`
		MS      float64   `json:"duration_ms"`
		Slowest []spanSum `json:"slowest"`
	}
	out := []traceSum{}
	for _, t := range ts {
		sum := traceSum{TraceID: t.TraceID, Spans: len(t.Spans), Error: t.hasError()}
		var all []spanSum
		var first int64 = 1<<62 - 1
		var last int64
		for _, s := range t.Spans {
			first, last = min(first, s.StartTime), max(last, s.StartTime+s.Duration)
			all = append(all, spanSum{Service: t.Processes[s.ProcessID].ServiceName, Operation: s.OperationName,
				DurationMS: float64(s.Duration) / 1000})
		}
		sum.MS = float64(last-first) / 1000
		sort.Slice(all, func(i, j int) bool { return all[i].DurationMS > all[j].DurationMS })
		if len(all) > 0 {
			sum.Root = all[0].Service + " " + all[0].Operation
		}
		sum.Slowest = all[:min(len(all), 5)]
		out = append(out, sum)
	}
	return json.Marshal(map[string]any{"traces": out})
}

func (o *otel) toolDeps(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Service string }
	_ = json.Unmarshal(args, &a)
	ds, err := o.deps(ctx, time.Now())
	if err != nil {
		return nil, err
	}
	out := []dependency{}
	for _, d := range ds {
		if a.Service == "" || d.Parent == a.Service || d.Child == a.Service {
			out = append(out, d)
		}
		if len(out) >= 200 {
			break
		}
	}
	return json.Marshal(map[string]any{"dependencies": out})
}
