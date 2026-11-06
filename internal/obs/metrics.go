// Package obs provides Prometheus-format metrics (text exposition 0.0.4) and
// OpenTelemetry tracing helpers. Metric labels are low-cardinality only: no
// run, step, session or user IDs (§20, §37).
package obs

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type kind int

const (
	counter kind = iota
	gauge
	histogram
)

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*series
}

type series struct {
	labelValues []string
	value       float64
	counts      []uint64
	sum         float64
	count       uint64
}

type collector = func(ctx context.Context, emit func(name string, labels map[string]string, v float64))

type Registry struct {
	mu         sync.Mutex
	families   []*family
	collectors []collector
	gaugeHelp  map[string]string
}

func NewRegistry() *Registry { return &Registry{gaugeHelp: map[string]string{}} }

func (r *Registry) add(f *family) *family {
	f.series = map[string]*series{}
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
	return f
}

type Counter struct{ f *family }
type Histogram struct{ f *family }
type Gauge struct{ f *family }

func (r *Registry) Counter(name, help string, labels ...string) Counter {
	return Counter{r.add(&family{name: name, help: help, kind: counter, labels: labels})}
}

func (r *Registry) Gauge(name, help string, labels ...string) Gauge {
	return Gauge{r.add(&family{name: name, help: help, kind: gauge, labels: labels})}
}

var DefBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) Histogram {
	if buckets == nil {
		buckets = DefBuckets
	}
	return Histogram{r.add(&family{name: name, help: help, kind: histogram, labels: labels, buckets: buckets})}
}

// Collect registers a scrape-time gauge collector (e.g. runs by status).
func (r *Registry) Collect(name, help string, fn func(ctx context.Context, emit func(name string, labels map[string]string, v float64))) {
	r.mu.Lock()
	r.gaugeHelp[name] = help
	r.collectors = append(r.collectors, fn)
	r.mu.Unlock()
}

func (f *family) get(lv []string) *series {
	if len(lv) != len(f.labels) {
		panic(fmt.Sprintf("metric %s: want %d labels, got %d", f.name, len(f.labels), len(lv)))
	}
	key := strings.Join(lv, "\xff")
	s, ok := f.series[key]
	if !ok {
		s = &series{labelValues: append([]string(nil), lv...)}
		if f.kind == histogram {
			s.counts = make([]uint64, len(f.buckets))
		}
		f.series[key] = s
	}
	return s
}

func (c Counter) Add(v float64, lv ...string) {
	if v < 0 {
		return
	}
	c.f.mu.Lock()
	c.f.get(lv).value += v
	c.f.mu.Unlock()
}

func (c Counter) Inc(lv ...string) { c.Add(1, lv...) }

func (g Gauge) Set(v float64, lv ...string) {
	g.f.mu.Lock()
	g.f.get(lv).value = v
	g.f.mu.Unlock()
}

func (g Gauge) Add(v float64, lv ...string) {
	g.f.mu.Lock()
	g.f.get(lv).value += v
	g.f.mu.Unlock()
}

func (h Histogram) Observe(v float64, lv ...string) {
	h.f.mu.Lock()
	s := h.f.get(lv)
	for i, b := range h.f.buckets {
		if v <= b {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
	h.f.mu.Unlock()
}

func esc(v string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(v)
}

func labelStr(names, values []string, extra ...string) string {
	var parts []string
	for i, n := range names {
		parts = append(parts, n+`="`+esc(values[i])+`"`)
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, extra[i]+`="`+esc(extra[i+1])+`"`)
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func fmtF(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func (r *Registry) Write(ctx context.Context, w io.Writer) {
	r.mu.Lock()
	fams := append([]*family(nil), r.families...)
	cols := append([]collector(nil), r.collectors...)
	help := map[string]string{}
	for k, v := range r.gaugeHelp {
		help[k] = v
	}
	r.mu.Unlock()
	for _, f := range fams {
		f.mu.Lock()
		typ := map[kind]string{counter: "counter", gauge: "gauge", histogram: "histogram"}[f.kind]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, typ)
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := f.series[k]
			if f.kind != histogram {
				fmt.Fprintf(w, "%s%s %s\n", f.name, labelStr(f.labels, s.labelValues), fmtF(s.value))
				continue
			}
			for i, b := range f.buckets {
				fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelStr(f.labels, s.labelValues, "le", fmtF(b)), s.counts[i])
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelStr(f.labels, s.labelValues, "le", "+Inf"), s.count)
			fmt.Fprintf(w, "%s_sum%s %s\n", f.name, labelStr(f.labels, s.labelValues), fmtF(s.sum))
			fmt.Fprintf(w, "%s_count%s %d\n", f.name, labelStr(f.labels, s.labelValues), s.count)
		}
		f.mu.Unlock()
	}
	type sample struct {
		labels map[string]string
		v      float64
	}
	collected := map[string][]sample{}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, c := range cols {
		c(cctx, func(name string, labels map[string]string, v float64) {
			collected[name] = append(collected[name], sample{labels, v})
		})
	}
	names := make([]string, 0, len(collected))
	for n := range collected {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", n, help[n], n)
		for _, s := range collected[n] {
			ks := make([]string, 0, len(s.labels))
			for k := range s.labels {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			vs := make([]string, len(ks))
			for i, k := range ks {
				vs[i] = s.labels[k]
			}
			fmt.Fprintf(w, "%s%s %s\n", n, labelStr(ks, vs), fmtF(s.v))
		}
	}
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(req.Context(), w)
	})
}

// ---------- harness metrics ----------

type Metrics struct {
	Reg *Registry

	httpRequests  Counter
	httpDuration  Histogram
	steps         Counter
	stepDuration  Histogram
	claims        Counter
	modelRequests Counter
	modelCost     Counter
	modelTokens   Counter
	modelTTFT     Histogram
	toolCalls     Counter
	retention     Counter
	sinkEvents    Counter
}

func NewMetrics() *Metrics {
	r := NewRegistry()
	return &Metrics{Reg: r,
		httpRequests:  r.Counter("harness_http_requests_total", "HTTP requests by route pattern and status code.", "method", "route", "code"),
		httpDuration:  r.Histogram("harness_http_request_duration_seconds", "HTTP request latency (SSE excluded).", nil, "route"),
		steps:         r.Counter("harness_steps_total", "Executed steps by kind and outcome.", "kind", "outcome"),
		stepDuration:  r.Histogram("harness_step_duration_seconds", "Step execution time (claim to commit).", nil, "kind"),
		claims:        r.Counter("harness_claims_total", "Claim attempts by result.", "result"),
		modelRequests: r.Counter("harness_model_requests_total", "Model gateway requests by alias, model and outcome.", "alias", "model", "outcome"),
		modelCost:     r.Counter("harness_model_cost_usd_total", "Estimated model cost in USD.", "model"),
		modelTokens:   r.Counter("harness_model_tokens_total", "Model tokens by direction.", "model", "direction"),
		modelTTFT:     r.Histogram("harness_model_ttft_seconds", "Time to first token.", []float64{.1, .25, .5, 1, 2, 4, 8, 16, 32}, "model"),
		toolCalls:     r.Counter("harness_tool_calls_total", "Tool invocations by tool source, risk and outcome.", "source", "risk", "outcome"),
		retention:     r.Counter("harness_retention_deleted_total", "Rows/objects removed by retention.", "kind"),
		sinkEvents:    r.Counter("harness_event_sink_total", "Event sink deliveries by sink and outcome (delivered, retried, skipped).", "sink", "outcome"),
	}
}

func (m *Metrics) ObserveHTTP(method, route string, code int, d time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	if i := strings.IndexByte(route, ' '); i >= 0 {
		route = route[i+1:]
	}
	m.httpRequests.Inc(method, route, strconv.Itoa(code))
	if !strings.HasSuffix(route, "/stream") {
		m.httpDuration.Observe(d.Seconds(), route)
	}
}

func (m *Metrics) ObserveStep(kind, outcome string, d time.Duration) {
	m.steps.Inc(kind, outcome)
	m.stepDuration.Observe(d.Seconds(), kind)
}

func (m *Metrics) ObserveClaim(result string) { m.claims.Inc(result) }

func (m *Metrics) ObserveSink(sink, outcome string) { m.sinkEvents.Inc(sink, outcome) }

func (m *Metrics) ObserveModel(alias, model, outcome string, costMicros, in, out, cached int64, ttft time.Duration) {
	if model == "" {
		model = "none"
	}
	m.modelRequests.Inc(alias, model, outcome)
	m.modelCost.Add(float64(costMicros)/1e6, model)
	m.modelTokens.Add(float64(in), model, "input")
	m.modelTokens.Add(float64(out), model, "output")
	m.modelTokens.Add(float64(cached), model, "cached_input")
	if ttft > 0 {
		m.modelTTFT.Observe(ttft.Seconds(), model)
	}
}

func (m *Metrics) ObserveTool(source, risk, outcome string) {
	if source == "" {
		source = "native"
	}
	m.toolCalls.Inc(source, risk, outcome)
}

func (m *Metrics) ObserveRetention(kind string, n int64) { m.retention.Add(float64(n), kind) }
