// Package sretools contains first-party SRE tools: read-only Prometheus
// queries (R0) and a Kubernetes rollout restart (R2, dry-run by default).
package sretools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type PrometheusConfig struct {
	URL         string            `json:"url"`
	Headers     map[string]string `json:"headers,omitempty"` // e.g. Authorization for a proxy
	Timeout     time.Duration     `json:"-"`
	MaxSeries   int               `json:"max_series,omitempty"`
	MaxPoints   int               `json:"max_points,omitempty"`
	MaxRangeHrs int               `json:"max_range_hours,omitempty"`
	HTTP        *http.Client      `json:"-"`
}

type Prometheus struct{ cfg PrometheusConfig }

func NewPrometheus(cfg PrometheusConfig) (*Prometheus, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("prometheus: invalid url %q", cfg.URL)
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.MaxSeries == 0 {
		cfg.MaxSeries = 20
	}
	if cfg.MaxPoints == 0 {
		cfg.MaxPoints = 120
	}
	if cfg.MaxRangeHrs == 0 {
		cfg.MaxRangeHrs = 24
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: cfg.Timeout}
	}
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	return &Prometheus{cfg: cfg}, nil
}

// Tools returns prom.query (instant/range) and prom.alerts, both R0.
func (p *Prometheus) Tools() []tools.Tool {
	return []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "prom.query", Version: "1", Risk: domain.RiskR0, TargetArg: "query",
			Trust: "T3", Source: "native", Description: "Run a read-only PromQL query. Instant by default; set range_minutes " +
				"for a range query. Returns at most a bounded number of series and points.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{` +
				`"query":{"type":"string","description":"PromQL expression"},` +
				`"range_minutes":{"type":"integer","description":"If set, query the last N minutes as a range"},` +
				`"step_seconds":{"type":"integer","description":"Range step (default: range/60)"}},` +
				`"required":["query"],"additionalProperties":false}`)},
			Fn: p.query},
		&tools.Func{Desc: tools.Descriptor{Name: "prom.alerts", Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
			Description: "List currently firing Prometheus alerts, optionally filtered by a label value substring.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"filter":{"type":"string"}},"additionalProperties":false}`)},
			Fn: p.alerts},
	}
}

func (p *Prometheus) get(ctx context.Context, path string, q url.Values) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.URL+path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range p.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := p.cfg.HTTP.Do(req)
	if err != nil {
		return nil, domain.Retryable(fmt.Errorf("prometheus: %w", err), 2*time.Second)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, domain.Retryable(err, time.Second)
	}
	var env struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
		Warnings  []string        `json:"warnings"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return nil, domain.Retryable(fmt.Errorf("prometheus: HTTP %d", resp.StatusCode), 5*time.Second)
		}
		return nil, fmt.Errorf("prometheus: HTTP %d: invalid response", resp.StatusCode)
	}
	if env.Status != "success" {
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return nil, domain.Retryable(fmt.Errorf("prometheus %s: %s", env.ErrorType, env.Error), 5*time.Second)
		}
		return nil, fmt.Errorf("prometheus %s: %s", env.ErrorType, env.Error)
	}
	return env.Data, nil
}

func (p *Prometheus) query(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Query        string `json:"query"`
		RangeMinutes int    `json:"range_minutes"`
		StepSeconds  int    `json:"step_seconds"`
	}
	if err := json.Unmarshal(args, &in); err != nil || strings.TrimSpace(in.Query) == "" {
		return nil, fmt.Errorf("prom.query: query is required")
	}
	if len(in.Query) > 4096 {
		return nil, fmt.Errorf("prom.query: query too long")
	}
	now := time.Now()
	q := url.Values{"query": {in.Query}}
	path := "/api/v1/query"
	if in.RangeMinutes > 0 {
		if in.RangeMinutes > p.cfg.MaxRangeHrs*60 {
			return nil, fmt.Errorf("prom.query: range exceeds %dh", p.cfg.MaxRangeHrs)
		}
		step := in.StepSeconds
		if step <= 0 {
			step = max(in.RangeMinutes*60/60, 15)
		}
		if in.RangeMinutes*60/step > 11000 {
			return nil, fmt.Errorf("prom.query: too many points; increase step_seconds")
		}
		path = "/api/v1/query_range"
		q.Set("start", strconv.FormatInt(now.Add(-time.Duration(in.RangeMinutes)*time.Minute).Unix(), 10))
		q.Set("end", strconv.FormatInt(now.Unix(), 10))
		q.Set("step", strconv.Itoa(step))
	} else {
		q.Set("time", strconv.FormatInt(now.Unix(), 10))
	}
	data, err := p.get(ctx, path, q)
	if err != nil {
		return nil, err
	}
	var d struct {
		ResultType string            `json:"resultType"`
		Result     []json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("prometheus: unexpected data: %w", err)
	}
	total := len(d.Result)
	series := d.Result
	if len(series) > p.cfg.MaxSeries {
		series = series[:p.cfg.MaxSeries]
	}
	out := make([]map[string]any, 0, len(series))
	for _, raw := range series {
		var s struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
			Values [][]any           `json:"values"`
		}
		_ = json.Unmarshal(raw, &s)
		item := map[string]any{"metric": s.Metric}
		if s.Value != nil {
			item["value"] = s.Value
		}
		if s.Values != nil {
			vals := s.Values
			if len(vals) > p.cfg.MaxPoints { // keep the most recent points
				vals = vals[len(vals)-p.cfg.MaxPoints:]
			}
			item["values"] = vals
			item["points_total"] = len(s.Values)
		}
		out = append(out, item)
	}
	if d.ResultType == "scalar" || d.ResultType == "string" {
		var v any
		_ = json.Unmarshal(data, &v)
		return json.Marshal(map[string]any{"query": in.Query, "result_type": d.ResultType, "data": v, "source": "prometheus"})
	}
	return json.Marshal(map[string]any{"query": in.Query, "result_type": d.ResultType, "series": out,
		"series_total": total, "truncated": total > len(series), "evaluated_at": now.UTC().Format(time.RFC3339),
		"source": "prometheus"})
}

func (p *Prometheus) alerts(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Filter string `json:"filter"`
	}
	_ = json.Unmarshal(args, &in)
	data, err := p.get(ctx, "/api/v1/alerts", url.Values{})
	if err != nil {
		return nil, err
	}
	var d struct {
		Alerts []struct {
			Labels   map[string]string `json:"labels"`
			State    string            `json:"state"`
			ActiveAt string            `json:"activeAt"`
			Value    string            `json:"value"`
		} `json:"alerts"`
	}
	_ = json.Unmarshal(data, &d)
	var out []map[string]any
	for _, a := range d.Alerts {
		if in.Filter != "" {
			match := false
			for _, v := range a.Labels {
				if strings.Contains(v, in.Filter) {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		if len(out) >= 50 {
			break
		}
		out = append(out, map[string]any{"labels": a.Labels, "state": a.State, "active_at": a.ActiveAt, "value": a.Value})
	}
	return json.Marshal(map[string]any{"alerts": out, "total": len(d.Alerts), "source": "prometheus"})
}
