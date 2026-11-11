package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	obsv "go-sre-harness/internal/observability"
)

// metrics reads a Prometheus-compatible HTTP API (/api/v1/query_range).
type metrics struct{ base }

func newMetrics(src obsv.Source, ep obsv.Endpoint, creds obsv.CredentialBroker) (obsv.Provider, error) {
	b, err := newBase(src, ep, creds)
	if err != nil {
		return nil, err
	}
	return &metrics{b}, nil
}

type promResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
			Value  *[2]any           `json:"value"`
		} `json:"result"`
	} `json:"data"`
	Error    string   `json:"error"`
	Warnings []string `json:"warnings"`
}

// Probe evaluates a constant: it proves reachability and authorization
// without reading any series.
func (m *metrics) Probe(ctx context.Context) error {
	var r promResponse
	if err := m.get(ctx, "/api/v1/query", url.Values{"query": {"vector(1)"}}, &r); err != nil {
		return err
	}
	if r.Status != "success" {
		return fmt.Errorf("backend answered %q", r.Status)
	}
	return nil
}

func sample(p [2]any) (time.Time, float64, bool) {
	ts, ok := p[0].(float64)
	s, ok2 := p[1].(string)
	if !ok || !ok2 {
		return time.Time{}, 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return time.Time{}, 0, false
	}
	return time.Unix(0, int64(ts*1e9)).UTC(), v, true
}

func labelString(l map[string]string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		if k != "__name__" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+l[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (m *metrics) Query(ctx context.Context, q obsv.Query) (obsv.Result, error) {
	// About 120 points per series, never finer than 15s.
	step := q.End.Sub(q.Start) / 120
	if step < 15*time.Second {
		step = 15 * time.Second
	}
	var raw json.RawMessage
	err := m.get(ctx, "/api/v1/query_range", url.Values{"query": {q.Rendered}, "start": {unix(q.Start)}, "end": {unix(q.End)},
		"step": {strconv.Itoa(int(step.Seconds()))}}, &raw)
	if err != nil {
		return obsv.Result{}, err
	}
	var r promResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return obsv.Result{}, fmt.Errorf("decode response: %w", err)
	}
	if r.Status != "success" {
		return obsv.Result{}, fmt.Errorf("%w: %s", obsv.ErrBadQuery, r.Error)
	}
	res := obsv.Result{Status: obsv.StatusOK, Payload: raw, Values: map[string]float64{}}
	if len(r.Warnings) > 0 {
		// The backend answered from a subset (a shard or store was down).
		res.Status = obsv.StatusPartial
		res.Warnings = append(res.Warnings, r.Warnings...)
	}
	series := r.Data.Result
	if len(series) > q.Limit {
		series, res.Truncated = series[:q.Limit], true
		res.Warnings = append(res.Warnings, fmt.Sprintf("more than %d series; the rest was dropped", q.Limit))
	}
	var lines []string
	first := true
	for _, s := range series {
		pts := s.Values
		if s.Value != nil {
			pts = append(pts, *s.Value)
		}
		lo, hi, sum, n, last := math.Inf(1), math.Inf(-1), 0.0, 0, 0.0
		for _, p := range pts {
			at, v, ok := sample(p)
			if !ok {
				continue
			}
			lo, hi, sum, last = math.Min(lo, v), math.Max(hi, v), sum+v, v
			n++
			if at.After(res.SampledAt) {
				res.SampledAt = at
			}
		}
		if n == 0 {
			continue
		}
		res.Count++
		if first {
			first = false
			res.Values["latest"], res.Values["min"], res.Values["max"], res.Values["avg"] = last, lo, hi, sum/float64(n)
		}
		if len(lines) < 8 {
			lines = append(lines, fmt.Sprintf("%s latest=%s min=%s max=%s avg=%s (%d points)", labelString(s.Metric), fnum(last),
				fnum(lo), fnum(hi), fnum(sum/float64(n)), n))
		}
	}
	if res.Count > 0 {
		res.Summary = fmt.Sprintf("%d series over %s:\n%s", res.Count, q.End.Sub(q.Start).Round(time.Second), strings.Join(lines, "\n"))
	}
	return res, nil
}
