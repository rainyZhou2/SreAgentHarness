package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	obsv "go-sre-harness/internal/observability"
)

// traceRow is a trace as both trace backends are summarized.
type traceRow struct {
	ID        string
	Root      string
	Start     time.Time
	Duration  time.Duration
	Spans     int
	HasError  bool
	ErrorSpan string
}

// summarize orders traces slowest first and renders the bounded summary.
// Trace ids are kept in the summary: they are the span links a follow-up
// query or a human uses to open the trace.
func summarize(rows []traceRow, res *obsv.Result) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Duration > rows[j].Duration })
	errs := 0
	var total time.Duration
	for _, r := range rows {
		total += r.Duration
		if r.HasError {
			errs++
		}
		if r.Start.After(res.SampledAt) {
			res.SampledAt = r.Start
		}
	}
	res.Count = len(rows)
	if len(rows) == 0 {
		return
	}
	res.Values = map[string]float64{"traces": float64(len(rows)), "error_traces": float64(errs),
		"max_ms": float64(rows[0].Duration.Milliseconds()), "avg_ms": float64(total.Milliseconds()) / float64(len(rows))}
	var b strings.Builder
	fmt.Fprintf(&b, "%d traces, %d with errors, slowest %s. Slowest:\n", len(rows), errs, rows[0].Duration.Round(time.Millisecond))
	for i, r := range rows {
		if i >= 5 {
			break
		}
		fmt.Fprintf(&b, "trace %s %s %s spans=%d", r.ID, r.Root, r.Duration.Round(time.Millisecond), r.Spans)
		if r.HasError {
			fmt.Fprintf(&b, " error_in=%s", r.ErrorSpan)
		}
		b.WriteByte('\n')
	}
	res.Summary = b.String()
}

// tempo reads Tempo's native search API with TraceQL (/api/search).
type tempo struct{ base }

func newTempo(src obsv.Source, ep obsv.Endpoint, creds obsv.CredentialBroker) (obsv.Provider, error) {
	b, err := newBase(src, ep, creds)
	if err != nil {
		return nil, err
	}
	return &tempo{b}, nil
}

func (t *tempo) Probe(ctx context.Context) error { return t.get(ctx, "/api/echo", nil, nil) }

type tempoSearch struct {
	Traces []struct {
		TraceID           string `json:"traceID"`
		RootServiceName   string `json:"rootServiceName"`
		RootTraceName     string `json:"rootTraceName"`
		StartTimeUnixNano string `json:"startTimeUnixNano"`
		DurationMs        int64  `json:"durationMs"`
		SpanSets          []struct {
			Matched int `json:"matched"`
			Spans   []struct {
				Name       string `json:"name"`
				Attributes []struct {
					Key   string `json:"key"`
					Value struct {
						StringValue string `json:"stringValue"`
					} `json:"value"`
				} `json:"attributes"`
			} `json:"spans"`
		} `json:"spanSets"`
	} `json:"traces"`
	Metrics struct {
		InspectedTraces int    `json:"inspectedTraces"`
		InspectedBytes  string `json:"inspectedBytes"`
	} `json:"metrics"`
}

func (t *tempo) Query(ctx context.Context, q obsv.Query) (obsv.Result, error) {
	var raw json.RawMessage
	err := t.get(ctx, "/api/search", url.Values{"q": {q.Rendered}, "start": {unix(q.Start)}, "end": {unix(q.End)},
		"limit": {strconv.Itoa(q.Limit)}}, &raw)
	if err != nil {
		return obsv.Result{}, err
	}
	var r tempoSearch
	if err := json.Unmarshal(raw, &r); err != nil {
		return obsv.Result{}, fmt.Errorf("decode response: %w", err)
	}
	res := obsv.Result{Status: obsv.StatusOK, Payload: raw}
	rows := make([]traceRow, 0, len(r.Traces))
	for _, tr := range r.Traces {
		row := traceRow{ID: tr.TraceID, Root: tr.RootServiceName + " " + tr.RootTraceName,
			Duration: time.Duration(tr.DurationMs) * time.Millisecond}
		if ns, err := strconv.ParseInt(tr.StartTimeUnixNano, 10, 64); err == nil {
			row.Start = time.Unix(0, ns).UTC()
		}
		for _, set := range tr.SpanSets {
			row.Spans += set.Matched
			for _, sp := range set.Spans {
				for _, a := range sp.Attributes {
					if a.Key == "status" && a.Value.StringValue == "error" {
						row.HasError, row.ErrorSpan = true, sp.Name
					}
				}
			}
		}
		rows = append(rows, row)
	}
	if len(rows) >= q.Limit {
		res.Truncated = true
		res.Warnings = append(res.Warnings, fmt.Sprintf("trace limit %d reached", q.Limit))
	}
	summarize(rows, &res)
	return res, nil
}

// jaeger reads the Jaeger query API (/api/traces). The query template is a
// URL query string: "service=${service}&minDuration=500ms&tags={\"error\":\"true\"}".
type jaeger struct{ base }

func newJaeger(src obsv.Source, ep obsv.Endpoint, creds obsv.CredentialBroker) (obsv.Provider, error) {
	b, err := newBase(src, ep, creds)
	if err != nil {
		return nil, err
	}
	return &jaeger{b}, nil
}

func (j *jaeger) Probe(ctx context.Context) error { return j.get(ctx, "/api/services", nil, nil) }

type jaegerResponse struct {
	Data []struct {
		TraceID string `json:"traceID"`
		Spans   []struct {
			SpanID        string `json:"spanID"`
			OperationName string `json:"operationName"`
			ProcessID     string `json:"processID"`
			StartTime     int64  `json:"startTime"` // µs
			Duration      int64  `json:"duration"`  // µs
			References    []any  `json:"references"`
			Tags          []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"tags"`
		} `json:"spans"`
		Processes map[string]struct {
			ServiceName string `json:"serviceName"`
		} `json:"processes"`
	} `json:"data"`
	Errors []struct {
		Msg string `json:"msg"`
	} `json:"errors"`
}

// jaegerParams are the query parameters a template may set.
var jaegerParams = map[string]bool{"service": true, "operation": true, "tags": true, "minDuration": true, "maxDuration": true}

func (j *jaeger) Query(ctx context.Context, q obsv.Query) (obsv.Result, error) {
	in, err := url.ParseQuery(q.Rendered)
	if err != nil || in.Get("service") == "" {
		return obsv.Result{}, fmt.Errorf("%w: a Jaeger query is a query string with service=", obsv.ErrBadQuery)
	}
	params := url.Values{"start": {strconv.FormatInt(q.Start.UnixMicro(), 10)}, "end": {strconv.FormatInt(q.End.UnixMicro(), 10)},
		"limit": {strconv.Itoa(q.Limit)}}
	for k, v := range in {
		if !jaegerParams[k] {
			return obsv.Result{}, fmt.Errorf("%w: parameter %q is not allowed", obsv.ErrBadQuery, k)
		}
		params[k] = v
	}
	var raw json.RawMessage
	if err := j.get(ctx, "/api/traces", params, &raw); err != nil {
		return obsv.Result{}, err
	}
	var r jaegerResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return obsv.Result{}, fmt.Errorf("decode response: %w", err)
	}
	res := obsv.Result{Status: obsv.StatusOK, Payload: raw}
	if len(r.Errors) > 0 {
		res.Status = obsv.StatusPartial
		for _, e := range r.Errors {
			res.Warnings = append(res.Warnings, e.Msg)
		}
	}
	rows := make([]traceRow, 0, len(r.Data))
	for _, tr := range r.Data {
		row := traceRow{ID: tr.TraceID, Spans: len(tr.Spans)}
		for _, sp := range tr.Spans {
			if len(sp.References) == 0 || row.Root == "" {
				row.Root = tr.Processes[sp.ProcessID].ServiceName + " " + sp.OperationName
				row.Start, row.Duration = time.UnixMicro(sp.StartTime).UTC(), time.Duration(sp.Duration)*time.Microsecond
			}
			for _, tag := range sp.Tags {
				if tag.Key == "error" && (tag.Value == true || tag.Value == "true") {
					row.HasError, row.ErrorSpan = true, tr.Processes[sp.ProcessID].ServiceName+" "+sp.OperationName
				}
			}
		}
		rows = append(rows, row)
	}
	if len(rows) >= q.Limit {
		res.Truncated = true
	}
	summarize(rows, &res)
	return res, nil
}
