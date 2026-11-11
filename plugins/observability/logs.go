package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	obsv "go-sre-harness/internal/observability"
)

// loki reads Loki's LogQL HTTP API (/loki/api/v1/query_range).
type loki struct{ base }

func newLoki(src obsv.Source, ep obsv.Endpoint, creds obsv.CredentialBroker) (obsv.Provider, error) {
	b, err := newBase(src, ep, creds)
	if err != nil {
		return nil, err
	}
	return &loki{b}, nil
}

// Probe lists label names over the last minute: cheap, and it exercises the
// same authorization as a query.
func (l *loki) Probe(ctx context.Context) error {
	now := time.Now()
	return l.get(ctx, "/loki/api/v1/labels", url.Values{"start": {unixNano(now.Add(-time.Minute))}, "end": {unixNano(now)}}, nil)
}

type lokiResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

var (
	varRe = regexp.MustCompile(`0x[0-9a-fA-F]+|\b[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}\b|\b[0-9a-fA-F]{16,}\b|\b\d+(\.\d+)?`)
	tsRe  = regexp.MustCompile(`^\s*\[?\d{4}-\d{2}-\d{2}[T ][0-9:.,]+(Z|[+-]\d{2}:?\d{2})?\]?\s*`)
)

// Template reduces a log line to its shape: timestamps, numbers, ids and
// hex values become placeholders, so lines that differ only in those group
// together.
func Template(line string) string {
	line = tsRe.ReplaceAllString(line, "")
	return cpkit.Truncate(strings.TrimSpace(varRe.ReplaceAllString(line, "<*>")), 200)
}

func (l *loki) Query(ctx context.Context, q obsv.Query) (obsv.Result, error) {
	limit := q.Limit * 20 // the source limit bounds streams; lines get 20 each
	if limit > 2000 {
		limit = 2000
	}
	var raw json.RawMessage
	err := l.get(ctx, "/loki/api/v1/query_range", url.Values{"query": {q.Rendered}, "start": {unixNano(q.Start)},
		"end": {unixNano(q.End)}, "limit": {strconv.Itoa(limit)}, "direction": {"backward"}}, &raw)
	if err != nil {
		return obsv.Result{}, err
	}
	var r lokiResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return obsv.Result{}, fmt.Errorf("decode response: %w", err)
	}
	if r.Status != "success" {
		return obsv.Result{}, fmt.Errorf("%w: status %s", obsv.ErrBadQuery, r.Status)
	}
	res := obsv.Result{Status: obsv.StatusOK, Payload: raw, Values: map[string]float64{}}
	type tpl struct {
		n       int
		example string
	}
	templates := map[string]*tpl{}
	for _, s := range r.Data.Result {
		for _, v := range s.Values {
			res.Count++
			if ns, err := strconv.ParseInt(v[0], 10, 64); err == nil {
				if at := time.Unix(0, ns).UTC(); at.After(res.SampledAt) {
					res.SampledAt = at
				}
			}
			key := Template(v[1])
			t := templates[key]
			if t == nil {
				t = &tpl{example: cpkit.Truncate(v[1], 300)}
				templates[key] = t
			}
			t.n++
		}
	}
	if res.Count >= limit {
		res.Truncated = true
		res.Warnings = append(res.Warnings, fmt.Sprintf("line limit %d reached; older lines were not read", limit))
	}
	res.Values["lines"], res.Values["streams"], res.Values["templates"] = float64(res.Count), float64(len(r.Data.Result)), float64(len(templates))
	keys := make([]string, 0, len(templates))
	for k := range templates {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if templates[keys[i]].n != templates[keys[j]].n {
			return templates[keys[i]].n > templates[keys[j]].n
		}
		return keys[i] < keys[j]
	})
	if res.Count > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "%d lines in %d streams, %d distinct shapes. Top shapes:\n", res.Count, len(r.Data.Result), len(templates))
		for i, k := range keys {
			if i >= 5 {
				break
			}
			fmt.Fprintf(&b, "%dx %s\n   e.g. %s\n", templates[k].n, k, templates[k].example)
		}
		res.Summary = b.String()
	}
	return res, nil
}
