// Package artifactq implements harness.artifact_query, the framework tool
// that lets a model read an offloaded tool result piece by piece instead of
// receiving it truncated. It only reads tool-output artifacts of the
// caller's tenant and never reaches an external system.
package artifactq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/outsum"
	"go-sre-harness/internal/tools"
)

// Name is the registered tool name.
const Name = tools.BuiltinPrefix + "artifact_query"

// Kind is the artifact kind of offloaded tool results.
const Kind = "tool-output"

const (
	defaultLimit = 50
	maxLimit     = 200
	maxOutput    = 16 << 10
)

var schema = json.RawMessage(`{"type":"object","properties":{
"handle":{"type":"string","description":"The handle of an offloaded tool result (from an observation's artifact.handle)."},
"op":{"type":"string","enum":["head","tail","lines","grep","path","time"],"description":"head/tail: first/last lines. lines: a line range. grep: lines matching a regular expression. path: an expression over JSON results, rooted at data (e.g. data.items[0].status). time: lines (or array items) within a time range."},
"pattern":{"type":"string","description":"grep: RE2 regular expression."},
"ignore_case":{"type":"boolean"},
"invert":{"type":"boolean","description":"grep: return non-matching lines."},
"start_line":{"type":"integer","minimum":1},
"end_line":{"type":"integer","minimum":1},
"path":{"type":"string","description":"path: expression rooted at data."},
"from":{"type":"string","description":"time: RFC 3339 start (inclusive)."},
"to":{"type":"string","description":"time: RFC 3339 end (exclusive)."},
"time_field":{"type":"string","description":"time: field holding the timestamp when the result is a JSON array of records."},
"limit":{"type":"integer","minimum":1,"maximum":200}
},"required":["handle","op"],"additionalProperties":false}`)

// Tool is the harness.artifact_query implementation.
type Tool struct {
	Artifacts artifact.Store
}

// New returns the tool.
func New(arts artifact.Store) *Tool { return &Tool{Artifacts: arts} }

func (t *Tool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: Name, Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
		Description: "Query an offloaded (large) tool result by its handle: head, tail, line ranges, grep, JSON paths or a time range. Use it instead of re-running the original tool.",
		InputSchema: schema}
}

type args struct {
	Handle     string `json:"handle"`
	Op         string `json:"op"`
	Pattern    string `json:"pattern"`
	IgnoreCase bool   `json:"ignore_case"`
	Invert     bool   `json:"invert"`
	StartLine  int    `json:"start_line"`
	EndLine    int    `json:"end_line"`
	Path       string `json:"path"`
	From       string `json:"from"`
	To         string `json:"to"`
	TimeField  string `json:"time_field"`
	Limit      int    `json:"limit"`
}

// ErrHandle is returned for handles outside the caller's tool outputs.
var ErrHandle = errors.New("handle does not name a tool output of this tenant")

// ValidHandle reports whether ref is a tool-output artifact of the tenant.
func ValidHandle(tenantID, ref string) bool {
	_, key, ok := strings.Cut(ref, "://")
	if !ok || tenantID == "" {
		return false
	}
	parts := strings.Split(key, "/")
	return len(parts) == 3 && parts[0] == tenantID && parts[1] == Kind && parts[2] != "" && !strings.Contains(key, "..")
}

func (t *Tool) Invoke(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
	ci, ok := tools.CallInfoFrom(ctx)
	if !ok || ci.TenantID == "" {
		return nil, fmt.Errorf("%s: missing call context: %w", Name, domain.ErrNotDispatched)
	}
	var a args
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("%s: invalid arguments: %v: %w", Name, err, domain.ErrNotDispatched)
	}
	if !ValidHandle(ci.TenantID, a.Handle) {
		return nil, fmt.Errorf("%s: %w", Name, ErrHandle)
	}
	data, err := t.Artifacts.Get(ctx, ci.TenantID, a.Handle)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Name, ErrHandle)
	}
	out, err := query(data, a)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", Name, err)
	}
	return out, nil
}

type line struct {
	N    int    `json:"n"`
	Text string `json:"text"`
}

func query(data []byte, a args) (json.RawMessage, error) {
	limit := a.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	limit = min(limit, maxLimit)
	ct, body := outsum.Detect(data)
	result := map[string]any{"op": a.Op, "content_type": ct}
	switch a.Op {
	case "path":
		if ct != outsum.TypeJSON {
			return nil, fmt.Errorf("path needs a JSON result; this one is text (use grep or lines)")
		}
		expr, err := flow.CompileExpr(strings.TrimSpace(a.Path))
		if err != nil {
			return nil, fmt.Errorf("path: %w", err)
		}
		var doc any
		if err := json.Unmarshal(body, &doc); err != nil {
			return nil, err
		}
		v, err := expr.Eval(map[string]any{"data": doc})
		if err != nil {
			return nil, fmt.Errorf("path: %w", err)
		}
		result["value"] = v
		return render(result)
	case "time":
		from, to, err := timeRange(a.From, a.To)
		if err != nil {
			return nil, err
		}
		if ct == outsum.TypeJSON && a.TimeField != "" {
			return timeItems(body, a.TimeField, from, to, limit, result)
		}
		lines := outsum.Lines(string(body))
		var sel []line
		matched := 0
		for i, l := range lines {
			ts, ok := outsum.ParseStamp(l)
			if !ok || ts.Before(from) || (!to.IsZero() && !ts.Before(to)) {
				continue
			}
			matched++
			if len(sel) < limit {
				sel = append(sel, line{i + 1, l})
			}
		}
		result["total_lines"], result["matched"], result["lines"] = len(lines), matched, sel
		return render(result)
	}
	lines := outsum.Lines(string(body))
	result["total_lines"] = len(lines)
	var sel []line
	switch a.Op {
	case "head":
		for i := 0; i < len(lines) && i < limit; i++ {
			sel = append(sel, line{i + 1, lines[i]})
		}
	case "tail":
		for i := max(0, len(lines)-limit); i < len(lines); i++ {
			sel = append(sel, line{i + 1, lines[i]})
		}
	case "lines":
		start := max(a.StartLine, 1)
		end := a.EndLine
		if end <= 0 || end > len(lines) {
			end = len(lines)
		}
		if end-start+1 > limit {
			end = start + limit - 1
		}
		for i := start; i <= end && i <= len(lines); i++ {
			sel = append(sel, line{i, lines[i-1]})
		}
	case "grep":
		if a.Pattern == "" || len(a.Pattern) > 300 {
			return nil, fmt.Errorf("grep needs a pattern of at most 300 characters")
		}
		src := a.Pattern
		if a.IgnoreCase {
			src = "(?i)" + src
		}
		re, err := regexp.Compile(src)
		if err != nil {
			return nil, fmt.Errorf("pattern: %w", err)
		}
		matched := 0
		for i, l := range lines {
			if re.MatchString(l) == a.Invert {
				continue
			}
			matched++
			if len(sel) < limit {
				sel = append(sel, line{i + 1, l})
			}
		}
		result["matched"] = matched
	default:
		return nil, fmt.Errorf("unknown op %q", a.Op)
	}
	result["lines"] = sel
	return render(result)
}

func timeRange(fromS, toS string) (time.Time, time.Time, error) {
	var from, to time.Time
	var err error
	if fromS == "" {
		return from, to, fmt.Errorf("time needs from")
	}
	if from, err = time.Parse(time.RFC3339Nano, fromS); err != nil {
		return from, to, fmt.Errorf("from: %w", err)
	}
	if toS != "" {
		if to, err = time.Parse(time.RFC3339Nano, toS); err != nil {
			return from, to, fmt.Errorf("to: %w", err)
		}
	}
	return from.UTC(), to.UTC(), nil
}

func timeItems(body []byte, field string, from, to time.Time, limit int, result map[string]any) (json.RawMessage, error) {
	var arr []map[string]any
	if err := json.Unmarshal(body, &arr); err != nil {
		return nil, fmt.Errorf("time_field needs a JSON array of records")
	}
	var items []any
	matched := 0
	for _, it := range arr {
		s, _ := it[field].(string)
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || ts.Before(from) || (!to.IsZero() && !ts.Before(to)) {
			continue
		}
		matched++
		if len(items) < limit {
			items = append(items, it)
		}
	}
	result["total_items"], result["matched"], result["items"] = len(arr), matched, items
	return render(result)
}

// render bounds the output so a query result is never offloaded again.
func render(result map[string]any) (json.RawMessage, error) {
	b, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	for len(b) > maxOutput {
		switch {
		case shrink(result, "lines"), shrink(result, "items"):
		default:
			s := string(b)
			b, _ = json.Marshal(map[string]any{"op": result["op"], "truncated": true, "partial": s[:maxOutput-200]})
			return b, nil
		}
		result["truncated"] = true
		b, _ = json.Marshal(result)
	}
	return b, nil
}

func shrink(result map[string]any, key string) bool {
	switch v := result[key].(type) {
	case []line:
		if len(v) > 1 {
			result[key] = v[:len(v)/2]
			return true
		}
		if len(v) == 1 && len(v[0].Text) > 2000 {
			v[0].Text = v[0].Text[:2000] + "…"
			return true
		}
	case []any:
		if len(v) > 1 {
			result[key] = v[:len(v)/2]
			return true
		}
	}
	return false
}
