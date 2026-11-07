// Package outsum summarizes large tool results. When a result is too big to
// inline into the prompt, the kernel stores it as an artifact and shows the
// model a structured summary plus a handle it can query. Summarizers are
// pluggable by content; the built-ins know JSON and line-oriented text and
// nothing about any particular tool.
package outsum

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Content types produced by Detect.
const (
	TypeJSON = "application/json"
	TypeText = "text/plain"
)

// Summarizer builds a summary for content of a type it accepts.
type Summarizer interface {
	Name() string
	Accepts(contentType string) bool
	Summarize(body []byte) map[string]any
}

// Registry tries summarizers in order.
type Registry []Summarizer

// Default returns the built-in summarizers.
func Default() Registry { return Registry{JSON{}, Text{}} }

// MaxSummaryBytes bounds a rendered summary.
const MaxSummaryBytes = 6000

// Summarize detects the content and summarizes it with the first accepting
// summarizer.
func (r Registry) Summarize(raw []byte) (string, map[string]any) {
	ct, body := Detect(raw)
	for _, s := range r {
		if s.Accepts(ct) {
			sum := s.Summarize(body)
			sum["summarizer"] = s.Name()
			return ct, bound(sum)
		}
	}
	return ct, map[string]any{"bytes": len(raw)}
}

// Detect unwraps common envelopes and classifies the content. An MCP-style
// {"content":[{"type":"text","text":...}]} result and a JSON string both
// become their text; other valid JSON stays JSON.
func Detect(raw []byte) (string, []byte) {
	trimmed := bytes.TrimSpace(raw)
	if !json.Valid(trimmed) {
		return TypeText, raw
	}
	var s string
	if json.Unmarshal(trimmed, &s) == nil {
		return detectInner([]byte(s))
	}
	var env struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(trimmed, &env) == nil && len(env.Content) > 0 {
		var b strings.Builder
		texts := 0
		for _, c := range env.Content {
			if c.Type == "text" {
				if texts > 0 {
					b.WriteByte('\n')
				}
				b.WriteString(c.Text)
				texts++
			}
		}
		if texts > 0 {
			return detectInner([]byte(b.String()))
		}
	}
	return TypeJSON, trimmed
}

func detectInner(b []byte) (string, []byte) {
	t := bytes.TrimSpace(b)
	if len(t) > 0 && (t[0] == '{' || t[0] == '[') && json.Valid(t) {
		return TypeJSON, t
	}
	return TypeText, b
}

// bound drops the largest optional fields until the summary fits.
func bound(sum map[string]any) map[string]any {
	for _, k := range []string{"sample", "frequent", "head", "tail", "fields", "nested", "keys"} {
		if b, _ := json.Marshal(sum); len(b) <= MaxSummaryBytes {
			return sum
		}
		if _, ok := sum[k]; ok {
			delete(sum, k)
			sum["trimmed"] = true
		}
	}
	return sum
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// ---------- text ----------

// Text summarizes line-oriented output (logs, command output).
type Text struct{}

func (Text) Name() string                      { return "text" }
func (Text) Accepts(ct string) bool            { return ct == TypeText }
func (Text) Summarize(b []byte) map[string]any { return summarizeText(string(b)) }

var (
	levelRE = regexp.MustCompile(`(?i)\b(fatal|panic|critical|error|err|exception|warn|warning|info|debug|trace)\b`)
	// variableRE matches tokens that carry a digit (ids, counters, hosts,
	// addresses, durations); they become "#" in log templates.
	variableRE = regexp.MustCompile(`[0-9A-Za-z_.:/-]*\d[0-9A-Za-z_.:/-]*`)
	stampRE    = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`)
)

var levelNames = map[string]string{"fatal": "fatal", "panic": "fatal", "critical": "fatal", "error": "error",
	"err": "error", "exception": "error", "warn": "warn", "warning": "warn", "info": "info", "debug": "debug", "trace": "debug"}

// Lines splits text into lines without the trailing empty line.
func Lines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
}

// ParseStamp finds the first timestamp in a line.
func ParseStamp(line string) (time.Time, bool) {
	m := stampRE.FindString(line)
	if m == "" {
		return time.Time{}, false
	}
	m = strings.Replace(m, " ", "T", 1)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z0700", "2006-01-02T15:04:05.999999999"} {
		if t, err := time.Parse(layout, m); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func summarizeText(s string) map[string]any {
	lines := Lines(s)
	sum := map[string]any{"bytes": len(s), "lines": len(lines)}
	levels := map[string]int{}
	patterns := map[string]int{}
	var first, last time.Time
	for _, l := range lines {
		if m := levelRE.FindString(l); m != "" {
			levels[levelNames[strings.ToLower(m)]]++
		}
		if t, ok := ParseStamp(l); ok {
			if first.IsZero() || t.Before(first) {
				first = t
			}
			if t.After(last) {
				last = t
			}
		}
		if p := strings.TrimSpace(variableRE.ReplaceAllString(stampRE.ReplaceAllString(l, ""), "#")); p != "" {
			patterns[clip(p, 200)]++
		}
	}
	if len(levels) > 0 {
		sum["levels"] = levels
	}
	if !first.IsZero() {
		sum["time_range"] = map[string]string{"first": first.Format(time.RFC3339), "last": last.Format(time.RFC3339)}
	}
	head, tail := 8, 8
	if len(lines) <= head+tail {
		sum["head"] = clipAll(lines, 300)
	} else {
		sum["head"] = clipAll(lines[:head], 300)
		sum["tail"] = clipAll(lines[len(lines)-tail:], 300)
	}
	type pc struct {
		p string
		n int
	}
	var freq []pc
	for p, n := range patterns {
		if n > 1 {
			freq = append(freq, pc{p, n})
		}
	}
	sort.Slice(freq, func(i, j int) bool {
		if freq[i].n != freq[j].n {
			return freq[i].n > freq[j].n
		}
		return freq[i].p < freq[j].p
	})
	if len(freq) > 5 {
		freq = freq[:5]
	}
	if len(freq) > 0 {
		out := make([]map[string]any, 0, len(freq))
		for _, f := range freq {
			out = append(out, map[string]any{"template": f.p, "count": f.n})
		}
		sum["frequent"] = out
	}
	return sum
}

func clipAll(lines []string, n int) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = clip(l, n)
	}
	return out
}

// ---------- json ----------

// JSON summarizes structured results: shape, sizes, samples and numeric
// statistics of record arrays.
type JSON struct{}

func (JSON) Name() string           { return "json" }
func (JSON) Accepts(ct string) bool { return ct == TypeJSON }

func (JSON) Summarize(b []byte) map[string]any {
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return summarizeText(string(b))
	}
	sum := map[string]any{"bytes": len(b)}
	describe(sum, normalizeNumbers(v), 0)
	return sum
}

func normalizeNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return x.String()
		}
		return f
	case []any:
		for i := range x {
			x[i] = normalizeNumbers(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalizeNumbers(x[k])
		}
	}
	return v
}

func typeName(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return fmt.Sprintf("array[%d]", len(x))
	case map[string]any:
		return fmt.Sprintf("object{%d}", len(x))
	}
	return "unknown"
}

func describe(sum map[string]any, v any, depth int) {
	sum["type"] = typeName(v)
	switch x := v.(type) {
	case []any:
		describeArray(sum, x)
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		shape := map[string]string{}
		nested := map[string]any{}
		for i, k := range keys {
			if i < 40 {
				shape[k] = typeName(x[k])
			}
			if arr, ok := x[k].([]any); ok && depth == 0 && len(nested) < 4 && len(arr) > 0 {
				n := map[string]any{}
				describeArray(n, arr)
				nested[k] = n
			}
		}
		sum["keys"] = shape
		if len(nested) > 0 {
			sum["nested"] = nested
		}
	}
}

func describeArray(sum map[string]any, arr []any) {
	sum["length"] = len(arr)
	if len(arr) == 0 {
		return
	}
	var samples []string
	for i := 0; i < len(arr) && i < 2; i++ {
		b, _ := json.Marshal(arr[i])
		samples = append(samples, clip(string(b), 500))
	}
	sum["sample"] = samples
	fieldCount := map[string]int{}
	numeric := map[string][]float64{}
	times := map[string][2]time.Time{}
	records := 0
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		records++
		for k, fv := range m {
			fieldCount[k]++
			switch f := fv.(type) {
			case float64:
				numeric[k] = append(numeric[k], f)
			case string:
				if t, err := time.Parse(time.RFC3339Nano, f); err == nil {
					r := times[k]
					if r[0].IsZero() || t.Before(r[0]) {
						r[0] = t
					}
					if t.After(r[1]) {
						r[1] = t
					}
					times[k] = r
				}
			}
		}
	}
	if records == 0 {
		return
	}
	fields := make([]string, 0, len(fieldCount))
	for k := range fieldCount {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	if len(fields) > 40 {
		fields = fields[:40]
	}
	sum["fields"] = fields
	stats := map[string]any{}
	names := make([]string, 0, len(numeric))
	for k := range numeric {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if len(stats) >= 8 {
			break
		}
		vals := numeric[k]
		lo, hi, total := math.Inf(1), math.Inf(-1), 0.0
		for _, f := range vals {
			lo, hi, total = math.Min(lo, f), math.Max(hi, f), total+f
		}
		stats[k] = map[string]any{"min": lo, "max": hi, "avg": round(total / float64(len(vals))), "n": len(vals)}
	}
	if len(stats) > 0 {
		sum["numeric"] = stats
	}
	if len(times) > 0 {
		tr := map[string]any{}
		for k, r := range times {
			tr[k] = map[string]string{"first": r[0].UTC().Format(time.RFC3339), "last": r[1].UTC().Format(time.RFC3339)}
		}
		sum["time_fields"] = tr
	}
}

func round(f float64) float64 { return math.Round(f*1000) / 1000 }
