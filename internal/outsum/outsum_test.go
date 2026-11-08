package outsum

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestDetectUnwrapsEnvelopes(t *testing.T) {
	cases := []struct {
		raw, ct, body string
	}{
		{`{"a":1}`, TypeJSON, `{"a":1}`},
		{`plain text`, TypeText, `plain text`},
		{`"line1\nline2"`, TypeText, "line1\nline2"},
		{`"[1,2]"`, TypeJSON, `[1,2]`},
		{`{"content":[{"type":"text","text":"a"},{"type":"image"},{"type":"text","text":"b"}]}`, TypeText, "a\nb"},
		{`{"content":[{"type":"text","text":"{\"x\":true}"}]}`, TypeJSON, `{"x":true}`},
	}
	for _, c := range cases {
		ct, body := Detect([]byte(c.raw))
		if ct != c.ct || string(body) != c.body {
			t.Errorf("Detect(%s) = %s %q, want %s %q", c.raw, ct, body, c.ct, c.body)
		}
	}
}

func TestTextSummary(t *testing.T) {
	var b strings.Builder
	for i := range 40 {
		lvl := "INFO"
		if i%4 == 0 {
			lvl = "ERROR"
		}
		fmt.Fprintf(&b, "2026-11-07T09:%02d:00Z %s request %d failed id=%x\n", i, lvl, i, 1000+i)
	}
	ct, sum := Default().Summarize([]byte(b.String()))
	if ct != TypeText || sum["summarizer"] != "text" || sum["lines"] != 40 {
		t.Fatalf("summary %v", sum)
	}
	levels := sum["levels"].(map[string]int)
	if levels["error"] != 10 || levels["info"] != 30 {
		t.Fatalf("levels %v", levels)
	}
	tr := sum["time_range"].(map[string]string)
	if tr["first"] != "2026-11-07T09:00:00Z" || tr["last"] != "2026-11-07T09:39:00Z" {
		t.Fatalf("time range %v", tr)
	}
	if len(sum["head"].([]string)) != 8 || len(sum["tail"].([]string)) != 8 {
		t.Fatal("head/tail")
	}
	freq := sum["frequent"].([]map[string]any)
	if len(freq) == 0 || freq[0]["count"].(int) != 30 {
		t.Fatalf("frequent %v", freq)
	}
}

func TestJSONSummary(t *testing.T) {
	var items []map[string]any
	for i := range 50 {
		items = append(items, map[string]any{"pod": fmt.Sprintf("p-%d", i), "restarts": i, "ts": fmt.Sprintf("2026-11-07T10:%02d:00Z", i)})
	}
	raw, _ := json.Marshal(map[string]any{"items": items, "kind": "List"})
	ct, sum := Default().Summarize(raw)
	if ct != TypeJSON || sum["summarizer"] != "json" || sum["type"] != "object{2}" {
		t.Fatalf("summary %v", sum)
	}
	nested := sum["nested"].(map[string]any)["items"].(map[string]any)
	if nested["length"] != 50 {
		t.Fatalf("nested %v", nested)
	}
	stats := nested["numeric"].(map[string]any)["restarts"].(map[string]any)
	if stats["min"] != 0.0 || stats["max"] != 49.0 || stats["avg"] != 24.5 {
		t.Fatalf("stats %v", stats)
	}
	if _, ok := nested["time_fields"].(map[string]any)["ts"]; !ok {
		t.Fatal("time field not detected")
	}
}

func TestSummaryIsBounded(t *testing.T) {
	var b strings.Builder
	for i := range 2000 {
		fmt.Fprintf(&b, "%s unique-%d\n", strings.Repeat("x", 290), i)
	}
	_, sum := Default().Summarize([]byte(b.String()))
	out, _ := json.Marshal(sum)
	if len(out) > MaxSummaryBytes {
		t.Fatalf("summary is %d bytes", len(out))
	}
}

func TestParseStamp(t *testing.T) {
	for _, l := range []string{"2026-11-07 09:00:00 boot", "x 2026-11-07T09:00:00.123+08:00 y", "2026-11-07T09:00:00"} {
		if _, ok := ParseStamp(l); !ok {
			t.Errorf("ParseStamp(%q) failed", l)
		}
	}
	if _, ok := ParseStamp("no time here"); ok {
		t.Error("false positive")
	}
}
