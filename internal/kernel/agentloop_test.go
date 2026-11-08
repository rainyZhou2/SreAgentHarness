package kernel

import (
	"encoding/json"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

func TestFillTimeArgs(t *testing.T) {
	now := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)
	start := now.Add(-20 * time.Minute)
	desc := &tools.Descriptor{Name: "logs.search", TimeArgs: &tools.TimeArgs{Start: "from", End: "to"}}

	got := fillTimeArgs(desc, json.RawMessage(`{"q":"err"}`), &domain.GoalContext{ReceivedAt: now, Window: &domain.TimeWindow{Start: start}})
	var m map[string]any
	_ = json.Unmarshal(got, &m)
	if m["from"] != start.Format(time.RFC3339) || m["to"] != now.Format(time.RFC3339) || m["q"] != "err" {
		t.Fatalf("window: %s", got)
	}

	// The model's own values win.
	got = fillTimeArgs(desc, json.RawMessage(`{"from":"2026-01-01T00:00:00Z"}`), &domain.GoalContext{ReceivedAt: now})
	_ = json.Unmarshal(got, &m)
	if m["from"] != "2026-01-01T00:00:00Z" || m["to"] != now.Format(time.RFC3339) {
		t.Fatalf("explicit: %s", got)
	}

	// Lookback and unix formats.
	desc.TimeArgs = &tools.TimeArgs{Start: "start", End: "end", Format: "unix_ms", Lookback: tools.Duration(30 * time.Minute)}
	got = fillTimeArgs(desc, nil, &domain.GoalContext{ReceivedAt: now})
	m = map[string]any{}
	_ = json.Unmarshal(got, &m)
	if int64(m["start"].(float64)) != now.Add(-30*time.Minute).UnixMilli() || int64(m["end"].(float64)) != now.UnixMilli() {
		t.Fatalf("lookback: %s", got)
	}

	// No time context or no time args: untouched.
	if string(fillTimeArgs(desc, json.RawMessage(`{"a":1}`), &domain.GoalContext{})) != `{"a":1}` {
		t.Fatal("zero context must leave args")
	}
	if string(fillTimeArgs(&tools.Descriptor{}, json.RawMessage(`{"a":1}`), &domain.GoalContext{ReceivedAt: now})) != `{"a":1}` {
		t.Fatal("tool without time args must leave args")
	}
	if string(fillTimeArgs(desc, json.RawMessage(`[1]`), &domain.GoalContext{ReceivedAt: now})) != `[1]` {
		t.Fatal("non-object args must be left alone")
	}
}
