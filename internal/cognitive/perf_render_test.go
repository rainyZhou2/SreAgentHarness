package cognitive

import (
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
)

func TestHistoryCacheBreakpoint(t *testing.T) {
	a := Assembler{PromptPrefixVersion: "t", MaxContextTokens: 100000, MaxObservationBytes: 4000, MaxOutputTokens: 1000}
	in := ReasoningInput{Goal: "g", HasToolSteps: true,
		Observations: []domain.Observation{{StepSeq: 1, Tool: "a", Outcome: "ok", Content: []byte(`{}`)}},
		MoreTools:    []ToolHint{{Name: "kafka.lag", Summary: "Consumer lag"}}}
	req, _ := a.Build(in, "m")
	var marked []string
	var all strings.Builder
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			all.WriteString(p.Text + "\n")
			if p.CacheBreakpoint {
				marked = append(marked, p.Text)
			}
		}
	}
	if len(marked) != 1 || !strings.Contains(marked[0], "[observation step=1") {
		t.Fatalf("breakpoint on %q", marked)
	}
	if !strings.Contains(all.String(), "[more tools") || !strings.Contains(all.String(), "- kafka.lag: Consumer lag") {
		t.Fatal("more tools not rendered")
	}
	// The dynamic tail comes after the breakpoint.
	text := all.String()
	if strings.Index(text, "[more tools") < strings.Index(text, "[observation step=1") {
		t.Fatal("dynamic section before the cached history")
	}
}

func TestTrackToolCall(t *testing.T) {
	calls := map[int]*streamCall{}
	evs := []gw.Event{
		{Type: gw.EvToolCallDelta, Index: 1, ToolName: "prom.query"},
		{Type: gw.EvToolCallDelta, Index: 1, ArgsDelta: `{"q":`},
		{Type: gw.EvToolCallDelta, Index: 1, ArgsDelta: `1}`},
	}
	for _, ev := range evs {
		if _, _, ok := trackToolCall(calls, ev); ok {
			t.Fatal("call reported before completion")
		}
	}
	name, args, ok := trackToolCall(calls, gw.Event{Type: gw.EvToolCallComplete, Index: 1})
	if !ok || name != "prom.query" || string(args) != `{"q":1}` {
		t.Fatalf("%q %s %v", name, args, ok)
	}
	if len(calls) != 0 {
		t.Fatal("completed call kept")
	}
}
