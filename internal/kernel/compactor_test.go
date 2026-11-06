package kernel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/tools"
)

// blankCompactor drops all history: the kernel must still keep safety facts.
type blankCompactor struct{ calls int }

func (*blankCompactor) Name() string    { return "blank" }
func (*blankCompactor) KeepRecent() int { return 1 }
func (b *blankCompactor) Compact(context.Context, kernel.CompactInput) (string, error) {
	b.calls++
	return "", nil
}

func TestCompactorPluginCannotDropSafetyFacts(t *testing.T) {
	// R3 is always denied by the framework; the run continues with a
	// "not executed" observation that compaction must preserve.
	broken := &tools.Func{Desc: tools.Descriptor{Name: "t.broken", Risk: domain.RiskR3},
		Fn: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return nil, fmt.Errorf("must never run: %w", domain.ErrNotDispatched)
		}}
	var mu sync.Mutex
	var summary string
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		mu.Lock()
		defer mu.Unlock()
		if in.Summary != "" {
			summary = in.Summary
			return domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", Answer: "ok"}, nil
		}
		if len(in.Observations) == 0 {
			return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "try", Tool: &domain.ToolCall{Name: "t.broken", Args: json.RawMessage(`{}`)}}, nil
		}
		return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "gather", Tool: metricsCall("checkout")}, nil
	}}
	comp := &blankCompactor{}
	h := start(t, opts{strategy: strat, tools: append(tools.DemoTools(), broken),
		worker: kernel.Config{CompactEvery: 3, Compactor: comp}})
	run := h.create(t, "investigate service=checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)
	mu.Lock()
	defer mu.Unlock()
	if comp.calls == 0 {
		t.Fatal("custom compactor was not used")
	}
	if !strings.Contains(summary, "t.broken was NOT executed successfully") {
		t.Fatalf("safety fact dropped; summary = %q", summary)
	}
}
