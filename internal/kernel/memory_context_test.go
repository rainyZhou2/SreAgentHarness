package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

type failingRecall struct{ err error }

func (f failingRecall) Retrieve(context.Context, domain.Lease, string) (domain.StepSuccess, error) {
	return domain.StepSuccess{}, nil
}
func (f failingRecall) RecallableMemory(context.Context, string, string, string, int64) (bool, error) {
	return false, f.err
}

func TestMemorySearchContextFailsClosed(t *testing.T) {
	content := json.RawMessage(`{"hits":[{"memory_id":"m","version":1,"content":"obsolete advice"}]}`)
	for _, raw := range []json.RawMessage{content, json.RawMessage(`{"hits":[{"content":"obsolete advice"}]`)} {
		in := cognitive.ReasoningInput{Observations: []domain.Observation{{Tool: domain.MemorySearchToolName, Outcome: "ok", Content: raw}}}
		w := &Worker{}
		if err := w.revalidateMemorySearches(context.Background(), domain.Lease{}, &in); err != nil {
			t.Fatal(err)
		}
		if len(in.RecalledMemories) > 0 || strings.Contains(string(in.Observations[0].Content), "obsolete advice") {
			t.Fatalf("unverifiable history retained: %+v", in)
		}
	}
	want := errors.New("ledger unavailable")
	w := &Worker{cfg: Config{Memory: failingRecall{err: want}}}
	in := cognitive.ReasoningInput{Observations: []domain.Observation{{Tool: domain.MemorySearchToolName, Outcome: "ok", Content: content}}}
	if err := w.revalidateMemorySearches(context.Background(), domain.Lease{}, &in); !errors.Is(err, want) {
		t.Fatalf("live ledger error must stop rendering: %v", err)
	}
}
