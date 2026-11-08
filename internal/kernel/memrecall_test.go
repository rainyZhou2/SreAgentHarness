package kernel

import (
	"encoding/json"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

func TestRecalledMemoriesAndBatchEvidence(t *testing.T) {
	content := json.RawMessage(`{"query":"q","hits":[{"ref":"memory:m1@2","memory_id":"m1","version":2,"title":"pool","trust":"T2"},{"memory_id":"","version":1}]}`)
	rec := recalledMemories(content)
	if len(rec) != 1 || rec[0].ID != "m1" || rec[0].Version != 2 {
		t.Fatalf("recalled: %+v", rec)
	}
	in := cognitive.ReasoningInput{
		Memories:         []cognitive.MemoryRef{{ID: "m0", Version: 1}},
		RecalledMemories: append(rec, cognitive.MemoryRef{ID: "m0", Version: 9}),
		Observations: []domain.Observation{
			{StepSeq: 1, Tool: domain.MemorySearchToolName, Outcome: "ok", Content: content},
			{StepSeq: 2, Part: 1, Tool: "a", Outcome: "error", Content: json.RawMessage(`"x"`)},
			{StepSeq: 2, Part: 2, Tool: "b", Outcome: "ok", Content: json.RawMessage(`"y"`)},
		},
	}
	refs := cognitive.NewRefSet(in)
	if got, ok := refs.Resolve("memory:m1@2"); !ok || got != "memory:m1@2" {
		t.Fatal("a recalled memory must be citable")
	}
	if _, ok := refs.Resolve("memory:m0@9"); ok {
		t.Fatal("the pinned version wins over a recalled one")
	}
	ev := conclusionEvidence(in)
	if e := ev["memory:m1@2"]; e.Kind != "memory" {
		t.Fatalf("recalled memory evidence: %+v", e)
	}
	if !ev[cognitive.ObsPartRef(2, 2)].OK || ev[cognitive.ObsPartRef(2, 1)].OK {
		t.Fatal("batch parts keep their own outcome")
	}
	if whole := ev[cognitive.ObsRef(2)]; !whole.OK || whole.Text == "" {
		t.Fatalf("whole batch step: %+v", whole)
	}
}
