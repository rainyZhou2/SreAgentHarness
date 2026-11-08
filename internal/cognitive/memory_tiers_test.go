package cognitive

import (
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
)

func TestNotesParsedAndRendered(t *testing.T) {
	resp := gw.Response{Text: "THOUGHT {\"plan\":\"p\",\"notes\":[{\"key\":\"found\",\"value\":\"pool\",\"scope\":\"run\"}]}\nchecking",
		ToolCalls: []gw.ToolCall{{Name: "x", Args: json.RawMessage(`{}`)}}}
	d, err := decisionFromResponse(resp)
	if err != nil || len(d.Notes) != 1 || d.Notes[0].Key != "found" {
		t.Fatalf("thought notes: %v %+v", err, d.Notes)
	}
	resp = gw.Response{ToolCalls: []gw.ToolCall{{Name: ConclusionTool,
		Args: json.RawMessage(`{"answer":"a","reason_summary":"r","notes":[{"key":"lang","value":"zh","scope":"user"}]}`)}}}
	d, err = decisionFromResponse(resp)
	if err != nil || len(d.Notes) != 1 || d.Notes[0].Scope != "user" {
		t.Fatalf("conclusion notes: %v %+v", err, d.Notes)
	}

	in := ReasoningInput{Goal: "g", ConversationDigest: "#1 user: old question",
		Conversation: []ConversationMessage{{Seq: 3, Role: "user", Text: "recent"}},
		Notes:        []domain.Note{{Key: "found", Value: "pool", Scope: "run", Step: 2}, {Key: "goal", Value: "migrate", Scope: "session"}},
		Memories: []MemoryRef{{ID: "p1", Version: 2, Title: "answer_language", Content: "中文", Scope: "user"},
			{ID: "m1", Version: 1, Title: "past", Content: "c", Scope: "service"}}}
	req, man := DefaultAssembler().Build(in, "m")
	all, _ := json.Marshal(req)
	text := string(all)
	for _, want := range []string{"earlier conversation", "old question", "working memory", "found = pool (step 2)",
		"goal = migrate (earlier turn)", "about the user", "memory:p1@2", "answer_language: 中文", "memory ref=memory:m1@1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q", want)
		}
	}
	if strings.Contains(text, "memory ref=memory:p1@2") {
		t.Fatal("personal memory must not be rendered as a past-incident memory")
	}
	if man.SectionHashes["W"] == "" || man.SectionHashes["P"] == "" || len(man.MemoryRefs) != 2 {
		t.Fatalf("manifest: %+v", man)
	}
	if refs := NewRefSet(in); refs == nil {
		t.Fatal("refs")
	} else if _, ok := refs.Resolve("memory:p1@2"); !ok {
		t.Fatal("personal memory citable")
	}
}
