package kernel

import (
	"fmt"
	"strings"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

func TestConversationWindow(t *testing.T) {
	var all []cognitive.ConversationMessage
	for i := 1; i <= 20; i++ {
		role := "user"
		if i%2 == 0 {
			role = "assistant"
		}
		all = append(all, cognitive.ConversationMessage{Seq: int64(i), Role: role,
			Text: fmt.Sprintf("message %d first paragraph\n\nsecond paragraph", i)})
	}
	recent, digest := conversationWindow(all, 4)
	if len(recent) != 4 || recent[0].Seq != 17 {
		t.Fatalf("window: %+v", recent)
	}
	if !strings.HasPrefix(digest, "#1 user: message 1") || !strings.Contains(digest, "#16 assistant: message 16 first paragraph") ||
		strings.Contains(digest, "#16 assistant: message 16 first paragraph second") {
		t.Fatalf("digest: %q", digest)
	}
	if r, d := conversationWindow(all, -1); len(r) != 20 || d != "" {
		t.Fatal("negative keeps all")
	}
	long := []cognitive.ConversationMessage{}
	for i := 1; i <= 60; i++ {
		long = append(long, cognitive.ConversationMessage{Seq: int64(i), Role: "user", Text: strings.Repeat("x", 500)})
	}
	_, d := conversationWindow(long, 2)
	if !strings.HasPrefix(d, "(") || !strings.Contains(d, "#58 user") || len([]rune(d)) > digestRunes+64 {
		t.Fatalf("bounded digest keeps the newest: %d %q", len([]rune(d)), d[:80])
	}
}

func TestRunMemory(t *testing.T) {
	if runMemory(nil, 3, nil) != nil {
		t.Fatal("nothing to hand on")
	}
	ns := &domain.NoteSet{}
	ns.Seed([]domain.Note{{Key: "goal", Value: "g", Scope: domain.NoteSession}})
	ns.Apply(2, []domain.NoteOp{{Key: "w", Value: "x"}, {Key: "lang", Value: "zh", Scope: "user"}})
	rm := runMemory(ns, 5, []domain.NoteOp{{Key: "goal", Scope: "session"}, {Key: "fact", Value: "f", Scope: "long_term"},
		{Key: "next", Value: "n", Scope: "session"}})
	if len(rm.Session) != 1 || rm.Session[0].Key != "next" || len(rm.User) != 1 || len(rm.LongTerm) != 1 {
		t.Fatalf("run memory: %+v", rm)
	}
}
