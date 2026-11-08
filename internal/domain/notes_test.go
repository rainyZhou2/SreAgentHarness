package domain

import (
	"strings"
	"testing"
)

func TestNoteSet(t *testing.T) {
	var s NoteSet
	s.Seed([]Note{{Key: "goal", Value: "migrate db", Scope: NoteSession}})
	s.Apply(3, []NoteOp{
		{Key: "found", Value: "pool exhausted"},
		{Key: "lang", Value: "中文", Scope: "personal"},
		{Key: "x", Value: "y", Scope: "bogus"},
		{Key: " ", Value: "y"},
		{Key: "goal", Value: "migrate db v2", Scope: "session"},
	})
	if got := s.Scope(NoteRun, false); len(got) != 1 || got[0].Step != 3 {
		t.Fatalf("run: %+v", got)
	}
	if got := s.Scope(NoteSession, false); len(got) != 1 || got[0].Value != "migrate db v2" {
		t.Fatalf("session overwrite: %+v", got)
	}
	s.Apply(4, []NoteOp{{Key: "found", Value: ""}, {Key: "lang", Scope: "user", Delete: true}, {Key: "never", Value: ""}})
	if len(s.Scope(NoteRun, false)) != 0 || len(s.Scope(NoteRun, true)) != 1 {
		t.Fatal("run delete keeps a tombstone only for keys that existed")
	}
	if u := s.Scope(NoteUser, true); len(u) != 1 || !u[0].Deleted {
		t.Fatalf("user tombstone: %+v", u)
	}
	s.Apply(5, []NoteOp{{Key: "pref", Value: "terse", Scope: "user"}, {Key: "ghost", Scope: "user", Delete: true}})
	if u := s.Scope(NoteUser, true); len(u) != 3 {
		t.Fatalf("user deletes of unknown keys must reach the store: %+v", u)
	}
	for i := 0; i < MaxNotesPerScope+5; i++ {
		s.Apply(int64(10+i), []NoteOp{{Key: strings.Repeat("k", i+1), Value: "v"}})
	}
	if n := len(s.Scope(NoteRun, false)); n != MaxNotesPerScope {
		t.Fatalf("eviction: %d", n)
	}
	s.Apply(99, []NoteOp{{Key: "long", Value: strings.Repeat("字", 900)}})
	for _, n := range s.Live() {
		if n.Key == "long" && len([]rune(n.Value)) != MaxNoteValueRunes+1 {
			t.Fatal("value clipped")
		}
	}
	live := s.Live()
	if live[0].Scope != NoteRun || live[len(live)-1].Scope != NoteUser {
		t.Fatalf("live order by tier: %+v", live[len(live)-1])
	}
}
