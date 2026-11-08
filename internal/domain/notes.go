package domain

import (
	"sort"
	"strings"
	"unicode/utf8"
)

// Memory notes are the agent's own writes to its memory tiers. The model
// emits them in a THOUGHT line or in its conclusion; the kernel rebuilds
// them from the stored decisions, so they are as durable and replayable as
// the decisions themselves.
//
//	run       working memory: findings, constraints, next checks (this run)
//	session   short-term memory: carried into later turns of the conversation
//	user      personal memory: a stable preference of the user who asked
//	long_term proposed team memory: written as a CANDIDATE for review
const (
	NoteRun      = "run"
	NoteSession  = "session"
	NoteUser     = "user"
	NoteLongTerm = "long_term"
)

// Note limits keep the tiers small enough to render on every step.
const (
	MaxNoteKeyRunes   = 64
	MaxNoteValueRunes = 500
	MaxNotesPerScope  = 24
)

// NoteOp is one write: set Key to Value in Scope, or delete it (Delete,
// or an empty Value).
type NoteOp struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Scope  string `json:"scope,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

// Note is the current value of a key. Deleted notes are kept as tombstones
// for the user and long_term scopes, whose deletes must reach the store.
type Note struct {
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Scope   string `json:"scope"`
	Step    int64  `json:"step,omitempty"` // step of the last write (0: an earlier turn)
	Deleted bool   `json:"deleted,omitempty"`
}

// NormalizeNoteScope maps a scope name to one of the four tiers ("" when
// unknown); an empty scope means run.
func NormalizeNoteScope(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "run", "working", "task":
		return NoteRun
	case "session", "conversation", "short_term":
		return NoteSession
	case "user", "personal", "preference":
		return NoteUser
	case "long_term", "longterm", "team", "memory":
		return NoteLongTerm
	}
	return ""
}

func clipRunes(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > n {
		s = string([]rune(s)[:n]) + "…"
	}
	return s
}

// NoteSet holds notes by scope and key, in write order.
type NoteSet struct {
	notes map[string]Note // scope + "\x00" + key
	order []string
}

func noteID(scope, key string) string { return scope + "\x00" + key }

// Seed adds notes carried over from an earlier turn.
func (s *NoteSet) Seed(notes []Note) {
	for _, n := range notes {
		s.put(n)
	}
}

func (s *NoteSet) put(n Note) {
	if s.notes == nil {
		s.notes = map[string]Note{}
	}
	id := noteID(n.Scope, n.Key)
	if _, ok := s.notes[id]; ok {
		for i, o := range s.order {
			if o == id {
				s.order = append(s.order[:i], s.order[i+1:]...)
				break
			}
		}
	}
	s.notes[id] = n
	s.order = append(s.order, id)
	// Oldest live notes of the scope go first when it is full.
	live := 0
	for _, o := range s.order {
		if x := s.notes[o]; x.Scope == n.Scope && !x.Deleted {
			live++
		}
	}
	for i := 0; live > MaxNotesPerScope && i < len(s.order); {
		x := s.notes[s.order[i]]
		if x.Scope == n.Scope && !x.Deleted {
			delete(s.notes, s.order[i])
			s.order = append(s.order[:i], s.order[i+1:]...)
			live--
			continue
		}
		i++
	}
}

// Apply records the ops of the decision at step. Ops with an unknown scope
// or an empty key are ignored; keys and values are clipped.
func (s *NoteSet) Apply(step int64, ops []NoteOp) {
	for _, op := range ops {
		scope := NormalizeNoteScope(op.Scope)
		key := clipRunes(op.Key, MaxNoteKeyRunes)
		if scope == "" || key == "" {
			continue
		}
		val := clipRunes(op.Value, MaxNoteValueRunes)
		if op.Delete || val == "" {
			if _, ok := s.notes[noteID(scope, key)]; !ok && (scope == NoteRun || scope == NoteSession) {
				continue
			}
			s.put(Note{Key: key, Scope: scope, Step: step, Deleted: true})
			continue
		}
		s.put(Note{Key: key, Value: val, Scope: scope, Step: step})
	}
}

// Scope returns the notes of one scope in write order; tombstones only
// when withDeleted.
func (s *NoteSet) Scope(scope string, withDeleted bool) []Note {
	var out []Note
	for _, id := range s.order {
		n := s.notes[id]
		if n.Scope == scope && (withDeleted || !n.Deleted) {
			out = append(out, n)
		}
	}
	return out
}

// Live returns every live note, sorted by scope then write order.
func (s *NoteSet) Live() []Note {
	rank := map[string]int{NoteRun: 0, NoteSession: 1, NoteUser: 2, NoteLongTerm: 3}
	var out []Note
	for _, id := range s.order {
		if n := s.notes[id]; !n.Deleted {
			out = append(out, n)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return rank[out[i].Scope] < rank[out[j].Scope] })
	return out
}

// RunMemory is what a finished run hands to later turns and to the
// consolidator (the "memory" field of the run result).
type RunMemory struct {
	// Session is the full short-term note set after this run (later turns
	// start from it).
	Session []Note `json:"session,omitempty"`
	// User and LongTerm are this run's writes (tombstones included).
	User     []Note `json:"user,omitempty"`
	LongTerm []Note `json:"long_term,omitempty"`
}
