package kernel

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
)

// Short-term memory: the recent conversation window, a digest of the turns
// before it, and the session notes carried from the previous turn.
const (
	DefaultShortTermMessages = 12
	// digestRunes bounds the digest of older turns.
	digestRunes = 4000
	// digestLineRunes bounds one digest line.
	digestLineRunes = 240
)

type stepNotes struct {
	seq int64
	ops []domain.NoteOp
}

func (w *Worker) shortTermMessages() int {
	if w.cfg.ShortTermMessages == 0 {
		return DefaultShortTermMessages
	}
	return w.cfg.ShortTermMessages
}

// conversationWindow keeps the last n messages verbatim and condenses the
// earlier ones into a digest: one clipped line per message, newest kept
// when the digest is full. n < 0 keeps everything.
func conversationWindow(all []cognitive.ConversationMessage, n int) ([]cognitive.ConversationMessage, string) {
	if n < 0 || len(all) <= n {
		return all, ""
	}
	older, recent := all[:len(all)-n], all[len(all)-n:]
	if n == 0 {
		recent = nil
	}
	lines := make([]string, 0, len(older))
	used, omitted := 0, 0
	for i := len(older) - 1; i >= 0; i-- {
		m := older[i]
		text := m.Text
		if m.Role == "assistant" {
			text = leadText(text)
		}
		line := fmt.Sprintf("#%d %s: %s", m.Seq, m.Role, clipLine(text, digestLineRunes))
		l := utf8.RuneCountInString(line) + 1
		if used+l > digestRunes {
			omitted = i + 1
			break
		}
		used += l
		lines = append(lines, line)
	}
	for i, j := 0, len(lines)-1; i < j; i, j = i+1, j-1 {
		lines[i], lines[j] = lines[j], lines[i]
	}
	digest := strings.Join(lines, "\n")
	if omitted > 0 {
		digest = fmt.Sprintf("(%d earlier messages omitted)\n", omitted) + digest
	}
	return recent, digest
}

// leadText is the first paragraph of an answer (its conclusion).
func leadText(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "\n\n"); i > 0 {
		s = s[:i]
	}
	return s
}

func clipLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > n {
		return string([]rune(s)[:n]) + "…"
	}
	return s
}

// priorSessionNotes returns the session notes left by the latest completed
// turn of the branch (its result artifact carries them). Branch ancestry
// is resolved by the conversation itself, so a fork sees only its own
// history.
func (w *Worker) priorSessionNotes(ctx context.Context, tenantID string, msgs []domain.Message) []domain.Note {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m.Role != "assistant" || m.Kind == "status" || strings.HasPrefix(m.ContentRef, "status:") {
			continue
		}
		var res struct {
			Memory *domain.RunMemory `json:"memory"`
		}
		if err := w.get(ctx, tenantID, m.ContentRef, &res); err != nil || res.Memory == nil {
			return nil
		}
		out := make([]domain.Note, 0, len(res.Memory.Session))
		for _, n := range res.Memory.Session {
			if n.Scope == domain.NoteSession && !n.Deleted && n.Key != "" {
				n.Step = 0
				out = append(out, n)
			}
		}
		return out
	}
	return nil
}

func capNoteOps(ops []domain.NoteOp) []domain.NoteOp {
	const max = 12
	if len(ops) > max {
		return ops[:max]
	}
	return ops
}

// runMemory is the memory a finished run hands on: the session notes for
// the next turn, and its user and long_term writes for the consolidator.
func runMemory(ns *domain.NoteSet, step int64, final []domain.NoteOp) *domain.RunMemory {
	if ns == nil {
		ns = &domain.NoteSet{}
	}
	ns.Apply(step, capNoteOps(final))
	rm := &domain.RunMemory{Session: ns.Scope(domain.NoteSession, false), User: ns.Scope(domain.NoteUser, true),
		LongTerm: ns.Scope(domain.NoteLongTerm, true)}
	if len(rm.Session) == 0 && len(rm.User) == 0 && len(rm.LongTerm) == 0 {
		return nil
	}
	return rm
}
