package memory

import (
	"context"
	"strings"
	"time"
)

// ---------- updating memory ----------
//
// A memory changes in four ways:
//   - reinforcement: the same or a near-identical memory is written again
//     from a new source; the existing one counts it instead of a duplicate
//     row (CreateMemory returns it with created=false);
//   - revision: UpdateMemory writes a new version of the content (pinned
//     citations of the old version stay valid); embeddings and search
//     terms follow the new version;
//   - supersession: activating a memory retires ACTIVE memories with the
//     same subject in the same scope (a newer answer to the same question);
//   - lifecycle: feedback, review and retention move it between statuses.

// NearDuplicateCosine is the embedding similarity at or above which two
// memories with the same scope and subject are the same memory.
const NearDuplicateCosine = 0.95

// NearDuplicateJaccard is the token overlap used when no embedding of the
// same model is available.
const NearDuplicateJaccard = 0.85

// MaxSourceRefs bounds the sources a memory accumulates by reinforcement.
const MaxSourceRefs = 50

// StatusRank orders live statuses by how validated they are.
func StatusRank(s string) int {
	switch s {
	case "ACTIVE":
		return 2
	case "VALIDATING":
		return 1
	case "CANDIDATE":
		return 0
	}
	return -1
}

// NearDuplicate reports whether existing (with its embedding in the new
// memory's model, if any) says the same as the new memory. Both must share
// scope and subject; the caller checks that.
func NearDuplicate(n NewMemory, existing Memory, existingEmb []float32) bool {
	if len(n.Embedding) > 0 && len(existingEmb) == len(n.Embedding) {
		return Cosine(n.Embedding, existingEmb) >= NearDuplicateCosine
	}
	a := tokenSet(n.Title + "\n" + n.Content)
	b := tokenSet(existing.Title + "\n" + existing.Content)
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	inter := 0
	for t := range a {
		if b[t] {
			inter++
		}
	}
	return float64(inter)/float64(len(a)+len(b)-inter) >= NearDuplicateJaccard
}

func tokenSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, t := range Tokenize(s) {
		out[t] = true
	}
	return out
}

// MergeSources appends the refs not already present, keeping at most
// MaxSourceRefs, and reports whether any was new.
func MergeSources(cur, add []string) ([]string, bool) {
	seen := make(map[string]bool, len(cur))
	for _, r := range cur {
		seen[r] = true
	}
	out, changed := append([]string(nil), cur...), false
	for _, r := range add {
		if r == "" || seen[r] {
			continue
		}
		seen[r] = true
		changed = true
		if len(out) < MaxSourceRefs {
			out = append(out, r)
		}
	}
	return out, changed
}

// ReinforcementBoost is the ranking factor of a memory written again from
// independent sources (1 for none, at most 1.3).
func ReinforcementBoost(n int) float64 {
	if n <= 0 {
		return 1
	}
	if n > 10 {
		n = 10
	}
	return 1 + 0.03*float64(n)
}

// Revision is a new version of a memory's content.
type Revision struct {
	TenantID        string
	ID              string
	ExpectedVersion int64 // 0: any
	// Prepared carries the new title, content, refs, hash, search terms and
	// embedding (Writer.PrepareRevision).
	Prepared NewMemory
	Actor    string
	Reason   string
}

// Updater is implemented by stores that revise and look up memories.
type Updater interface {
	// LiveMemoryBySubject returns the most validated live memory
	// (ACTIVE, VALIDATING, CANDIDATE) with this scope and subject.
	LiveMemoryBySubject(ctx context.Context, tenantID, scope, scopeID, subject string) (Memory, bool, error)
	UpdateMemory(ctx context.Context, r Revision) (Memory, error)
	TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (Memory, error)
}

// PrepareRevision validates and prepares new content for an existing
// memory: the same scope, subject and kind, a new title and/or content.
func (w Writer) PrepareRevision(ctx context.Context, cur Memory, title, content string, evidence []string, source string) (NewMemory, error) {
	if strings.TrimSpace(title) == "" {
		title = cur.Title
	}
	if strings.TrimSpace(content) == "" {
		content = cur.Content
	}
	srcs, _ := MergeSources(cur.SourceRefs, []string{source})
	if evidence == nil {
		evidence = cur.EvidenceRefs
	}
	return w.Prepare(ctx, NewMemory{TenantID: cur.TenantID, Scope: cur.Scope, ScopeID: cur.ScopeID, Type: cur.Type,
		Status: cur.Status, Subject: cur.Subject, Title: title, Content: content, SourceRefs: srcs, EvidenceRefs: evidence,
		Trust: cur.Trust, Sensitivity: cur.Sensitivity, Confidence: cur.Confidence, Environment: cur.Environment,
		ExpiresAt: cur.ExpiresAt, CreatedBy: cur.CreatedBy})
}

// ---------- personal memory ----------

// PersonalStore is implemented by stores that keep per-user memory.
type PersonalStore interface {
	// RunInitiator returns the user who started the run ("" when it was
	// not a user: a trigger, a schedule, a parent flow).
	RunInitiator(ctx context.Context, tenantID, runID string) (string, error)
	// PersonalMemories returns the user's ACTIVE, unexpired memories,
	// most recently updated first.
	PersonalMemories(ctx context.Context, tenantID, userID string, limit int) ([]Memory, error)
}

// MaxPersonalMemories bounds the personal memories pinned for one run.
const MaxPersonalMemories = 12

// PersonalSubject is the subject of a user's preference with this key.
func PersonalSubject(key string) string { return "pref:" + subjectKey(key) }

// NoteSubject is the subject of a long_term note with this key.
func NoteSubject(key string) string { return "note:" + subjectKey(key) }

func subjectKey(key string) string { return strings.ToLower(strings.Join(strings.Fields(key), "_")) }

// expired reports whether a memory's expiry has passed.
func expired(m Memory, now time.Time) bool { return m.ExpiresAt != nil && !m.ExpiresAt.After(now) }
