package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// Personal memory: the stable preferences the agent keeps about the user
// (language, format, defaults). Only the owner sees and edits them.

type personalMemory struct {
	Subject string `json:"subject"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

// MemoryList prints the user's active personal memories.
func MemoryList(ctx context.Context, c *Client, out io.Writer) error {
	var r struct {
		Memories []personalMemory `json:"memories"`
	}
	if err := c.Do(ctx, "GET", "/v1/me/memories", nil, &r); err != nil {
		return err
	}
	n := 0
	for _, m := range r.Memories {
		if m.Status != "" && m.Status != "ACTIVE" {
			continue
		}
		// The subject key is what set/rm accept (the server normalizes keys).
		key := strings.TrimPrefix(m.Subject, "pref:")
		if key == "" {
			key = m.Title
		}
		fmt.Fprintf(out, "%-24s %s\n", key, m.Content)
		n++
	}
	if n == 0 {
		fmt.Fprintln(out, "no personal memories yet")
	}
	return nil
}

// MemorySet creates or revises one personal memory.
func MemorySet(ctx context.Context, c *Client, out io.Writer, key, value string) error {
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if key == "" || value == "" {
		return errors.New("a key and a value are required")
	}
	var r struct {
		Created    bool `json:"created"`
		Redactions int  `json:"redactions"`
	}
	if err := c.Do(ctx, "PUT", "/v1/me/memories/"+url.PathEscape(key), map[string]string{"value": value}, &r); err != nil {
		return err
	}
	verb := "updated"
	if r.Created {
		verb = "saved"
	}
	fmt.Fprintf(out, "%s %q\n", verb, key)
	if r.Redactions > 0 {
		fmt.Fprintf(out, "  note: %d sensitive value(s) were redacted before saving\n", r.Redactions)
	}
	return nil
}

// MemoryRemove archives one personal memory.
func MemoryRemove(ctx context.Context, c *Client, out io.Writer, key string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("a key is required")
	}
	if err := c.Do(ctx, "DELETE", "/v1/me/memories/"+url.PathEscape(key), nil, nil); err != nil {
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == 404 {
			return fmt.Errorf("no personal memory %q", key)
		}
		return err
	}
	fmt.Fprintf(out, "removed %q\n", key)
	return nil
}

// ---- learned (long-term) memory: the review queue ----
//
// The agent proposes long-term memories (CANDIDATE). Only ACTIVE ones are
// used in later runs, so someone has to review the proposals: without this
// the agent's learning never takes effect.

// Learned is a long-term memory as the review commands see it.
type Learned struct {
	ID           string   `json:"id"`
	Scope        string   `json:"scope"`
	ScopeID      string   `json:"scope_id"`
	Type         string   `json:"memory_type"`
	Status       string   `json:"status"`
	Version      int64    `json:"version"`
	Subject      string   `json:"subject"`
	Title        string   `json:"title"`
	Content      string   `json:"content"`
	SourceRefs   []string `json:"source_refs"`
	EvidenceRefs []string `json:"evidence_refs"`
	Trust        string   `json:"trust"`
	Confidence   *float64 `json:"confidence"`
	CreatedBy    string   `json:"created_by"`
	UpdatedAt    string   `json:"updated_at"`
}

// ListLearned returns the tenant's non-personal memories with a status.
func ListLearned(ctx context.Context, c *Client, status string) ([]Learned, error) {
	var r struct {
		Memories []Learned `json:"memories"`
	}
	if err := c.Do(ctx, "GET", "/v1/memories?limit=200&status="+url.QueryEscape(status), nil, &r); err != nil {
		return nil, err
	}
	out := r.Memories[:0]
	for _, m := range r.Memories {
		if m.Scope != "user" {
			out = append(out, m)
		}
	}
	return out, nil
}

// Pending returns proposals awaiting review (CANDIDATE and VALIDATING).
func Pending(ctx context.Context, c *Client) ([]Learned, error) {
	var out []Learned
	for _, st := range []string{"CANDIDATE", "VALIDATING"} {
		l, err := ListLearned(ctx, c, st)
		if err != nil {
			return nil, err
		}
		out = append(out, l...)
	}
	return out, nil
}

// Replaces finds the ACTIVE memory a proposal would supersede (same subject
// and scope), so a reviewer sees what changes.
func Replaces(m Learned, active []Learned) *Learned {
	if m.Subject == "" {
		return nil
	}
	for i := range active {
		a := active[i]
		if a.ID != m.ID && a.Subject == m.Subject && a.Scope == m.Scope && a.ScopeID == m.ScopeID {
			return &a
		}
	}
	return nil
}

func snippet(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

// MemoryReview prints the proposals awaiting review.
func MemoryReview(ctx context.Context, c *Client, out io.Writer) error {
	pend, err := Pending(ctx, c)
	if err != nil {
		return err
	}
	stale, _ := ListLearned(ctx, c, "STALE")
	defer func() {
		if len(stale) > 0 {
			fmt.Fprintf(out, "\nset aside (not used in runs; `harness memory approve ID` restores):\n")
			for _, m := range stale {
				fmt.Fprintf(out, "  %s  %s/%s  %s\n", shortID(m.ID), m.Scope, m.Type, m.Title)
			}
		}
	}()
	if len(pend) == 0 {
		fmt.Fprintln(out, "nothing to review")
		return nil
	}
	active, err := ListLearned(ctx, c, "ACTIVE")
	if err != nil {
		return err
	}
	for _, m := range pend {
		fmt.Fprintf(out, "%s  %-10s %s/%s  trust %s  %s\n", shortID(m.ID), m.Status, m.Scope, m.Type, m.Trust, m.Title)
		fmt.Fprintf(out, "    %s\n", snippet(m.Content, 200))
		if old := Replaces(m, active); old != nil {
			fmt.Fprintf(out, "    replaces %s: %s\n", shortID(old.ID), snippet(old.Content, 120))
		}
	}
	fmt.Fprintf(out, "\n%d to review: harness memory approve|reject ID   (show ID for details)\n", len(pend))
	return nil
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// ResolveLearned finds a memory by full id or unique prefix among those
// with the given statuses.
func ResolveLearned(ctx context.Context, c *Client, prefix string, statuses ...string) (Learned, error) {
	prefix = strings.TrimSpace(prefix)
	if len(prefix) < 4 {
		return Learned{}, errors.New("give at least 4 characters of the memory id")
	}
	var found []Learned
	for _, st := range statuses {
		l, err := ListLearned(ctx, c, st)
		if err != nil {
			return Learned{}, err
		}
		for _, m := range l {
			if strings.HasPrefix(m.ID, prefix) {
				found = append(found, m)
			}
		}
	}
	switch len(found) {
	case 0:
		return Learned{}, fmt.Errorf("no memory starts with %q", prefix)
	case 1:
		return found[0], nil
	}
	return Learned{}, fmt.Errorf("%q matches %d memories: use more characters", prefix, len(found))
}

// MemoryShow prints one memory in full.
func MemoryShow(ctx context.Context, c *Client, out io.Writer, prefix string) error {
	m, err := ResolveLearned(ctx, c, prefix, "CANDIDATE", "VALIDATING", "ACTIVE", "STALE")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "id:       %s (v%d)\nstatus:   %s\nscope:    %s %s  type %s  trust %s\ntitle:    %s\nby:       %s\n", m.ID, m.Version, m.Status,
		m.Scope, m.ScopeID, m.Type, m.Trust, m.Title, m.CreatedBy)
	if m.Confidence != nil {
		fmt.Fprintf(out, "confidence: %.2f\n", *m.Confidence)
	}
	fmt.Fprintf(out, "\n%s\n", m.Content)
	for _, r := range m.SourceRefs {
		fmt.Fprintf(out, "source:   %s\n", r)
	}
	for _, r := range m.EvidenceRefs {
		fmt.Fprintf(out, "evidence: %s\n", r)
	}
	if m.Status == "CANDIDATE" || m.Status == "VALIDATING" {
		if active, err := ListLearned(ctx, c, "ACTIVE"); err == nil {
			if old := Replaces(m, active); old != nil {
				fmt.Fprintf(out, "\nreplaces %s:\n%s\n", shortID(old.ID), old.Content)
			}
		}
	}
	return nil
}

// MemoryDecide approves or rejects a proposal, or archives an active memory.
// action is "approve", "reject" or "archive".
func MemoryDecide(ctx context.Context, c *Client, out io.Writer, prefix, action, reason string) error {
	var statuses []string
	var to string
	switch action {
	case "approve": // also restores a memory the curator set aside (STALE)
		statuses, to = []string{"CANDIDATE", "VALIDATING", "STALE"}, "ACTIVE"
	case "reject":
		statuses, to = []string{"CANDIDATE", "VALIDATING"}, "REJECTED"
	case "archive":
		statuses, to = []string{"ACTIVE", "STALE"}, "ARCHIVED"
	default:
		return fmt.Errorf("unknown action %q", action)
	}
	m, err := ResolveLearned(ctx, c, prefix, statuses...)
	if err != nil {
		return err
	}
	body := map[string]string{"expected_status": m.Status, "to": to, "reason": reason}
	if err := c.Do(ctx, "POST", "/v1/memories/"+url.PathEscape(m.ID)+"/transition", body, nil); err != nil {
		return err
	}
	verb := map[string]string{"approve": "approved", "reject": "rejected", "archive": "archived"}[action]
	if action == "approve" && m.Status == "STALE" {
		verb = "restored"
	}
	fmt.Fprintf(out, "%s %s (%s)\n", verb, shortID(m.ID), m.Title)
	return nil
}
