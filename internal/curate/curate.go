// Package curate keeps long-term memory from rotting. Memories accumulate
// duplicates, facts that were true once and then changed, lessons later runs
// refuted, and entries nobody retrieves any more. Nothing else removes them:
// retrieval only ranks.
//
// The curator is an offline pass in the spirit of "dreaming" (reflect on
// stored experience, correct mistakes), with the properties an operations
// system needs:
//
//   - It only PROPOSES. A proposal says "set these memories aside, and why";
//     a person approves or dismisses it. Nothing changes without that.
//   - It writes no new text. It never drafts, merges or rewrites a memory, so
//     a poisoned run cannot get its words into memory through this path; the
//     worst a bad signal can do is propose to set a true memory aside, which a
//     reviewer sees and refuses.
//   - Setting aside is reversible: the memory becomes STALE (out of
//     retrieval) and can be restored with one transition.
//   - Every proposal carries its evidence: the memories compared and the
//     verified runs that contradicted a lesson. Ground truth is the run
//     record, and only runs an independent check SUPPORTED count.
//   - It is idempotent: the same finding is never proposed twice, and a
//     dismissed finding is not raised again until a memory in it changes.
//
// Signals, strongest first: a lesson refuted by later verified runs; a fact
// superseded by a newer memory with different figures; a duplicate; a memory
// unused for a long time.
package curate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/runsearch"
)

// Signal is why a memory is proposed to be set aside.
type Signal string

const (
	RefutedByHistory Signal = "refuted_by_history"
	SupersededFact   Signal = "superseded_fact"
	Duplicate        Signal = "duplicate"
	Unused           Signal = "unused"
)

var priority = map[Signal]int{RefutedByHistory: 0, SupersededFact: 1, Duplicate: 2, Unused: 3}

// Proposal statuses.
const (
	Pending   = "PENDING"
	Applied   = "APPLIED"
	Dismissed = "DISMISSED"
	// Outdated: a target changed after the proposal was made, so it no
	// longer describes reality; nothing was done.
	Outdated = "OUTDATED"
)

// Target is a memory as the proposal saw it.
type Target struct {
	MemoryID string `json:"memory_id"`
	Version  int64  `json:"version"`
	Title    string `json:"title"`
	Content  string `json:"content"`
	Scope    string `json:"scope"`
	ScopeID  string `json:"scope_id"`
}

// Proposal asks to set Targets aside.
type Proposal struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	Signal    Signal `json:"signal"`
	Status    string `json:"status"`
	Signature string `json:"signature"`
	// Targets are set aside (ACTIVE to STALE) when the proposal is approved.
	Targets []Target `json:"targets"`
	// Keeper is the memory that stays, when the signal compares two.
	Keeper   *Target  `json:"keeper,omitempty"`
	Reason   string   `json:"reason"`
	Evidence []string `json:"evidence,omitempty"`
	Job      string   `json:"job"`

	CreatedAt      time.Time  `json:"created_at"`
	DecidedAt      *time.Time `json:"decided_at,omitempty"`
	DecidedBy      string     `json:"decided_by,omitempty"`
	DecisionReason string     `json:"decision_reason,omitempty"`
}

// Store persists proposals.
type Store interface {
	// CreateProposal stores p unless a proposal with the same signature
	// already exists in any status (created=false then).
	CreateProposal(ctx context.Context, p Proposal) (stored Proposal, created bool, err error)
	ListProposals(ctx context.Context, tenantID, status string, limit int) ([]Proposal, error)
	GetProposal(ctx context.Context, tenantID, id string) (Proposal, error)
	// DecideProposal moves a PENDING proposal to status.
	DecideProposal(ctx context.Context, tenantID, id, status, actor, reason string, at time.Time) (Proposal, error)
}

// Memories is the memory store the curator reads and changes.
type Memories interface {
	// ActiveMemories returns up to limit ACTIVE memories of the tenant.
	ActiveMemories(ctx context.Context, tenantID string, limit int) ([]memory.Memory, error)
	GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error)
	TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error)
	// MemoryLastHits is when each memory was last retrieved into a run.
	MemoryLastHits(ctx context.Context, tenantID string, ids []string) (map[string]time.Time, error)
}

// History is the run record used as ground truth.
type History interface {
	Outcomes(ctx context.Context, tenantID string, since time.Time, max int) ([]runsearch.Outcome, error)
}

// Config tunes the thresholds.
type Config struct {
	// StaleAfter: a memory neither created, reinforced nor retrieved for this
	// long is proposed as unused (default 90 days; 0 disables the signal).
	StaleAfter time.Duration
	// DuplicateSimilarity is the token Jaccard at which two memories of one
	// scope count as duplicates (default 0.85).
	DuplicateSimilarity float64
	// FactSimilarity is the word Jaccard (numbers ignored) at which two
	// memories state the same fact (default 0.6).
	FactSimilarity float64
	// MinRefutations is how many distinct verified runs must refute a lesson
	// with none supporting it (default 2).
	MinRefutations int
	// HistoryDays is how far back runs are read (default 60).
	HistoryDays int
	// MaxProposals caps one pass (default 50).
	MaxProposals int
	// Now is the clock (tests).
	Now func() time.Time
}

func (c *Config) defaults() {
	if c.StaleAfter == 0 {
		c.StaleAfter = 90 * 24 * time.Hour
	}
	if c.DuplicateSimilarity <= 0 {
		c.DuplicateSimilarity = 0.85
	}
	if c.FactSimilarity <= 0 {
		c.FactSimilarity = 0.6
	}
	if c.MinRefutations <= 0 {
		c.MinRefutations = 2
	}
	if c.HistoryDays <= 0 {
		c.HistoryDays = 60
	}
	if c.MaxProposals <= 0 {
		c.MaxProposals = 50
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Curator runs curation passes.
type Curator struct {
	Memories Memories
	History  History // nil: the history signal is skipped
	Store    Store
	Cfg      Config
}

// Report is the outcome of one pass.
type Report struct {
	Job      string
	Examined int
	Created  map[Signal]int
	// Existing counts findings already proposed (pending or decided).
	Existing int
	// Skipped says what could not be checked.
	Skipped []string
}

// Total is the number of new proposals.
func (r Report) Total() int {
	n := 0
	for _, v := range r.Created {
		n += v
	}
	return n
}

type finding struct {
	signal   Signal
	targets  []memory.Memory
	keeper   *memory.Memory
	reason   string
	evidence []string
}

// Run examines the tenant's live long-term memories and records new proposals.
func (c *Curator) Run(ctx context.Context, tenantID string) (Report, error) {
	c.Cfg.defaults()
	rep := Report{Job: "curate-" + c.Cfg.Now().UTC().Format("20060102T150405Z"), Created: map[Signal]int{}}
	all, err := c.Memories.ActiveMemories(ctx, tenantID, 500)
	if err != nil {
		return rep, err
	}
	var live []memory.Memory
	for _, m := range all {
		if m.Scope != "user" { // personal memory is the user's, not the tenant's
			live = append(live, m)
		}
	}
	rep.Examined = len(live)
	var found []finding
	if c.History != nil {
		fs, err := c.refutedByHistory(ctx, tenantID, live)
		if err != nil {
			rep.Skipped = append(rep.Skipped, "history: "+err.Error())
		}
		found = append(found, fs...)
	} else {
		rep.Skipped = append(rep.Skipped, "history: no run history available")
	}
	found = append(found, supersededFacts(live, c.Cfg)...)
	found = append(found, duplicates(live, c.Cfg)...)
	if c.Cfg.StaleAfter > 0 {
		fs, err := c.unused(ctx, tenantID, live)
		if err != nil {
			rep.Skipped = append(rep.Skipped, "unused: "+err.Error())
		}
		found = append(found, fs...)
	}

	// A memory appears in its strongest finding only.
	sort.SliceStable(found, func(i, j int) bool { return priority[found[i].signal] < priority[found[j].signal] })
	claimed := map[string]bool{}
	for _, f := range found {
		var free []memory.Memory
		for _, t := range f.targets {
			if !claimed[t.ID] {
				free = append(free, t)
			}
		}
		if len(free) == 0 {
			continue
		}
		if rep.Total() >= c.Cfg.MaxProposals {
			rep.Skipped = append(rep.Skipped, fmt.Sprintf("capped at %d proposals; run again after reviewing", c.Cfg.MaxProposals))
			break
		}
		f.targets = free
		p := c.proposal(tenantID, rep.Job, f)
		_, created, err := c.Store.CreateProposal(ctx, p)
		if err != nil {
			return rep, err
		}
		for _, t := range free {
			claimed[t.ID] = true
		}
		if created {
			rep.Created[f.signal]++
		} else {
			rep.Existing++
		}
	}
	return rep, nil
}

func (c *Curator) proposal(tenantID, job string, f finding) Proposal {
	p := Proposal{ID: uuid.NewString(), TenantID: tenantID, Signal: f.signal, Status: Pending, Reason: f.reason,
		Evidence: f.evidence, Job: job, CreatedAt: c.Cfg.Now().UTC()}
	for _, t := range f.targets {
		p.Targets = append(p.Targets, target(t))
	}
	sort.Slice(p.Targets, func(i, j int) bool { return p.Targets[i].MemoryID < p.Targets[j].MemoryID })
	if f.keeper != nil {
		k := target(*f.keeper)
		p.Keeper = &k
		p.Evidence = append([]string{fmt.Sprintf("memory:%s@%d", k.MemoryID, k.Version)}, p.Evidence...)
	}
	for _, t := range p.Targets {
		p.Evidence = append(p.Evidence, fmt.Sprintf("memory:%s@%d", t.MemoryID, t.Version))
	}
	p.Evidence = uniq(p.Evidence)
	p.Signature = signature(f.signal, p.Targets)
	return p
}

func target(m memory.Memory) Target {
	return Target{MemoryID: m.ID, Version: m.Version, Title: m.Title, Content: m.Content, Scope: m.Scope, ScopeID: m.ScopeID}
}

// signature identifies a finding by signal and the exact memory versions, so
// a dismissed finding returns only when a memory in it changed.
func signature(sig Signal, ts []Target) string {
	parts := make([]string, 0, len(ts))
	for _, t := range ts {
		parts = append(parts, fmt.Sprintf("%s@%d", t.MemoryID, t.Version))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(string(sig) + "|" + strings.Join(parts, ",")))
	return hex.EncodeToString(sum[:16])
}

func uniq(xs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

// ---- signals ----

type tokset map[string]bool

func tokens(m memory.Memory) (all, words, nums tokset) {
	all, words, nums = tokset{}, tokset{}, tokset{}
	for _, t := range memory.Tokenize(m.Title + " " + m.Content) {
		all[t] = true
		if strings.ContainsAny(t, "0123456789") {
			nums[t] = true
		} else {
			words[t] = true
		}
	}
	return
}

func jaccard(a, b tokset) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for t := range a {
		if b[t] {
			inter++
		}
	}
	return float64(inter) / float64(len(a)+len(b)-inter)
}

func sameSet(a, b tokset) bool {
	if len(a) != len(b) {
		return false
	}
	for t := range a {
		if !b[t] {
			return false
		}
	}
	return true
}

type scopeKey struct{ scope, id string }

func byScope(ms []memory.Memory) map[scopeKey][]memory.Memory {
	g := map[scopeKey][]memory.Memory{}
	for _, m := range ms {
		k := scopeKey{m.Scope, m.ScopeID}
		g[k] = append(g[k], m)
	}
	return g
}

// better ranks memories to keep: verified, reinforced, confident, recent.
func better(a, b memory.Memory) bool {
	if (a.VerifiedBy != nil) != (b.VerifiedBy != nil) {
		return a.VerifiedBy != nil
	}
	if a.Reinforcements != b.Reinforcements {
		return a.Reinforcements > b.Reinforcements
	}
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	return a.ID < b.ID
}

func duplicates(ms []memory.Memory, cfg Config) []finding {
	var out []finding
	for _, group := range byScope(ms) {
		n := len(group)
		toks := make([]tokset, n)
		for i, m := range group {
			toks[i], _, _ = tokens(m)
		}
		parent := make([]int, n)
		for i := range parent {
			parent[i] = i
		}
		var find func(int) int
		find = func(i int) int {
			for parent[i] != i {
				parent[i] = parent[parent[i]]
				i = parent[i]
			}
			return i
		}
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				if jaccard(toks[i], toks[j]) >= cfg.DuplicateSimilarity {
					parent[find(i)] = find(j)
				}
			}
		}
		clusters := map[int][]memory.Memory{}
		for i, m := range group {
			clusters[find(i)] = append(clusters[find(i)], m)
		}
		for _, cl := range clusters {
			if len(cl) < 2 {
				continue
			}
			sort.Slice(cl, func(i, j int) bool { return better(cl[i], cl[j]) })
			keeper := cl[0]
			out = append(out, finding{signal: Duplicate, keeper: &keeper, targets: cl[1:],
				reason: fmt.Sprintf("Says the same as %q (similarity at least %.0f%%); keeping the better-validated, more recent one.", keeper.Title, cfg.DuplicateSimilarity*100)})
		}
	}
	return out
}

func supersededFacts(ms []memory.Memory, cfg Config) []finding {
	var out []finding
	for _, group := range byScope(ms) {
		all := make([]tokset, len(group))
		words := make([]tokset, len(group))
		nums := make([]tokset, len(group))
		for i, m := range group {
			all[i], words[i], nums[i] = tokens(m)
		}
		for i := 0; i < len(group); i++ {
			for j := i + 1; j < len(group); j++ {
				if jaccard(all[i], all[j]) >= cfg.DuplicateSimilarity { // duplicates are handled elsewhere
					continue
				}
				if jaccard(words[i], words[j]) < cfg.FactSimilarity || (len(nums[i]) == 0 && len(nums[j]) == 0) || sameSet(nums[i], nums[j]) {
					continue
				}
				newer, older := group[i], group[j]
				if older.UpdatedAt.After(newer.UpdatedAt) {
					newer, older = older, newer
				}
				if !newer.UpdatedAt.After(older.UpdatedAt) {
					continue // the same instant: no basis to prefer one
				}
				k := newer
				out = append(out, finding{signal: SupersededFact, keeper: &k, targets: []memory.Memory{older},
					reason: fmt.Sprintf("States the same fact as the newer %q but with different figures (%s vs %s). The newer memory was written %s after it.",
						newer.Title, joinSet(nums[indexOf(group, older.ID)]), joinSet(nums[indexOf(group, newer.ID)]),
						humanSpan(newer.UpdatedAt.Sub(older.UpdatedAt)))})
			}
		}
	}
	return out
}

func indexOf(ms []memory.Memory, id string) int {
	for i, m := range ms {
		if m.ID == id {
			return i
		}
	}
	return 0
}

func joinSet(s tokset) string {
	if len(s) == 0 {
		return "none"
	}
	var xs []string
	for t := range s {
		xs = append(xs, t)
	}
	sort.Strings(xs)
	return strings.Join(xs, ", ")
}

// refutedByHistory finds lessons that later verified runs refuted. A
// hypothesis matches a memory when most of its words appear in the memory.
func (c *Curator) refutedByHistory(ctx context.Context, tenantID string, ms []memory.Memory) ([]finding, error) {
	since := c.Cfg.Now().AddDate(0, 0, -c.Cfg.HistoryDays)
	outcomes, err := c.History.Outcomes(ctx, tenantID, since, 0)
	if err != nil {
		return nil, err
	}
	var verified []runsearch.Outcome
	for _, o := range outcomes {
		if o.Verdict == "SUPPORTED" { // only independently supported runs are ground truth
			verified = append(verified, o)
		}
	}
	var out []finding
	for _, m := range ms {
		_, mw, _ := tokens(m)
		var refuted, supported []string
		for _, o := range verified {
			if !o.At.After(m.UpdatedAt) || !concerns(m, o) {
				continue
			}
			ref, sup := false, false
			for _, h := range o.Hypotheses {
				if !matches(h.Statement, mw) {
					continue
				}
				switch h.Status {
				case "REFUTED":
					ref = true
				case "VERIFIED":
					sup = true
				}
			}
			if sup {
				supported = append(supported, o.RunID)
			} else if ref {
				refuted = append(refuted, o.RunID)
			}
		}
		if len(refuted) >= c.Cfg.MinRefutations && len(supported) == 0 {
			ev := make([]string, 0, len(refuted))
			for _, r := range refuted[:min(len(refuted), 5)] {
				ev = append(ev, "run:"+r)
			}
			out = append(out, finding{signal: RefutedByHistory, targets: []memory.Memory{m}, evidence: ev,
				reason: fmt.Sprintf("Later runs refuted this lesson %d times, with independent verification, and none confirmed it.", len(refuted))})
		}
	}
	return out, nil
}

// matches: at least three words of the hypothesis, three quarters of them in
// the memory.
func matches(statement string, memWords tokset) bool {
	hw := map[string]bool{}
	for _, t := range memory.Tokenize(statement) {
		if !strings.ContainsAny(t, "0123456789") {
			hw[t] = true
		}
	}
	if len(hw) < 3 {
		return false
	}
	in := 0
	for t := range hw {
		if memWords[t] {
			in++
		}
	}
	return float64(in)/float64(len(hw)) >= 0.75
}

// concerns: a service-scoped memory is only judged by runs about that service.
func concerns(m memory.Memory, o runsearch.Outcome) bool {
	if m.Scope != "service" || m.ScopeID == "" {
		return true
	}
	svc := strings.ToLower(m.ScopeID)
	if strings.Contains(strings.ToLower(o.Goal), svc) || strings.Contains(strings.ToLower(o.Answer), svc) {
		return true
	}
	for _, s := range o.Services {
		if strings.EqualFold(s, m.ScopeID) {
			return true
		}
	}
	return false
}

func (c *Curator) unused(ctx context.Context, tenantID string, ms []memory.Memory) ([]finding, error) {
	ids := make([]string, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	hits, err := c.Memories.MemoryLastHits(ctx, tenantID, ids)
	if err != nil {
		return nil, err
	}
	now := c.Cfg.Now()
	var out []finding
	for _, m := range ms {
		last := m.CreatedAt
		if m.UpdatedAt.After(last) {
			last = m.UpdatedAt
		}
		if m.LastReinforcedAt != nil && m.LastReinforcedAt.After(last) {
			last = *m.LastReinforcedAt
		}
		if h, ok := hits[m.ID]; ok && h.After(last) {
			last = h
		}
		if now.Sub(last) >= c.Cfg.StaleAfter {
			out = append(out, finding{signal: Unused, targets: []memory.Memory{m},
				reason: fmt.Sprintf("Not created, changed, reinforced or retrieved into a run for %d days.", int(now.Sub(last).Hours()/24))})
		}
	}
	return out, nil
}

// ---- decisions ----

// ErrOutdated means a target changed after the proposal was made.
var ErrOutdated = errors.New("a memory in the proposal changed since it was made")

// Apply sets a PENDING proposal's targets aside (ACTIVE to STALE). If any
// target is no longer the ACTIVE version the proposal saw, nothing is changed
// and the proposal becomes OUTDATED.
func Apply(ctx context.Context, st Store, mem Memories, tenantID, id, actor, reason string) (Proposal, error) {
	p, err := st.GetProposal(ctx, tenantID, id)
	if err != nil {
		return p, err
	}
	if p.Status != Pending {
		return p, &domain.RejectedError{Reason: "proposal_not_pending"}
	}
	for _, t := range p.Targets {
		cur, err := mem.GetMemory(ctx, tenantID, t.MemoryID)
		if err != nil || cur.Status != "ACTIVE" || cur.Version != t.Version {
			if out, derr := st.DecideProposal(ctx, tenantID, id, Outdated, actor, "a memory changed since this was proposed", time.Now().UTC()); derr == nil {
				return out, ErrOutdated
			}
			return p, ErrOutdated
		}
	}
	for _, t := range p.Targets {
		if _, err := mem.TransitionMemory(ctx, tenantID, t.MemoryID, "ACTIVE", "STALE", actor,
			fmt.Sprintf("curation %s: %s", p.Signal, firstLine(p.Reason))); err != nil {
			return p, fmt.Errorf("set aside %s: %w", t.MemoryID, err)
		}
	}
	return st.DecideProposal(ctx, tenantID, id, Applied, actor, reason, time.Now().UTC())
}

// Dismiss refuses a proposal; the same finding is not raised again.
func Dismiss(ctx context.Context, st Store, tenantID, id, actor, reason string) (Proposal, error) {
	return st.DecideProposal(ctx, tenantID, id, Dismissed, actor, reason, time.Now().UTC())
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

// humanSpan renders a span for a reason line: "moments", "3 hours", "12 days".
func humanSpan(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "moments"
	case d < time.Hour:
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d hours", int(d.Hours()))
	}
	return fmt.Sprintf("%d days", int(d.Hours()/24))
}
