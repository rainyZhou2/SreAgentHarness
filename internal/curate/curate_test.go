package curate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/runsearch"
)

var t0 = time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)

type fakeMem struct {
	mems  map[string]memory.Memory
	hits  map[string]time.Time
	trans []string
}

func (f *fakeMem) ActiveMemories(_ context.Context, _ string, _ int) ([]memory.Memory, error) {
	var out []memory.Memory
	for _, m := range f.mems {
		if m.Status == "ACTIVE" {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (f *fakeMem) GetMemory(_ context.Context, _, id string) (memory.Memory, error) {
	m, ok := f.mems[id]
	if !ok {
		return m, domain.ErrNotFound
	}
	return m, nil
}
func (f *fakeMem) TransitionMemory(_ context.Context, _, id, from, to, actor, reason string) (memory.Memory, error) {
	m := f.mems[id]
	if m.Status != from {
		return m, domain.ErrVersionConflict
	}
	m.Status = to
	f.mems[id] = m
	f.trans = append(f.trans, fmt.Sprintf("%s:%s->%s:%s", id, from, to, actor))
	return m, nil
}
func (f *fakeMem) MemoryLastHits(_ context.Context, _ string, ids []string) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	for _, id := range ids {
		if t, ok := f.hits[id]; ok {
			out[id] = t
		}
	}
	return out, nil
}

type fakeHist struct{ outs []runsearch.Outcome }

func (f fakeHist) Outcomes(context.Context, string, time.Time, int) ([]runsearch.Outcome, error) {
	return f.outs, nil
}

type memProposals struct{ ps map[string]Proposal }

func (s *memProposals) CreateProposal(_ context.Context, p Proposal) (Proposal, bool, error) {
	if s.ps == nil {
		s.ps = map[string]Proposal{}
	}
	for _, q := range s.ps {
		if q.TenantID == p.TenantID && q.Signature == p.Signature {
			return q, false, nil
		}
	}
	s.ps[p.ID] = p
	return p, true, nil
}
func (s *memProposals) ListProposals(_ context.Context, _, status string, _ int) ([]Proposal, error) {
	var out []Proposal
	for _, p := range s.ps {
		if status == "" || p.Status == status {
			out = append(out, p)
		}
	}
	return out, nil
}
func (s *memProposals) GetProposal(_ context.Context, _, id string) (Proposal, error) {
	p, ok := s.ps[id]
	if !ok {
		return p, domain.ErrNotFound
	}
	return p, nil
}
func (s *memProposals) DecideProposal(_ context.Context, _, id, status, actor, reason string, at time.Time) (Proposal, error) {
	p := s.ps[id]
	if p.Status != Pending {
		return p, domain.ErrVersionConflict
	}
	p.Status, p.DecidedBy, p.DecisionReason, p.DecidedAt = status, actor, reason, &at
	s.ps[id] = p
	return p, nil
}

func mem(id, scopeID, title, content string, age time.Duration) memory.Memory {
	return memory.Memory{ID: id, TenantID: "t", Scope: "service", ScopeID: scopeID, Status: "ACTIVE", Version: 1,
		Title: title, Content: content, Type: "semantic", CreatedAt: t0.Add(-age), UpdatedAt: t0.Add(-age)}
}

func newCurator(ms []memory.Memory, h History) (*Curator, *fakeMem, *memProposals) {
	f := &fakeMem{mems: map[string]memory.Memory{}, hits: map[string]time.Time{}}
	for _, m := range ms {
		f.mems[m.ID] = m
	}
	st := &memProposals{}
	return &Curator{Memories: f, History: h, Store: st, Cfg: Config{Now: func() time.Time { return t0 }}}, f, st
}

func TestDuplicateKeepsTheBetterValidatedOne(t *testing.T) {
	a := mem("a", "checkout", "pool limit", "checkout db connection pool max is 100 saturation shows p99 above 2s", 5*24*time.Hour)
	b := mem("b", "checkout", "pool limit", "checkout db connection pool max is 100 saturation shows p99 above 2s", 2*24*time.Hour)
	v := "alice"
	a.VerifiedBy = &v // verified beats recent
	c, _, st := newCurator([]memory.Memory{a, b}, nil)
	rep, err := c.Run(context.Background(), "t")
	if err != nil || rep.Created[Duplicate] != 1 {
		t.Fatalf("%v %+v", err, rep)
	}
	ps, _ := st.ListProposals(context.Background(), "t", Pending, 10)
	if len(ps) != 1 || ps[0].Keeper.MemoryID != "a" || len(ps[0].Targets) != 1 || ps[0].Targets[0].MemoryID != "b" {
		t.Fatalf("%+v", ps)
	}
}

func TestSupersededFactNeedsDifferentFiguresAndASameScope(t *testing.T) {
	old := mem("old", "checkout", "pool", "checkout database connection pool maximum size is 100 connections", 40*24*time.Hour)
	neu := mem("new", "checkout", "pool", "checkout database connection pool maximum size is 150 connections", 2*24*time.Hour)
	other := mem("other", "payments", "pool", "checkout database connection pool maximum size is 150 connections", time.Hour) // another scope
	c, _, st := newCurator([]memory.Memory{old, neu, other}, nil)
	rep, _ := c.Run(context.Background(), "t")
	ps, _ := st.ListProposals(context.Background(), "t", Pending, 10)
	if rep.Created[SupersededFact] != 1 || len(ps) != 1 || ps[0].Targets[0].MemoryID != "old" || ps[0].Keeper.MemoryID != "new" {
		t.Fatalf("%+v %+v", rep, ps)
	}
	if !strings.Contains(ps[0].Reason, "100") || !strings.Contains(ps[0].Reason, "150") {
		t.Fatalf("the reason must show the differing figures: %s", ps[0].Reason)
	}
	// Different words are not the same fact.
	x := mem("x", "checkout", "timeout", "checkout http client timeout is 30 seconds", 30*24*time.Hour)
	y := mem("y", "checkout", "retries", "checkout retry budget is 3 attempts", time.Hour)
	c, _, st = newCurator([]memory.Memory{x, y}, nil)
	if rep, _ := c.Run(context.Background(), "t"); rep.Total() != 0 {
		t.Fatalf("unrelated facts must not be proposed: %+v", rep)
	}
	_ = st
}

func outcome(id string, at time.Time, verdict string, hyps ...runsearch.Hypothesis) runsearch.Outcome {
	return runsearch.Outcome{RunID: id, At: at, Goal: "checkout slow", Hypotheses: hyps, Verdict: verdict}
}

func TestRefutedByHistoryNeedsVerifiedRunsAndNoSupport(t *testing.T) {
	m := mem("m", "checkout", "root cause", "checkout latency is caused by traffic spike saturating cpu", 30*24*time.Hour)
	refuted := runsearch.Hypothesis{Statement: "traffic spike saturating cpu", Status: "REFUTED"}
	verified := runsearch.Hypothesis{Statement: "traffic spike saturating cpu", Status: "VERIFIED"}
	day := 24 * time.Hour

	// two verified refutations, none supporting: proposed
	c, _, st := newCurator([]memory.Memory{m}, fakeHist{[]runsearch.Outcome{
		outcome("r1", t0.Add(-5*day), "SUPPORTED", refuted), outcome("r2", t0.Add(-3*day), "SUPPORTED", refuted)}})
	rep, _ := c.Run(context.Background(), "t")
	ps, _ := st.ListProposals(context.Background(), "t", Pending, 10)
	if rep.Created[RefutedByHistory] != 1 || len(ps) != 1 || !contains(ps[0].Evidence, "run:r1") || !contains(ps[0].Evidence, "run:r2") {
		t.Fatalf("%+v %+v", rep, ps)
	}
	// unverified runs are not ground truth
	c, _, _ = newCurator([]memory.Memory{m}, fakeHist{[]runsearch.Outcome{
		outcome("r1", t0.Add(-5*day), "", refuted), outcome("r2", t0.Add(-3*day), "INSUFFICIENT", refuted)}})
	if rep, _ := c.Run(context.Background(), "t"); rep.Created[RefutedByHistory] != 0 {
		t.Fatalf("unverified runs must not refute a memory: %+v", rep)
	}
	// a supporting run blocks it
	c, _, _ = newCurator([]memory.Memory{m}, fakeHist{[]runsearch.Outcome{
		outcome("r1", t0.Add(-5*day), "SUPPORTED", refuted), outcome("r2", t0.Add(-3*day), "SUPPORTED", refuted),
		outcome("r3", t0.Add(-2*day), "SUPPORTED", verified)}})
	if rep, _ := c.Run(context.Background(), "t"); rep.Created[RefutedByHistory] != 0 {
		t.Fatalf("a run that confirmed the lesson must block the proposal: %+v", rep)
	}
	// runs from before the memory was written do not count; one refutation is not enough
	c, _, _ = newCurator([]memory.Memory{m}, fakeHist{[]runsearch.Outcome{
		outcome("old1", t0.Add(-60*day), "SUPPORTED", refuted), outcome("old2", t0.Add(-50*day), "SUPPORTED", refuted),
		outcome("r1", t0.Add(-5*day), "SUPPORTED", refuted)}})
	if rep, _ := c.Run(context.Background(), "t"); rep.Created[RefutedByHistory] != 0 {
		t.Fatalf("%+v", rep)
	}
	// a run about another service says nothing about this memory
	other := outcome("r9", t0.Add(-1*day), "SUPPORTED", refuted)
	other.Goal = "payments slow"
	c, _, _ = newCurator([]memory.Memory{m}, fakeHist{[]runsearch.Outcome{other, outcome("r1", t0.Add(-5*day), "SUPPORTED", refuted)}})
	if rep, _ := c.Run(context.Background(), "t"); rep.Created[RefutedByHistory] != 0 {
		t.Fatalf("%+v", rep)
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func TestUnusedUsesLastRetrieval(t *testing.T) {
	day := 24 * time.Hour
	old := mem("old", "a", "old fact", "something nobody asks about at all ever", 200*day)
	used := mem("used", "b", "used fact", "something engineers ask about every week often", 200*day)
	fresh := mem("fresh", "c", "fresh fact", "recently written memory about a new subject entirely", 10*day)
	c, f, st := newCurator([]memory.Memory{old, used, fresh}, nil)
	f.hits["used"] = t0.Add(-3 * day)
	rep, _ := c.Run(context.Background(), "t")
	ps, _ := st.ListProposals(context.Background(), "t", Pending, 10)
	if rep.Created[Unused] != 1 || len(ps) != 1 || ps[0].Targets[0].MemoryID != "old" {
		t.Fatalf("%+v %+v", rep, ps)
	}
	c.Cfg.StaleAfter = -1 // disabled
	c2, _, st2 := newCurator([]memory.Memory{old}, nil)
	c2.Cfg.StaleAfter = -1
	_ = c
	if rep, _ := c2.Run(context.Background(), "t"); rep.Total() != 0 {
		t.Fatalf("a negative StaleAfter disables the signal: %+v %v", rep, st2.ps)
	}
}

func TestPersonalMemoryIsNeverExamined(t *testing.T) {
	p := mem("p", "alice", "answer language", "answer in chinese", 400*24*time.Hour)
	p.Scope = "user"
	c, _, _ := newCurator([]memory.Memory{p}, nil)
	rep, _ := c.Run(context.Background(), "t")
	if rep.Examined != 0 || rep.Total() != 0 {
		t.Fatalf("a user's own preferences are not the tenant's to curate: %+v", rep)
	}
}

func TestOneProposalPerMemoryAndIdempotence(t *testing.T) {
	day := 24 * time.Hour
	// "old" is both a superseded fact and unused: only the stronger signal fires.
	old := mem("old", "checkout", "pool", "checkout database connection pool maximum size is 100 connections", 200*day)
	neu := mem("new", "checkout", "pool", "checkout database connection pool maximum size is 150 connections", 2*day)
	c, _, st := newCurator([]memory.Memory{old, neu}, nil)
	rep, _ := c.Run(context.Background(), "t")
	if rep.Created[SupersededFact] != 1 || rep.Created[Unused] != 0 {
		t.Fatalf("a memory is proposed once, under its strongest signal: %+v", rep)
	}
	rep, _ = c.Run(context.Background(), "t")
	if rep.Total() != 0 || rep.Existing == 0 {
		t.Fatalf("a repeat pass must not duplicate proposals: %+v", rep)
	}
	ps, _ := st.ListProposals(context.Background(), "t", "", 10)
	if _, err := Dismiss(context.Background(), st, "t", ps[0].ID, "alice", "keep it"); err != nil {
		t.Fatal(err)
	}
	if rep, _ = c.Run(context.Background(), "t"); rep.Total() != 0 {
		t.Fatalf("a dismissed finding must not return: %+v", rep)
	}
}

func TestApplySetsAsideReversiblyAndRefusesStaleProposals(t *testing.T) {
	day := 24 * time.Hour
	old := mem("old", "checkout", "pool", "checkout database connection pool maximum size is 100 connections", 40*day)
	neu := mem("new", "checkout", "pool", "checkout database connection pool maximum size is 150 connections", 2*day)
	c, f, st := newCurator([]memory.Memory{old, neu}, nil)
	_, _ = c.Run(context.Background(), "t")
	ps, _ := st.ListProposals(context.Background(), "t", Pending, 10)
	p, err := Apply(context.Background(), st, f, "t", ps[0].ID, "alice", "ok")
	if err != nil || p.Status != Applied || f.mems["old"].Status != "STALE" || f.mems["new"].Status != "ACTIVE" {
		t.Fatalf("approving sets the target aside as STALE (reversible) and keeps the keeper: %v %+v %s", err, p, f.mems["old"].Status)
	}
	if len(f.trans) != 1 || !strings.HasSuffix(f.trans[0], ":alice") {
		t.Fatalf("%v", f.trans)
	}
	if _, err := Apply(context.Background(), st, f, "t", ps[0].ID, "alice", ""); err == nil {
		t.Fatal("a decided proposal cannot be applied again")
	}

	// A memory edited after the proposal makes it outdated: nothing changes.
	old2 := mem("o2", "checkout", "pool", "checkout database connection pool maximum size is 100 connections", 40*day)
	new2 := mem("n2", "checkout", "pool", "checkout database connection pool maximum size is 150 connections", 2*day)
	c, f, st = newCurator([]memory.Memory{old2, new2}, nil)
	_, _ = c.Run(context.Background(), "t")
	ps, _ = st.ListProposals(context.Background(), "t", Pending, 10)
	m := f.mems["o2"]
	m.Version = 2
	f.mems["o2"] = m
	p, err = Apply(context.Background(), st, f, "t", ps[0].ID, "alice", "")
	if !errors.Is(err, ErrOutdated) || p.Status != Outdated || f.mems["o2"].Status != "ACTIVE" {
		t.Fatalf("an outdated proposal must change nothing: %v %+v", err, p)
	}
}

func TestProposalCap(t *testing.T) {
	var ms []memory.Memory
	for i := 0; i < 6; i++ {
		ms = append(ms, mem(fmt.Sprintf("m%d", i), fmt.Sprintf("svc%d", i), fmt.Sprintf("t%d", i), fmt.Sprintf("distinct memory number %d about subject%d", i, i*7), 300*24*time.Hour))
	}
	c, _, _ := newCurator(ms, nil)
	c.Cfg.MaxProposals = 3
	rep, _ := c.Run(context.Background(), "t")
	if rep.Total() != 3 || len(rep.Skipped) == 0 {
		t.Fatalf("%+v", rep)
	}
}
