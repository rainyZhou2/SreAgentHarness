package improve

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
)

type fakeStore struct {
	scores   []reasoning.RunScore
	mems     map[string]memory.Memory
	cands    []eval.Candidate
	spec     string
	holdout  []string
	lastFilt reasoning.ScoreFilter
}

func (f *fakeStore) ReasoningScores(_ context.Context, _ string, fl reasoning.ScoreFilter) ([]reasoning.RunScore, error) {
	f.lastFilt = fl
	return f.scores, nil
}
func (f *fakeStore) GetMemory(_ context.Context, _, id string) (memory.Memory, error) {
	m, ok := f.mems[id]
	if !ok {
		return m, domain.ErrNotFound
	}
	return m, nil
}
func (f *fakeStore) ListCandidates(context.Context, string, string) ([]eval.Candidate, error) {
	return f.cands, nil
}
func (f *fakeStore) ActiveVersion(context.Context, string, string) (int64, error) { return 3, nil }
func (f *fakeStore) ProfileSpec(context.Context, string, string, int64) (json.RawMessage, error) {
	return json.RawMessage(f.spec), nil
}
func (f *fakeStore) ListCases(context.Context, string, string, string) ([]eval.Case, error) {
	var out []eval.Case
	for _, id := range f.holdout {
		id := id
		out = append(out, eval.Case{SourceRunID: &id})
	}
	return out, nil
}

type fakeEvo struct {
	patch     json.RawMessage
	actor     string
	evaluated bool
	pass      bool
}

func (e *fakeEvo) Propose(_ context.Context, _, profileID string, base int64, patch json.RawMessage, _, actor string) (eval.Candidate, error) {
	if _, _, err := profile.ApplyPatch(profile.Spec{}, patch); err != nil {
		return eval.Candidate{}, err
	}
	e.patch, e.actor = patch, actor
	return eval.Candidate{ID: "c1", ProfileID: profileID, BaseVersion: base, Status: "PROPOSED", ProposedBy: actor}, nil
}
func (e *fakeEvo) EvaluateCandidate(context.Context, string, string, string, string) (eval.Candidate, eval.GateResult, error) {
	e.evaluated = true
	return eval.Candidate{ID: "c1", Status: "PENDING_APPROVAL"}, eval.GateResult{Pass: e.pass}, nil
}

func weakScores(n int, memID string) []reasoning.RunScore {
	var out []reasoning.RunScore
	for i := 0; i < n; i++ {
		b := 0.4
		out = append(out, reasoning.RunScore{RunID: fmt.Sprintf("r%d", i), MemoryIDs: []string{memID},
			Metrics: reasoning.Metrics{Completed: true, Conclusion: "UNVERIFIED", Brier: &b, Claims: 10, Grounded: 3,
				ToolCalls: 10, RedundantCalls: 3, Quality: 0.3, Structured: 1}})
	}
	return out
}

// runs builds n scored runs of a service; the first ok of them succeed.
func runs(prefix, service string, n, ok int, mems, cited []string) []reasoning.RunScore {
	var out []reasoning.RunScore
	for i := 0; i < n; i++ {
		concl := "UNVERIFIED"
		if i < ok {
			concl = "VERIFIED"
		}
		b := 0.1
		out = append(out, reasoning.RunScore{RunID: fmt.Sprintf("%s%d", prefix, i), Service: service, MemoryIDs: mems,
			Metrics: reasoning.Metrics{Completed: true, Conclusion: concl, Brier: &b, Claims: 10, Grounded: 9, ToolCalls: 10,
				StepsToVerdict: 2, Quality: 0.8, Structured: 1, CitedMemories: cited}})
	}
	return out
}

func cat(xs ...[]reasoning.RunScore) []reasoning.RunScore {
	var out []reasoning.RunScore
	for _, x := range xs {
		out = append(out, x...)
	}
	return out
}

func newImprover(st *fakeStore, ev *fakeEvo, dataset string) *Improver {
	return &Improver{Config: Config{Tenants: []string{"t"}, ProfileID: "p", DatasetID: dataset},
		Store: st, Evolution: ev, Now: func() time.Time { return time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC) }}
}

func pattern(id string) memory.Memory {
	return memory.Memory{ID: id, Type: "procedural", Subject: "api:" + reasoning.PatternSubject, Status: "ACTIVE"}
}

func reasoningPatch(t *testing.T, ev *fakeEvo) profile.ReasoningSpec {
	t.Helper()
	var p map[string]profile.ReasoningSpec
	if err := json.Unmarshal(ev.patch, &p); err != nil || len(p) != 1 {
		t.Fatalf("patch must only touch reasoning: %s", ev.patch)
	}
	return p["reasoning"]
}

func TestProposesReasoningOnlyPatch(t *testing.T) {
	scores := cat(weakScores(25, "m1"), runs("b", "", 30, 27, nil, nil))
	st := &fakeStore{scores: scores, mems: map[string]memory.Memory{"m1": pattern("m1")},
		spec: `{"reasoning":{"playbook":"- keep this line"}}`, holdout: []string{"r0", "r1"}}
	ev := &fakeEvo{pass: true}
	o := newImprover(st, ev, "ds").TickTenant(context.Background(), "t")
	if o.Status != "evaluated" || !ev.evaluated || ev.actor != Actor {
		t.Fatalf("outcome: %+v", o)
	}
	if o.Diagnosis.Excluded != 2 || st.lastFilt.Version != 3 || st.lastFilt.Since.IsZero() {
		t.Fatalf("holdout/filter: %+v %+v", o.Diagnosis, st.lastFilt)
	}
	r := reasoningPatch(t, ev)
	if len(r.PatternsExcluded) != 1 || r.PatternsExcluded[0] != "m1" || r.PatternProbeRate != 0.05 {
		t.Fatalf("excluded: %+v", r)
	}
	if !strings.HasPrefix(r.Playbook, "- keep this line") || !strings.Contains(r.Playbook, "Before concluding, run one check") {
		t.Fatalf("playbook: %q", r.Playbook)
	}
}

func TestSkips(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{scores: weakScores(5, "m1"), spec: `{}`}
	if o := newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t"); o.Status != "skipped" || !strings.Contains(o.Reason, "need 20") {
		t.Fatalf("min runs: %+v", o)
	}
	st = &fakeStore{scores: weakScores(25, "m1"), spec: `{}`, cands: []eval.Candidate{{ID: "x", ProposedBy: Actor, Status: "PENDING_APPROVAL"}}}
	if o := newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t"); o.Status != "skipped" || !strings.Contains(o.Reason, "x") {
		t.Fatalf("in flight: %+v", o)
	}
	st.cands = []eval.Candidate{{ID: "h", ProposedBy: "alice", Status: "PROPOSED"}, {ID: "y", ProposedBy: Actor, Status: "REJECTED"}}
	ev := &fakeEvo{}
	if o := newImprover(st, ev, "").TickTenant(ctx, "t"); o.Status != "proposed" || ev.evaluated {
		t.Fatalf("propose without dataset: %+v", o)
	}
	if o := newImprover(&fakeStore{scores: runs("g", "api", 25, 25, nil, nil), spec: `{}`}, &fakeEvo{}, "").TickTenant(ctx, "t"); o.Status != "skipped" {
		t.Fatalf("healthy: %+v", o)
	}
}

func TestRetiresRelativeToServiceBaseline(t *testing.T) {
	ctx := context.Background()
	mems := map[string]memory.Memory{"m1": pattern("m1")}
	// api succeeds 80% without m1 and 10% with it: retire.
	st := &fakeStore{scores: cat(runs("a", "api", 30, 24, nil, nil), runs("m", "api", 20, 2, []string{"m1"}, nil)), mems: mems, spec: `{}`}
	o := newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t")
	if o.Status != "proposed" || len(o.Diagnosis.Patterns.Retire) != 1 || o.Diagnosis.Patterns.Retire[0].Basis != "retrieved" {
		t.Fatalf("retire: %+v", o.Diagnosis)
	}
	// A hard service: 20% success with or without m1. An absolute threshold
	// would retire m1; relative to the baseline it is neutral.
	st = &fakeStore{scores: cat(runs("a", "hard", 30, 6, nil, nil), runs("m", "hard", 20, 4, []string{"m1"}, nil)), mems: mems, spec: `{}`}
	o = newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t")
	if len(o.Diagnosis.Patterns.Retire) != 0 {
		t.Fatalf("hard service must not retire: %+v", o.Diagnosis.Patterns)
	}
	// Retrieved by every run (no retrieval baseline) but cited only by the
	// failing ones: the citation effect decides.
	st = &fakeStore{scores: cat(runs("a", "api", 30, 27, []string{"m1"}, nil), runs("c", "api", 20, 1, []string{"m1"}, []string{"m1"})),
		mems: mems, spec: `{}`}
	o = newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t")
	if len(o.Diagnosis.Patterns.Retire) != 1 || o.Diagnosis.Patterns.Retire[0].Basis != "cited" {
		t.Fatalf("cited basis: %+v", o.Diagnosis.Patterns)
	}
}

func TestReinstatesFromProbeEvidence(t *testing.T) {
	ctx := context.Background()
	mems := map[string]memory.Memory{"m1": pattern("m1"), "gone": {ID: "gone", Status: "ARCHIVED"}}
	spec := `{"reasoning":{"patterns_excluded":["gone","m1"],"pattern_probe_rate":0.05}}`
	// Probed runs with m1 do as well as the baseline.
	st := &fakeStore{scores: cat(runs("a", "api", 30, 24, nil, nil), runs("p", "api", 10, 9, []string{"m1"}, nil)), mems: mems, spec: spec}
	ev := &fakeEvo{}
	o := newImprover(st, ev, "").TickTenant(ctx, "t")
	pl := o.Diagnosis.Patterns
	if o.Status != "proposed" || len(pl.Reinstate) != 1 || len(pl.Pruned) != 1 || pl.Pruned[0] != "gone" || len(pl.Next) != 0 {
		t.Fatalf("reinstate: %+v %+v", o, pl)
	}
	if r := reasoningPatch(t, ev); len(r.PatternsExcluded) != 0 || r.PatternProbeRate != 0.05 {
		t.Fatalf("patch: %+v", r)
	}
	// Still worse than baseline (but not significantly enough to retire):
	// stays excluded, and pruning alone proposes nothing.
	st.scores = cat(runs("a", "api", 30, 24, nil, nil), runs("p", "api", 10, 6, []string{"m1"}, nil))
	if o := newImprover(st, &fakeEvo{}, "").TickTenant(ctx, "t"); o.Status != "skipped" || len(o.Diagnosis.Patterns.Reinstate) != 0 {
		t.Fatalf("must stay excluded: %+v", o.Diagnosis.Patterns)
	}
}

func TestExclusionCapacity(t *testing.T) {
	ctx := context.Background()
	mems := map[string]memory.Memory{}
	var current []string
	for i := 0; i < profile.MaxExcludedPatterns; i++ {
		id := fmt.Sprintf("x%03d", i)
		current = append(current, id)
		mems[id] = pattern(id)
	}
	delete(mems, "x000") // gone: pruned, frees one slot
	stats := map[string]reasoning.PatternStat{}
	for i, z := range []float64{-5, -3, -4} {
		id := fmt.Sprintf("n%d", i)
		mems[id] = pattern(id)
		stats[id] = reasoning.PatternStat{MemoryID: id, Uses: 10, Effect: &reasoning.Effect{N: 10, Z: z, Lift: -0.5}}
	}
	plan, err := PlanExclusions(ctx, &fakeStore{mems: mems}, "t", current, stats, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Pruned) != 1 || len(plan.Retire) != 1 || plan.Retire[0].MemoryID != "n0" || len(plan.Deferred) != 2 ||
		len(plan.Next) != profile.MaxExcludedPatterns {
		t.Fatalf("capacity: pruned=%v retire=%v deferred=%d next=%d", plan.Pruned, plan.Retire, len(plan.Deferred), len(plan.Next))
	}
}

func TestOnlyActivePatternsAreRetired(t *testing.T) {
	m := pattern("m1")
	m.Status = "CANDIDATE"
	other := memory.Memory{ID: "m2", Type: "semantic", Status: "ACTIVE"}
	scores := cat(runs("a", "api", 30, 27, nil, nil), runs("m", "api", 20, 1, []string{"m1", "m2"}, nil))
	st := &fakeStore{scores: scores, mems: map[string]memory.Memory{"m1": m, "m2": other}, spec: `{}`}
	o := newImprover(st, &fakeEvo{}, "").TickTenant(context.Background(), "t")
	if len(o.Diagnosis.Patterns.Retire) != 0 {
		t.Fatalf("must not retire candidates or non-pattern memories: %+v", o.Diagnosis.Patterns)
	}
}

func TestStructureDimension(t *testing.T) {
	th := Config{}.Defaults().Thresholds
	if w := weakDimensions(reasoning.Summary{Structured: 0.5}, th); !contains(w, DimStructure) {
		t.Fatalf("low structured share: %v", w)
	}
	if w := weakDimensions(reasoning.Summary{Structured: 1, Settled: 10, CalibrationCoverage: 0.3}, th); !contains(w, DimStructure) {
		t.Fatalf("low coverage: %v", w)
	}
	if w := weakDimensions(reasoning.Summary{Structured: 1, Grounding: 1}, th); contains(w, DimStructure) {
		t.Fatalf("no settled hypotheses: coverage not judged: %v", w)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func TestManualExclusionChange(t *testing.T) {
	ctx := context.Background()
	st := &fakeStore{mems: map[string]memory.Memory{"m1": pattern("m1"), "m2": pattern("m2")},
		spec: `{"reasoning":{"patterns_excluded":["m2"],"playbook":"- x"}}`}
	ev := &fakeEvo{}
	if _, err := ProposeExclusionChange(ctx, st, ev, "t", "p", []string{"m1"}, []string{"m2"}, "alice", "n"); err != nil {
		t.Fatal(err)
	}
	r := reasoningPatch(t, ev)
	if ev.actor != "alice" || len(r.PatternsExcluded) != 1 || r.PatternsExcluded[0] != "m1" || r.Playbook != "- x" {
		t.Fatalf("%+v by %s", r, ev.actor)
	}
	for _, c := range []struct{ add, rm []string }{{nil, nil}, {[]string{"m2"}, nil}, {nil, []string{"m1"}}, {[]string{"nope"}, nil}} {
		if _, err := ProposeExclusionChange(ctx, st, ev, "t", "p", c.add, c.rm, "alice", ""); err == nil {
			t.Fatalf("must reject %+v", c)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	base := Config{Tenants: []string{"t"}, ProfileID: "p"}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.ReinstateZ = -3
	if bad.Validate() == nil {
		t.Fatal("reinstate below -retire_z would flap")
	}
	bad = base
	bad.ProbeRate = 0.5
	if bad.Validate() == nil {
		t.Fatal("probe rate above the profile limit")
	}
}

func TestStatsPlaybookBounded(t *testing.T) {
	cur := strings.Repeat("x", profile.MaxPlaybookRunes-10)
	pb := StatsPlaybook([]string{DimCalibration}, cur)
	if n := len([]rune(pb)); n > profile.MaxPlaybookRunes || !strings.Contains(pb, "Calibrate") {
		t.Fatalf("len=%d", n)
	}
	if StatsPlaybook([]string{DimGrounding}, StatsPlaybook([]string{DimGrounding}, "")) != StatsPlaybook([]string{DimGrounding}, "") {
		t.Fatal("guidance must not duplicate")
	}
}

func TestCitationWeakDimension(t *testing.T) {
	th := Config{}.Defaults().Thresholds
	s := reasoning.Summary{N: 30, Grounding: 0.9, Structured: 1, Citations: 40, CitationValidity: 0.7}
	w := weakDimensions(s, th)
	if len(w) != 1 || w[0] != DimCitation {
		t.Fatalf("weak: %v", w)
	}
	if pb := StatsPlaybook(w, ""); !strings.Contains(pb, "obs:step<N>") {
		t.Fatalf("playbook: %q", pb)
	}
}
