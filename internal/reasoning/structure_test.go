package reasoning

import (
	"testing"

	"go-sre-harness/internal/domain"
)

func withID(th domain.Thought, id string) domain.Thought {
	th.Attrs = map[string]any{HypothesisIDAttr: id}
	return th
}

func TestHypothesisIDDedup(t *testing.T) {
	tr := Trace{Completed: true, Steps: []Step{
		{Reason: true, Thoughts: []domain.Thought{withID(hypo("Traffic spike", domain.ThoughtUnverified, 0.3), "h2")}},
		{Reason: true, Thoughts: []domain.Thought{withID(hypo("Traffic spike (CPU bound)", domain.ThoughtRefuted, 0.1), "H2")}},
	}}
	m := Score(tr)
	if m.Hypotheses != 1 || m.Refuted != 1 {
		t.Fatalf("reworded hypothesis with the same id must count once: %+v", m)
	}
	// Without ids the reworded statement is a different hypothesis.
	tr.Steps[0].Thoughts[0].Attrs, tr.Steps[1].Thoughts[0].Attrs = nil, nil
	if m := Score(tr); m.Hypotheses != 2 {
		t.Fatalf("fallback to statements: %+v", m)
	}
}

func TestCoverageAndImputedCalibration(t *testing.T) {
	settled := func(c1, c2 float64) Metrics {
		return Score(Trace{Completed: true, Steps: []Step{{Reason: true, Thoughts: []domain.Thought{
			hypo("a", domain.ThoughtVerified, c1), hypo("b", domain.ThoughtRefuted, c2)}}}})
	}
	full, none, half := settled(0.9, 0.1), settled(0, 0), settled(0.9, 0)
	if full.CalibrationCoverage != 1 || none.CalibrationCoverage != 0 || half.CalibrationCoverage != 0.5 {
		t.Fatalf("coverage: %v %v %v", full.CalibrationCoverage, none.CalibrationCoverage, half.CalibrationCoverage)
	}
	if none.Brier != nil || half.CalibrationN != 1 {
		t.Fatal("Brier stays defined over stated confidences only")
	}
	bad := settled(0.1, 0.9)
	if !(full.Quality > half.Quality && half.Quality > none.Quality && none.Quality > bad.Quality) {
		t.Fatalf("quality order: full=%v half=%v none=%v bad=%v", full.Quality, half.Quality, none.Quality, bad.Quality)
	}
	s := Summarize([]Metrics{full, none})
	if s.Settled != 4 || s.CalibrationCoverage != 0.5 {
		t.Fatalf("summary coverage: %+v", s)
	}
}

func TestCitedMemories(t *testing.T) {
	m := Score(Trace{Steps: []Step{{Reason: true, Thoughts: []domain.Thought{
		{Kind: domain.ThoughtPlan, Summary: "p", Refs: []domain.ThoughtRef{{Ref: "memory:auto@1", Relation: "based_on"}}},
		{Kind: domain.ThoughtDecision, Summary: "d", Refs: []domain.ThoughtRef{{Ref: "memory:m2@3", Relation: "supports"}, {Ref: "obs:1"}}},
		{Kind: domain.ThoughtHypothesis, Summary: "h", Refs: []domain.ThoughtRef{{Ref: "memory:m1@1", Relation: "supports"}, {Ref: "memory:m2@4", Relation: "refutes"}}},
	}}}})
	if len(m.CitedMemories) != 2 || m.CitedMemories[0] != "m1" || m.CitedMemories[1] != "m2" {
		t.Fatalf("cited: %v", m.CitedMemories)
	}
}

func TestStructureGate(t *testing.T) {
	base := Summary{N: 10, Structured: 0.95, Settled: 10, CalibrationCoverage: 0.9}
	cand := Summary{N: 10, Structured: 0.7, Settled: 10, CalibrationCoverage: 0.5}
	if f := Gate(base, cand, DefaultGate()); len(f) != 2 {
		t.Fatalf("structure regressions: %v", f)
	}
	// Older configs without the new margins still check them (default 0.10).
	old := GateConfig{MaxBrierIncrease: 1, MaxGroundingDrop: 1, MaxRedundancyIncrease: 1, MaxQualityDrop: 1}
	if f := Gate(base, cand, old); len(f) != 2 {
		t.Fatalf("defaults for zero margins: %v", f)
	}
	cand.Settled = 0
	if f := Gate(base, cand, DefaultGate()); len(f) != 1 {
		t.Fatalf("coverage needs settled hypotheses on both sides: %v", f)
	}
}

func score(run, service string, ok bool, mems, cited []string) RunScore {
	c := "UNVERIFIED"
	if ok {
		c = "VERIFIED"
	}
	return RunScore{RunID: run, Service: service, MemoryIDs: mems, Metrics: Metrics{Completed: true, Conclusion: c, CitedMemories: cited}}
}

func TestPatternEffect(t *testing.T) {
	var scores []RunScore
	// svc a: 8/10 without m; m used in 5 runs of a, 1 success.
	for i := 0; i < 10; i++ {
		scores = append(scores, score("a"+string(rune('0'+i)), "a", i < 8, nil, nil))
	}
	for i := 0; i < 5; i++ {
		scores = append(scores, score("m"+string(rune('0'+i)), "a", i < 1, []string{"m"}, nil))
	}
	// svc z has only exposed runs: no baseline, not comparable.
	scores = append(scores, score("z", "z", false, []string{"m"}, nil))
	st := PatternStats(scores)["m"]
	if st.Uses != 6 || st.Effect == nil || st.Effect.N != 5 || st.Effect.Successes != 1 {
		t.Fatalf("effect: %+v %+v", st, st.Effect)
	}
	// baseline (8+1)/(10+2)=0.75 → expected 3.75, lift (1-3.75)/5=-0.55
	if st.Effect.Expected != 3.75 || st.Effect.Lift != -0.55 || st.Effect.Z >= -2 {
		t.Fatalf("numbers: %+v", st.Effect)
	}
	if ef, basis := st.Decisive(5); ef == nil || basis != "retrieved" {
		t.Fatal("retrieval decides without citations")
	}
	if ef, _ := st.Decisive(6); ef != nil {
		t.Fatal("below min uses")
	}
	if st.CitedEffect != nil || st.Cited != 0 {
		t.Fatal("no citations")
	}
}
