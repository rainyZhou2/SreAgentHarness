package reasoning

import (
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
)

func hypo(s string, st domain.ThoughtStatus, c float64, refs ...string) domain.Thought {
	t := domain.Thought{Kind: domain.ThoughtHypothesis, Summary: s, Status: st, Confidence: c, Structured: true}
	for _, r := range refs {
		t.Refs = append(t.Refs, domain.ThoughtRef{Ref: r, Relation: "supports"})
	}
	return t
}

func TestScoreTrace(t *testing.T) {
	tr := Trace{Completed: true, Steps: []Step{
		{Seq: 1, Reason: true, Thoughts: []domain.Thought{
			{Kind: domain.ThoughtDecision, Summary: "check metrics", Refs: []domain.ThoughtRef{{Ref: "obs:1", Relation: "supports"}}},
			hypo("DB pool exhausted", domain.ThoughtUnverified, 0.7),
			hypo("Bad deploy", domain.ThoughtUnverified, 0.3)}},
		{Seq: 2, Tool: "metrics", Args: json.RawMessage(`{"a":1,"b":2}`)},
		{Seq: 3, Tool: "metrics", Args: json.RawMessage(`{"b":2, "a":1}`)},
		{Seq: 4, Reason: true, Thoughts: []domain.Thought{
			hypo("db pool  exhausted!", domain.ThoughtVerified, 0.9, "obs:2"),
			hypo("Bad deploy", domain.ThoughtRefuted, 0, "obs:3")}},
		{Seq: 5, Reason: true, Thoughts: []domain.Thought{
			{Kind: domain.ThoughtVerification, Summary: "pool metric dropped", Status: domain.ThoughtVerified}}},
	}}
	m := Score(tr)
	if m.Hypotheses != 2 || m.Verified != 1 || m.Refuted != 1 || m.Unverified != 0 {
		t.Fatalf("hypotheses: %+v", m)
	}
	if m.RedundantCalls != 1 || m.Redundancy != 0.5 {
		t.Fatalf("redundancy: %+v", m)
	}
	// (0.9-1)^2=0.01, (0.3-0)^2=0.09 -> 0.05 (confidence carried forward for "Bad deploy")
	if m.Brier == nil || *m.Brier != 0.05 || m.CalibrationN != 2 {
		t.Fatalf("brier: %+v", m.Brier)
	}
	if m.Claims != 5 || m.Grounded != 3 {
		t.Fatalf("grounding: claims=%d grounded=%d", m.Claims, m.Grounded)
	}
	if m.StepsToVerdict != 2 || m.Conclusion != "VERIFIED" || !m.Success() {
		t.Fatalf("verdict: %+v", m)
	}
	if m.Quality <= 0 || m.Quality > 1 {
		t.Fatalf("quality out of range: %v", m.Quality)
	}
	no := false
	tr.Correct = &no
	if Score(tr).Success() {
		t.Fatal("operator feedback must override the model's verdict")
	}
}

func TestDecisionThoughtsRoundTrip(t *testing.T) {
	d := domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", EvidenceRefs: []string{"obs:1"}, Answer: "root cause",
		Hypotheses: []domain.Hypothesis{{Statement: "x", Status: "verified", ConfidenceEstimate: 0.8, Supports: []string{"obs:1"}}},
		Thought:    &domain.ThoughtChain{Structured: true, Verification: &domain.Verification{Verdict: "VERIFIED"}}}
	m := Score(Trace{Completed: true, Steps: []Step{{Reason: true, Thoughts: DecisionThoughts(d)}}})
	if m.Verified != 1 || m.Conclusion != "VERIFIED" || m.Structured != 1 {
		t.Fatalf("%+v", m)
	}
}

func TestSummaryGateAndStats(t *testing.T) {
	b1, b2 := 0.1, 0.4
	base := Summarize([]Metrics{{Quality: 0.8, Brier: &b1, Grounded: 9, Claims: 10, ToolCalls: 10, Conclusion: "VERIFIED", Completed: true, StepsToVerdict: 2}})
	cand := Summarize([]Metrics{{Quality: 0.6, Brier: &b2, Grounded: 5, Claims: 10, ToolCalls: 10, RedundantCalls: 3, Completed: true}})
	f := Gate(base, cand, DefaultGate())
	if len(f) != 4 {
		t.Fatalf("want 4 regressions, got %v", f)
	}
	if len(Gate(base, base, DefaultGate())) != 0 {
		t.Fatal("identical summaries must pass")
	}
	if cand.NoVerdictRate != 1 || base.VerifiedRate != 1 {
		t.Fatalf("rates: %+v %+v", base, cand)
	}
	if lo := WilsonLower(10, 10); lo < 0.69 || lo > 0.73 {
		t.Fatalf("wilson(10,10)=%v", lo)
	}
	ok := Metrics{Completed: true, Conclusion: "VERIFIED", Quality: 1}
	bad := Metrics{Completed: true, Conclusion: "UNVERIFIED"}
	st := PatternStats([]RunScore{{MemoryIDs: []string{"m1", "m1", "m2"}, Metrics: ok}, {MemoryIDs: []string{"m1"}, Metrics: bad}})
	if st["m1"].Uses != 2 || st["m1"].Successes != 1 || st["m2"].Uses != 1 || st["m1"].Quality != 0.5 {
		t.Fatalf("stats: %+v", st)
	}
	if s := SortedStats(st); s[0].MemoryID != "m1" {
		t.Fatalf("order: %+v", s)
	}
}

func TestDistillPattern(t *testing.T) {
	run := domain.Run{ID: "r1", TenantID: "t", Status: domain.RunCompleted}
	tr := Trace{Completed: true, Steps: []Step{
		{Seq: 1, Tool: "logs"},
		{Seq: 2, Reason: true, Thoughts: []domain.Thought{hypo("cache stampede", domain.ThoughtRefuted, 0.4), hypo("disk full", domain.ThoughtVerified, 0.8, "obs:step3")}},
		{Seq: 3, Tool: "restart"},
		{Seq: 4, Reason: true, Thoughts: []domain.Thought{{Kind: domain.ThoughtVerification, Summary: "df shows 100%", Status: domain.ThoughtVerified}}},
	}}
	m := Score(tr)
	nm, ok := DistillPattern(run, "api is slow", "api", tr, m, "VERIFIED")
	if !ok {
		t.Fatal("verified run must distill")
	}
	if nm.Status != "VALIDATING" || nm.Subject != "api:"+PatternSubject || nm.Type != "procedural" || nm.ScopeID != "api" {
		t.Fatalf("%+v", nm)
	}
	for _, want := range []string{"1. cache stampede → REFUTED (confidence 0.40)", "2. disk full → VERIFIED", "Tools in order until the first verdict: logs", "df shows 100%"} {
		if !strings.Contains(nm.Content, want) {
			t.Fatalf("content missing %q:\n%s", want, nm.Content)
		}
	}
	if strings.Contains(nm.Content, "restart") {
		t.Fatal("tools after the first verdict must not be listed")
	}
	if _, ok := DistillPattern(run, "g", "api", tr, m, "DISPUTED"); ok {
		t.Fatal("disputed runs must not distill")
	}
	if _, ok := DistillPattern(run, "g", "", tr, m, ""); ok {
		t.Fatal("runs without a service must not distill")
	}
	m.Conclusion = "UNVERIFIED"
	if _, ok := DistillPattern(run, "g", "api", tr, m, ""); ok {
		t.Fatal("unverified conclusions must not distill")
	}
}
