package reasoning

import (
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
)

func TestUnresolvedCitationsDoNotGround(t *testing.T) {
	bad := domain.UnresolvedRefPrefix + "obs:step99"
	tr := Trace{Completed: true, Steps: []Step{
		{Seq: 1, Reason: true, Thoughts: []domain.Thought{
			hypo("disk full", domain.ThoughtVerified, 0.9, bad),
			{Kind: domain.ThoughtVerification, Summary: "VERIFIED", Status: domain.ThoughtVerified,
				Refs: []domain.ThoughtRef{{Ref: bad, Relation: "supports"}}},
			{Kind: domain.ThoughtConclusion, Summary: "disk", Refs: []domain.ThoughtRef{{Ref: bad, Relation: "supports"}}},
		}},
	}}
	m := Score(tr)
	if m.Grounded != 0 || m.Citations != 1 || m.Unresolved != 1 || m.CitationValidity != 0 {
		t.Fatalf("citations: %+v", m)
	}
	if m.UnsupportedVerdicts != 1 || !m.UnsupportedConclusion || m.VerifiedConclusion() || m.Success() {
		t.Fatalf("a self-declared verdict must not count: %+v", m)
	}
	if _, ok := DistillPattern(domain.Run{ID: "r", Status: domain.RunCompleted}, "g", "svc", tr, m, ""); ok {
		t.Fatal("unsupported conclusion must not distill")
	}
	// The same trace with a resolved ref is a supported verification.
	tr.Steps[0].Thoughts[0] = hypo("disk full", domain.ThoughtVerified, 0.9, "obs:step1")
	m = Score(tr)
	if m.UnsupportedConclusion || !m.Success() || m.UnsupportedVerdicts != 0 || m.Citations != 2 || m.CitationValidity != 0.5 {
		t.Fatalf("resolved: %+v", m)
	}

	// Summary and gate.
	good := Metrics{Citations: 10, Unresolved: 0, Verified: 2, Conclusion: "VERIFIED", Completed: true}
	poor := Metrics{Citations: 10, Unresolved: 5, Verified: 2, UnsupportedVerdicts: 2, Conclusion: "VERIFIED",
		UnsupportedConclusion: true, Completed: true}
	base, cand := Summarize([]Metrics{good, good}), Summarize([]Metrics{poor, poor})
	if cand.CitationValidity != 0.5 || cand.UnsupportedVerdictRate != 1 || cand.UnsupportedConclusionRate != 1 || cand.VerifiedRate != 0 {
		t.Fatalf("summary: %+v", cand)
	}
	fs := strings.Join(Gate(base, cand, DefaultGate()), ";")
	for _, want := range []string{"citation validity", "unsupported verdicts", "unsupported conclusions"} {
		if !strings.Contains(fs, want) {
			t.Fatalf("gate missing %q: %s", want, fs)
		}
	}
}

func TestResolvedDecisionThoughts(t *testing.T) {
	d := domain.Decision{Kind: domain.DecisionFinal, Answer: "a", ReasonSummary: "r", EvidenceRefs: []string{"obs:2", "obs:9"},
		Hypotheses: []domain.Hypothesis{{ID: "h1", Statement: "x", Status: "VERIFIED", ConfidenceEstimate: 0.8, Supports: []string{"obs:2"}}}}
	resolve := func(s string) (string, bool) { return "obs:step2", s == "obs:2" }
	m := Score(Trace{Completed: true, Steps: []Step{{Reason: true, Thoughts: ResolvedDecisionThoughts(d, resolve)}}})
	if m.Unresolved != 1 || m.Citations != 2 || m.UnsupportedVerdicts != 0 {
		t.Fatalf("%+v", m)
	}
}
