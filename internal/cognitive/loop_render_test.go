package cognitive

import (
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
)

func TestAssemblerRendersKernelNoticesAndParts(t *testing.T) {
	a := Assembler{PromptPrefixVersion: "t", MaxContextTokens: 100000, MaxObservationBytes: 4000, MaxOutputTokens: 1000}
	in := ReasoningInput{Goal: "g", HasToolSteps: true, StepSeq: 4,
		Observations: []domain.Observation{
			{StepSeq: 2, Part: 1, Tool: "a", Outcome: "ok", TrustTag: "T2", Content: []byte(`{}`)},
			{StepSeq: 2, Part: 2, Tool: "b", Outcome: "error", TrustTag: "T2", Content: []byte(`{"error":"x"}`)},
		},
		Advisories: []string{"Run budget 85% used"}, Repair: "tool \"z\" does not exist", ForceConclude: true}
	req, man := a.Build(in, "test")
	var all strings.Builder
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			all.WriteString(p.Text + "\n")
		}
	}
	text := all.String()
	for _, want := range []string{"[step 2.2] I called b", "step=2.2 tool=b outcome=error", "ref=obs:step2.1", "ref=obs:step2.2",
		"[kernel notice]\n- Run budget 85% used", "[correction] Your previous response for this step was rejected", "Stop investigating."} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	if man.SectionHashes["N"] == "" || man.SectionHashes["R"] == "" {
		t.Fatal("notice sections not hashed")
	}
}

func TestRefSetBatchParts(t *testing.T) {
	r := NewRefSet(ReasoningInput{Observations: []domain.Observation{
		{StepSeq: 1, Tool: "prom.query"},
		{StepSeq: 3, Part: 1, Tool: "prom.query"}, {StepSeq: 3, Part: 2, Tool: "logs.search"}}})
	cases := map[string]string{"obs:step3.2": "obs:step3.2", "obs:3.1": "obs:step3.1", "obs:step3": "obs:step3",
		"obs:logs.search@step3": "obs:step3.2", "obs:prom.query": "obs:step3.1", "obs:prom.query@step1": "obs:step1"}
	for in, want := range cases {
		if got, ok := r.Resolve(in); !ok || got != want {
			t.Errorf("Resolve(%q) = %q,%v want %q", in, got, ok, want)
		}
	}
	for _, bad := range []string{"obs:step3.3", "obs:step1.1", "obs:prom.query@step3.2"} {
		if _, ok := r.Resolve(bad); ok {
			t.Errorf("Resolve(%q) resolved", bad)
		}
	}
}
