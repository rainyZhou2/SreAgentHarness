package cognitive

import (
	"strings"
	"testing"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
)

func TestRefSetResolve(t *testing.T) {
	in := ReasoningInput{CompactedBefore: 3,
		Observations: []domain.Observation{{StepSeq: 4, Tool: "prom.query"}, {StepSeq: 6, Tool: "logs.search",
			Artifact: &domain.ObservationArtifact{Handle: "art_abc"}}, {StepSeq: 8, Tool: "prom.query"}},
		Evidence: []domain.Evidence{{Key: "ticket", Version: 2}},
		Memories: []MemoryRef{{ID: "m1", Version: 3}}}
	r := NewRefSet(in)
	cases := map[string]string{
		"obs:step4": "obs:step4", "obs:4": "obs:step4", "step:6": "obs:step6", "obs:prom.query": "obs:step8",
		"obs:prom.query@step4": "obs:step4", "art_abc": "obs:step6", "obs:step2": "obs:step2",
		"kv:ticket@2": "kv:ticket@2", "kv:ticket": "kv:ticket@2", "memory:m1@3": "memory:m1@3", "memory:m1": "memory:m1@3",
	}
	for raw, want := range cases {
		if got, ok := r.Resolve(raw); !ok || got != want {
			t.Errorf("Resolve(%q) = %q,%v want %q", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"obs:step5", "obs:step99", "obs:logs.search@step4", "obs:k8s.restart", "kv:ticket@1",
		"kv:other", "memory:m2", "memory:m1@2", "https://example.com", "", "obs:step2@x", "obs:0"} {
		if got, ok := r.Resolve(raw); ok {
			t.Errorf("Resolve(%q) = %q, want unresolved", raw, got)
		}
	}
	refs, n := r.ResolveRefs([]string{"obs:4", "obs:step4", "obs:step99"}, "supports")
	if n != 1 || len(refs) != 2 || refs[0].Ref != "obs:step4" || refs[1].Ref != domain.UnresolvedRefPrefix+"obs:step99" || refs[1].Resolved() {
		t.Fatalf("ResolveRefs: %+v %d", refs, n)
	}
}

func TestObservationHeaderShowsRef(t *testing.T) {
	in := ReasoningInput{Goal: "g", Observations: []domain.Observation{{StepSeq: 4, Tool: "prom.query", Outcome: "ok",
		Content: []byte(`{"v":1}`)}}}
	req, _ := DefaultAssembler().Build(in, "m")
	found := false
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			if strings.HasPrefix(p.Text, "[observation step=4") && strings.Contains(p.Text, "ref=obs:step4]") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("observation header must carry its ref")
	}
	// The scripted parser still reads the header.
	if got := ParseRequest(req); len(got.Observations) != 1 || got.Observations[0].StepSeq != 4 {
		t.Fatalf("parse: %+v", got.Observations)
	}
	_ = gw.Request{}
}
