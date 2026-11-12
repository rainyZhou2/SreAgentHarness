package compacteval

import (
	"context"
	"testing"

	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/kernel"
)

// echoGen "summarizes" by returning the digest it was given, so the test
// measures exactly what a model could at best retain from its input.
type echoGen struct{}

func (echoGen) Generate(_ context.Context, _ gw.Scope, req gw.Request, _ gw.Sink) (gw.Response, error) {
	return gw.Response{Text: req.Messages[0].Parts[0].Text}, nil
}

func TestDeterministicBaseline(t *testing.T) {
	rs := Run(context.Background(), kernel.DeterministicCompactor{}, DefaultCases())
	s := Summarize(rs)
	if s.Errors != 0 || s.Leaks != 0 {
		t.Fatalf("%+v", rs)
	}
	if s.CitationsKept != 1 {
		t.Errorf("the deterministic compactor must keep every citation, got %.2f", s.CitationsKept)
	}
	// Documents the weakness the model-backed compactor addresses: facts
	// deep in long output do not survive a 400-byte excerpt.
	if s.FactRecall >= 0.95 {
		t.Errorf("baseline recall %.2f: the cases no longer stress deep facts", s.FactRecall)
	}
}

func TestLLMInputCoversDeepFacts(t *testing.T) {
	det := Summarize(Run(context.Background(), kernel.DeterministicCompactor{}, DefaultCases()))
	c := &kernel.LLMCompactor{Gateway: echoGen{}, Alias: "x"}
	rs := Run(context.Background(), c, DefaultCases())
	s := Summarize(rs)
	if !s.HasCoverage {
		t.Fatal("an LLM compactor must report input coverage")
	}
	if s.InputCoverage <= det.FactRecall {
		t.Errorf("the model must be shown more than the deterministic excerpt: coverage %.2f vs recall %.2f", s.InputCoverage, det.FactRecall)
	}
	if s.Leaks != 0 {
		t.Errorf("planted instructions leaked into the summary: %+v", rs)
	}
	if s.CitationsKept != 1 {
		t.Errorf("citations must survive: %.2f", s.CitationsKept)
	}
}
