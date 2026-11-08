package reasoning

import (
	"context"
	"strings"
	"testing"
)

func overconfident(n int) []Forecast {
	var fs []Forecast
	for i := 0; i < n; i++ {
		// Stated 0.9, right 60% of the time; stated 0.3, right 30%.
		fs = append(fs, Forecast{P: 0.9, Y: map[bool]int{true: 1}[i%5 < 3]})
		fs = append(fs, Forecast{P: 0.3, Y: map[bool]int{true: 1}[i%10 < 3]})
	}
	return fs
}

func TestCalibrator(t *testing.T) {
	if FitCalibrator(overconfident(5), 30) != nil {
		t.Fatal("too few forecasts")
	}
	c := FitCalibrator(overconfident(50), 30)
	if c == nil || c.N != 100 {
		t.Fatalf("%+v", c)
	}
	if p := c.Apply(0.9); p < 0.55 || p > 0.65 {
		t.Fatalf("0.9 should map near 0.6, got %v", p)
	}
	if p := c.Apply(0.3); p < 0.25 || p > 0.35 {
		t.Fatalf("0.3 should map near 0.3, got %v", p)
	}
	if c.Apply(0.95) < c.Apply(0.2) {
		t.Fatal("not monotone")
	}
	if c.ECE < 0.14 || c.ECE > 0.16 {
		t.Fatalf("ECE %v", c.ECE)
	}
	note := c.Note()
	if !strings.Contains(note, "held 60% of the time (n=50)") {
		t.Fatalf("note:\n%s", note)
	}
	var nilCal *Calibrator
	if nilCal.Apply(0.7) != 0.7 || nilCal.Note() != "" {
		t.Fatal("nil calibrator must be a no-op")
	}
	// Non-monotone data is pooled.
	fs := append(overconfident(30), Forecast{P: 0.5, Y: 1}, Forecast{P: 0.6, Y: 0})
	c = FitCalibrator(fs, 30)
	for i := 1; i < len(c.Blocks); i++ {
		if c.Blocks[i].Value < c.Blocks[i-1].Value {
			t.Fatalf("blocks not monotone: %+v", c.Blocks)
		}
	}
}

type scoreStore struct{ scores []RunScore }

func (s scoreStore) ReasoningScores(context.Context, string, ScoreFilter) ([]RunScore, error) {
	return s.scores, nil
}

func TestCalibrationServicePrefersIndependent(t *testing.T) {
	var scores []RunScore
	for _, f := range overconfident(20) {
		scores = append(scores, RunScore{Metrics: Metrics{Forecasts: []Forecast{f}}})
	}
	svc := &CalibrationService{Store: scoreStore{scores}}
	c, err := svc.Calibrator(context.Background(), "t", "p")
	if err != nil || c == nil || c.N != 40 {
		t.Fatalf("%+v %v", c, err)
	}
	for i := 0; i < 30; i++ {
		scores = append(scores, RunScore{Metrics: Metrics{Forecasts: []Forecast{{P: 0.8, Y: 1, Independent: true}}}})
	}
	svc = &CalibrationService{Store: scoreStore{scores}}
	if c, _ := svc.Calibrator(context.Background(), "t", "p"); c == nil || c.N != 30 {
		t.Fatalf("independent forecasts should be used once there are enough: %+v", c)
	}
}

func TestSummaryCalibrationAndVerifierGate(t *testing.T) {
	good := Metrics{Independent: "SUPPORTED", Forecasts: []Forecast{{P: 0.8, Y: 1}, {P: 0.2, Y: 0}}}
	bad := Metrics{Independent: "CONTRADICTED", Forecasts: []Forecast{{P: 0.9, Y: 0}, {P: 0.9, Y: 0}}}
	var base, cand []Metrics
	for i := 0; i < 12; i++ {
		base = append(base, good)
		cand = append(cand, bad)
	}
	b, c := Summarize(base), Summarize(cand)
	if b.ECE == nil || c.ECE == nil || *c.ECE < 0.8 || c.ContradictedRate != 1 {
		t.Fatalf("summary: %+v", c)
	}
	fs := strings.Join(Gate(b, c, DefaultGate()), ";")
	if !strings.Contains(fs, "calibration error rose") || !strings.Contains(fs, "independently contradicted") {
		t.Fatalf("gate: %s", fs)
	}
}
