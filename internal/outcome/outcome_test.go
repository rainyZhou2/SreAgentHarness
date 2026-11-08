package outcome

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
)

type fakeMetric struct {
	fn func(q MetricQuery) (MetricResult, error)
}

func (f fakeMetric) QueryRange(_ context.Context, q MetricQuery) (MetricResult, error) {
	return f.fn(q)
}

func series(end time.Time, n int, step time.Duration, v func(i int) float64) []Sample {
	out := make([]Sample, n)
	for i := 0; i < n; i++ {
		out[i] = Sample{T: end.Add(-time.Duration(n-1-i) * step), V: v(i)}
	}
	return out
}

func latencySpec(stable time.Duration) Spec {
	s := Spec{AllOf: []Objective{{ID: "latency", Kind: KindMetric, TargetRef: "service://prod/search-api",
		Metric: &MetricExpr{Source: "prom", Query: "p99", Operator: "<=", Threshold: 350},
		Window: cpkit.Duration(10 * time.Minute), Freshness: cpkit.Duration(time.Minute), MinSamples: 5}},
		StableFor: cpkit.Duration(stable)}
	if err := s.Normalize(); err != nil {
		panic(err)
	}
	return s
}

var t0 = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func TestMissingDataIsInconclusive(t *testing.T) { // T15
	spec := latencySpec(0)
	for name, src := range map[string]fakeMetric{
		"empty": {fn: func(MetricQuery) (MetricResult, error) { return MetricResult{}, nil }},
		"error": {fn: func(MetricQuery) (MetricResult, error) { return MetricResult{}, errors.New("connection refused") }},
		"nan": {fn: func(MetricQuery) (MetricResult, error) {
			return MetricResult{Samples: series(t0, 10, time.Minute, func(int) float64 { return nanv() })}, nil
		}},
		"stale": {fn: func(MetricQuery) (MetricResult, error) {
			return MetricResult{Samples: series(t0.Add(-5*time.Minute), 6, 30*time.Second, func(int) float64 { return 100 })}, nil
		}},
		"few": {fn: func(MetricQuery) (MetricResult, error) {
			return MetricResult{Samples: series(t0, 3, time.Minute, func(int) float64 { return 100 })}, nil
		}},
		"future": {fn: func(MetricQuery) (MetricResult, error) {
			return MetricResult{Samples: series(t0.Add(time.Hour), 10, time.Second, func(int) float64 { return 1 })}, nil
		}},
	} {
		e := &Evaluator{Metrics: MetricSourceMap{"prom": src}}
		ev := e.Evaluate(context.Background(), spec, Input{Now: t0})
		if ev.Verdict != Inconclusive {
			t.Errorf("%s: verdict %s, want INCONCLUSIVE (%v)", name, ev.Verdict, ev.Reasons)
		}
		if len(ev.Missing) != 1 {
			t.Errorf("%s: missing %v", name, ev.Missing)
		}
	}
	// No source registered at all.
	ev := (&Evaluator{}).Evaluate(context.Background(), spec, Input{Now: t0})
	if ev.Verdict != Inconclusive {
		t.Fatalf("no source: %s", ev.Verdict)
	}
}

func nanv() float64 { var z float64; return z / z }

func TestMetricVerdicts(t *testing.T) {
	spec := latencySpec(0)
	good := fakeMetric{fn: func(q MetricQuery) (MetricResult, error) {
		if q.TargetRef != "service://prod/search-api" || !q.End.Equal(t0) || !q.Start.Equal(t0.Add(-10*time.Minute)) {
			t.Errorf("unexpected query %+v", q)
		}
		return MetricResult{Samples: series(t0, 10, time.Minute, func(int) float64 { return 200 }), EvidenceRef: "artifact://q1"}, nil
	}}
	e := &Evaluator{Metrics: MetricSourceMap{"prom": good}}
	ev := e.Evaluate(context.Background(), spec, Input{Now: t0})
	if ev.Verdict != Satisfied || len(ev.EvidenceRefs) != 1 {
		t.Fatalf("got %+v", ev)
	}
	spike := fakeMetric{fn: func(MetricQuery) (MetricResult, error) {
		return MetricResult{Samples: series(t0, 10, time.Minute, func(i int) float64 {
			if i == 4 {
				return 900
			}
			return 200
		})}, nil
	}}
	ev = (&Evaluator{Metrics: MetricSourceMap{"prom": spike}}).Evaluate(context.Background(), spec, Input{Now: t0})
	if ev.Verdict != NotSatisfied || *ev.Objectives[0].Value != 900 {
		t.Fatalf("one bad sample must fail an 'all' objective: %+v", ev)
	}
}

func TestStableForRequiresContinuousRecovery(t *testing.T) { // T16
	spec := latencySpec(5 * time.Minute)
	val := 200.0
	src := fakeMetric{fn: func(q MetricQuery) (MetricResult, error) {
		return MetricResult{Samples: series(q.End, 10, time.Minute, func(int) float64 { return val })}, nil
	}}
	e := &Evaluator{Metrics: MetricSourceMap{"prom": src}}
	ev1 := e.Evaluate(context.Background(), spec, Input{Now: t0})
	if ev1.Verdict != Inconclusive || ev1.RawVerdict != Satisfied {
		t.Fatalf("first satisfied evaluation must be stabilizing: %+v", ev1)
	}
	ev2 := e.Evaluate(context.Background(), spec, Input{Now: t0.Add(3 * time.Minute), Previous: &ev1})
	if ev2.Verdict != Inconclusive || !ev2.SatisfiedSince.Equal(t0) {
		t.Fatalf("still stabilizing: %+v", ev2)
	}
	// Rebound: the stabilization clock resets.
	val = 800
	ev3 := e.Evaluate(context.Background(), spec, Input{Now: t0.Add(4 * time.Minute), Previous: &ev2})
	if ev3.Verdict != NotSatisfied || ev3.SatisfiedSince != nil {
		t.Fatalf("rebound: %+v", ev3)
	}
	val = 200
	ev4 := e.Evaluate(context.Background(), spec, Input{Now: t0.Add(6 * time.Minute), Previous: &ev3})
	if ev4.Verdict != Inconclusive || !ev4.SatisfiedSince.Equal(t0.Add(6*time.Minute)) {
		t.Fatalf("restart clock: %+v", ev4)
	}
	ev5 := e.Evaluate(context.Background(), spec, Input{Now: t0.Add(11 * time.Minute), Previous: &ev4})
	if ev5.Verdict != Satisfied {
		t.Fatalf("held for 5m: %+v", ev5)
	}
	// A different spec (e.g. a lowered threshold) does not inherit the clock.
	other := latencySpec(5 * time.Minute)
	other.AllOf[0].Metric.Threshold = 1000
	ev6 := e.Evaluate(context.Background(), other, Input{Now: t0.Add(12 * time.Minute), Previous: &ev5})
	if ev6.Verdict != Inconclusive {
		t.Fatalf("changed spec must restart stabilization: %+v", ev6)
	}
}

type fakeState map[string]FactValue

func (f fakeState) CurrentFact(_ context.Context, _, subject, pred string, _ time.Time) (FactValue, bool, error) {
	v, ok := f[subject+"|"+pred]
	return v, ok, nil
}

type fakeEvents int

func (f fakeEvents) CountEvents(context.Context, string, string, string, time.Time, time.Time) (int, error) {
	return int(f), nil
}

func TestStateEventHumanAndAnyOf(t *testing.T) {
	spec := Spec{
		AllOf: []Objective{
			{ID: "rolled-back", Kind: KindState, TargetRef: "k8s://c/ns/Deployment/search",
				State: &StateExpr{Predicate: "image.version", Equals: "v1"}, Freshness: cpkit.Duration(time.Minute)},
			{ID: "ack", Kind: KindHuman},
			{ID: "no-alert", Kind: KindEvent, Event: &EventExpr{Type: "alert.firing", Absent: true}, Window: cpkit.Duration(10 * time.Minute)},
		},
		AnyOf: []Objective{
			{ID: "a", Kind: KindHuman},
			{ID: "b", Kind: KindHuman, Optional: true},
		},
	}
	if err := spec.Normalize(); err != nil {
		t.Fatal(err)
	}
	e := &Evaluator{
		States: fakeState{"k8s://c/ns/Deployment/search|image.version": {Value: "v1", ObservedAt: t0.Add(-10 * time.Second)}},
		Events: fakeEvents(0),
	}
	ev := e.Evaluate(context.Background(), spec, Input{Now: t0, Human: map[string]bool{"ack": true}})
	if ev.Verdict != Inconclusive {
		t.Fatalf("any_of awaiting confirmation: %+v", ev)
	}
	ev = e.Evaluate(context.Background(), spec, Input{Now: t0, Human: map[string]bool{"ack": true, "a": true}})
	if ev.Verdict != Satisfied {
		t.Fatalf("all satisfied: %+v", ev)
	}
	e.Events = fakeEvents(2)
	ev = e.Evaluate(context.Background(), spec, Input{Now: t0, Human: map[string]bool{"ack": true, "a": true}})
	if ev.Verdict != NotSatisfied {
		t.Fatalf("alert still firing: %+v", ev)
	}
	e.Events = fakeEvents(0)
	e.States = fakeState{"k8s://c/ns/Deployment/search|image.version": {Value: "v1", ObservedAt: t0.Add(-10 * time.Minute)}}
	ev = e.Evaluate(context.Background(), spec, Input{Now: t0, Human: map[string]bool{"ack": true, "a": true}})
	if ev.Verdict != Inconclusive {
		t.Fatalf("stale fact must be inconclusive: %+v", ev)
	}
	ev = e.Evaluate(context.Background(), spec, Input{Now: t0, Human: map[string]bool{"ack": false, "a": true}})
	if ev.Verdict != NotSatisfied {
		t.Fatalf("human rejection: %+v", ev)
	}
}

func TestSpecValidation(t *testing.T) {
	bad := []string{
		`{}`,
		`{"version":2,"all_of":[{"id":"a","kind":"human"}]}`,
		`{"all_of":[{"id":"a","kind":"metric","metric":{"source":"p","query":"q","operator":"~","threshold":1},"window":"1m"}]}`,
		`{"all_of":[{"id":"a","kind":"metric","metric":{"source":"p","query":"q","operator":"lt","threshold":1}}]}`,
		`{"all_of":[{"id":"a","kind":"human"},{"id":"a","kind":"human"}]}`,
		`{"all_of":[{"id":"a","kind":"state","target_ref":"svc://x/y","state":{"predicate":"p"}}]}`,
		`{"all_of":[{"id":"a","kind":"exec"}]}`,
		`{"all_of":[{"id":"a","kind":"human","target_ref":"not a ref"}]}`,
	}
	for _, b := range bad {
		var s Spec
		if err := json.Unmarshal([]byte(b), &s); err != nil {
			t.Fatal(err)
		}
		if err := s.Normalize(); !errors.Is(err, ErrInvalidSpec) {
			t.Errorf("%s accepted (%v)", b, err)
		}
	}
	var s Spec
	_ = json.Unmarshal([]byte(`{"all_of":[{"id":"lat","kind":"metric","metric":{"source":"p","query":"q","operator":"less_than_or_equal","threshold":350},"window":"10m"}]}`), &s)
	if err := s.Normalize(); err != nil {
		t.Fatal(err)
	}
	if s.AllOf[0].Metric.Operator != OpLTE || s.AllOf[0].Metric.Aggregate != AggAll || s.AllOf[0].MinSamples != 1 {
		t.Fatalf("defaults: %+v", s.AllOf[0])
	}
}
