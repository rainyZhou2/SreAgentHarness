package outcome

import (
	"context"
	"testing"
	"time"
)

func TestStabilizationRejectsUnrelatedOrUncoveredPrevious(t *testing.T) {
	spec := latencySpec(5 * time.Minute)
	e := &Evaluator{Metrics: MetricSourceMap{"prom": fakeMetric{fn: func(q MetricQuery) (MetricResult, error) {
		return MetricResult{Samples: series(q.End, 10, time.Minute, func(int) float64 { return 100 })}, nil
	}}}}
	first := e.Evaluate(context.Background(), spec, Input{TenantID: "t", WorkID: "w", Now: t0})
	for _, tc := range []struct {
		name   string
		delta  time.Duration
		change func(*Evaluation)
	}{
		{"foreign tenant", 5 * time.Minute, func(p *Evaluation) { p.TenantID = "other" }},
		{"foreign work", 5 * time.Minute, func(p *Evaluation) { p.WorkID = "other" }},
		{"unobserved gap", time.Hour, func(*Evaluation) {}},
		{"future evaluation", 5 * time.Minute, func(p *Evaluation) { p.EvaluatedAt = t0.Add(time.Hour) }},
		{"missing stabilization time", 5 * time.Minute, func(p *Evaluation) { zero := time.Time{}; p.SatisfiedSince = &zero }},
		{"invalid chronology", 5 * time.Minute, func(p *Evaluation) { s := t0.Add(time.Minute); p.SatisfiedSince = &s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := first
			tc.change(&prev)
			now := t0.Add(tc.delta)
			got := e.Evaluate(context.Background(), spec, Input{TenantID: "t", WorkID: "w", Now: now, Previous: &prev})
			if got.Verdict != Inconclusive || got.SatisfiedSince == nil || !got.SatisfiedSince.Equal(now) {
				t.Fatalf("inherited invalid clock: %+v", got)
			}
		})
	}
}

func TestStateRejectsFutureAndMissingObservationTime(t *testing.T) {
	spec := Spec{AllOf: []Objective{{ID: "ready", Kind: KindState, TargetRef: "service://prod/api", State: &StateExpr{Predicate: "ready", Equals: "yes"}}}}
	if err := spec.Normalize(); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{{}, t0.Add(time.Second)} {
		e := Evaluator{States: fakeState{"service://prod/api|ready": {Value: "yes", ObservedAt: at}}}
		got := e.Evaluate(context.Background(), spec, Input{Now: t0})
		if got.Verdict != Inconclusive {
			t.Fatalf("invalid observed_at %s accepted: %+v", at, got)
		}
	}
}

func TestStabilizationCannotBridgeMetricSampleGap(t *testing.T) {
	spec := latencySpec(5 * time.Minute)
	now := t0.Add(5 * time.Minute)
	e := Evaluator{Metrics: MetricSourceMap{"prom": fakeMetric{fn: func(q MetricQuery) (MetricResult, error) {
		samples := series(q.End, 4, time.Second, func(int) float64 { return 100 })
		samples = append(samples, Sample{T: t0.Add(-time.Second), V: 100})
		return MetricResult{Samples: samples}, nil
	}}}}
	prev := Evaluation{TenantID: "t", WorkID: "w", SpecHash: spec.Hash(), RawVerdict: Satisfied, Verdict: Inconclusive, EvaluatedAt: t0, SatisfiedSince: &t0}
	got := e.Evaluate(context.Background(), spec, Input{TenantID: "t", WorkID: "w", Now: now, Previous: &prev})
	if got.Verdict != Inconclusive || !got.SatisfiedSince.Equal(now) {
		t.Fatalf("sample gap bridged: %+v", got)
	}
}
