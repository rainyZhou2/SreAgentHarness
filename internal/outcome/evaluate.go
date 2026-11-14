package outcome

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Sample is one metric point.
type Sample struct {
	T time.Time `json:"t"`
	V float64   `json:"v"`
}

// MetricQuery asks a metric source for a range of samples. TargetRef lets a
// source scope its query (e.g. fill a service label); it is never trusted
// for authorization.
type MetricQuery struct {
	TenantID  string
	Source    string
	Query     string
	TargetRef string
	Start     time.Time
	End       time.Time
}

// MetricResult is what a metric source returns. EvidenceRef points at the
// stored raw response (artifact) when the source keeps one.
type MetricResult struct {
	Samples     []Sample
	EvidenceRef string
}

// MetricSource runs deterministic, read-only metric queries.
type MetricSource interface {
	QueryRange(ctx context.Context, q MetricQuery) (MetricResult, error)
}

// ErrUnknownSource is returned by a source registry for unregistered names.
var ErrUnknownSource = errors.New("unknown metric source")

// MetricSources resolves a source by name.
type MetricSources interface {
	Metric(name string) (MetricSource, bool)
}

// MetricSourceMap is a static registry.
type MetricSourceMap map[string]MetricSource

func (m MetricSourceMap) Metric(name string) (MetricSource, bool) { s, ok := m[name]; return s, ok }

// FactValue is the current value of a world fact as seen by the evaluator.
type FactValue struct {
	Value      string
	ObservedAt time.Time
	Ref        string
}

// StateSource reads current world facts.
type StateSource interface {
	CurrentFact(ctx context.Context, tenantID, subjectRef, predicate string, at time.Time) (FactValue, bool, error)
}

// EventSource counts committed events.
type EventSource interface {
	CountEvents(ctx context.Context, tenantID, eventType, subjectRef string, since, until time.Time) (int, error)
}

// Input carries everything an evaluation depends on besides the spec.
type Input struct {
	TenantID string
	WorkID   string
	Now      time.Time
	// DefaultTarget fills objectives without a target_ref (the Work's
	// primary subject).
	DefaultTarget string
	// Previous is the last evaluation of the same Work; it carries the
	// stabilization clock.
	Previous *Evaluation
	// Human maps human objective ids to a confirmation (true = satisfied).
	Human map[string]bool
}

// Evaluator evaluates specs. Nil sources make their objectives
// INCONCLUSIVE ("source unavailable"), never satisfied.
type Evaluator struct {
	Metrics MetricSources
	States  StateSource
	Events  EventSource
	// QueryTimeout bounds each source call (default 15s).
	QueryTimeout time.Duration
}

// Evaluate computes the verdict of spec. It never returns an error for data
// problems: those are part of the verdict. spec must be normalized.
func (e *Evaluator) Evaluate(ctx context.Context, spec Spec, in Input) Evaluation {
	now := in.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	ev := Evaluation{TenantID: in.TenantID, WorkID: in.WorkID, SpecHash: spec.Hash(), EvaluatedAt: now}
	eval := func(list []Objective) []ObjectiveResult {
		out := make([]ObjectiveResult, 0, len(list))
		for _, o := range list {
			r := e.objective(ctx, o, in, now)
			r.Optional = o.Optional
			if r.EvidenceRef != "" {
				ev.EvidenceRefs = append(ev.EvidenceRefs, r.EvidenceRef)
			}
			if r.Verdict != Satisfied && r.Verdict != NotSatisfied {
				ev.Missing = append(ev.Missing, o.ID)
			}
			out = append(out, r)
		}
		return out
	}
	all, any := eval(spec.AllOf), eval(spec.AnyOf)
	ev.Objectives = append(append(ev.Objectives, all...), any...)
	raw := combineAll(all)
	if len(spec.AnyOf) > 0 {
		raw = combineAll2(raw, combineAny(any))
	}
	ev.RawVerdict, ev.Verdict = raw, raw
	for _, r := range ev.Objectives {
		if r.Verdict != Satisfied && r.Reason != "" {
			ev.Reasons = append(ev.Reasons, r.ID+": "+r.Reason)
		}
	}
	if raw == Satisfied {
		since := now
		if p := in.Previous; p != nil && p.TenantID == ev.TenantID && p.WorkID == ev.WorkID &&
			p.SpecHash == ev.SpecHash && p.RawVerdict == Satisfied && p.SatisfiedSince != nil && !p.SatisfiedSince.IsZero() &&
			!p.EvaluatedAt.IsZero() && !p.EvaluatedAt.After(now) &&
			!p.SatisfiedSince.After(p.EvaluatedAt) && observationCovers(ev.Objectives, p.EvaluatedAt) {
			since = *p.SatisfiedSince
		}
		ev.SatisfiedSince = &since
		if held := now.Sub(since); held < spec.StableFor.D() {
			ev.Verdict = Inconclusive
			ev.Reasons = append(ev.Reasons, fmt.Sprintf("stabilizing: satisfied for %s of required %s",
				held.Truncate(time.Second), spec.StableFor.D()))
		}
	}
	sort.Strings(ev.Missing)
	return ev
}

// observationCovers prevents a previous success from bridging an interval
// that none of the current observations cover. Any-of choices may change,
// so every currently satisfied required objective must cover the interval.
func observationCovers(rs []ObjectiveResult, previous time.Time) bool {
	for _, r := range rs {
		if !r.Optional && r.Verdict == Satisfied && (r.ObservedFrom == nil || r.ObservedFrom.After(previous)) {
			return false
		}
	}
	return true
}

func required(rs []ObjectiveResult) []ObjectiveResult {
	out := rs[:0:0]
	for _, r := range rs {
		if !r.Optional {
			out = append(out, r)
		}
	}
	return out
}

// combineAll: any NOT_SATISFIED fails; any missing data is inconclusive.
func combineAll(rs []ObjectiveResult) Verdict {
	rs = required(rs)
	v := Satisfied
	for _, r := range rs {
		switch r.Verdict {
		case NotSatisfied:
			return NotSatisfied
		case Inconclusive, Unknown:
			v = Inconclusive
		}
	}
	return v
}

func combineAll2(a, b Verdict) Verdict {
	switch {
	case a == NotSatisfied || b == NotSatisfied:
		return NotSatisfied
	case a == Satisfied && b == Satisfied:
		return Satisfied
	}
	return Inconclusive
}

// combineAny: one SATISFIED suffices; otherwise inconclusive if any is.
func combineAny(rs []ObjectiveResult) Verdict {
	rs = required(rs)
	if len(rs) == 0 {
		return Satisfied
	}
	v := NotSatisfied
	for _, r := range rs {
		switch r.Verdict {
		case Satisfied:
			return Satisfied
		case Inconclusive, Unknown:
			v = Inconclusive
		}
	}
	return v
}

func (e *Evaluator) timeout() time.Duration {
	if e.QueryTimeout > 0 {
		return e.QueryTimeout
	}
	return 15 * time.Second
}

func (e *Evaluator) objective(ctx context.Context, o Objective, in Input, now time.Time) ObjectiveResult {
	r := ObjectiveResult{ID: o.ID, Kind: o.Kind, Verdict: Inconclusive}
	target := o.TargetRef
	if target == "" {
		target = in.DefaultTarget
	}
	switch o.Kind {
	case KindHuman:
		from := now.Add(-time.Minute)
		r.ObservedFrom = &from
		v, ok := in.Human[o.ID]
		switch {
		case !ok:
			r.Reason = "awaiting human confirmation"
		case v:
			r.Verdict = Satisfied
		default:
			r.Verdict, r.Reason = NotSatisfied, "rejected by a human reviewer"
		}
		return r
	case KindMetric:
		return e.metric(ctx, o, in, target, now)
	case KindState:
		if e.States == nil {
			r.Reason = "state source unavailable"
			return r
		}
		if target == "" {
			r.Reason = "no target: set target_ref or a Work subject"
			return r
		}
		cctx, cancel := context.WithTimeout(ctx, e.timeout())
		defer cancel()
		fv, ok, err := e.States.CurrentFact(cctx, in.TenantID, target, o.State.Predicate, now)
		switch {
		case err != nil:
			r.Reason = "state query failed: " + cpkit.Truncate(err.Error(), 200)
			return r
		case !ok:
			r.Reason = "no current fact " + o.State.Predicate
			return r
		}
		r.Observed, r.EvidenceRef = fv.Value, fv.Ref
		if fv.ObservedAt.IsZero() || fv.ObservedAt.After(now) {
			r.Reason = "fact observation time is missing or in the future"
			return r
		}
		// State snapshots are point observations. Bound polling continuity by
		// the declared freshness, with a one-minute default when unspecified.
		gap := o.Freshness.D()
		if gap <= 0 {
			gap = time.Minute
		}
		from := now.Add(-gap)
		r.ObservedFrom = &from
		if f := o.Freshness.D(); f > 0 && now.Sub(fv.ObservedAt) > f {
			r.Reason = fmt.Sprintf("fact is stale (observed %s ago, freshness %s)", now.Sub(fv.ObservedAt).Truncate(time.Second), f)
			return r
		}
		s := o.State
		pass := false
		switch {
		case s.Equals != "":
			pass = fv.Value == s.Equals
		case len(s.In) > 0:
			for _, v := range s.In {
				if fv.Value == v {
					pass = true
				}
			}
		default:
			pass = fv.Value != s.NotEquals
		}
		if pass {
			r.Verdict = Satisfied
		} else {
			r.Verdict, r.Reason = NotSatisfied, fmt.Sprintf("%s is %q", s.Predicate, fv.Value)
		}
		return r
	case KindEvent:
		if e.Events == nil {
			r.Reason = "event source unavailable"
			return r
		}
		cctx, cancel := context.WithTimeout(ctx, e.timeout())
		defer cancel()
		n, err := e.Events.CountEvents(cctx, in.TenantID, o.Event.Type, target, now.Add(-o.Window.D()), now)
		if err != nil {
			r.Reason = "event query failed: " + cpkit.Truncate(err.Error(), 200)
			return r
		}
		r.Samples = n
		from := now.Add(-o.Window.D())
		r.ObservedFrom = &from
		if (n > 0) != o.Event.Absent {
			r.Verdict = Satisfied
		} else if o.Event.Absent {
			r.Verdict, r.Reason = NotSatisfied, fmt.Sprintf("%d %s events in window", n, o.Event.Type)
		} else {
			r.Verdict, r.Reason = NotSatisfied, "no "+o.Event.Type+" event in window"
		}
		return r
	}
	r.Verdict, r.Reason = Unknown, "unsupported objective kind"
	return r
}

func (e *Evaluator) metric(ctx context.Context, o Objective, in Input, target string, now time.Time) ObjectiveResult {
	r := ObjectiveResult{ID: o.ID, Kind: o.Kind, Verdict: Inconclusive}
	m := o.Metric
	var src MetricSource
	if e.Metrics != nil {
		src, _ = e.Metrics.Metric(m.Source)
	}
	if src == nil {
		r.Reason = "metric source " + m.Source + " unavailable"
		return r
	}
	cctx, cancel := context.WithTimeout(ctx, e.timeout())
	defer cancel()
	start := now.Add(-o.Window.D())
	res, err := src.QueryRange(cctx, MetricQuery{TenantID: in.TenantID, Source: m.Source, Query: m.Query,
		TargetRef: target, Start: start, End: now})
	r.EvidenceRef = res.EvidenceRef
	if err != nil {
		r.Reason = "metric query failed: " + cpkit.Truncate(err.Error(), 200)
		return r
	}
	// Only finite samples inside the window count; a source that returns
	// data outside the requested range cannot fake freshness.
	samples := make([]Sample, 0, len(res.Samples))
	for _, s := range res.Samples {
		if math.IsNaN(s.V) || math.IsInf(s.V, 0) || s.T.Before(start) || s.T.After(now) {
			continue
		}
		samples = append(samples, s)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].T.Before(samples[j].T) })
	r.Samples = len(samples)
	if len(samples) == 0 {
		r.Reason = "no data in window"
		return r
	}
	if len(samples) < o.MinSamples {
		r.Reason = fmt.Sprintf("%d samples, need %d", len(samples), o.MinSamples)
		return r
	}
	from := samples[0].T
	// Only the contiguous tail can carry the stabilization clock. A lone
	// older point must not conceal an observation outage between samples.
	gap := o.Freshness.D()
	if gap <= 0 {
		gap = time.Minute
	}
	for i := 1; i < len(samples); i++ {
		if samples[i].T.Sub(samples[i-1].T) > gap {
			from = samples[i].T
		}
	}
	r.ObservedFrom = &from
	newest := samples[len(samples)-1].T
	if f := o.Freshness.D(); f > 0 && now.Sub(newest) > f {
		r.Reason = fmt.Sprintf("newest sample is %s old, freshness %s", now.Sub(newest).Truncate(time.Second), f)
		return r
	}
	pass := func(v float64) bool { return compare(m.Operator, v, m.Threshold) }
	var value float64
	ok := true
	switch m.Aggregate {
	case AggAll:
		worst := samples[0].V
		for _, s := range samples {
			if !pass(s.V) {
				ok = false
				worst = s.V
				break
			}
			worst = s.V
		}
		value = worst
	case AggAvg:
		sum := 0.0
		for _, s := range samples {
			sum += s.V
		}
		value = sum / float64(len(samples))
		ok = pass(value)
	case AggMax:
		value = samples[0].V
		for _, s := range samples {
			value = math.Max(value, s.V)
		}
		ok = pass(value)
	case AggMin:
		value = samples[0].V
		for _, s := range samples {
			value = math.Min(value, s.V)
		}
		ok = pass(value)
	case AggLast:
		value = samples[len(samples)-1].V
		ok = pass(value)
	}
	r.Value = &value
	if ok {
		r.Verdict = Satisfied
	} else {
		r.Verdict = NotSatisfied
		r.Reason = fmt.Sprintf("%s=%g violates %s %g", m.Aggregate, value, m.Operator, m.Threshold)
	}
	return r
}

func compare(op string, v, t float64) bool {
	switch op {
	case OpLT:
		return v < t
	case OpLTE:
		return v <= t
	case OpGT:
		return v > t
	case OpGTE:
		return v >= t
	case OpEQ:
		return v == t
	case OpNE:
		return v != t
	}
	return false
}

// ValidateSpec checks that a recorded evaluation contains exactly the declared
// objectives and that its raw verdict follows from their verdicts. It validates
// structure, not the authenticity of source observations.
func (ev Evaluation) ValidateSpec(spec Spec) error {
	if ev.SpecHash != spec.Hash() || len(ev.Objectives) != len(spec.AllOf)+len(spec.AnyOf) {
		return fmt.Errorf("outcome does not match the declared spec")
	}
	results := make(map[string]ObjectiveResult, len(ev.Objectives))
	for _, r := range ev.Objectives {
		if _, exists := results[r.ID]; exists || !r.Verdict.Valid() {
			return fmt.Errorf("outcome has duplicate objectives or an invalid verdict")
		}
		results[r.ID] = r
	}
	collect := func(objectives []Objective) ([]ObjectiveResult, error) {
		out := make([]ObjectiveResult, 0, len(objectives))
		for _, o := range objectives {
			r, ok := results[o.ID]
			if !ok || r.Kind != o.Kind || r.Optional != o.Optional {
				return nil, fmt.Errorf("outcome objective %s does not match the spec", o.ID)
			}
			out = append(out, r)
		}
		return out, nil
	}
	all, err := collect(spec.AllOf)
	if err != nil {
		return err
	}
	any, err := collect(spec.AnyOf)
	if err != nil {
		return err
	}
	raw := combineAll(all)
	if len(spec.AnyOf) > 0 {
		raw = combineAll2(raw, combineAny(any))
	}
	if raw != ev.RawVerdict {
		return fmt.Errorf("outcome raw verdict contradicts its objectives")
	}
	return nil
}
