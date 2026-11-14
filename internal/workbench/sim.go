package workbench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// simClock is the simulated wall clock.
type simClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *simClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *simClock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// ---- runs ----

type simRun struct {
	script    *ScriptedRun
	req       orchestration.RunRequest
	started   time.Time
	finishAt  time.Time
	stage     string
	role      string
	canceled  bool
	activated bool
}

// simRuns answers run starts from the case script on the simulated clock.
type simRuns struct {
	mu      sync.Mutex
	clock   *simClock
	scripts []ScriptedRun
	used    map[string]int // stage/role -> runs started
	runs    map[string]*simRun
	byKey   map[string]string
	order   []string
}

func newSimRuns(clock *simClock, scripts []ScriptedRun) *simRuns {
	return &simRuns{clock: clock, scripts: scripts, used: map[string]int{}, runs: map[string]*simRun{}, byKey: map[string]string{}}
}

func (f *simRuns) ReserveRun(_ context.Context, req orchestration.RunRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.byKey[req.IdempotencyKey]; ok {
		return id, nil
	}
	stage := req.Labels["work_stage"]
	role := req.IdempotencyKey[strings.LastIndexByte(req.IdempotencyKey, ':')+1:]
	var matches []int
	for i, s := range f.scripts {
		if s.Stage == stage && (s.Role == "" || s.Role == role) {
			matches = append(matches, i)
		}
	}
	now := f.clock.Now()
	r := &simRun{req: req, started: now, stage: stage, role: role}
	if len(matches) > 0 {
		k := stage + "/" + role
		n := f.used[k]
		f.used[k] = n + 1
		s := f.scripts[matches[min(n, len(matches)-1)]]
		r.script = &s
		d := s.Duration.D()
		if d <= 0 {
			d = time.Minute
		}
		r.finishAt = now.Add(d)
	} else {
		r.finishAt = now // unscripted runs fail at once
	}
	// Deterministic UUID-shaped ids: a replay produces the same run ids.
	id := fmt.Sprintf("00000000-0000-4000-8000-%012d", len(f.order)+1)
	f.runs[id], f.byKey[req.IdempotencyKey] = r, id
	f.order = append(f.order, id)
	return id, nil
}

func (f *simRuns) RunState(_ context.Context, _, runID string) (orchestration.RunState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[runID]
	if !ok {
		return orchestration.RunState{}, domain.ErrNotFound
	}
	switch {
	case r.canceled:
		return orchestration.RunState{Status: "CANCELLED", Terminal: true}, nil
	case !r.activated:
		return orchestration.RunState{Status: "PENDING_ACTIVATION"}, nil
	case f.clock.Now().Before(r.finishAt):
		return orchestration.RunState{Status: "RUNNING"}, nil
	case r.script == nil:
		return orchestration.RunState{Status: "FAILED", Terminal: true,
			FailureReason: fmt.Sprintf("no scripted run for stage %q role %q", r.stage, r.role)}, nil
	case r.script.Status == "failed":
		return orchestration.RunState{Status: "FAILED", Terminal: true, FailureReason: cpkit.Truncate(firstNonEmpty(r.script.FailureReason, "scripted failure"), 500)}, nil
	}
	res := r.script.Result
	if len(res) == 0 {
		res = json.RawMessage(`{}`)
	}
	return orchestration.RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: res}, nil
}

func (f *simRuns) CancelRun(_ context.Context, _, runID, _, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.runs[runID]; r != nil && (!r.activated || f.clock.Now().Before(r.finishAt)) {
		r.canceled = true
	}
	return nil
}

func (f *simRuns) toolCalls(runID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r := f.runs[runID]; r != nil && r.script != nil {
		return r.script.ToolCalls
	}
	return 0
}

// unused lists scripted runs never started (a sign the case and the
// template disagree).
func (f *simRuns) unused() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	perKey := map[string]int{}
	var out []string
	for _, s := range f.scripts {
		k := s.Stage + "/" + firstNonEmpty(s.Role, "*")
		perKey[k]++
		started := 0
		for key, n := range f.used {
			st, role, _ := strings.Cut(key, "/")
			if st == s.Stage && (s.Role == "" || s.Role == role) {
				started += n
			}
		}
		if started < perKey[k] {
			out = append(out, "run "+k)
		}
	}
	return out
}

// ---- metrics ----

type seriesKey struct{ source, query string }

// simMetrics is a scripted metric source: each series is a step function.
type simMetrics struct {
	mu     sync.Mutex
	series map[seriesKey][]outcome.Sample
}

func newSimMetrics(start time.Time, ss []Series) *simMetrics {
	m := &simMetrics{series: map[seriesKey][]outcome.Sample{}}
	for _, s := range ss {
		k := seriesKey{s.Source, s.Query}
		for _, p := range s.Points {
			m.series[k] = append(m.series[k], outcome.Sample{T: start.Add(p.At.D()), V: p.V})
		}
	}
	return m
}

func (m *simMetrics) set(source, query string, at time.Time, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := seriesKey{source, query}
	pts := m.series[k]
	// An effect overrides scripted points after it.
	kept := pts[:0]
	for _, p := range pts {
		if p.T.Before(at) {
			kept = append(kept, p)
		}
	}
	m.series[k] = append(kept, outcome.Sample{T: at, V: v})
}

// source binds one metric source name.
func (m *simMetrics) source(name string) outcome.MetricSource { return metricSource{m: m, name: name} }

type metricSource struct {
	m    *simMetrics
	name string
}

// QueryRange samples the step function over [start, end].
func (s metricSource) QueryRange(_ context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	s.m.mu.Lock()
	pts := append([]outcome.Sample(nil), s.m.series[seriesKey{s.name, q.Query}]...)
	s.m.mu.Unlock()
	if len(pts) == 0 {
		return outcome.MetricResult{}, fmt.Errorf("bench: no series for %s %q", s.name, q.Query)
	}
	step := max(15*time.Second, q.End.Sub(q.Start)/500)
	var out []outcome.Sample
	for t := q.Start; !t.After(q.End); t = t.Add(step) {
		i := sort.Search(len(pts), func(i int) bool { return pts[i].T.After(t) }) - 1
		if i < 0 {
			continue // before the first point: no data
		}
		out = append(out, outcome.Sample{T: t, V: pts[i].V})
	}
	return outcome.MetricResult{Samples: out, EvidenceRef: fmt.Sprintf("bench://%s/%s", s.name, q.Query)}, nil
}

// metricRegistry resolves sources (unknown names have no data).
type metricRegistry struct{ m *simMetrics }

func (r metricRegistry) Metric(name string) (outcome.MetricSource, bool) {
	return r.m.source(name), true
}

// ---- actions ----

// simExecutor is a scripted action executor.
type simExecutor struct {
	spec    SimAction
	world   *world.Service
	metrics *simMetrics
	mu      sync.Mutex
	calls   int
}

func (e *simExecutor) Descriptor() action.Descriptor {
	kinds := e.spec.ResourceKinds
	if len(kinds) == 0 {
		kinds = []string{"*"}
	}
	return action.Descriptor{Name: e.spec.Name, Version: "bench", ResourceKinds: kinds, Risk: domain.RiskClass(e.spec.Risk),
		IdempotencyMode: action.IdemNative, BlastRadius: "single_resource", Description: "simulated by the Work bench",
		RequiredFacts: e.spec.RequiredFacts}
}

func (e *simExecutor) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}

func (e *simExecutor) Execute(ctx context.Context, p action.Plan, _ string) (json.RawMessage, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	switch e.spec.Outcome {
	case "not_applied":
		return nil, fmt.Errorf("%w: simulated rejection", action.ErrNotApplied)
	case "unknown":
		if e.inspect() == "applied" {
			if err := e.apply(ctx, p); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("bench: simulated timeout after dispatch")
	}
	if err := e.apply(ctx, p); err != nil {
		return nil, err
	}
	return json.RawMessage(`{"simulated":true}`), nil
}

func (e *simExecutor) inspect() string {
	if e.spec.Inspect != "" {
		return e.spec.Inspect
	}
	return firstNonEmpty(e.spec.Outcome, "applied")
}

func (e *simExecutor) Inspect(context.Context, action.Plan) (action.InspectResult, error) {
	switch e.inspect() {
	case "applied":
		t := true
		return action.InspectResult{Applied: &t, Detail: json.RawMessage(`"simulated"`)}, nil
	case "not_applied":
		f := false
		return action.InspectResult{Applied: &f, Detail: json.RawMessage(`"simulated"`)}, nil
	}
	return action.InspectResult{Detail: json.RawMessage(`"simulated: effect cannot be determined"`)}, nil
}

func (e *simExecutor) apply(ctx context.Context, p action.Plan) error {
	now := e.world.Clock()
	for _, f := range e.spec.Facts {
		subj := firstNonEmpty(f.Subject, p.TargetRef)
		if _, _, err := e.world.Store.AssertFact(ctx, world.Fact{TenantID: p.TenantID, SubjectRef: subj, Predicate: f.Predicate,
			Value: f.Value, Source: "bench:" + e.spec.Name, TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
			return err
		}
	}
	for _, m := range e.spec.Metrics {
		e.metrics.set(m.Source, m.Query, now, m.V)
	}
	return nil
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}

func (f *simRuns) ActivateRun(_ context.Context, req work.RunActivation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[req.RunID]
	if !ok {
		return domain.ErrNotFound
	}
	if r.canceled {
		return &domain.RejectedError{Reason: "run_cancelled"}
	}
	if !r.activated {
		duration := r.finishAt.Sub(r.started)
		r.started = f.clock.Now()
		r.finishAt = r.started.Add(duration)
		r.activated = true
	}
	return nil
}
