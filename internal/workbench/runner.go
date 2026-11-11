package workbench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/provenance"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
	"go-sre-harness/internal/world"
)

// Runner runs cases.
type Runner struct {
	// Dir holds the per-case SQLite stores (default: a temp dir, removed
	// after the case).
	Dir string
	Log *slog.Logger
}

// Result is the outcome of one case.
type Result struct {
	Case             string             `json:"case"`
	WorkID           string             `json:"work_id,omitempty"`
	Works            []string           `json:"works,omitempty"`
	Status           work.Status        `json:"status,omitempty"`
	Reason           string             `json:"reason,omitempty"`
	Scorecard        workeval.Scorecard `json:"scorecard"`
	RootCause        string             `json:"root_cause,omitempty"`
	Actions          []string           `json:"actions,omitempty"` // name:state, in proposal order
	SimulatedSeconds float64            `json:"simulated_seconds"`
	Ticks            int                `json:"ticks"`
	HorizonReached   bool               `json:"horizon_reached,omitempty"`
	Unused           []string           `json:"unused,omitempty"` // scripted steps never consumed
	Failures         []string           `json:"failures,omitempty"`
	Passed           bool               `json:"passed"`
	// Report is the primary Work's postmortem; Events its timeline types.
	Report           *postmortem.Report `json:"-"`
	Events           []string           `json:"-"`
	actionsProposed  map[string]bool
	actionsApplied   map[string]bool
	signalWorkCounts int
}

type env struct {
	c       Case
	ctx     context.Context
	clock   *simClock
	store   *cpsql.Store
	runs    *simRuns
	metrics *simMetrics
	world   *world.Service
	acts    *action.Runtime
	orch    *orchestration.Orchestrator
	rec     *orchestration.Reconciler
	ing     *orchestration.Ingestor
	svc     *orchestration.Service

	primary string
	works   []string
}

// Run executes one case. Errors are infrastructure failures; assertion
// failures are reported in the result.
func (r *Runner) Run(ctx context.Context, c Case) (Result, error) {
	if err := c.Normalize(); err != nil {
		return Result{Case: c.Name}, err
	}
	dir, cleanup, err := r.dir(c.Name)
	if err != nil {
		return Result{Case: c.Name}, err
	}
	defer cleanup()
	e, err := r.setup(ctx, c, filepath.Join(dir, "cp.db"))
	if err != nil {
		return Result{Case: c.Name}, err
	}
	defer e.store.Close()
	res, err := e.simulate()
	if err != nil {
		return res, fmt.Errorf("case %s: %w", c.Name, err)
	}
	return res, nil
}

func (r *Runner) dir(name string) (string, func(), error) {
	if r.Dir != "" {
		d := filepath.Join(r.Dir, safeName(name))
		if err := os.MkdirAll(d, 0o700); err != nil {
			return "", nil, err
		}
		_ = os.Remove(filepath.Join(d, "cp.db"))
		return d, func() {}, nil
	}
	d, err := os.MkdirTemp("", "workbench-*")
	if err != nil {
		return "", nil, err
	}
	return d, func() { _ = os.RemoveAll(d) }, nil
}

func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}

func (r *Runner) setup(ctx context.Context, c Case, path string) (*env, error) {
	log := r.Log
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s, err := cpsql.OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	clock := &simClock{t: c.Start}
	s.Clock = clock.Now
	ws := &world.Service{Store: s, Clock: clock.Now}
	metrics := newSimMetrics(c.Start, c.Metrics)
	ev := &outcome.Evaluator{Metrics: metricRegistry{metrics}, States: orchestration.WorldFacts{World: ws}, Events: s}
	reg := action.NewRegistry()
	for _, a := range c.Actions {
		if err := reg.Register(&simExecutor{spec: a, world: ws, metrics: metrics}); err != nil {
			s.Close()
			return nil, err
		}
	}
	acts := &action.Runtime{Store: s, Registry: reg, World: ws, Outcomes: ev, Clock: clock.Now, Log: log,
		Policy:   policy.Engine{Version: "bench", Environment: c.Environment, AllowR1WithoutApproval: c.Environment != "production"},
		Identity: action.WorldIdentity{World: ws},
		// The production fact-quality policy: disputed or orphaned facts do
		// not justify an action in a benchmark either.
		Quality: provenance.Quality{Health: s, Authority: provenance.DefaultAuthority(), Clock: clock.Now}}
	cat, err := orchestration.NewCatalog(c.Templates)
	if err != nil {
		s.Close()
		return nil, err
	}
	router, err := eventing.NewRouter(c.Rules)
	if err != nil {
		s.Close()
		return nil, err
	}
	runs := newSimRuns(clock, c.Runs)
	ing := &orchestration.Ingestor{Store: s, Router: router, Catalog: cat, Clock: clock.Now, Log: log, Changes: s}
	e := &env{c: c, ctx: ctx, clock: clock, store: s, runs: runs, metrics: metrics, world: ws, acts: acts, ing: ing,
		orch: &orchestration.Orchestrator{Store: s, Runs: runs, Actions: acts, Evaluator: ev, Clock: clock.Now, Owner: "bench",
			RunPoll: min(5*time.Second, c.Tick.D()), Log: log, Catalog: cat},
		svc: &orchestration.Service{Store: s, Runs: runs, Catalog: cat, Clock: clock.Now, Log: log},
		rec: &orchestration.Reconciler{Store: s, Ingestor: ing, Actions: acts, Clock: clock.Now, Log: log}}
	return e, nil
}

// fixtures tracks which scripted inputs were applied.
type fixtures struct {
	workDone  bool
	signals   []bool
	facts     []bool
	evidence  []bool
	approvals []bool
	human     []bool
}

func (e *env) simulate() (Result, error) {
	c := e.c
	res := Result{Case: c.Name, actionsProposed: map[string]bool{}, actionsApplied: map[string]bool{}}
	fx := fixtures{signals: make([]bool, len(c.Signals)), facts: make([]bool, len(c.Facts)), evidence: make([]bool, len(c.Evidence)),
		approvals: make([]bool, len(c.Approvals)), human: make([]bool, len(c.Human))}
	deadline := c.Start.Add(c.Horizon.D())
	for {
		now := e.clock.Now()
		if err := e.applyDue(&fx, now); err != nil {
			return res, err
		}
		if _, err := e.orch.Tick(e.ctx); err != nil {
			return res, fmt.Errorf("orchestrator: %w", err)
		}
		if _, err := e.rec.FireDue(e.ctx); err != nil {
			return res, fmt.Errorf("reconciler: %w", err)
		}
		if err := e.decide(&fx); err != nil {
			return res, err
		}
		if err := e.human(&fx, now); err != nil {
			return res, err
		}
		res.Ticks++
		done, err := e.settled(&fx)
		if err != nil {
			return res, err
		}
		if done {
			break
		}
		if !now.Add(c.Tick.D()).Before(deadline) {
			res.HorizonReached = true
			break
		}
		e.clock.Add(c.Tick.D())
	}
	res.SimulatedSeconds = e.clock.Now().Sub(c.Start).Seconds()
	if err := e.collect(&res); err != nil {
		return res, err
	}
	for i, used := range fx.approvals {
		if !used {
			res.Unused = append(res.Unused, "approval "+c.Approvals[i].Action)
		}
	}
	for i, used := range fx.human {
		if !used {
			res.Unused = append(res.Unused, "human "+c.Human[i].Command+" "+c.Human[i].Stage)
		}
	}
	res.Unused = append(res.Unused, e.runs.unused()...)
	check(c, &res)
	return res, nil
}

func (e *env) at(d cpkit.Duration) time.Time { return e.c.Start.Add(d.D()) }

func (e *env) applyDue(fx *fixtures, now time.Time) error {
	c := e.c
	for i, f := range c.Facts {
		if fx.facts[i] || e.at(f.At).After(now) {
			continue
		}
		fx.facts[i] = true
		trust := firstNonEmpty(f.Trust, world.TrustFirstParty)
		if _, _, err := e.store.AssertFact(e.ctx, world.Fact{TenantID: c.Tenant, SubjectRef: f.Subject, Predicate: f.Predicate,
			Value: f.Value, Source: firstNonEmpty(f.Source, "bench"), TrustTier: trust, ValidFrom: now, ObservedAt: now}); err != nil {
			return fmt.Errorf("fact %s/%s: %w", f.Subject, f.Predicate, err)
		}
	}
	if c.Work != nil && !fx.workDone && !e.at(c.Work.At).After(now) {
		fx.workDone = true
		w, _, err := e.svc.Create(e.ctx, orchestration.CreateRequest{TenantID: c.Tenant, ActorID: "bench", ActorType: "user",
			IdempotencyKey: "bench:" + c.Name, Kind: c.Work.Kind, Title: c.Work.Title, TemplateID: c.Work.Template, Goal: c.Work.Goal})
		if err != nil {
			return fmt.Errorf("create work: %w", err)
		}
		e.addWork(w.ID)
	}
	for i, s := range c.Signals {
		if fx.signals[i] || e.at(s.At).After(now) {
			continue
		}
		fx.signals[i] = true
		env := s.Event
		env.TenantID = c.Tenant
		if env.ID == "" {
			env.ID = fmt.Sprintf("bench-%s-%d", safeName(c.Name), i)
		}
		if env.OccurredAt.IsZero() {
			env.OccurredAt = now
		}
		env.ObservedAt = now
		if env.Confidence == 0 {
			env.Confidence = 1
		}
		rec, _, err := e.ing.Ingest(e.ctx, env)
		if err != nil {
			return fmt.Errorf("signal %d: %w", i, err)
		}
		if rec.WorkID != "" {
			e.addWork(rec.WorkID)
		}
	}
	if e.primary == "" {
		return nil
	}
	for i, ev := range c.Evidence {
		if fx.evidence[i] || e.at(ev.At).After(now) {
			continue
		}
		fx.evidence[i] = true
		if _, _, err := e.store.AddEvidence(e.ctx, evidence.Record{TenantID: c.Tenant, WorkID: e.primary, SourceType: ev.SourceType,
			SourceRef: ev.SourceRef, TrustTier: ev.TrustTier, Summary: ev.Summary, CapturedAt: now, ActorID: "bench"}); err != nil {
			return fmt.Errorf("evidence %s: %w", ev.SourceRef, err)
		}
	}
	return nil
}

func (e *env) addWork(id string) {
	if e.primary == "" {
		e.primary = id
	}
	if !slices.Contains(e.works, id) {
		e.works = append(e.works, id)
	}
}

// decide answers actions waiting for approval from the script.
func (e *env) decide(fx *fixtures) error {
	for _, wid := range e.works {
		acts, err := e.store.ListActions(e.ctx, e.c.Tenant, wid)
		if err != nil {
			return err
		}
		for _, a := range acts {
			if a.State != action.StateWaitingApproval {
				continue
			}
			for i, ap := range e.c.Approvals {
				if fx.approvals[i] || ap.Action != a.ActionName {
					continue
				}
				fx.approvals[i] = true
				decision := firstNonEmpty(ap.Decision, "approve")
				if _, err := e.acts.Decide(e.ctx, e.c.Tenant, a.ID, firstNonEmpty(ap.Approver, "bench-approver"), decision,
					a.IntentHash, ap.Comment, "work:"+wid); err != nil {
					return fmt.Errorf("approval of %s: %w", a.ActionName, err)
				}
				if err := e.store.WakeWork(e.ctx, e.c.Tenant, wid, "approval", "user", firstNonEmpty(ap.Approver, "bench-approver"), nil); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

// human sends scripted commands to the primary Work.
func (e *env) human(fx *fixtures, now time.Time) error {
	if e.primary == "" {
		return nil
	}
	w, err := e.store.GetWork(e.ctx, e.c.Tenant, e.primary)
	if err != nil {
		return err
	}
	for i, h := range e.c.Human {
		if fx.human[i] {
			continue
		}
		due := h.At != nil && !e.at(*h.At).After(now)
		if h.At == nil && h.Stage != "" && w.Status == work.StatusWaiting {
			if st, ok := w.CurrentStage(); ok && st.ID == h.Stage {
				due = true
			}
		}
		if h.At != nil && h.Stage != "" { // both: at or after the offset, at that stage
			st, ok := w.CurrentStage()
			due = due && ok && st.ID == h.Stage && w.Status == work.StatusWaiting
		}
		if !due {
			continue
		}
		fx.human[i] = true
		if _, err := e.svc.Command(e.ctx, orchestration.CommandRequest{TenantID: e.c.Tenant, WorkID: e.primary,
			ActorID: firstNonEmpty(h.Actor, "bench-human"), IdempotencyKey: fmt.Sprintf("bench-human-%d", i), Type: h.Command,
			Payload: h.Payload}); err != nil {
			return fmt.Errorf("human %s: %w", h.Command, err)
		}
		return nil // one command per tick; the Work re-evaluates first
	}
	return nil
}

// settled: every Work is terminal and no future signal can reopen or
// create one.
func (e *env) settled(fx *fixtures) (bool, error) {
	if len(e.works) == 0 {
		return false, nil
	}
	if !fx.workDone && e.c.Work != nil || slices.Contains(fx.signals, false) {
		return false, nil
	}
	for _, id := range e.works {
		w, err := e.store.GetWork(e.ctx, e.c.Tenant, id)
		if err != nil {
			return false, err
		}
		if !w.Status.Terminal() {
			return false, nil
		}
	}
	return true, nil
}

func (e *env) collect(res *Result) error {
	res.Works = e.works
	res.signalWorkCounts = len(e.works)
	if e.primary == "" {
		res.Failures = append(res.Failures, "no Work was created")
		return nil
	}
	res.WorkID = e.primary
	w, err := e.store.GetWork(e.ctx, e.c.Tenant, e.primary)
	if err != nil {
		return err
	}
	res.Status, res.Reason = w.Status, w.Reason
	acts, err := e.store.ListActions(e.ctx, e.c.Tenant, e.primary)
	if err != nil {
		return err
	}
	for _, a := range acts {
		res.Actions = append(res.Actions, a.ActionName+":"+string(a.State))
		res.actionsProposed[a.ActionName] = true
		switch a.State {
		case action.StateApplied, action.StateVerified:
			res.actionsApplied[a.ActionName] = true
		}
	}
	ev := &workeval.Evaluator{Ledger: e.store, ToolCalls: func(_ context.Context, _, runID string) (int, error) {
		return e.runs.toolCalls(runID), nil
	}}
	sc, err := ev.Score(e.ctx, e.c.Tenant, e.primary)
	if err != nil {
		return err
	}
	res.Scorecard = sc
	rep, err := (&postmortem.Builder{Ledger: e.store}).Build(e.ctx, e.c.Tenant, e.primary)
	if err != nil {
		return err
	}
	res.Report = &rep
	if rep.RootCause != nil {
		res.RootCause = rep.RootCause.Statement
	}
	evs, err := e.store.ListWorkEvents(e.ctx, e.c.Tenant, e.primary, 0, 1000)
	if err != nil {
		return err
	}
	for _, x := range evs {
		res.Events = append(res.Events, x.Type)
	}
	return nil
}

// check applies the expectations.
func check(c Case, res *Result) {
	x := c.Expect
	fail := func(format string, a ...any) { res.Failures = append(res.Failures, fmt.Sprintf(format, a...)) }
	sc := res.Scorecard
	if x.Status != "" && res.Status != x.Status {
		fail("status %s, want %s (%s)", res.Status, x.Status, res.Reason)
	}
	if x.Pass != nil && sc.Pass != *x.Pass {
		fail("scorecard pass=%v, want %v (failed gates %v)", sc.Pass, *x.Pass, sc.FailedGates)
	}
	for _, g := range x.Gates {
		if !slices.Contains(sc.FailedGates, g) {
			fail("gate %s did not fail (failed: %v)", g, sc.FailedGates)
		}
	}
	if x.MaxRuns > 0 && sc.Runs > x.MaxRuns {
		fail("%d runs, max %d", sc.Runs, x.MaxRuns)
	}
	if x.MaxActions > 0 && sc.ActionsProposed > x.MaxActions {
		fail("%d actions proposed, max %d", sc.ActionsProposed, x.MaxActions)
	}
	for _, a := range x.Applied {
		if !res.actionsApplied[a] {
			fail("action %s did not take effect (actions %v)", a, res.Actions)
		}
	}
	for _, a := range x.Forbidden {
		if res.actionsProposed[a] {
			fail("forbidden action %s was proposed", a)
		}
	}
	if x.Works > 0 && res.signalWorkCounts != x.Works {
		fail("%d Works, want %d", res.signalWorkCounts, x.Works)
	}
	switch {
	case x.RootCause == "unknown" && res.RootCause != "":
		fail("root cause %q, want unknown", res.RootCause)
	case x.RootCause != "" && x.RootCause != "unknown" && !strings.Contains(strings.ToLower(res.RootCause), strings.ToLower(x.RootCause)):
		fail("root cause %q does not mention %q", res.RootCause, x.RootCause)
	}
	if x.MaxDuration > 0 && sc.DurationSeconds > x.MaxDuration.D().Seconds() {
		fail("duration %.0fs exceeds %s", sc.DurationSeconds, x.MaxDuration.D())
	}
	if res.HorizonReached && x.Status.Terminal() {
		fail("horizon reached before the Work settled")
	}
	res.Passed = len(res.Failures) == 0
}

// Suite is the result of a set of cases.
type Suite struct {
	Results []Result         `json:"results"`
	Summary workeval.Summary `json:"summary"`
	Passed  int              `json:"cases_passed"`
	Failed  int              `json:"cases_failed"`
	// Regressions against a baseline summary (when given).
	Regressions []string `json:"regressions,omitempty"`
}

// RunSuite runs every case and aggregates the scorecards; a baseline adds
// the regression gate.
func (r *Runner) RunSuite(ctx context.Context, cases []Case, baseline *workeval.Summary) (Suite, error) {
	var s Suite
	var cards []workeval.Scorecard
	for _, c := range cases {
		res, err := r.Run(ctx, c)
		if err != nil {
			return s, err
		}
		s.Results = append(s.Results, res)
		cards = append(cards, res.Scorecard)
		if res.Passed {
			s.Passed++
		} else {
			s.Failed++
		}
	}
	s.Summary = workeval.Aggregate(cards)
	if baseline != nil {
		s.Regressions = workeval.Compare(*baseline, s.Summary)
	}
	return s, nil
}

// OK reports whether every case passed and nothing regressed.
func (s Suite) OK() bool { return s.Failed == 0 && len(s.Regressions) == 0 }

// MarshalIndent is a helper for CLI output.
func MarshalIndent(v any) []byte {
	b, _ := json.MarshalIndent(v, "", "  ")
	return append(b, '\n')
}
