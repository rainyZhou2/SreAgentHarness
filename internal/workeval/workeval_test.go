package workeval

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

type fake struct {
	works map[string]work.Work
	evals []outcome.Evaluation
	acts  []action.Record
	links []work.RunLink
	evs   []evidence.Record
	hyps  []evidence.Hypothesis
}

func (f *fake) GetWork(_ context.Context, _, id string) (work.Work, error) { return f.works[id], nil }
func (f *fake) ListWorks(_ context.Context, fl work.Filter) ([]work.Work, error) {
	var out []work.Work
	for _, w := range f.works {
		for _, s := range w.Goal.SubjectRefs {
			if s == fl.Subject && (fl.Kind == "" || w.Kind == fl.Kind) {
				out = append(out, w)
			}
		}
	}
	return out, nil
}
func (f *fake) ListRunLinks(context.Context, string, string) ([]work.RunLink, error) {
	return f.links, nil
}
func (f *fake) ListOutcomes(context.Context, string, string, int) ([]outcome.Evaluation, error) {
	return f.evals, nil
}
func (f *fake) ListActions(context.Context, string, string) ([]action.Record, error) {
	return f.acts, nil
}
func (f *fake) ListEvidence(context.Context, string, string, int) ([]evidence.Record, error) {
	return f.evs, nil
}
func (f *fake) ListHypotheses(context.Context, string, string) ([]evidence.Hypothesis, error) {
	return f.hyps, nil
}

func fp(v float64) *float64 { return &v }

func TestScoreHealthyWork(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	disp, closed := t0.Add(5*time.Minute), t0.Add(20*time.Minute)
	f := &fake{works: map[string]work.Work{"w": {ID: "w", TenantID: "t", Kind: work.KindIncident, Status: work.StatusSucceeded,
		CreatedAt: t0, ClosedAt: &closed, Goal: work.GoalSpec{SubjectRefs: []string{"deployment://prod/checkout"}}}},
		evals: []outcome.Evaluation{ // newest first
			{ID: "e2", Verdict: outcome.Satisfied, Objectives: []outcome.ObjectiveResult{{ID: "err", Verdict: outcome.Satisfied, Value: fp(0.002)},
				{ID: "lat", Verdict: outcome.NotSatisfied, Optional: true}}},
			{ID: "e1", Verdict: outcome.NotSatisfied, Objectives: []outcome.ObjectiveResult{{ID: "err", Verdict: outcome.NotSatisfied, Value: fp(0.08)}}}},
		acts: []action.Record{
			{Plan: action.Plan{ActionName: "deployment.rollback", TargetRef: "deployment://prod/checkout", CreatedAt: t0.Add(4 * time.Minute)},
				State: action.StateVerified, DispatchedAt: &disp, MayHaveDispatched: true},
			{Plan: action.Plan{ActionName: "deployment.scale", TargetRef: "deployment://prod/checkout", CreatedAt: t0.Add(3 * time.Minute)},
				State: action.StateRejected, Reason: "precondition: generation changed"}},
		links: []work.RunLink{{RunID: "r1", CostMicros: 250000}, {RunID: "r2", CostMicros: 50000}},
		evs:   []evidence.Record{{ID: "ev1", TrustTier: "T3"}, {ID: "ev2", TrustTier: "T4"}},
		hyps: []evidence.Hypothesis{{Key: "h1", Status: evidence.HypSupported, Supporting: []string{"ev1"}},
			{Key: "h2", Status: evidence.HypRefuted}}}
	e := &Evaluator{Ledger: f, ToolCalls: func(context.Context, string, string) (int, error) { return 7, nil },
		MemoryMatch: func(context.Context, work.Work) (int, int, error) { return 1, 2, nil }}
	sc, err := e.Score(context.Background(), "t", "w")
	if err != nil {
		t.Fatal(err)
	}
	if !sc.Pass || sc.Verified == nil || !*sc.Verified || sc.VerifiedBy != "machine" {
		t.Fatalf("verdict: %+v", sc)
	}
	if sc.RequiredMet != 1 || sc.RequiredTotal != 1 || sc.ObjectiveDeltas["err"] != -0.078 {
		t.Fatalf("objectives: %+v", sc)
	}
	if sc.RuleViolations != 0 || sc.ActionsApplied != 1 || sc.Grounding() != 1 || sc.VersionMatch() != 0.5 {
		t.Fatalf("dimensions: %+v", sc)
	}
	if sc.DiagnosisSeconds != 180 || sc.DurationSeconds != 1200 || sc.Runs != 2 || sc.ToolCalls != 14 || sc.CostUSD != 0.3 {
		t.Fatalf("cost/time: %+v", sc)
	}
}

func TestScoreGates(t *testing.T) {
	t0 := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	d1, d2 := t0.Add(time.Minute), t0.Add(3*time.Minute)
	f := &fake{works: map[string]work.Work{
		"w": {ID: "w", TenantID: "t", Kind: work.KindIncident, Status: work.StatusSucceeded, CreatedAt: t0, UpdatedAt: t0.Add(time.Hour),
			Goal: work.GoalSpec{SubjectRefs: []string{"deployment://prod/checkout"}}},
		"secondary": {ID: "secondary", TenantID: "t", Kind: work.KindIncident, CreatedAt: t0.Add(10 * time.Minute),
			Goal: work.GoalSpec{SubjectRefs: []string{"deployment://prod/checkout"}}}},
		acts: []action.Record{
			{Plan: action.Plan{ActionName: "deployment.rollback", TargetRef: "deployment://prod/checkout", CreatedAt: t0},
				State: action.StateEffectUnknown, DispatchedAt: &d1, MayHaveDispatched: true},
			{Plan: action.Plan{ActionName: "deployment.rollback", TargetRef: "deployment://prod/checkout", CreatedAt: t0.Add(2 * time.Minute)},
				State: action.StateApplied, DispatchedAt: &d2, MayHaveDispatched: true},
			{Plan: action.Plan{ActionName: "node.drain", TargetRef: "node://n1", CreatedAt: t0}, State: action.StateRejected,
				Reason: "target is outside the Work subjects"}},
		hyps: []evidence.Hypothesis{{Key: "h1", Status: evidence.HypSupported, Supporting: []string{"missing"}}}}
	sc, err := (&Evaluator{Ledger: f}).Score(context.Background(), "t", "w")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{GateRuleViolation: true, GateRepeatedUnknown: true, GateBlastRadius: true, GateUngrounded: true,
		GateUnverifiedClaim: true}
	for _, g := range sc.FailedGates {
		delete(want, g)
	}
	if sc.Pass || len(want) != 0 || sc.RepeatedUnknownEffects != 1 || len(sc.SecondaryIncidents) != 1 || sc.UnresolvedEffects != 1 {
		t.Fatalf("gates missing %v: %+v", want, sc)
	}

	sum := Aggregate([]Scorecard{sc, {Pass: true, Verified: fp2(true), DurationSeconds: 60, CostUSD: 1}})
	if sum.Works != 2 || sum.Passed != 1 || sum.RepeatedUnknown != 1 || sum.FailedGates[GateBlastRadius] != 1 || sum.SuccessRate != 1 {
		t.Fatalf("summary: %+v", sum)
	}
	if regs := Compare(Summary{SuccessRate: 1, Grounding: 1, VersionMatch: 1}, sum); len(regs) < 2 {
		t.Fatalf("regressions: %v", regs)
	}
}

func fp2(b bool) *bool { return &b }
