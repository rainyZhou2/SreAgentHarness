package postmortem

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

type fake struct {
	w     work.Work
	evs   []work.Event
	acts  []action.Record
	evals []outcome.Evaluation
	recs  []evidence.Record
	hyps  []evidence.Hypothesis
}

func (f *fake) GetWork(context.Context, string, string) (work.Work, error)           { return f.w, nil }
func (f *fake) ListWorks(context.Context, work.Filter) ([]work.Work, error)          { return nil, nil }
func (f *fake) ListRunLinks(context.Context, string, string) ([]work.RunLink, error) { return nil, nil }
func (f *fake) ListOutcomes(context.Context, string, string, int) ([]outcome.Evaluation, error) {
	return f.evals, nil
}
func (f *fake) ListActions(context.Context, string, string) ([]action.Record, error) {
	return f.acts, nil
}
func (f *fake) ListEvidence(context.Context, string, string, int) ([]evidence.Record, error) {
	return f.recs, nil
}
func (f *fake) ListHypotheses(context.Context, string, string) ([]evidence.Hypothesis, error) {
	return f.hyps, nil
}
func (f *fake) ListWorkEvents(_ context.Context, _, _ string, after int64, limit int) ([]work.Event, error) {
	var out []work.Event
	for _, e := range f.evs {
		if e.Seq > after && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

var t0 = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)

func ev(seq int64, typ string, at time.Duration, payload map[string]any) work.Event {
	b, _ := json.Marshal(payload)
	return work.Event{Seq: seq, Type: typ, ActorType: "system", OccurredAt: t0.Add(at), Payload: b}
}

func fp(v float64) *float64 { return &v }

func incident() *fake {
	closed := t0.Add(40 * time.Minute)
	disp := t0.Add(22 * time.Minute)
	f := &fake{
		w: work.Work{ID: "w1", TenantID: "t", Generation: 1, Kind: work.KindIncident, Status: work.StatusSucceeded, Title: "checkout 5xx",
			CreatedAt: t0, ClosedAt: &closed, Goal: work.GoalSpec{SubjectRefs: []string{"deployment://prod/checkout"}}},
		evs: []work.Event{
			ev(1, "work.created", 0, map[string]any{"kind": "incident", "title": "checkout 5xx"}),
			ev(2, "work.event_attached", time.Second, map[string]any{"type": "alert.firing", "subject_ref": "deployment://prod/checkout"}),
			ev(3, "work.wake", 2*time.Second, nil),
			ev(4, "work.stage_completed", 20*time.Minute, map[string]any{"stage_id": "investigate"}),
		},
		acts: []action.Record{{Plan: action.Plan{ID: "a1", ActionName: "deployment.rollback", TargetRef: "deployment://prod/checkout",
			Risk: domain.RiskR2, CreatedAt: t0.Add(21 * time.Minute)}, State: action.StateVerified, DispatchedAt: &disp,
			MayHaveDispatched: true, UpdatedAt: t0.Add(25 * time.Minute)}},
		evals: []outcome.Evaluation{ // newest first
			{ID: "e3", Verdict: outcome.Satisfied, EvaluatedAt: t0.Add(39 * time.Minute),
				Objectives: []outcome.ObjectiveResult{{ID: "error_rate", Verdict: outcome.Satisfied, Value: fp(0.002)}}},
			{ID: "e2", Verdict: outcome.Satisfied, EvaluatedAt: t0.Add(35 * time.Minute)},
			{ID: "e1", Verdict: outcome.NotSatisfied, EvaluatedAt: t0.Add(5 * time.Minute)},
		},
		recs: []evidence.Record{{ID: "ev-deploy", TrustTier: "T1"}, {ID: "ev-log", TrustTier: "T4"}, {ID: "ev-metric", TrustTier: "T2"}},
		hyps: []evidence.Hypothesis{
			testHypothesis("release v2 introduced a nil map write", evidence.CatRootCause, evidence.DecisionConfirmed, []string{"ev-deploy", "ev-metric"}),
			testHypothesis("database saturation", "", evidence.DecisionInconclusive, []string{"refutation"}),
			{Key: "h3", Statement: "the model says DNS", Status: evidence.HypSupported, Supporting: []string{"ev-log"}},
			testHypothesis("cache stampede", evidence.CatContributing, evidence.DecisionConfirmed, []string{"ev-metric"}),
		},
	}
	seen := map[string]bool{}
	f.recs = nil
	for _, h := range f.hyps {
		if h.Decision == nil {
			continue
		}
		for _, r := range h.Decision.Input.Evidence {
			if !seen[r.ID] {
				f.recs = append(f.recs, r)
				seen[r.ID] = true
			}
		}
	}

	return f
}

func TestBuildIncidentReport(t *testing.T) {
	f := incident()
	r, err := (&Builder{Ledger: f, Eval: &workeval.Evaluator{Ledger: f}}).Build(context.Background(), "t", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if r.RootCause == nil || r.RootCause.Statement != "release v2 introduced a nil map write" ||
		strings.Join(r.RootCause.EvidenceRefs, ",") != "evidence:ev-deploy,evidence:ev-metric" {
		t.Fatalf("root cause: %+v", r.RootCause)
	}
	if len(r.Contributing) != 1 || r.Contributing[0].Statement != "cache stampede" {
		t.Fatalf("contributing: %+v", r.Contributing)
	}
	// Support from untrusted evidence never becomes a conclusion.
	if len(r.OpenHypotheses) != 1 || !strings.Contains(r.OpenHypotheses[0], "unadjudicated") || len(r.Refuted) != 1 {
		t.Fatalf("hypotheses: %+v / %+v", r.OpenHypotheses, r.Refuted)
	}
	if !strings.HasPrefix(r.Trigger, "signal: type=alert.firing") || r.DurationSeconds != 2400 {
		t.Fatalf("trigger/duration: %q %v", r.Trigger, r.DurationSeconds)
	}
	if r.Outcome == nil || !r.Outcome.Verified || r.Outcome.Objectives[0] != "error_rate: SATISFIED (value 0.002)" {
		t.Fatalf("outcome: %+v", r.Outcome)
	}
	// Timeline: sorted, bookkeeping dropped, only verdict changes kept.
	var kinds []string
	for i, e := range r.Timeline {
		if i > 0 && e.At.Before(r.Timeline[i-1].At) {
			t.Fatal("timeline not sorted")
		}
		kinds = append(kinds, e.Source+":"+e.Type)
	}
	want := "work:created,work:event_attached,outcome:verdict,work:stage_completed,action:proposed,action:dispatched,action:verified,outcome:verdict"
	if strings.Join(kinds, ",") != want {
		t.Fatalf("timeline:\n got %s\nwant %s", strings.Join(kinds, ","), want)
	}
	// 21 minutes to the first mitigation exceeds the 15 minute target.
	// h3 is "supported" by T4 evidence only: an ungrounded conclusion gap.
	if r.Scorecard == nil || len(r.Gaps) != 2 || !strings.Contains(r.Gaps[0], "response gap") || !strings.Contains(r.Gaps[1], "h3") ||
		len(r.FollowUps) != 2 || r.FollowUps[0].Title != "Shorten time to first action proposal" || r.FollowUps[1].Priority != "low" {
		t.Fatalf("gaps/follow-ups: %v %+v", r.Gaps, r.FollowUps)
	}
	md := Markdown(r)
	for _, s := range []string{"# Postmortem: checkout 5xx", "## Root cause\n\nrelease v2", "| deployment.rollback |", "### Ruled out", "[medium] **Shorten time to first action proposal**"} {
		if !strings.Contains(md, s) {
			t.Errorf("markdown misses %q:\n%s", s, md)
		}
	}
	// Deterministic: same ledgers, same report.
	r2, _ := (&Builder{Ledger: f, Eval: &workeval.Evaluator{Ledger: f}}).Build(context.Background(), "t", "w1")
	if Markdown(r2) != md {
		t.Fatal("report is not deterministic")
	}
}

func TestUnknownRootCauseAndUnresolvedEffects(t *testing.T) {
	f := incident()
	f.w.Status = work.StatusFailed
	f.hyps = []evidence.Hypothesis{{Key: "h", Statement: "network flap", Status: evidence.HypChallenged}}
	f.acts[0].State = action.StateEffectUnknown
	f.evals = []outcome.Evaluation{{ID: "e", Verdict: outcome.NotSatisfied, EvaluatedAt: t0,
		Objectives: []outcome.ObjectiveResult{{ID: "error_rate", Verdict: outcome.NotSatisfied}, {ID: "lat", Verdict: outcome.NotSatisfied, Optional: true}}}}
	r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if r.RootCause != nil || r.Outcome.Verified {
		t.Fatalf("root cause must be unknown and outcome unverified: %+v %+v", r.RootCause, r.Outcome)
	}
	titles := map[string]string{}
	for _, f := range r.FollowUps {
		titles[f.Title] = f.Priority
	}
	if titles["Close the diagnosis gap"] != "high" || titles["Restore objective error_rate"] != "high" ||
		titles["Reconcile deployment.rollback on deployment://prod/checkout"] != "high" || len(titles) != 3 {
		t.Fatalf("follow-ups: %+v", r.FollowUps)
	}
	if !strings.Contains(Markdown(r), "Unknown. No unique confirmed root-cause decision.") {
		t.Fatal("markdown must say the root cause is unknown")
	}
}

func TestRepeatedSignalsCollapse(t *testing.T) {
	f := incident()
	for i := 0; i < 30; i++ {
		f.evs = append(f.evs, ev(int64(100+i), "work.event_attached", time.Minute+time.Duration(i)*5*time.Second,
			map[string]any{"type": "alert.firing", "subject_ref": "deployment://prod/checkout"}))
	}
	r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range r.Timeline {
		if e.Type == "event_attached" {
			n++
			if e.Count != 31 || e.Until == nil {
				t.Fatalf("collapsed entry: %+v", e)
			}
		}
	}
	if n != 1 || !strings.Contains(Markdown(r), "(×31 until 10:03:25)") {
		t.Fatalf("signals: %d\n%s", n, Markdown(r))
	}
}

func TestBriefIsBounded(t *testing.T) {
	f := incident()
	for i := 0; i < 400; i++ {
		f.evs = append(f.evs, ev(int64(10+i), "work.stage_failed", time.Duration(i)*time.Second,
			map[string]any{"stage_id": "verify", "reason": fmt.Sprintf("timeout %d waiting for metrics %s", i, strings.Repeat("x", 300))}))
	}
	r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Truncated || len(r.Timeline) != MaxTimeline {
		t.Fatalf("timeline: %d truncated=%v", len(r.Timeline), r.Truncated)
	}
	b := Brief(r, work.MaxDescription)
	if len(b) > work.MaxDescription || !strings.Contains(b, "report truncated") {
		t.Fatalf("brief: %d bytes", len(b))
	}
	for _, e := range r.Timeline {
		if len(e.Text) > maxText+len("…") {
			t.Fatalf("unbounded text: %d", len(e.Text))
		}
	}
}

func TestBuiltinReviewTemplates(t *testing.T) {
	ids := map[string]work.Template{}
	for _, tp := range work.Builtin() {
		ids[tp.ID] = tp
	}
	pm, ok := ids[work.TemplatePostmortem]
	if !ok || pm.Stages[len(pm.Stages)-1].Type != work.StageConfirm {
		t.Fatalf("postmortem template: %+v", pm)
	}
	fu, ok := ids[work.TemplateFollowUp]
	if !ok {
		t.Fatal("follow-up template missing")
	}
	var types []string
	for _, s := range fu.Stages {
		types = append(types, string(s.Type))
	}
	if strings.Join(types, ",") != "run,action,wait,confirm" || fu.Stages[1].Action.Allowed[0] != "ticket.create" {
		t.Fatalf("follow-up stages: %v", types)
	}
}

func TestCorrelationCannotBecomeRootCause(t *testing.T) {
	f := incident()
	f.recs = []evidence.Record{{ID: "change", TrustTier: "T2", Quality: evidence.QualityValid, Relation: evidence.RelCorrelates}}
	f.hyps = []evidence.Hypothesis{{Key: "change", Statement: "deployment caused outage", Status: evidence.HypSupported, Supporting: []string{"change"}}}
	r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
	if err != nil {
		t.Fatal(err)
	}
	if r.RootCause != nil {
		t.Fatalf("correlation became certain root cause: %+v", r.RootCause)
	}
}

func testHypothesis(statement, category, verdict string, refs []string) evidence.Hypothesis {
	relation := evidence.RelSupports
	if verdict == evidence.DecisionConfirmed {
		relation = evidence.RelCausedBy
	}
	if verdict == evidence.DecisionInconclusive {
		relation = evidence.RelRefutes
	}
	var records []evidence.Record
	for _, ref := range refs {
		records = append(records, evidence.Record{ID: ref, TenantID: "t", WorkID: "w1", SourceRef: "obs://" + ref, ContentHash: ref, TrustTier: "T2", Quality: evidence.QualityValid, Relation: relation, CapturedAt: t0})
	}
	h := evidence.ResultHypothesis{Statement: statement, Category: category, Supports: refs}
	if relation == evidence.RelRefutes {
		h.Supports, h.Refutes = nil, refs
	}
	_, hypotheses, err := evidence.Adjudicate(evidence.DecisionScope{TenantID: "t", WorkID: "w1", Generation: 1}, []evidence.ResultEnvelope{{RunID: "r", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{h}}}, records, t0)
	if err != nil {
		panic(err)
	}
	return hypotheses[0]
}

func TestPostmortemConsumesCurrentDecisionOnly(t *testing.T) {
	for _, verdict := range []string{evidence.DecisionProbable, evidence.DecisionInconclusive, evidence.DecisionContested} {
		f := incident()
		f.hyps = f.hyps[:1]
		f.hyps[0].Decision.Verdict = verdict
		r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
		if err != nil || r.RootCause != nil {
			t.Fatalf("%s became certain: %+v %v", verdict, r.RootCause, err)
		}
	}
	f := incident()
	f.w.Generation++
	r, err := (&Builder{Ledger: f}).Build(context.Background(), "t", "w1")
	if err != nil || r.RootCause != nil {
		t.Fatalf("old generation accepted: %+v %v", r.RootCause, err)
	}
}
