package incident

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

var (
	testNow = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	testAt  = testNow.Add(-time.Hour)
)

type fakeStore struct {
	children     []work.Work
	outcomes     map[string][]outcome.Evaluation
	hypotheses   map[string][]evidence.Hypothesis
	ledger       map[string][]evidence.Record
	postmortems  []work.Work
	subjects     map[string][]work.SubjectLink
	secondary    []work.SecondaryIncident
	err          error
	subjectsErr  error
	secondaryErr error
}

func (f *fakeStore) Children(context.Context, string, string) ([]work.Work, error) {
	return f.children, f.err
}
func (f *fakeStore) ListOutcomes(_ context.Context, _, id string, _ int) ([]outcome.Evaluation, error) {
	return f.outcomes[id], f.err
}
func (f *fakeStore) ListHypotheses(_ context.Context, _, id string) ([]evidence.Hypothesis, error) {
	return f.hypotheses[id], f.err
}
func (f *fakeStore) ListEvidence(_ context.Context, _, id string, _ int) ([]evidence.Record, error) {
	return f.ledger[id], f.err
}
func (f *fakeStore) PostmortemsFor(context.Context, string, string) ([]work.Work, error) {
	return f.postmortems, f.err
}
func (f *fakeStore) ListSubjects(_ context.Context, _, id string) ([]work.SubjectLink, error) {
	return f.subjects[id], f.subjectsErr
}
func (f *fakeStore) SecondaryIncidentsFor(context.Context, string, string) ([]work.SecondaryIncident, error) {
	return f.secondary, f.secondaryErr
}

func incidentWork() work.Work {
	spec := outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState,
		State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}
	if err := spec.Normalize(); err != nil {
		panic(err)
	}
	return work.Work{ID: "inc", TenantID: "tenant", Kind: work.KindIncident, Status: work.StatusActive,
		Generation: 2, Goal: work.GoalSpec{Success: &spec}}
}

func childWork(id, role string, status work.Status) work.Work {
	w := incidentWork()
	w.ID, w.ParentID, w.Status = id, "inc", status
	w.Goal.Labels = map[string]string{"child_key": role}
	return w
}

func evaluation(w work.Work, verdict, raw outcome.Verdict) outcome.Evaluation {
	return outcome.Evaluation{ID: "eval-" + string(verdict), TenantID: w.TenantID, WorkID: w.ID,
		Generation: w.Generation, SpecHash: w.Goal.Success.Hash(), Verdict: verdict, RawVerdict: raw, EvaluatedAt: testNow}
}

func projectForTest(t *testing.T, w work.Work, store *fakeStore) Projection {
	t.Helper()
	got, err := Project(context.Background(), store, w, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRecoveryUsesOnlyCurrentMachineOutcome(t *testing.T) {
	for _, tc := range []struct {
		name     string
		children []work.Work
		outcome  *outcome.Evaluation
		want     RecoveryStatus
	}{
		{name: "impact active", want: RecoveryImpactActive},
		{name: "mitigating", children: []work.Work{childWork("mit", "mitigation", work.StatusActive)}, want: RecoveryMitigating},
		{name: "stabilizing", want: RecoveryStabilizing},
		{name: "recovered", want: RecoveryRecovered},
		{name: "impact stopped by mitigation", children: []work.Work{childWork("mit", "mitigation", work.StatusSucceeded)}, want: RecoveryImpactStopped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := incidentWork()
			store := &fakeStore{children: tc.children, outcomes: map[string][]outcome.Evaluation{}}
			switch tc.want {
			case RecoveryStabilizing:
				ev := evaluation(w, outcome.Inconclusive, outcome.Satisfied)
				store.outcomes[w.ID] = []outcome.Evaluation{ev}
			case RecoveryRecovered:
				ev := evaluation(w, outcome.Satisfied, outcome.Satisfied)
				store.outcomes[w.ID] = []outcome.Evaluation{ev}
			case RecoveryImpactStopped:
				ev := evaluation(tc.children[0], outcome.Satisfied, outcome.Satisfied)
				store.outcomes[tc.children[0].ID] = []outcome.Evaluation{ev}
			}
			got := projectForTest(t, w, store)
			if got.Recovery.Status != tc.want {
				t.Fatalf("recovery=%s want %s", got.Recovery.Status, tc.want)
			}
		})
	}
}

func TestRecoveryReboundAndStaleEvidence(t *testing.T) {
	w := incidentWork()
	current := evaluation(w, outcome.Satisfied, outcome.Satisfied)
	rebound := evaluation(w, outcome.NotSatisfied, outcome.NotSatisfied)
	store := &fakeStore{outcomes: map[string][]outcome.Evaluation{w.ID: {rebound, current}}}
	if got := projectForTest(t, w, store); got.Recovery.Status != RecoveryImpactActive {
		t.Fatalf("old success hid rebound: %+v", got.Recovery)
	}

	stale := current
	stale.Generation--
	store.outcomes[w.ID] = []outcome.Evaluation{stale}
	if got := projectForTest(t, w, store); got.Recovery.Status != RecoveryImpactActive || got.Recovery.Outcome != nil {
		t.Fatalf("old generation used: %+v", got.Recovery)
	}
	stale = current
	stale.SpecHash = "old-spec"
	store.outcomes[w.ID] = []outcome.Evaluation{stale}
	if got := projectForTest(t, w, store); got.Recovery.Status != RecoveryImpactActive {
		t.Fatalf("old success spec used: %+v", got.Recovery)
	}
}

func adjudicatedHypothesis(t *testing.T, w work.Work, statement, relation string, refuted bool) (evidence.Hypothesis, []evidence.Record) {
	t.Helper()
	scope := evidence.DecisionScope{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation}
	ledger := []evidence.Record{{ID: "support", TenantID: w.TenantID, WorkID: w.ID, SourceRef: "obs://support",
		ContentHash: "support", CapturedAt: testAt, TrustTier: "T2", Quality: evidence.QualityValid, Relation: relation}}
	h := evidence.ResultHypothesis{Statement: statement, Category: evidence.CatRootCause, Status: "VERIFIED", Supports: []string{"evidence:support"}}
	if refuted {
		ledger = append(ledger, evidence.Record{ID: "refute", TenantID: w.TenantID, WorkID: w.ID, SourceRef: "obs://refute",
			ContentHash: "refute", CapturedAt: testAt, TrustTier: "T2", Quality: evidence.QualityValid, Relation: evidence.RelRefutes})
		h.Refutes = []string{"evidence:refute"}
	}
	_, hs, err := evidence.Adjudicate(scope, []evidence.ResultEnvelope{{RunID: "run", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{h}}}, ledger, testAt)
	if err != nil || len(hs) != 1 {
		t.Fatalf("adjudicate: %+v %v", hs, err)
	}
	return hs[0], ledger
}

func TestRCAAggregatesCurrentRootCauseDecisions(t *testing.T) {
	rca := childWork("rca", "rca", work.StatusActive)
	confirmed, ledger := adjudicatedHypothesis(t, rca, "bad deploy", evidence.RelCausedBy, false)
	probable, probableLedger := adjudicatedHypothesis(t, rca, "bad deploy", evidence.RelSupports, false)
	contested, contestedLedger := adjudicatedHypothesis(t, rca, "bad deploy", evidence.RelCausedBy, true)

	for _, tc := range []struct {
		name       string
		children   []work.Work
		hypothesis *evidence.Hypothesis
		ledger     []evidence.Record
		want       RCAStatus
		known      bool
	}{
		{name: "not started", want: RCANotStarted},
		{name: "investigating", children: []work.Work{rca}, want: RCAInvestigating},
		{name: "confirmed", children: []work.Work{rca}, hypothesis: &confirmed, ledger: ledger, want: RCAConfirmed, known: true},
		{name: "probable", children: []work.Work{rca}, hypothesis: &probable, ledger: probableLedger, want: RCAProbable},
		{name: "contested", children: []work.Work{rca}, hypothesis: &contested, ledger: contestedLedger, want: RCAContested},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{children: tc.children, hypotheses: map[string][]evidence.Hypothesis{}, ledger: map[string][]evidence.Record{}}
			if tc.hypothesis != nil {
				store.hypotheses[rca.ID] = []evidence.Hypothesis{*tc.hypothesis}
				store.ledger[rca.ID] = tc.ledger
			}
			got := projectForTest(t, incidentWork(), store)
			if got.RCA.Status != tc.want || got.RCA.RootCauseKnown != tc.known {
				t.Fatalf("RCA=%+v want %s known=%v", got.RCA, tc.want, tc.known)
			}
		})
	}
}

func TestRCAConfirmedIgnoresOpenAlternativesButNotCompetingCause(t *testing.T) {
	rca := childWork("rca", "rca", work.StatusActive)
	confirmed, ledger := adjudicatedHypothesis(t, rca, "bad deploy", evidence.RelCausedBy, false)
	open := evidence.Hypothesis{TenantID: rca.TenantID, WorkID: rca.ID, Key: "open", Statement: "untested capacity", Category: evidence.CatRootCause}
	store := &fakeStore{children: []work.Work{rca}, hypotheses: map[string][]evidence.Hypothesis{rca.ID: {confirmed, open}}, ledger: map[string][]evidence.Record{rca.ID: ledger}}
	got := projectForTest(t, incidentWork(), store)
	if got.RCA.Status != RCAConfirmed || got.RCA.OpenHypotheses != 1 {
		t.Fatalf("open alternative hid confirmed cause: %+v", got.RCA)
	}

	other := childWork("rca2", "rca", work.StatusSucceeded)
	second, secondLedger := adjudicatedHypothesis(t, other, "database failover", evidence.RelCausedBy, false)
	store.children = append(store.children, other)
	store.hypotheses[other.ID] = []evidence.Hypothesis{second}
	store.ledger[other.ID] = secondLedger
	got = projectForTest(t, incidentWork(), store)
	if got.RCA.Status != RCAContested {
		t.Fatalf("competing causes not contested: %+v", got.RCA)
	}
}

func TestRCARejectsDecisionInvalidatedByCurrentLedger(t *testing.T) {
	rca := childWork("rca", "rca", work.StatusActive)
	h, ledger := adjudicatedHypothesis(t, rca, "bad deploy", evidence.RelCausedBy, false)
	ledger = append(ledger, evidence.Record{ID: "new", TenantID: rca.TenantID, WorkID: rca.ID, SourceRef: "obs://new",
		ContentHash: "new", CapturedAt: testAt.Add(time.Minute), TrustTier: "T2", Quality: evidence.QualityValid, Relation: evidence.RelRefutes,
		Supersedes: "support"})
	store := &fakeStore{children: []work.Work{rca}, hypotheses: map[string][]evidence.Hypothesis{rca.ID: {h}}, ledger: map[string][]evidence.Record{rca.ID: ledger}}
	got := projectForTest(t, incidentWork(), store)
	if got.RCA.Status != RCAInconclusive || got.RCA.RootCauseKnown || len(got.RCA.Decisions) != 0 {
		t.Fatalf("obsolete decision accepted: %+v", got.RCA)
	}
}

func TestRCATerminalWithoutConclusionIsExplicitlyUnknown(t *testing.T) {
	rca := childWork("rca", "rca", work.StatusSucceeded)
	got := projectForTest(t, incidentWork(), &fakeStore{children: []work.Work{rca}})
	if got.RCA.Status != RCAInconclusive || got.RCA.RootCauseKnown {
		t.Fatalf("unknown root cause not explicit: %+v", got.RCA)
	}
}

func TestProjectPropagatesStoreFailure(t *testing.T) {
	want := errors.New("ledger unavailable")
	_, err := Project(context.Background(), &fakeStore{err: want}, incidentWork(), testNow)
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
}

func TestProjectRejectsNonIncident(t *testing.T) {
	w := incidentWork()
	w.Kind = work.KindRoutine
	_, err := Project(context.Background(), &fakeStore{}, w, testNow)
	if !errors.Is(err, work.ErrInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func TestProjectionDoesNotMutateGoal(t *testing.T) {
	w := incidentWork()
	before := cpkit.MustHash(w.Goal)
	_ = projectForTest(t, w, &fakeStore{})
	if after := cpkit.MustHash(w.Goal); after != before {
		t.Fatal("projection mutated incident goal")
	}
}

func postmortemWork(id, source string, status work.Status, stage int) work.Work {
	return work.Work{ID: id, TenantID: "tenant", Kind: work.KindRequest, Status: status, Stage: stage,
		Goal: work.GoalSpec{Labels: map[string]string{"source_work": source}}, Template: work.Template{ID: work.TemplatePostmortem,
			Stages: []work.Stage{{ID: "draft", Type: work.StageRun}, {ID: "review", Type: work.StageConfirm}}}}
}

func TestPostmortemRequiresHumanReviewToPublish(t *testing.T) {
	for _, tc := range []struct {
		name string
		work *work.Work
		want PostmortemStatus
	}{
		{name: "not started", want: PostmortemNotStarted},
		{name: "drafting", work: func() *work.Work { w := postmortemWork("pm", "inc", work.StatusActive, 0); return &w }(), want: PostmortemDrafting},
		{name: "review required", work: func() *work.Work { w := postmortemWork("pm", "inc", work.StatusWaiting, 1); return &w }(), want: PostmortemReviewRequired},
		{name: "published", work: func() *work.Work { w := postmortemWork("pm", "inc", work.StatusSucceeded, 2); return &w }(), want: PostmortemPublished},
		{name: "failed after draft is not published", work: func() *work.Work { w := postmortemWork("pm", "inc", work.StatusFailed, 2); return &w }(), want: PostmortemDrafting},
		{name: "success without review is not published", work: func() *work.Work { w := postmortemWork("pm", "inc", work.StatusSucceeded, 0); return &w }(), want: PostmortemDrafting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{}
			if tc.work != nil {
				store.postmortems = []work.Work{*tc.work}
			}
			got := projectForTest(t, incidentWork(), store)
			if got.Postmortem.Status != tc.want {
				t.Fatalf("postmortem=%+v want %s", got.Postmortem, tc.want)
			}
		})
	}
}

func TestPostmortemUsesNewestMatchingWork(t *testing.T) {
	old := postmortemWork("old", "inc", work.StatusSucceeded, 2)
	newer := postmortemWork("new", "inc", work.StatusWaiting, 1)
	other := postmortemWork("other", "another", work.StatusSucceeded, 2)
	got := projectForTest(t, incidentWork(), &fakeStore{postmortems: []work.Work{newer, old, other}})
	if got.Postmortem.Status != PostmortemReviewRequired || got.Postmortem.WorkID != "new" || got.Postmortem.Stage != "review" {
		t.Fatalf("wrong postmortem selected: %+v", got.Postmortem)
	}
}

func TestPostmortemOverflowFailsClosed(t *testing.T) {
	items := make([]work.Work, 201)
	for i := range items {
		items[i] = postmortemWork("pm", "inc", work.StatusActive, 0)
	}
	_, err := Project(context.Background(), &fakeStore{postmortems: items}, incidentWork(), testNow)
	if err == nil {
		t.Fatal("truncated postmortem set projected as complete")
	}
}
