package orchestration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

func rec(id, group, relation, quality string, at time.Time) evidence.Record {
	if relation == "" {
		relation = evidence.RelSupports
	}
	if group == "jaeger|q3|w" {
		relation = evidence.RelRefutes
	}
	return evidence.Record{TrustTier: "T2", ID: id, IndependenceGroup: group, Relation: relation, Quality: quality, CapturedAt: at, ContentHash: id}
}

func hyp(statement, status string, supports, refutes []string) evidence.ResultHypothesis {
	return evidence.ResultHypothesis{Statement: statement, Status: status, Supports: supports, Refutes: refutes}
}

// §11.2: citing evidence does not make it true. Unknown refs, later
// evidence and unusable evidence are rejected; the same observation cited
// by several agents counts once; correlation does not support a cause; and
// opposite verdicts are contested, not outvoted.
func TestSynthesizeValidatesFindings(t *testing.T) {
	now := time.Date(2026, 11, 10, 13, 0, 0, 0, time.UTC)
	e1, e2, e3, e4, e5, e6 := cpkit.NewID(), cpkit.NewID(), cpkit.NewID(), cpkit.NewID(), cpkit.NewID(), cpkit.NewID()
	ledger := []evidence.Record{
		rec(e1, "prom|q1|w", "", "valid", now.Add(-time.Minute)),
		rec(e2, "prom|q1|w", "", "valid", now.Add(-time.Minute)), // the same observation again
		rec(e3, "loki|q2|w", "", "stale", now.Add(-time.Minute)),
		rec(e4, "ledger|c|w", evidence.RelCorrelates, "valid", now.Add(-time.Minute)),
		rec(e5, "jaeger|q3|w", "", "valid", now.Add(-time.Minute)),
		rec(e6, "prom|q9|w", "", "valid", now.Add(time.Hour)), // not known at investigation time
	}
	ref := func(id string) string { return "evidence:" + id }
	const pool, deploy = "the connection pool is exhausted", "release v2.14 caused the latency"
	envs := []evidence.ResultEnvelope{
		{RunID: cpkit.NewID(), AgentRole: "metrics", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{
			hyp(pool, "SUPPORTED", []string{ref(e1), ref(e2), ref(e3), ref(e6), "evidence:" + cpkit.NewID(), "metric:made-up"}, nil)}},
		{RunID: cpkit.NewID(), AgentRole: "infra", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{
			hyp(pool, "SUPPORTED", []string{ref(e1)}, nil)}},
		{RunID: cpkit.NewID(), AgentRole: "traces", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{
			hyp(pool, "REFUTED", nil, []string{ref(e5)})}},
		{RunID: cpkit.NewID(), AgentRole: "changes", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{
			hyp(deploy, "VERIFIED", []string{ref(e4)}, nil), hyp("the cache is cold", "VERIFIED", nil, nil)}},
		{RunID: cpkit.NewID(), AgentRole: "gpu", Status: "FAILED", FailureReason: "assignment time budget exceeded"},
	}
	syn := Synthesize(envs, ledger, now)
	by := map[string]Finding{}
	for _, f := range syn.Findings {
		by[f.Role+"|"+f.Statement] = f
	}
	m := by["metrics|"+pool]
	if m.IndependentSupport != 1 || len(m.Supports) != 2 || len(m.Rejected) != 4 {
		t.Fatalf("two refs to one observation are one piece of evidence; four refs are unusable: %+v", m)
	}
	for _, role := range []string{"metrics", "infra", "traces"} {
		if f := by[role+"|"+pool]; f.Verdict != evidence.FindingContested {
			t.Errorf("%s: two agents for and one against is contested, not a majority: %s", role, f.Verdict)
		}
	}
	if len(syn.Contested) != 1 || len(syn.NextChecks) != 1 || !strings.Contains(syn.NextChecks[0], "only one side predicts") {
		t.Fatalf("contested: %v checks: %v", syn.Contested, syn.NextChecks)
	}
	if f := by["changes|"+deploy]; f.Verdict != evidence.FindingInsufficient || !strings.Contains(strings.Join(f.Gaps, " "), "only correlates") {
		t.Fatalf("a change that precedes a symptom is not its cause: %+v", f)
	}
	if f := by["changes|the cache is cold"]; f.Verdict != evidence.FindingInsufficient || f.Claimed != "verified" {
		t.Fatalf("a verdict without evidence is insufficient: %+v", f)
	}
	if syn.Unavailable["gpu"] == "" {
		t.Fatalf("a failed agent is a recorded gap: %v", syn.Unavailable)
	}
}

// A stage records findings, keeps the partial result of a timed-out agent
// as a gap, and cancels the run that exceeded its budget.
func TestStageRecordsFindingsAndEnforcesBudget(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.withTemplates(work.Template{ID: "triage", Version: 1, Stages: []work.Stage{
		{ID: "triage", Type: work.StageAgents, Agents: []work.AgentSpec{
			{Role: "metrics", RunSpec: work.RunSpec{Goal: "metrics of ${subjects}"}, Required: true},
			{Role: "slow", RunSpec: work.RunSpec{Goal: "deep dive", Budget: &work.AgentBudget{MaxDuration: cpkit.Duration(30 * time.Second)}}}}},
		{ID: "confirm", Type: work.StageConfirm}}})
	w := h.create("triage", nil)
	h.tick(1)
	stored, _, err := h.store.AddEvidence(h.ctx, evidence.Record{TenantID: "t1", WorkID: w.ID, SourceType: evidence.SourceMetric,
		SourceRef: "obs://prom/error_rate#abc", Summary: "error rate 9%", TrustTier: "T2", Relation: evidence.RelSupports, Quality: evidence.QualityValid, IndependenceGroup: "prom|abc|w"})
	if err != nil {
		t.Fatal(err)
	}
	// Only the metrics agent finishes; the slow one runs past its budget.
	h.runs.mu.Lock()
	for _, id := range h.runs.order {
		r := h.runs.runs[id]
		if strings.Contains(r.req.Goal, "metrics of") {
			r.state = RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: []byte(`{"answer":"errors spiked",
				"hypotheses":[{"statement":"the pool is exhausted","status":"SUPPORTED","supports":["evidence:` + stored.ID + `"]}]}`)}
		}
	}
	h.runs.mu.Unlock()
	h.clock.Add(time.Minute)
	h.tick(3)
	got, _ := h.store.GetWork(h.ctx, "t1", w.ID)
	if st, _ := got.CurrentStage(); st.ID != "confirm" {
		t.Fatalf("the stage must complete with the partial result: stage=%s status=%s reason=%q", st.ID, got.Status, got.Reason)
	}
	if len(h.runs.cancelled) != 1 {
		t.Fatalf("the over-budget run must be cancelled: %v", h.runs.cancelled)
	}
	fs, err := h.store.ListFindings(h.ctx, "t1", w.ID, 10)
	if err != nil || len(fs) != 1 || fs[0].Verdict != evidence.FindingSupported || fs[0].Role != "metrics" || fs[0].IndependentSupport != 1 {
		t.Fatalf("findings: %+v %v", fs, err)
	}
}

func TestStageDecisionDrivesHypothesisReportAndScore(t *testing.T) {
	for _, tc := range []struct {
		name, quality, relation, verdict string
		grounded                         int
		root                             bool
	}{
		{"correlation", evidence.QualityValid, evidence.RelCorrelates, evidence.DecisionInconclusive, 0, false},
		{"stale", evidence.QualityStale, evidence.RelCausedBy, evidence.DecisionInconclusive, 0, false},
		{"causal", evidence.QualityValid, evidence.RelCausedBy, evidence.DecisionConfirmed, 1, true},
		{"probable", evidence.QualityValid, evidence.RelSupports, evidence.DecisionProbable, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, domain.RiskR1)
			reg := obs.NewRegistry()
			h.orch.Metrics = NewMetrics(reg, nil)
			h.withTemplates(work.Template{ID: "triage", Version: 1, Stages: []work.Stage{
				{ID: "triage", Type: work.StageRun, Run: &work.RunSpec{Goal: "diagnose"}},
				{ID: "confirm", Type: work.StageConfirm}}})
			w := h.create("triage", nil)
			h.tick(1)
			rec, _, err := h.store.AddEvidence(h.ctx, evidence.Record{TenantID: "t1", WorkID: w.ID, SourceType: evidence.SourceChange, SourceRef: "change://version", TrustTier: "T2", Quality: tc.quality, Relation: tc.relation})
			if err != nil {
				t.Fatal(err)
			}
			h.runs.mu.Lock()
			for _, r := range h.runs.runs {
				r.state = RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: []byte(`{"hypotheses":[{"statement":"release caused outage","category":"root_cause","status":"VERIFIED","supports":["` + rec.SourceRef + `"]}]}`)}
			}
			h.runs.mu.Unlock()
			h.clock.Add(time.Minute)
			h.tick(3)
			hyps, err := h.store.ListHypotheses(h.ctx, "t1", w.ID)
			if err != nil || len(hyps) != 1 || hyps[0].Decision == nil || hyps[0].Decision.Verdict != tc.verdict {
				t.Fatalf("hypotheses: %+v %v", hyps, err)
			}
			report, err := (&postmortem.Builder{Ledger: h.store}).Build(h.ctx, "t1", w.ID)
			if err != nil || (report.RootCause != nil) != tc.root {
				t.Fatalf("report: %+v %v", report, err)
			}
			score, err := (&workeval.Evaluator{Ledger: h.store}).Score(h.ctx, "t1", w.ID)
			if err != nil || score.GroundedConclusions != tc.grounded {
				t.Fatalf("score: %+v %v", score, err)
			}
			if tc.grounded == 0 && hyps[0].Status == evidence.HypSupported {
				t.Fatal("raw citation promoted hypothesis")
			}
			var metrics strings.Builder
			reg.Write(h.ctx, &metrics)
			verdict := evidence.FindingSupported
			if tc.grounded == 0 {
				verdict = evidence.FindingInsufficient
			}
			if !strings.Contains(metrics.String(), fmt.Sprintf("harness_agent_finding_total{verdict=%q} 1", verdict)) {
				t.Fatalf("committed finding not observed: %s", metrics.String())
			}
		})
	}
}

func TestOversizedDecisionBlocksWithoutRepeatingStage(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.withTemplates(work.Template{ID: "triage", Version: 1, Stages: []work.Stage{
		{ID: "triage", Type: work.StageRun, Run: &work.RunSpec{Goal: "diagnose"}},
		{ID: "confirm", Type: work.StageConfirm}}})
	w := h.create("triage", nil)
	h.tick(1)
	var refs []string
	for i := 0; i < 25; i++ {
		r, _, err := h.store.AddEvidence(h.ctx, evidence.Record{TenantID: "t1", WorkID: w.ID, SourceType: evidence.SourceMetric, SourceRef: fmt.Sprintf("obs://metric/%d", i), Summary: strings.Repeat("x", 2000), TrustTier: "T2", Quality: evidence.QualityValid, Relation: evidence.RelCausedBy})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, "evidence:"+r.ID)
	}
	h.runs.finishAll(map[string]any{"hypotheses": []evidence.ResultHypothesis{{Statement: "cause", Status: "VERIFIED", Supports: refs}}}, true)
	h.clock.Add(time.Minute)
	h.tick(3)
	got, err := h.store.GetWork(h.ctx, "t1", w.ID)
	if err != nil || got.Status != work.StatusBlocked || !strings.Contains(got.Reason, "48 KiB") || got.WakeAt != nil {
		t.Fatalf("oversized decision must block: %+v %v", got, err)
	}
	hyps, err := h.store.ListHypotheses(h.ctx, "t1", w.ID)
	if err != nil || len(hyps) != 0 {
		t.Fatalf("partially persisted decision: %+v %v", hyps, err)
	}
	h.clock.Add(time.Hour)
	h.tick(2)
	after, err := h.store.GetWork(h.ctx, "t1", w.ID)
	if err != nil || after.Version != got.Version || len(h.runs.order) != 1 {
		t.Fatalf("blocked stage repeated: %+v %v", after, err)
	}
}

func TestReadjudicationRetainsUnsupportedClaim(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.withTemplates(work.Template{ID: "triage", Version: 1, Stages: []work.Stage{
		{ID: "triage", Type: work.StageRun, Run: &work.RunSpec{Goal: "diagnose"}},
		{ID: "followup", Type: work.StageRun, Run: &work.RunSpec{Goal: "check alternatives"}},
		{ID: "confirm", Type: work.StageConfirm}}})
	w := h.create("triage", nil)
	h.tick(1)
	h.runs.finishAll(map[string]any{"hypotheses": []evidence.ResultHypothesis{{Statement: "unsupported cause", Status: "VERIFIED"}}}, true)
	h.clock.Add(time.Minute)
	h.tick(3)
	if len(h.runs.order) != 2 {
		t.Fatalf("follow-up run not started: %v", h.runs.order)
	}
	h.runs.finishAll(map[string]any{"hypotheses": []evidence.ResultHypothesis{{Statement: "alternative", Status: "OPEN"}}}, true)
	h.clock.Add(time.Minute)
	h.tick(3)
	hyps, err := h.store.ListHypotheses(h.ctx, "t1", w.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, hyp := range hyps {
		if hyp.Key == evidence.HypothesisKey("unsupported cause") {
			found = true
			if hyp.Decision == nil || !hyp.Decision.ClaimedSupport || hyp.Decision.Verdict != evidence.DecisionInconclusive {
				t.Fatalf("prior claim erased: %+v", hyp.Decision)
			}
		}
	}
	score, err := (&workeval.Evaluator{Ledger: h.store}).Score(h.ctx, "t1", w.ID)
	if !found || err != nil || len(score.UngroundedRefs) != 1 {
		t.Fatalf("unsupported claim no longer gated: %+v %v", score, err)
	}
}
