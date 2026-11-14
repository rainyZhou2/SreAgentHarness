package workbench

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

// benchDir is the local benchmark suite; it is not checked in, so tests
// that replay it skip when it is absent.
func benchDir(t *testing.T) string {
	t.Helper()
	const dir = "../../bench/cases"
	if _, err := os.Stat(dir); err != nil {
		t.Skip("bench/cases is not present (kept local, not in the repository)")
	}
	return dir
}

func TestShippedSuite(t *testing.T) {
	cases, err := LoadCases(benchDir(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) < 4 {
		t.Fatalf("cases: %d", len(cases))
	}
	r := &Runner{Dir: t.TempDir()}
	for _, c := range cases {
		res, err := r.Run(context.Background(), c)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		t.Logf("%s: status=%s reason=%q actions=%v gates=%v unused=%v ticks=%d sim=%.0fs root=%q events=%v",
			c.Name, res.Status, res.Reason, res.Actions, res.Scorecard.FailedGates, res.Unused, res.Ticks, res.SimulatedSeconds, res.RootCause, res.Events)
		if !res.Passed {
			t.Errorf("%s failed: %v", c.Name, res.Failures)
		}
	}
}

func load(t *testing.T, name string) Case {
	t.Helper()
	cs, err := LoadCases(benchDir(t) + "/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return cs[0]
}

func TestDeterministic(t *testing.T) {
	c := load(t, "01-rollback-verified.json")
	r := &Runner{}
	a, err := r.Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	ja, jb := string(MarshalIndent(a)), string(MarshalIndent(b))
	// Work ids are random; everything else must match.
	ja = strings.ReplaceAll(ja, a.WorkID, "W")
	jb = strings.ReplaceAll(jb, b.WorkID, "W")
	if ja != jb {
		t.Fatalf("runs differ:\n%s\n---\n%s", ja, jb)
	}
	if a.Scorecard.ToolCalls != 9 || a.Scorecard.CostUSD != 0.31 || a.Scorecard.Runs != 1 {
		t.Fatalf("scripted cost/tool calls: %+v", a.Scorecard)
	}
}

func TestUnknownEffectIsNeverReplayed(t *testing.T) {
	c := load(t, "02-unknown-effect.json")
	res, err := (&Runner{}).Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Actions) != 1 || res.Actions[0] != "deployment.rollback:EFFECT_UNKNOWN" || res.Scorecard.UnresolvedEffects != 1 ||
		res.Scorecard.RepeatedUnknownEffects != 0 || !res.HorizonReached || !res.Passed {
		t.Fatalf("%+v", res)
	}
}

func TestExpectationFailuresAreReported(t *testing.T) {
	c := load(t, "01-rollback-verified.json")
	c.Approvals = nil // the R2 rollback is never approved
	c.Horizon = cpkit.Duration(20 * time.Minute)
	res, err := (&Runner{}).Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if res.Passed || res.Status != work.StatusWaiting || len(res.Failures) < 2 {
		t.Fatalf("want failures, got %+v", res)
	}
	joined := strings.Join(res.Failures, "\n")
	for _, want := range []string{"status WAITING, want SUCCEEDED", "did not take effect", "horizon reached"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing %q in\n%s", want, joined)
		}
	}
}

func TestSuiteAndBaselineGate(t *testing.T) {
	cases, err := LoadCases(benchDir(t))
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{}
	s, err := r.RunSuite(context.Background(), cases, nil)
	if err != nil || !s.OK() || s.Summary.Works != len(cases) {
		t.Fatalf("suite: %+v %v", s, err)
	}
	// A baseline that was better on every dimension flags regressions.
	better := s.Summary
	better.SuccessRate, better.Grounding = 1, 1
	better.Passed = better.Works
	if s2, _ := r.RunSuite(context.Background(), cases, &better); s2.OK() || len(s2.Regressions) == 0 {
		t.Fatalf("baseline gate: %+v", s2.Regressions)
	}
}

func TestRecordAndReplay(t *testing.T) {
	dir := t.TempDir()
	c := load(t, "01-rollback-verified.json")
	orig, err := (&Runner{Dir: dir}).Run(context.Background(), c)
	if err != nil || !orig.Passed {
		t.Fatalf("original: %+v %v", orig, err)
	}
	src, err := cpsql.OpenSQLite(filepath.Join(dir, safeName(c.Name), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	rec, err := Record(context.Background(), src, c.Tenant, orig.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Runs) != 1 || len(rec.Actions) != 1 || rec.Actions[0].Risk != "R2" || len(rec.Approvals) != 1 ||
		len(rec.Metrics) != 1 || len(rec.Evidence) != 2 || rec.Expect.Status != work.StatusSucceeded {
		t.Fatalf("recorded case: %s", MarshalIndent(rec))
	}
	if !strings.Contains(string(rec.Runs[0].Result), "deployment.rollback") || !strings.Contains(string(rec.Runs[0].Result), "deploy v2") {
		t.Fatalf("run result not rebuilt: %s", rec.Runs[0].Result)
	}
	// The recorded case round-trips through JSON.
	if _, err := ParseCases(MarshalIndent(rec)); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	cmp, err := Replay(context.Background(), &Runner{}, src, &workeval.Evaluator{Ledger: src}, c.Tenant, orig.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Match {
		t.Fatalf("replay diverged: %v failures=%v\n%s", cmp.Diffs, cmp.Replay.Failures, MarshalIndent(cmp.Replay))
	}
	// A changed decision is a diff.
	worse := cmp.Original
	worse.ActionsApplied, worse.Pass = 0, false
	if d := Diff(worse, cmp.Replay.Scorecard); len(d) != 2 {
		t.Fatalf("diff: %v", d)
	}
}

func TestCaseValidation(t *testing.T) {
	bad := []string{
		`{"work":{"kind":"incident","goal":{"intent":"x","subject_refs":["service://a/b"]}}}`,
		`{"name":"x"}`,
		`{"name":"x","work":{"kind":"incident","goal":{}},"actions":[{"name":"a","risk":"R3"}]}`,
		`{"name":"x","work":{"kind":"incident","goal":{}},"runs":[{"status":"failed"}]}`,
		`{"name":"x","work":{"kind":"incident","goal":{}},"typo":1}`,
		`{"name":"x","work":{"kind":"incident","goal":{}},"horizon":"720h","tick":"1s"}`,
	}
	for _, b := range bad {
		if _, err := ParseCases([]byte(b)); err == nil {
			t.Errorf("accepted %s", b)
		}
	}
}

func TestCausalEvidenceRecordReplay(t *testing.T) {
	// This fixture is a controlled isolated experiment, unlike the temporal
	// correlations in the mitigation cases. Only the tested version changes.
	cs, err := ParseCases([]byte(`{
	  "name":"controlled-version-reproduction",
	  "templates":[{"id":"causal","stages":[{"id":"investigate","type":"run","run":{"goal":"diagnose"}},{"id":"confirm","type":"confirm"}]}],
	  "work":{"kind":"incident","template":"causal","goal":{"intent":"explain regression","subject_refs":["service://test/checkout"]}},
	  "evidence":[{"source_type":"tool","source_ref":"experiment://checkout/controlled-v1-v2-v1","trust_tier":"T2",
	    "quality":"valid","relation":"caused_by","kind":"observation","independence_group":"controlled-run-7",
	    "summary":"Identical isolated requests and dependencies: v1 passes, v2 reproducibly panics, reversion to v1 passes."}],
	  "runs":[{"stage":"investigate","duration":"1m","result":{"hypotheses":[{"statement":"checkout v2 introduced a panic","category":"root_cause","status":"VERIFIED","supports":["experiment://checkout/controlled-v1-v2-v1"]}]}}],
	  "human":[{"stage":"confirm","command":"confirm","payload":{"verdict":"satisfied"}}],
	  "expect":{"status":"SUCCEEDED","pass":true,"root_cause":"checkout v2"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	c, dir := cs[0], t.TempDir()
	original, err := (&Runner{Dir: dir}).Run(context.Background(), c)
	if err != nil || !original.Passed {
		t.Fatalf("causal case: %+v %v", original, err)
	}
	src, err := cpsql.OpenSQLite(filepath.Join(dir, safeName(c.Name), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	recorded, err := Record(context.Background(), src, c.Tenant, original.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	var exported []map[string]any
	if err := json.Unmarshal(MarshalIndent(recorded.Evidence), &exported); err != nil {
		t.Fatal(err)
	}
	if len(exported) != 1 || exported[0]["quality"] != evidence.QualityValid || exported[0]["relation"] != evidence.RelCausedBy || exported[0]["kind"] != evidence.KindObservation || exported[0]["independence_group"] != "controlled-run-7" {
		t.Fatalf("evidence dimensions lost: %s", MarshalIndent(recorded.Evidence))
	}
	if len(recorded.Runs) != 1 || !strings.Contains(string(recorded.Runs[0].Result), c.Evidence[0].SourceRef) || strings.Contains(string(recorded.Runs[0].Result), "evidence:") {
		t.Fatalf("run references were not rebound to stable sources: %s", MarshalIndent(recorded.Runs))
	}
	cmp, err := Replay(context.Background(), &Runner{}, src, &workeval.Evaluator{Ledger: src}, c.Tenant, original.WorkID)
	if err != nil || !cmp.Match || cmp.Replay.RootCause != original.RootCause || cmp.Replay.Scorecard.GroundedConclusions != 1 {
		t.Fatalf("causal replay differs: %+v %v", cmp, err)
	}
	links, err := src.ListRunLinks(context.Background(), c.Tenant, original.WorkID)
	if err != nil || len(links) != 1 {
		t.Fatalf("run link: %+v %v", links, err)
	}
	if _, _, err := src.AddEvidence(context.Background(), evidence.Record{TenantID: c.Tenant, WorkID: original.WorkID, RunID: links[0].RunID,
		SourceType: evidence.SourceTool, SourceRef: "experiment://checkout/tool-observation", TrustTier: "T2", Quality: evidence.QualityValid,
		Relation: evidence.RelSupports, Kind: evidence.KindObservation, Summary: "Tool observation collected by the investigation run"}); err != nil {
		t.Fatal(err)
	}
	recorded, err = Record(context.Background(), src, c.Tenant, original.WorkID)
	if err != nil || len(recorded.Evidence) != 2 {
		t.Fatalf("run-owned tool evidence lost: %+v %v", recorded.Evidence, err)
	}
}

func TestReplayPreservesRejectedClaim(t *testing.T) {
	c, dir := load(t, "04-ungrounded-conclusion.json"), t.TempDir()
	original, err := (&Runner{Dir: dir}).Run(context.Background(), c)
	if err != nil || !original.Passed || original.Scorecard.Pass {
		t.Fatalf("ungrounded fixture did not exercise gate: %+v %v", original, err)
	}
	src, err := cpsql.OpenSQLite(filepath.Join(dir, safeName(c.Name), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	cmp, err := Replay(context.Background(), &Runner{}, src, &workeval.Evaluator{Ledger: src}, c.Tenant, original.WorkID)
	if err != nil || !cmp.Match || cmp.Replay.Scorecard.Pass {
		t.Fatalf("replay erased the rejected claim: %+v %v", cmp, err)
	}
}

func TestReplayPreservesSupersession(t *testing.T) {
	c, dir := load(t, "009_controlled_version_reproduction.json"), t.TempDir()
	var correction Evidence
	if err := json.Unmarshal([]byte(`{"source_type":"tool","source_ref":"experiment://checkout/correction","trust_tier":"T2","quality":"valid","relation":"refutes","kind":"observation","supersedes":"experiment://checkout/controlled-v1-v2-v1","summary":"The original run used mismatched dependencies; corrected reproduction does not reproduce the failure."}`), &correction); err != nil {
		t.Fatal(err)
	}
	c.Evidence = append(c.Evidence, correction)
	c.Expect.RootCause = "unknown"
	pass := false
	c.Expect.Pass, c.Expect.Gates = &pass, []string{workeval.GateUngrounded}
	original, err := (&Runner{Dir: dir}).Run(context.Background(), c)
	if err != nil || !original.Passed {
		t.Fatalf("superseded evidence counted: %+v %v", original, err)
	}
	src, err := cpsql.OpenSQLite(filepath.Join(dir, safeName(c.Name), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	cmp, err := Replay(context.Background(), &Runner{}, src, &workeval.Evaluator{Ledger: src}, c.Tenant, original.WorkID)
	if err != nil || !cmp.Match || cmp.Replay.RootCause != "" {
		t.Fatalf("replay revived superseded evidence: %+v %v", cmp, err)
	}
}

func TestReplayPreservesCanonicalRefWithAmbiguousSource(t *testing.T) {
	c, dir := load(t, "009_controlled_version_reproduction.json"), t.TempDir()
	original, err := (&Runner{Dir: dir}).Run(context.Background(), c)
	if err != nil || !original.Passed {
		t.Fatalf("original: %+v %v", original, err)
	}
	src, err := cpsql.OpenSQLite(filepath.Join(dir, safeName(c.Name), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	ctx := context.Background()
	ledger, err := src.ListEvidence(ctx, c.Tenant, original.WorkID, 100)
	if err != nil {
		t.Fatal(err)
	}
	var cause evidence.Record
	for _, r := range ledger {
		if r.SourceRef == c.Evidence[0].SourceRef {
			cause = r
		}
	}
	copy := cause
	copy.ID, copy.ContentHash, copy.Summary = "", "another-observation", "Another observation shares the source address"
	copy.IndependenceGroup = "other"
	if _, _, err := src.AddEvidence(ctx, copy); err != nil {
		t.Fatal(err)
	}
	ledger, err = src.ListEvidence(ctx, c.Tenant, original.WorkID, 100)
	if err != nil {
		t.Fatal(err)
	}
	w, err := src.GetWork(ctx, c.Tenant, original.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := src.ListHypotheses(ctx, c.Tenant, original.WorkID)
	if err != nil || len(previous) != 1 {
		t.Fatalf("hypotheses: %+v %v", previous, err)
	}
	syn, hyps, err := evidence.Adjudicate(evidence.DecisionScope{TenantID: c.Tenant, WorkID: w.ID, Generation: w.Generation},
		[]evidence.ResultEnvelope{{RunID: cpkit.NewID(), AgentRole: "experiment", Status: "COMPLETED", Hypotheses: []evidence.ResultHypothesis{{Statement: previous[0].Statement, Category: evidence.CatRootCause, Status: "VERIFIED", Supports: []string{"evidence:" + cause.ID}}}}}, ledger, w.UpdatedAt)
	if err != nil || len(hyps) != 1 || hyps[0].Decision.Verdict != evidence.DecisionConfirmed {
		t.Fatalf("exact canonical ref should resolve: %+v %v", hyps, err)
	}
	hyps[0].ID, hyps[0].Version = previous[0].ID, previous[0].Version
	if _, err := src.ApplyWork(ctx, work.Mutation{TenantID: c.Tenant, WorkID: w.ID, ExpectedVersion: w.Version, ActorType: "system", Now: w.UpdatedAt, Findings: syn.Findings, Hypotheses: hyps}); err != nil {
		t.Fatal(err)
	}
	cmp, err := Replay(ctx, &Runner{}, src, &workeval.Evaluator{Ledger: src}, c.Tenant, w.ID)
	if err != nil || !cmp.Match || cmp.Replay.RootCause == "" {
		t.Fatalf("canonical ref became ambiguous on replay: %+v %v", cmp, err)
	}
}
