package workbench

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

func TestShippedSuite(t *testing.T) {
	cases, err := LoadCases("../../bench/cases")
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
	cs, err := LoadCases("../../bench/cases/" + name)
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
	cases, err := LoadCases("../../bench/cases")
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
