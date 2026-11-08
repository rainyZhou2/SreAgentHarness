package evidence

import (
	"testing"
	"time"
)

func TestMergeKeepsEvidenceAndReportsGaps(t *testing.T) { // T17
	a := ParseResult("run-a", "metrics", "COMPLETED", []byte(`{"answer":"pool exhausted","hypotheses":[
		{"statement":"DB connection pool exhaustion","status":"VERIFIED","supports":["obs:step2"]},
		{"statement":"Traffic spike","status":"REFUTED","refutes":["obs:step2"]}],"loop":{"cost_usd":0.4}}`))
	b := ParseResult("run-b", "changes", "COMPLETED", []byte(`{"answer":"deploy v2","hypotheses":[
		{"statement":"db connection pool  exhaustion.","status":"REFUTED","refutes":["obs:step4"]},
		{"statement":"Bad deploy v2","status":"VERIFIED","supports":["obs:step1"]}],"loop":{"cost_usd":0.6}}`))
	c := ResultEnvelope{RunID: "run-c", AgentRole: "traces", Status: "FAILED"}
	rep := Merge([]ResultEnvelope{a, b, c})
	if rep.Verdict != "PARTIAL" || len(rep.Gaps) != 1 || rep.Gaps[0] != "traces" {
		t.Fatalf("gaps: %+v", rep)
	}
	if rep.CostUSD != 1.0 {
		t.Fatalf("cost %v", rep.CostUSD)
	}
	if len(rep.Conflicts) != 1 || rep.Conflicts[0].SupportBy[0] != "metrics" || rep.Conflicts[0].RefuteBy[0] != "changes" {
		t.Fatalf("conflict: %+v", rep.Conflicts)
	}
	var pool *MergedHypothesis
	for i := range rep.Hypotheses {
		if rep.Hypotheses[i].Key == HypothesisKey("DB connection pool exhaustion") {
			pool = &rep.Hypotheses[i]
		}
	}
	if pool == nil || pool.Status != HypChallenged || len(pool.Supporting) != 1 || len(pool.Refuting) != 1 ||
		pool.Supporting[0] != "run:run-a#obs:step2" {
		t.Fatalf("pool: %+v", pool)
	}
	c.Required = true
	if rep := Merge([]ResultEnvelope{a, c}); rep.Verdict != "FAILED" || len(rep.RequiredFailed) != 1 {
		t.Fatalf("required failure: %+v", rep)
	}
}

func TestDeriveStatus(t *testing.T) {
	if DeriveStatus(0, 0) != HypOpen || DeriveStatus(1, 0) != HypSupported || DeriveStatus(0, 2) != HypRefuted || DeriveStatus(1, 1) != HypChallenged {
		t.Fatal("status rule")
	}
}

func TestRecordNormalize(t *testing.T) {
	r := Record{SourceType: "metric", SourceRef: "prom://q/1"}
	if err := r.Normalize(time.Now()); err != nil || r.ContentHash == "" || r.TrustTier != "T4" {
		t.Fatalf("%+v %v", r, err)
	}
	bad := Record{SourceType: "gossip", SourceRef: "x"}
	if err := bad.Normalize(time.Now()); err == nil {
		t.Fatal("unknown source accepted")
	}
}

func TestParseResultBoundsUntrusted(t *testing.T) {
	env := ParseResult("r", "", "COMPLETED", []byte(`not json at all`))
	if env.Summary != "not json at all" {
		t.Fatal(env.Summary)
	}
	env = ParseResult("r", "", "COMPLETED", []byte(`{"answer":"x","proposed_action":{"name":"deployment.rollback","target":"k8s://c/ns/Deployment/a"}}`))
	if len(env.Proposed) == 0 {
		t.Fatal("proposal dropped")
	}
}
