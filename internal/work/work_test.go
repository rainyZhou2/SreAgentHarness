package work

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go-sre-harness/internal/outcome"
)

func TestTransitions(t *testing.T) {
	ok := [][2]Status{
		{StatusNew, StatusActive}, {StatusActive, StatusWaiting}, {StatusWaiting, StatusActive},
		{StatusActive, StatusVerifying}, {StatusVerifying, StatusSucceeded}, {StatusVerifying, StatusActive},
		{StatusBlocked, StatusActive}, {StatusSuspended, StatusActive}, {StatusWaiting, StatusCancelled},
	}
	for _, e := range ok {
		if err := Transition(e[0], e[1]); err != nil {
			t.Errorf("%s->%s refused: %v", e[0], e[1], err)
		}
	}
	bad := [][2]Status{
		{StatusActive, StatusSucceeded}, // success only through verification
		{StatusWaiting, StatusSucceeded},
		{StatusSucceeded, StatusActive}, // no implicit revival
		{StatusFailed, StatusActive},
		{StatusCancelled, StatusCancelled},
		{StatusSuspended, StatusWaiting},
	}
	for _, e := range bad {
		err := Transition(e[0], e[1])
		if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s->%s allowed", e[0], e[1])
		}
	}
	for _, s := range AllStatuses {
		if s.Terminal() && len(edges[s]) != 0 {
			t.Errorf("terminal %s has outgoing edges", s)
		}
		if _, ok := edges[s]; !ok {
			t.Errorf("status %s missing from the graph", s)
		}
	}
}

func TestSucceedRequiresSatisfiedOutcome(t *testing.T) {
	st := StatusSucceeded
	m := Mutation{TenantID: "t", WorkID: "w", ActorType: "system", Status: &st}
	if err := CheckMutation(&m); !errors.Is(err, ErrInvalid) {
		t.Fatal("success without outcome accepted")
	}
	m.Outcome = &outcome.Evaluation{Verdict: outcome.Inconclusive}
	if err := CheckMutation(&m); !errors.Is(err, ErrInvalid) {
		t.Fatal("success with inconclusive outcome accepted")
	}
	m.Outcome.Verdict = outcome.Satisfied
	if err := CheckMutation(&m); err != nil {
		t.Fatal(err)
	}
}

func TestBuiltinTemplates(t *testing.T) {
	ts := Builtin()
	if len(ts) < 3 {
		t.Fatal("builtin templates missing")
	}
	for _, tpl := range ts {
		if tpl.Limits.MaxRuns != DefaultMaxRuns || tpl.Limits.MaxReplans != DefaultMaxReplans {
			t.Errorf("%s limits not defaulted: %+v", tpl.ID, tpl.Limits)
		}
	}
}

func TestTemplateValidation(t *testing.T) {
	bad := []string{
		`{"id":"x","stages":[]}`,
		`{"id":"Bad Id","stages":[{"id":"a","type":"confirm"}]}`,
		`{"id":"x","stages":[{"id":"a","type":"run"}]}`,
		`{"id":"x","stages":[{"id":"a","type":"run","run":{"goal":"g"},"wait":{"mode":"time","delay":"1m"}}]}`,
		`{"id":"x","stages":[{"id":"a","type":"confirm"},{"id":"a","type":"confirm"}]}`,
		`{"id":"x","stages":[{"id":"a","type":"confirm","on_failure":"b"},{"id":"b","type":"confirm"}]}`,
		`{"id":"x","stages":[{"id":"a","type":"wait","wait":{"mode":"periodic","interval":"1s","timeout":"1h"}}]}`,
		`{"id":"x","stages":[{"id":"a","type":"action","action":{"name":"x","from_proposal":"a"}}]}`,
		`{"id":"x","stages":[{"id":"a","type":"teleport"}]}`,
		`{"id":"x","limits":{"max_runs":100000},"stages":[{"id":"a","type":"confirm"}]}`,
	}
	for _, b := range bad {
		var tpl Template
		if err := json.Unmarshal([]byte(b), &tpl); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		if err := tpl.Normalize(); !errors.Is(err, ErrInvalid) {
			t.Errorf("accepted %s (%v)", b, err)
		}
	}
	var tpl Template
	_ = json.Unmarshal([]byte(`{"id":"mitigate","stages":[
		{"id":"diag","type":"run","run":{"goal":"diagnose ${subject}"}},
		{"id":"act","type":"action","action":{"name":"deployment.rollback","from_proposal":"diag","allowed":["deployment.rollback"]}},
		{"id":"settle","type":"wait","wait":{"mode":"time","delay":"2m"}},
		{"id":"check","type":"verify","on_failure":"diag"}]}`), &tpl)
	if err := tpl.Normalize(); err != nil {
		t.Fatal(err)
	}
	if tpl.Stages[3].Verify.RecheckEvery.D().Minutes() != 1 {
		t.Fatal("verify defaults not applied")
	}
}

func TestGoalNormalize(t *testing.T) {
	g := GoalSpec{Intent: "mitigate", SubjectRefs: []string{"service://prod//b", "service://prod/a", "service://prod/b"}}
	if err := g.Normalize(); err != nil {
		t.Fatal(err)
	}
	if strings.Join(g.SubjectRefs, ",") != "service://prod/a,service://prod/b" || g.Constraints.ProductionWrites != WritesApprovalRequired {
		t.Fatalf("got %+v", g)
	}
	bad := GoalSpec{Intent: "x", SubjectRefs: []string{"search api"}}
	if err := bad.Normalize(); !errors.Is(err, ErrInvalid) {
		t.Fatal("free-text subject accepted")
	}
}

func TestRender(t *testing.T) {
	w := &Work{ID: "w1", Title: "P99 high", Generation: 2, Goal: GoalSpec{Intent: "mitigate",
		SubjectRefs: []string{"service://prod/search"}, Labels: map[string]string{"team": "search"}}}
	got := Render("${title} on ${subject} gen ${generation} team ${label.team} ${unknown} ${stage.diag.summary}", w,
		map[string]string{"diag": "cpu"})
	if got != "P99 high on service://prod/search gen 2 team search  cpu" {
		t.Fatalf("got %q", got)
	}
	doc, err := RenderJSON(json.RawMessage(`{"target":"${subject}","n":3,"list":["${work.id}"]}`), w)
	if err != nil || string(doc) != `{"list":["w1"],"n":3,"target":"service://prod/search"}` {
		t.Fatalf("got %s %v", doc, err)
	}
}

func TestNewWorkRequestHashStable(t *testing.T) {
	a := NewWork{TenantID: "t", Kind: KindIncident, Goal: GoalSpec{Intent: "x"}, Template: Builtin()[1], IdempotencyKey: "k"}
	b := a
	if err := a.Prepare(); err != nil {
		t.Fatal(err)
	}
	_ = b.Prepare()
	if a.RequestHash() != b.RequestHash() {
		t.Fatal("hash not stable")
	}
	b.Priority = 5
	if a.RequestHash() == b.RequestHash() {
		t.Fatal("hash ignores priority")
	}
}
