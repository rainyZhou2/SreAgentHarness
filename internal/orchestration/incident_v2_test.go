package orchestration

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/plugins/sre"
)

func inheritedGraph(t *testing.T) work.Template {
	t.Helper()
	var tpl work.Template
	if err := json.Unmarshal([]byte(`{"id":"inherited","version":2,"kind":"incident","graph":{"children":[{"key":"mitigation","template":"mitigate","required":true,"inherit_success":true}]},"stages":[{"id":"coordinate","type":"children"},{"id":"verify","type":"verify"}]}`), &tpl); err != nil {
		t.Fatal(err)
	}
	return tpl
}

func TestInheritedChildRequiresMachineGoal(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec *outcome.Spec
	}{
		{"missing", nil},
		{"human", &outcome.Spec{AllOf: []outcome.Objective{{ID: "ack", Kind: outcome.KindHuman}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, domain.RiskR2)
			h.withTemplates(inheritedGraph(t))
			_, _, err := h.svc.Create(h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(), Kind: work.KindIncident, TemplateID: "inherited", Goal: work.GoalSpec{Intent: "recover", SubjectRefs: []string{"deployment://prod/checkout"}, Success: tc.spec}})
			if !errors.Is(err, work.ErrInvalid) {
				t.Fatalf("unsafe inherited goal accepted: %v", err)
			}
			if h.runs.started() != 0 || h.exec.calls != 0 {
				t.Fatal("work dispatched before goal validation")
			}
		})
	}
}

func TestInferenceIncidentV2RecoveryDoesNotWaitForRCA(t *testing.T) {
	h := newHarness(t, domain.RiskR2)
	h.withTemplates(sre.InferenceIncidentTemplates()...)
	spec := &outcome.Spec{AllOf: []outcome.Objective{{ID: "image", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "image", Equals: "v1"}, Freshness: cpkit.Duration(time.Hour)}}}
	parent := h.create("inference-incident-v2", spec)
	h.tick(4)
	kids, err := h.store.Children(h.ctx, "t1", parent.ID)
	if err != nil || len(kids) != 2 {
		t.Fatalf("children=%d err=%v", len(kids), err)
	}
	var mit, rca work.Work
	for _, k := range kids {
		if k.Goal.Labels["child_key"] == "mitigation" {
			mit = k
		} else {
			rca = k
		}
	}
	if mit.Goal.Success == nil || mit.Goal.Success.Hash() != parent.Goal.Success.Hash() {
		t.Fatal("mitigation did not inherit exact machine goal")
	}
	if rca.Goal.Success != nil || rca.Goal.Constraints.ProductionWrites != work.WritesForbidden {
		t.Fatal("RCA inherited recovery goal or write authority")
	}
	if h.runs.started() != 2 {
		t.Fatalf("children not parallel: runs=%d", h.runs.started())
	}
	// Finish the two investigations. RCA explicitly remains inconclusive, never a recovery signal.
	h.runs.mu.Lock()
	for _, id := range h.runs.order {
		rr := h.runs.runs[id]
		result := any(map[string]any{"summary": "root cause unknown; insufficient causal evidence", "hypotheses": []evidence.ResultHypothesis{{Statement: "release caused the breach", Category: evidence.CatRootCause, Status: "VERIFIED"}}})
		if rr.req.WorkID == mit.ID {
			result = rcaResult
		}
		b, _ := json.Marshal(result)
		rr.state = RunState{Status: "COMPLETED", Terminal: true, Succeeded: true, Result: b}
	}
	h.runs.mu.Unlock()
	h.tick(6)
	acts, err := h.store.ListActions(h.ctx, "t1", mit.ID)
	if err != nil || len(acts) != 1 || acts[0].State != action.StateWaitingApproval || h.exec.calls != 0 {
		t.Fatalf("approval not enforced: %+v err=%v calls=%d", acts, err, h.exec.calls)
	}
	if _, err := h.command(mit.ID, work.CmdConfirm, CommandPayload{Verdict: "satisfied"}); err != nil {
		t.Fatal(err)
	}
	h.tick(2)
	pending, _ := h.store.GetWork(h.ctx, "t1", mit.ID)
	if pending.Status == work.StatusSucceeded || h.exec.calls != 0 {
		t.Fatal("human confirmation bypassed machine/action stage")
	}
	if _, err := h.acts.Decide(h.ctx, "t1", acts[0].ID, "carol", "approve", acts[0].IntentHash, "ok", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := h.store.WakeWork(h.ctx, "t1", mit.ID, "approval", "user", "carol", nil); err != nil {
		t.Fatal(err)
	}
	got := h.settle(parent.ID, work.StatusSucceeded, work.StatusFailed, work.StatusBlocked)
	if got.Status != work.StatusSucceeded || h.exec.calls != 1 {
		t.Fatalf("recovery status=%s reason=%s calls=%d", got.Status, got.Reason, h.exec.calls)
	}
	rca, _ = h.store.GetWork(h.ctx, "t1", rca.ID)
	hyps, err := h.store.ListHypotheses(h.ctx, "t1", rca.ID)
	if err != nil || len(hyps) != 1 || hyps[0].Decision == nil || hyps[0].Decision.Verdict != evidence.DecisionInconclusive {
		t.Fatalf("RCA is not explicitly inconclusive: %+v %v", hyps, err)
	}
	if rca.Status.Terminal() {
		t.Fatalf("recovery closed RCA: %s", rca.Status)
	}
	for _, id := range []string{mit.ID, parent.ID} {
		outs, err := h.store.ListOutcomes(h.ctx, "t1", id, 10)
		if err != nil || len(outs) == 0 || outs[0].Verdict != outcome.Satisfied || outs[0].SpecHash != parent.Goal.Success.Hash() {
			t.Fatalf("no independent machine outcome for %s: %+v %v", id, outs, err)
		}
	}
}

// Old persisted Works or replans must also fail at spawn, even if they bypassed
// the creation-time check. No child is created with a human fallback goal.
func TestInheritedChildSpawnRejectsMissingGoal(t *testing.T) {
	h := newHarness(t, domain.RiskR2)
	tpl := inheritedGraph(t)
	h.withTemplates(tpl)
	parent := h.create(tpl.ID, &outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}})
	parent.Goal.Success = nil
	d := driver{o: h.orch, w: parent}
	if _, err := d.spawnChild(h.ctx, tpl.Stages[0], tpl.Graph.Children[0], nil); !errors.Is(err, work.ErrInvalid) {
		t.Fatalf("spawn accepted missing machine goal: %v", err)
	}
	kids, err := h.store.Children(h.ctx, "t1", parent.ID)
	if err != nil || len(kids) != 0 {
		t.Fatalf("spawn created fallback child: %+v %v", kids, err)
	}
}
