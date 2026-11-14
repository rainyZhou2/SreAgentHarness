package sre

import (
	"errors"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"testing"
)

func TestInferenceIncidentTemplatesRegistered(t *testing.T) {
	templates := (&inference{}).WorkTemplates()
	byID := map[string]work.Template{}
	for _, tpl := range templates {
		if err := tpl.Normalize(); err != nil {
			t.Fatalf("%s: %v", tpl.ID, err)
		}
		byID[tpl.ID] = tpl
	}
	for _, id := range []string{"inference-latency", "inference-continuous", "inference-incident-v2", "inference-mitigation-v2", "inference-rca-v2"} {
		if _, ok := byID[id]; !ok {
			t.Fatalf("missing registered template %s", id)
		}
	}
	parent := byID["inference-incident-v2"]
	if parent.Graph == nil || len(parent.Graph.Children) != 2 || parent.Stages[0].Type != work.StageChildren || parent.Stages[len(parent.Stages)-1].Type != work.StageVerify {
		t.Fatalf("not a machine-verified parallel parent: %+v", parent)
	}
	for _, c := range parent.Graph.Children {
		if len(c.After) != 0 {
			t.Fatal("mitigation/RCA must start independently")
		}
		if c.Key == "rca" && (c.Required || c.Constraints.ProductionWrites != work.WritesForbidden) {
			t.Fatal("RCA blocks recovery or permits writes")
		}
	}
	mit := byID["inference-mitigation-v2"]
	if mit.Stages[1].Action == nil || mit.Stages[1].Action.FromProposal != "investigate" || len(mit.Stages[1].Action.Allowed) == 0 || mit.Stages[len(mit.Stages)-1].Type != work.StageVerify {
		t.Fatalf("missing controlled mitigation: %+v", mit)
	}
}

func TestInferenceMitigationRequiresMachineGoalWhenStandalone(t *testing.T) {
	for _, tpl := range InferenceIncidentTemplates() {
		if tpl.ID == "inference-rca-v2" {
			continue
		}
		for _, spec := range []*outcome.Spec{nil, {AllOf: []outcome.Objective{{ID: "ack", Kind: outcome.KindHuman}}}, {AllOf: []outcome.Objective{{ID: "ready", Kind: outcome.KindState, Optional: true, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}} {
			n := work.NewWork{TenantID: "tenant", Kind: work.KindIncident, Template: tpl, Goal: work.GoalSpec{Intent: "recover", Success: spec}, IdempotencyKey: "create"}
			if err := n.Prepare(); !errors.Is(err, work.ErrInvalid) {
				t.Fatalf("%s accepted non-machine success: %v", tpl.ID, err)
			}
		}
	}
}
